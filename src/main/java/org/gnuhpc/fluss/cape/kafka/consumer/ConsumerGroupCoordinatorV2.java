/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gnuhpc.fluss.cape.kafka.consumer;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ConsumerGroupCoordinatorV2 implements AutoCloseable {
    
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerGroupCoordinatorV2.class);
    
    private static final String DATABASE = "default";
    
    private final Connection flussConnection;
    private final CompletableFuture<Void> initializationFuture;
    private Table groupTable;
    private Table offsetTable;
    private Lookuper groupLookuper;
    private Lookuper offsetLookuper;
    private UpsertWriter groupWriter;
    private UpsertWriter offsetWriter;
    private final ScheduledExecutorService heartbeatExecutor;
    private final ConcurrentHashMap<String, ConsumerGroupState> groupStates;
    
    public ConsumerGroupCoordinatorV2(Connection flussConnection) {
        this.flussConnection = flussConnection;
        this.groupStates = new ConcurrentHashMap<>();
        
        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "consumer-group-heartbeat-checker");
            t.setDaemon(true);
            return t;
        });
        
        this.initializationFuture = initializeAsync();
        
        LOG.info("ConsumerGroupCoordinator initialization started");
    }
    
    private CompletableFuture<Void> initializeAsync() {
        return ensureTablesExistAsync()
            .thenCompose(v -> {
                TablePath groupTablePath = new TablePath(DATABASE, ConsumerGroupSchemas.CONSUMER_GROUPS_TABLE);
                TablePath offsetTablePath = new TablePath(DATABASE, ConsumerGroupSchemas.CONSUMER_OFFSETS_TABLE);
                
                return retryGetTables(groupTablePath, offsetTablePath, 5, 1000);
            })
            .thenRun(() -> {
                try {
                    this.groupLookuper = groupTable.newLookup().createLookuper();
                    this.offsetLookuper = offsetTable.newLookup().createLookuper();
                    
                    this.groupWriter = groupTable.newUpsert().createWriter();
                    this.offsetWriter = offsetTable.newUpsert().createWriter();
                    
                    startHeartbeatChecker();
                    
                    LOG.info("ConsumerGroupCoordinator initialized successfully");
                } catch (Exception e) {
                    LOG.error("Error initializing coordinator resources", e);
                    throw new RuntimeException("Failed to initialize coordinator", e);
                }
            });
    }
    
    private CompletableFuture<Void> retryGetTables(TablePath groupTablePath, TablePath offsetTablePath, 
                                                     int maxRetries, long delayMs) {
        return CompletableFuture.supplyAsync(() -> {
            int attempt = 0;
            Exception lastException = null;
            
            while (attempt < maxRetries) {
                try {
                    this.groupTable = flussConnection.getTable(groupTablePath);
                    this.offsetTable = flussConnection.getTable(offsetTablePath);
                    LOG.info("Successfully obtained table references after {} attempts", attempt + 1);
                    return null;
                } catch (Exception e) {
                    lastException = e;
                    attempt++;
                    if (attempt < maxRetries) {
                        LOG.warn("Failed to get tables (attempt {}/{}), retrying after {}ms", 
                                attempt, maxRetries, delayMs);
                        try {
                            Thread.sleep(delayMs);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException("Interrupted while waiting to retry", ie);
                        }
                    }
                }
            }
            
            LOG.error("Failed to get tables after {} attempts", maxRetries);
            throw new RuntimeException("Failed to obtain table references", lastException);
        });
    }
    
    private CompletableFuture<Void> ensureTablesExistAsync() {
        Admin admin = flussConnection.getAdmin();
        
        TablePath groupTablePath = new TablePath(DATABASE, ConsumerGroupSchemas.CONSUMER_GROUPS_TABLE);
        TablePath offsetTablePath = new TablePath(DATABASE, ConsumerGroupSchemas.CONSUMER_OFFSETS_TABLE);
        
        return admin.tableExists(groupTablePath)
            .thenCompose(groupExists -> {
                if (!groupExists) {
                    LOG.info("Creating consumer groups table: {}", groupTablePath);
                    TableDescriptor groupDescriptor = TableDescriptor.builder()
                            .schema(ConsumerGroupSchemas.consumerGroupSchema())
                            .distributedBy(1)
                            .build();
                    return admin.createTable(groupTablePath, groupDescriptor, false)
                        .thenRun(() -> LOG.info("Created consumer groups table"));
                }
                return CompletableFuture.completedFuture(null);
            })
            .thenCompose(v -> admin.tableExists(offsetTablePath))
            .thenCompose(offsetExists -> {
                if (!offsetExists) {
                    LOG.info("Creating consumer offsets table: {}", offsetTablePath);
                    TableDescriptor offsetDescriptor = TableDescriptor.builder()
                            .schema(ConsumerGroupSchemas.consumerOffsetSchema())
                            .distributedBy(1)
                            .build();
                    return admin.createTable(offsetTablePath, offsetDescriptor, false)
                        .thenRun(() -> LOG.info("Created consumer offsets table"));
                }
                return CompletableFuture.completedFuture(null);
            });
    }
    
    public CompletableFuture<Void> getInitializationFuture() {
        return initializationFuture;
    }
    
    private void startHeartbeatChecker() {
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                checkExpiredHeartbeats();
            } catch (Exception e) {
                LOG.error("Error checking expired heartbeats", e);
            }
        }, 30, 30, TimeUnit.SECONDS);
    }
    
    private void checkExpiredHeartbeats() {
        long now = System.currentTimeMillis();
        groupStates.forEach((groupId, state) -> {
            state.getMembers().removeIf(member -> {
                long timeSinceHeartbeat = now - member.getLastHeartbeat();
                if (timeSinceHeartbeat > member.getSessionTimeoutMs()) {
                    LOG.warn("Removing member {} from group {} due to heartbeat timeout", 
                            member.getMemberId(), groupId);
                    return true;
                }
                return false;
            });
            
            if (state.getMembers().isEmpty() && state.getState() != GroupState.EMPTY) {
                LOG.info("Group {} is now empty", groupId);
                state.setState(GroupState.EMPTY);
            }
        });
    }
    
    public Lookuper getGroupLookuper() {
        return groupLookuper;
    }
    
    public Lookuper getOffsetLookuper() {
        return offsetLookuper;
    }
    
    public UpsertWriter getGroupWriter() {
        return groupWriter;
    }
    
    public UpsertWriter getOffsetWriter() {
        return offsetWriter;
    }
    
    public ConsumerGroupState getOrCreateGroupState(String groupId) {
        return groupStates.computeIfAbsent(groupId, id -> {
            try {
                InternalRow groupRow = null;
                if (groupRow != null) {
                    return ConsumerGroupState.fromRow(groupRow);
                } else {
                    ConsumerGroupState newState = new ConsumerGroupState(groupId);
                    LOG.info("Created new consumer group state for {}", groupId);
                    return newState;
                }
            } catch (Exception e) {
                LOG.error("Error loading group state for {}", groupId, e);
                return new ConsumerGroupState(groupId);
            }
        });
    }
    
    @Override
    public void close() {
        LOG.info("Closing ConsumerGroupCoordinator");
        
        try {
            heartbeatExecutor.shutdownNow();
            
            groupStates.clear();
            LOG.info("ConsumerGroupCoordinator closed successfully");
        } catch (Exception e) {
            LOG.error("Error during coordinator close", e);
        }
    }
}
