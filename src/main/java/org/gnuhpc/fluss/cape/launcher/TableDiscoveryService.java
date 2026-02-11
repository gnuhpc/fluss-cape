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

package org.gnuhpc.fluss.cape.launcher;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.RowType;
import org.apache.hadoop.hbase.TableName;
import org.gnuhpc.fluss.cape.common.configuration.CapeConfig;
import org.gnuhpc.fluss.cape.hbase.executor.*;
import org.gnuhpc.fluss.cape.hbase.mapping.CellConverter;
import org.gnuhpc.fluss.cape.hbase.mapping.RowKeyEncoder;
import org.gnuhpc.fluss.cape.hbase.metadata.VirtualRegionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class TableDiscoveryService implements ServerComponent {
    private static final Logger LOG = LoggerFactory.getLogger(TableDiscoveryService.class);
    
    private static final int CORE_THREADS = 2;
    private static final int MAX_THREADS = 8;
    private static final int QUEUE_SIZE = 100;
    private static final int MAX_RETRY_ATTEMPTS = 5;
    private static final long INITIAL_RETRY_DELAY_MS = 200;
    
    private final Connection flussConnection;
    private final Admin flussAdmin;
    private final HBaseRequestRouter router;
    private final VirtualRegionManager regionManager;
    private final CapeConfig capeConfig;
    
    private ExecutorService discoveryExecutor;
    private volatile boolean running = false;
    
    public TableDiscoveryService(Connection flussConnection, Admin flussAdmin,
                                 HBaseRequestRouter router, VirtualRegionManager regionManager,
                                 CapeConfig capeConfig) {
        this.flussConnection = flussConnection;
        this.flussAdmin = flussAdmin;
        this.router = router;
        this.regionManager = regionManager;
        this.capeConfig = capeConfig;
    }
    
    @Override
    public void start() throws Exception {
        this.discoveryExecutor = new ThreadPoolExecutor(
                CORE_THREADS, MAX_THREADS,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(QUEUE_SIZE),
                r -> {
                    Thread t = new Thread(r, "table-discovery-" + System.currentTimeMillis());
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        
        if (capeConfig.isAutoDiscoveryEnabled()) {
            discoverExistingTables();
        }
        
        running = true;
        LOG.info("Table discovery service started");
    }
    
    @Override
    public void close() throws Exception {
        running = false;
        
        if (discoveryExecutor != null) {
            discoveryExecutor.shutdown();
            if (!discoveryExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                LOG.warn("Discovery executor did not terminate in time, forcing shutdown");
                discoveryExecutor.shutdownNow();
            }
        }
        
        LOG.info("Table discovery service stopped");
    }
    
    @Override
    public String getName() {
        return "TableDiscoveryService";
    }
    
    @Override
    public boolean isRunning() {
        return running;
    }
    
    public CompletableFuture<Void> registerTable(TablePath tablePath) {
        return registerTable(tablePath, null);
    }
    
    public CompletableFuture<Void> registerTable(TablePath tablePath, Consumer<TablePath> onUnregister) {
        return retryGetTableInfo(tablePath, MAX_RETRY_ATTEMPTS, INITIAL_RETRY_DELAY_MS)
                .thenComposeAsync(tableInfo -> {
                    if (tableInfo == null) {
                        return CompletableFuture.failedFuture(
                                new IllegalArgumentException("Table not found: " + tablePath));
                    }
                    
                    return registerTableExecutors(tablePath, tableInfo, onUnregister);
                }, discoveryExecutor);
    }
    
    public void unregisterTable(TablePath tablePath) {
        String tableName = tablePath.getTableName();
        router.unregisterExecutors("Get-" + tableName);
        router.unregisterExecutors("Mutate-" + tableName);
        router.unregisterExecutors("Scan-" + tableName);
        router.unregisterExecutors("Multi-" + tableName);
        
        LOG.info("Unregistered table executors: {}", tableName);
    }
    
    private void discoverExistingTables() {
        try {
            List<String> databases = flussAdmin.listDatabases().get(5, TimeUnit.SECONDS);
            int discoveredCount = 0;
            
            for (String db : databases) {
                List<String> tables = flussAdmin.listTables(db).get(5, TimeUnit.SECONDS);
                for (String t : tables) {
                    if (!t.startsWith("__") && !t.startsWith("redis_")) {
                        TablePath tablePath = TablePath.of(db, t);
                        registerTable(tablePath).exceptionally(e -> {
                            LOG.warn("Failed to auto-discover table: {}", tablePath, e);
                            return null;
                        });
                        discoveredCount++;
                    }
                }
            }
            
            LOG.info("Auto-discovery completed: {} tables queued for registration", discoveredCount);
        } catch (Exception e) {
            LOG.warn("Auto-discovery failed", e);
        }
    }
    
    private CompletableFuture<Void> registerTableExecutors(TablePath tablePath, TableInfo tableInfo,
                                                           Consumer<TablePath> onUnregister) {
        RowType rowType = tableInfo.getRowType();
        List<String> primaryKeys = tableInfo.getPrimaryKeys();
        RowKeyEncoder rowKeyEncoder = new RowKeyEncoder(rowType, primaryKeys);
        CellConverter cellConverter = new CellConverter(rowType,
                CellConverter.ColumnFamilyMapping.createDefault(rowType, "cf"));
        String tableName = tablePath.getTableName();
        
        router.registerExecutor("Get-" + tableName,
                new GetExecutor(flussConnection, tablePath, rowKeyEncoder, cellConverter));
        router.registerExecutor("Mutate-" + tableName,
                new PutExecutor(flussConnection, tablePath, rowType, rowKeyEncoder, cellConverter));
        router.registerExecutor("Scan-" + tableName,
                new ScanExecutorStreaming(flussConnection, tablePath, rowKeyEncoder, cellConverter));
        router.registerExecutor("Multi-" + tableName,
                new MultiExecutor(flussConnection, tablePath, rowType, rowKeyEncoder, cellConverter));
        
        regionManager.registerTable(TableName.valueOf(tableName), tableInfo.getNumBuckets());
        
        LOG.info("Registered table executors: {}", tableName);
        
        return CompletableFuture.completedFuture(null);
    }
    
    private CompletableFuture<TableInfo> retryGetTableInfo(TablePath tablePath, int maxAttempts, long delayMs) {
        return retryGetTableInfoAttempt(tablePath, maxAttempts, delayMs, 1);
    }
    
    private CompletableFuture<TableInfo> retryGetTableInfoAttempt(TablePath tablePath, int maxAttempts,
                                                                  long delayMs, int attempt) {
        return flussAdmin.getTableInfo(tablePath).thenCompose(info -> {
            if (info != null || attempt >= maxAttempts) {
                return CompletableFuture.completedFuture(info);
            }
            
            CompletableFuture<TableInfo> delayed = new CompletableFuture<>();
            discoveryExecutor.execute(() -> {
                try {
                    Thread.sleep(delayMs);
                    retryGetTableInfoAttempt(tablePath, maxAttempts, delayMs * 2, attempt + 1)
                            .whenComplete((r, t) -> {
                                if (t != null) {
                                    delayed.completeExceptionally(t);
                                } else {
                                    delayed.complete(r);
                                }
                            });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    delayed.completeExceptionally(e);
                }
            });
            return delayed;
        });
    }
}
