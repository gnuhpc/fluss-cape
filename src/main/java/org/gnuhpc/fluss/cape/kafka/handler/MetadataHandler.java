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

package org.gnuhpc.fluss.cape.kafka.handler;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.gnuhpc.fluss.cape.kafka.config.KafkaCompatConfig;
import org.gnuhpc.fluss.cape.kafka.converter.TablePathResolver;
import org.gnuhpc.fluss.cape.kafka.protocol.KafkaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class MetadataHandler {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataHandler.class);
    private static final long TIMEOUT_MS = 5000;

    private final Connection flussConnection;
    private final KafkaCompatConfig config;
    private final TablePathResolver tablePathResolver;

    public MetadataHandler(Connection flussConnection, KafkaCompatConfig config, 
                          TablePathResolver tablePathResolver) {
        this.flussConnection = flussConnection;
        this.config = config;
        this.tablePathResolver = tablePathResolver;
    }

    public void handle(KafkaRequest request) {
        try {
            MetadataRequest metadataRequest = request.request();
            LOG.info("Handling metadata request for request ID: {}", request.requestId());
            handleMetadataAsync(metadataRequest, request.apiVersion())
                .thenAccept(response -> {
                    LOG.info("Completing metadata request ID: {} with {} topics", 
                            request.requestId(), response.data().topics().size());
                    request.complete(response);
                })
                .exceptionally(ex -> {
                    LOG.error("Error handling metadata request", ex);
                    request.fail(ex);
                    return null;
                });
        } catch (Exception e) {
            LOG.error("Error handling metadata request", e);
            request.fail(e);
        }
    }

    private CompletableFuture<MetadataResponse> handleMetadataAsync(MetadataRequest metadataRequest, short apiVersion) {
        LOG.info("Processing metadata request: isAllTopics={}, topics={}", 
                metadataRequest.isAllTopics(), metadataRequest.topics());
        
        long startTime = System.currentTimeMillis();
        
        MetadataResponseData responseData = new MetadataResponseData();
        
        String advertiseHost = config.getHost().equals("0.0.0.0") ? "localhost" : config.getHost();
        Node brokerNode = new Node(config.getNodeId(), advertiseHost, config.getPort());
        LOG.info("Advertising broker in metadata: nodeId={}, host={}, port={}", 
                 brokerNode.id(), brokerNode.host(), brokerNode.port());
        responseData.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(brokerNode.id())
                .setHost(brokerNode.host())
                .setPort(brokerNode.port()));

        responseData.setClusterId("fluss-cape-cluster");
        responseData.setControllerId(config.getNodeId());

        try {
            Admin admin = flussConnection.getAdmin();
            
            CompletableFuture<Void> metadataFuture;
            
            if (metadataRequest.isAllTopics()) {
                LOG.debug("Fetching metadata for all topics");
                metadataFuture = admin.listDatabases()
                    .orTimeout(TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .thenCompose(databases -> {
                        List<CompletableFuture<Void>> futures = new ArrayList<>();
                        for (String database : databases) {
                            CompletableFuture<Void> f = admin.listTables(database)
                                .orTimeout(TIMEOUT_MS, TimeUnit.MILLISECONDS)
                                .thenCompose(tables -> {
                                    List<CompletableFuture<Void>> tableFutures = new ArrayList<>();
                                    for (String tableName : tables) {
                                        TablePath tablePath = new TablePath(database, tableName);
                                        tableFutures.add(addTopicMetadataAsync(responseData, tablePath, admin));
                                    }
                                    return CompletableFuture.allOf(tableFutures.toArray(new CompletableFuture[0]));
                                });
                            futures.add(f);
                        }
                        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
                    });
            } else {
                LOG.debug("Fetching metadata for specific topics: {}", metadataRequest.topics());
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                for (String topicName : metadataRequest.topics()) {
                    TablePath tablePath = tablePathResolver.resolve(topicName);
                    futures.add(addTopicMetadataAsync(responseData, tablePath, admin));
                }
                metadataFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            }
            
            return metadataFuture
                .handle((result, ex) -> {
                    if (ex != null) {
                        LOG.error("Error fetching table metadata from Fluss", ex);
                    }
                    long duration = System.currentTimeMillis() - startTime;
                    LOG.info("Metadata request completed in {}ms with {} topics", duration, responseData.topics().size());
                    return new MetadataResponse(responseData, apiVersion);
                });
            
        } catch (Exception e) {
            LOG.error("Error processing metadata request", e);
            return CompletableFuture.completedFuture(new MetadataResponse(responseData, apiVersion));
        }
    }
    
    private CompletableFuture<Void> addTopicMetadataAsync(MetadataResponseData responseData, TablePath tablePath, Admin admin) {
        return admin.tableExists(tablePath)
            .orTimeout(TIMEOUT_MS, TimeUnit.MILLISECONDS)
            .thenCompose(exists -> {
                if (!exists && config.isAutoCreateTables()) {
                    LOG.info("Auto-creating Kafka table for metadata request: {}", tablePath);
                    return createKafkaTable(admin, tablePath)
                        .thenCompose(created -> fetchAndAddTopicMetadata(responseData, tablePath, admin));
                } else if (!exists) {
                    String topicName = formatTopicName(tablePath);
                    MetadataResponseData.MetadataResponseTopic topicData =
                        new MetadataResponseData.MetadataResponseTopic()
                            .setName(topicName)
                            .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                            .setIsInternal(false);
                    synchronized (responseData.topics()) {
                        responseData.topics().add(topicData);
                    }
                    LOG.debug("Topic {} does not exist in Fluss", topicName);
                    return CompletableFuture.completedFuture(null);
                } else {
                    return fetchAndAddTopicMetadata(responseData, tablePath, admin);
                }
            })
            .exceptionally(ex -> {
                LOG.error("Failed to get topic metadata for {}", tablePath, ex);
                String topicName = formatTopicName(tablePath);
                MetadataResponseData.MetadataResponseTopic topicData =
                    new MetadataResponseData.MetadataResponseTopic()
                        .setName(topicName)
                        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                        .setIsInternal(false);
                synchronized (responseData.topics()) {
                    responseData.topics().add(topicData);
                }
                return null;
            });
    }
    
    private CompletableFuture<Void> createKafkaTable(Admin admin, TablePath tablePath) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Schema schema = Schema.newBuilder()
                    .column("key", DataTypes.BYTES())
                    .column("value", DataTypes.BYTES())
                    .column("timestamp", DataTypes.BIGINT())
                    .build();
                
                TableDescriptor tableDescriptor = TableDescriptor.builder()
                    .schema(schema)
                    .distributedBy(config.getDefaultNumBuckets())
                    .build();
                
                admin.createTable(tablePath, tableDescriptor, false).get();
                LOG.info("Successfully auto-created Kafka table: {}", tablePath);
                return null;
            } catch (Exception e) {
                LOG.error("Failed to auto-create Kafka table: {}", tablePath, e);
                throw new RuntimeException("Failed to create table: " + tablePath, e);
            }
        });
    }
    
    private CompletableFuture<Void> fetchAndAddTopicMetadata(MetadataResponseData responseData, TablePath tablePath, Admin admin) {
        return admin.getTableInfo(tablePath)
            .orTimeout(TIMEOUT_MS, TimeUnit.MILLISECONDS)
            .thenAccept(tableInfo -> {
                String topicName = formatTopicName(tablePath);
                int bucketCount = tableInfo.getNumBuckets();
                
                MetadataResponseData.MetadataResponseTopic topicData =
                    new MetadataResponseData.MetadataResponseTopic()
                        .setName(topicName)
                        .setErrorCode(Errors.NONE.code())
                        .setIsInternal(false);
                
                for (int i = 0; i < bucketCount; i++) {
                    topicData.partitions().add(new MetadataResponseData.MetadataResponsePartition()
                        .setPartitionIndex(i)
                        .setLeaderId(config.getNodeId())
                        .setReplicaNodes(List.of(config.getNodeId()))
                        .setIsrNodes(List.of(config.getNodeId()))
                        .setErrorCode(Errors.NONE.code()));
                }
                
                synchronized (responseData.topics()) {
                    responseData.topics().add(topicData);
                }
                LOG.info("Added metadata for topic {} with {} partitions, leader nodeId={}", 
                        topicName, bucketCount, config.getNodeId());
            });
    }
    
    private String formatTopicName(TablePath tablePath) {
        return tablePath.getDatabaseName().equals("default") 
            ? tablePath.getTableName() 
            : tablePath.getDatabaseName() + "." + tablePath.getTableName();
    }

    public void close() {
    }
}
