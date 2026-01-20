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

import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.gnuhpc.fluss.cape.kafka.consumer.ConsumerGroupCoordinatorV2;
import org.gnuhpc.fluss.cape.kafka.protocol.KafkaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class OffsetFetchHandler {
    private static final Logger LOG = LoggerFactory.getLogger(OffsetFetchHandler.class);

    private final ConsumerGroupCoordinatorV2 coordinator;

    public OffsetFetchHandler(ConsumerGroupCoordinatorV2 coordinator) {
        this.coordinator = coordinator;
    }

    public void handle(KafkaRequest request) {
        coordinator.getInitializationFuture()
            .thenCompose(v -> {
                try {
                    org.apache.kafka.common.requests.OffsetFetchRequest fetchRequest = request.request();
                    OffsetFetchRequestData fetchRequestData = fetchRequest.data();
                    return handleOffsetFetchAsync(fetchRequestData, request.apiVersion());
                } catch (Exception e) {
                    LOG.error("Error handling offset fetch request", e);
                    throw new RuntimeException(e);
                }
            })
            .thenAccept(request::complete)
            .exceptionally(ex -> {
                LOG.error("Error handling offset fetch request", ex);
                request.fail(ex);
                return null;
            });
    }

    private CompletableFuture<OffsetFetchResponse> handleOffsetFetchAsync(
            OffsetFetchRequestData request, 
            short apiVersion) {
        
        try {
            Lookuper offsetLookuper = coordinator.getOffsetLookuper();
            OffsetFetchResponseData responseData = new OffsetFetchResponseData();
            
            if (apiVersion >= 8) {
                return handleOffsetFetchV8PlusAsync(request, apiVersion, offsetLookuper, responseData);
            } else {
                return handleOffsetFetchV0to7Async(request, apiVersion, offsetLookuper, responseData);
            }
            
        } catch (Exception e) {
            LOG.error("Error handling offset fetch request (v{})", apiVersion, e);
            OffsetFetchResponseData errorData = new OffsetFetchResponseData();
            if (apiVersion >= 8) {
                if (!request.groups().isEmpty()) {
                    OffsetFetchResponseData.OffsetFetchResponseGroup group = 
                        new OffsetFetchResponseData.OffsetFetchResponseGroup()
                            .setGroupId(request.groups().get(0).groupId())
                            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
                    errorData.groups().add(group);
                }
            } else {
                errorData.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
            }
            return CompletableFuture.completedFuture(new OffsetFetchResponse(errorData, apiVersion));
        }
    }
    
    private CompletableFuture<OffsetFetchResponse> handleOffsetFetchV0to7Async(
            OffsetFetchRequestData request,
            short apiVersion,
            Lookuper offsetLookuper,
            OffsetFetchResponseData responseData) {
        
        String groupId = request.groupId();
        LOG.info("OffsetFetch v{} from group {}, {} topics", 
                apiVersion, groupId, request.topics() != null ? request.topics().size() : 0);
        
        if (request.topics() == null || request.topics().isEmpty()) {
            responseData.setErrorCode(Errors.NONE.code());
            return CompletableFuture.completedFuture(new OffsetFetchResponse(responseData, apiVersion));
        }
        
        List<CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseTopic>> topicFutures = new ArrayList<>();
        
        for (OffsetFetchRequestData.OffsetFetchRequestTopic topic : request.topics()) {
            String topicName = topic.name();
            
            List<CompletableFuture<OffsetFetchResponseData.OffsetFetchResponsePartition>> partitionFutures = new ArrayList<>();
            
            for (int partitionIndex : topic.partitionIndexes()) {
                InternalRow lookupKey = buildLookupKey(groupId, topicName, partitionIndex);
                CompletableFuture<OffsetFetchResponseData.OffsetFetchResponsePartition> partFuture = 
                    offsetLookuper.lookup(lookupKey)
                        .thenApply(lookupResult -> {
                            List<InternalRow> resultRows = lookupResult.getRowList();
                            
                            if (resultRows != null && !resultRows.isEmpty()) {
                                InternalRow resultRow = resultRows.get(0);
                                long offset = resultRow.getLong(3);
                                String metadata = resultRow.isNullAt(4) ? "" : resultRow.getString(4).toString();
                                
                                LOG.trace("Fetched offset {} for {}-{} in group {}", 
                                        offset, topicName, partitionIndex, groupId);
                                
                                return new OffsetFetchResponseData.OffsetFetchResponsePartition()
                                    .setPartitionIndex(partitionIndex)
                                    .setCommittedOffset(offset)
                                    .setMetadata(metadata)
                                    .setErrorCode(Errors.NONE.code());
                            } else {
                                return new OffsetFetchResponseData.OffsetFetchResponsePartition()
                                    .setPartitionIndex(partitionIndex)
                                    .setCommittedOffset(-1L)
                                    .setMetadata("")
                                    .setErrorCode(Errors.NONE.code());
                            }
                        })
                        .exceptionally(e -> {
                            LOG.error("Error fetching offset for {}-{} in group {}", 
                                    topicName, partitionIndex, groupId, e);
                            return new OffsetFetchResponseData.OffsetFetchResponsePartition()
                                .setPartitionIndex(partitionIndex)
                                .setCommittedOffset(-1L)
                                .setMetadata("")
                                .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
                        });
                
                partitionFutures.add(partFuture);
            }
            
            CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseTopic> topicFuture = 
                CompletableFuture.allOf(partitionFutures.toArray(new CompletableFuture[0]))
                    .thenApply(v -> {
                        OffsetFetchResponseData.OffsetFetchResponseTopic responseTopic = 
                            new OffsetFetchResponseData.OffsetFetchResponseTopic()
                                .setName(topicName);
                        
                        for (CompletableFuture<OffsetFetchResponseData.OffsetFetchResponsePartition> pf : partitionFutures) {
                            responseTopic.partitions().add(pf.join());
                        }
                        
                        return responseTopic;
                    });
            
            topicFutures.add(topicFuture);
        }
        
        return CompletableFuture.allOf(topicFutures.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                for (CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseTopic> tf : topicFutures) {
                    responseData.topics().add(tf.join());
                }
                
                responseData.setErrorCode(Errors.NONE.code());
                return new OffsetFetchResponse(responseData, apiVersion);
            });
    }
    
    private CompletableFuture<OffsetFetchResponse> handleOffsetFetchV8PlusAsync(
            OffsetFetchRequestData request,
            short apiVersion,
            Lookuper offsetLookuper,
            OffsetFetchResponseData responseData) {
        
        if (request.groups() == null || request.groups().isEmpty()) {
            LOG.info("OffsetFetch v{}: empty groups request", apiVersion);
            return CompletableFuture.completedFuture(new OffsetFetchResponse(responseData, apiVersion));
        }
        
        List<CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup>> groupFutures = new ArrayList<>();
        
        for (OffsetFetchRequestData.OffsetFetchRequestGroup requestGroup : request.groups()) {
            String groupId = requestGroup.groupId();
            LOG.info("OffsetFetch v{} from group {}, {} topics", 
                    apiVersion, groupId, requestGroup.topics() != null ? requestGroup.topics().size() : 0);
            
            if (requestGroup.topics() == null || requestGroup.topics().isEmpty()) {
                OffsetFetchResponseData.OffsetFetchResponseGroup responseGroup = 
                    new OffsetFetchResponseData.OffsetFetchResponseGroup()
                        .setGroupId(groupId)
                        .setErrorCode(Errors.NONE.code());
                groupFutures.add(CompletableFuture.completedFuture(responseGroup));
                continue;
            }
            
            List<CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseTopics>> topicFutures = new ArrayList<>();
            
            for (OffsetFetchRequestData.OffsetFetchRequestTopics requestTopic : requestGroup.topics()) {
                String topicName = requestTopic.name();
                
                List<CompletableFuture<OffsetFetchResponseData.OffsetFetchResponsePartitions>> partitionFutures = new ArrayList<>();
                
                for (int partitionIndex : requestTopic.partitionIndexes()) {
                    InternalRow lookupKey = buildLookupKey(groupId, topicName, partitionIndex);
                    CompletableFuture<OffsetFetchResponseData.OffsetFetchResponsePartitions> partFuture = 
                        offsetLookuper.lookup(lookupKey)
                            .thenApply(lookupResult -> {
                                List<InternalRow> resultRows = lookupResult.getRowList();
                                
                                if (resultRows != null && !resultRows.isEmpty()) {
                                    InternalRow resultRow = resultRows.get(0);
                                    long offset = resultRow.getLong(3);
                                    String metadata = resultRow.isNullAt(4) ? "" : resultRow.getString(4).toString();
                                    
                                    LOG.trace("Fetched offset {} for {}-{} in group {}", 
                                            offset, topicName, partitionIndex, groupId);
                                    
                                    return new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                        .setPartitionIndex(partitionIndex)
                                        .setCommittedOffset(offset)
                                        .setMetadata(metadata)
                                        .setErrorCode(Errors.NONE.code());
                                } else {
                                    return new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                        .setPartitionIndex(partitionIndex)
                                        .setCommittedOffset(-1L)
                                        .setMetadata("")
                                        .setErrorCode(Errors.NONE.code());
                                }
                            })
                            .exceptionally(e -> {
                                LOG.error("Error fetching offset for {}-{} in group {}", 
                                        topicName, partitionIndex, groupId, e);
                                return new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                    .setPartitionIndex(partitionIndex)
                                    .setCommittedOffset(-1L)
                                    .setMetadata("")
                                    .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
                            });
                    
                    partitionFutures.add(partFuture);
                }
                
                CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseTopics> topicFuture = 
                    CompletableFuture.allOf(partitionFutures.toArray(new CompletableFuture[0]))
                        .thenApply(v -> {
                            OffsetFetchResponseData.OffsetFetchResponseTopics responseTopic = 
                                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                                    .setName(topicName);
                            
                            for (CompletableFuture<OffsetFetchResponseData.OffsetFetchResponsePartitions> pf : partitionFutures) {
                                responseTopic.partitions().add(pf.join());
                            }
                            
                            return responseTopic;
                        });
                
                topicFutures.add(topicFuture);
            }
            
            CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup> groupFuture = 
                CompletableFuture.allOf(topicFutures.toArray(new CompletableFuture[0]))
                    .thenApply(v -> {
                        OffsetFetchResponseData.OffsetFetchResponseGroup responseGroup = 
                            new OffsetFetchResponseData.OffsetFetchResponseGroup()
                                .setGroupId(groupId);
                        
                        for (CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseTopics> tf : topicFutures) {
                            responseGroup.topics().add(tf.join());
                        }
                        
                        responseGroup.setErrorCode(Errors.NONE.code());
                        
                        LOG.info("Built v8+ response for group {}: {} topics", 
                                groupId, responseGroup.topics().size());
                        
                        return responseGroup;
                    });
            
            groupFutures.add(groupFuture);
        }
        
        return CompletableFuture.allOf(groupFutures.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                for (CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup> gf : groupFutures) {
                    responseData.groups().add(gf.join());
                }
                
                OffsetFetchResponse response = new OffsetFetchResponse(responseData, apiVersion);
                LOG.info("Created OffsetFetchResponse v{}: {} groups", 
                        apiVersion, response.data().groups().size());
                return response;
            });
    }
    
    private InternalRow buildLookupKey(String groupId, String topic, int partition) {
        return GenericRow.of(
            BinaryString.fromString(groupId),
            BinaryString.fromString(topic),
            partition
        );
    }

    public void close() {
    }
}
