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
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.metadata.TablePath;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.gnuhpc.fluss.cape.kafka.config.KafkaCompatConfig;
import org.gnuhpc.fluss.cape.kafka.converter.TablePathResolver;
import org.gnuhpc.fluss.cape.kafka.protocol.KafkaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ListOffsetsHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ListOffsetsHandler.class);

    private final Connection flussConnection;
    private final TablePathResolver tablePathResolver;

    public ListOffsetsHandler(Connection flussConnection, KafkaCompatConfig config,
                             TablePathResolver tablePathResolver) {
        this.flussConnection = flussConnection;
        this.tablePathResolver = tablePathResolver;
    }

    public void handle(KafkaRequest request) {
        try {
            ListOffsetsRequest listOffsetsRequest = request.request();
            handleListOffsetsAsync(listOffsetsRequest, request.apiVersion())
                .thenAccept(request::complete)
                .exceptionally(ex -> {
                    LOG.error("Error handling list offsets request", ex);
                    request.fail(ex);
                    return null;
                });
        } catch (Exception e) {
            LOG.error("Error handling list offsets request", e);
            request.fail(e);
        }
    }

    private CompletableFuture<ListOffsetsResponse> handleListOffsetsAsync(
            ListOffsetsRequest listOffsetsRequest, 
            short apiVersion) {
        
        ListOffsetsResponseData responseData = new ListOffsetsResponseData();
        Admin admin = flussConnection.getAdmin();
        
        List<CompletableFuture<Void>> topicFutures = new ArrayList<>();
        
        listOffsetsRequest.topics().forEach(topicData -> {
            String topicName = topicData.name();
            ListOffsetsResponseData.ListOffsetsTopicResponse topicResponse =
                new ListOffsetsResponseData.ListOffsetsTopicResponse()
                    .setName(topicName);
            
            try {
                TablePath tablePath = tablePathResolver.resolve(topicName);
                
                List<CompletableFuture<Void>> partitionFutures = new ArrayList<>();
                
                topicData.partitions().forEach(partitionData -> {
                    int partition = partitionData.partitionIndex();
                    long timestamp = partitionData.timestamp();
                    
                    ListOffsetsResponseData.ListOffsetsPartitionResponse partResponse =
                        new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                            .setPartitionIndex(partition);
                    
                    List<Integer> bucketIds = new ArrayList<>();
                    bucketIds.add(partition);
                    
                    OffsetSpec offsetSpec;
                    if (timestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP) {
                        offsetSpec = new OffsetSpec.EarliestSpec();
                    } else if (timestamp == ListOffsetsRequest.LATEST_TIMESTAMP) {
                        offsetSpec = new OffsetSpec.LatestSpec();
                    } else {
                        offsetSpec = new OffsetSpec.TimestampSpec(timestamp);
                    }
                    
                    ListOffsetsResult result = admin.listOffsets(tablePath, bucketIds, offsetSpec);
                    
                    CompletableFuture<Void> partitionFuture = result.bucketResult(partition)
                        .thenAccept(offset -> {
                            if (apiVersion == 0) {
                                partResponse
                                    .setErrorCode(Errors.NONE.code())
                                    .setOldStyleOffsets(Arrays.asList(offset));
                            } else {
                                partResponse
                                    .setErrorCode(Errors.NONE.code())
                                    .setTimestamp(timestamp)
                                    .setOffset(offset);
                            }
                        })
                        .exceptionally(e -> {
                            LOG.error("Error getting offset for partition {} of topic {}", 
                                     partition, topicName, e);
                            partResponse
                                .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                                .setTimestamp(-1L)
                                .setOffset(-1L);
                            return null;
                        });
                    
                    partitionFutures.add(partitionFuture);
                    topicResponse.partitions().add(partResponse);
                });
                
                CompletableFuture<Void> topicFuture = 
                    CompletableFuture.allOf(partitionFutures.toArray(new CompletableFuture[0]))
                        .exceptionally(e -> {
                            LOG.error("Error handling list offsets for topic {}", topicName, e);
                            return null;
                        });
                
                topicFutures.add(topicFuture);
                
            } catch (Exception e) {
                LOG.error("Error handling list offsets for topic {}", topicName, e);
                topicData.partitions().forEach(partitionData -> {
                    ListOffsetsResponseData.ListOffsetsPartitionResponse partResponse =
                        new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                            .setPartitionIndex(partitionData.partitionIndex())
                            .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                            .setTimestamp(-1L)
                            .setOffset(-1L);
                    topicResponse.partitions().add(partResponse);
                });
            }
            
            responseData.topics().add(topicResponse);
        });
        
        return CompletableFuture.allOf(topicFutures.toArray(new CompletableFuture[0]))
            .thenApply(v -> new ListOffsetsResponse(responseData));
    }
}
