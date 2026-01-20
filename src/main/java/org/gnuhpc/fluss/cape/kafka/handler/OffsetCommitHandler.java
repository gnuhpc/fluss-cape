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

import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.gnuhpc.fluss.cape.kafka.consumer.ConsumerGroupCoordinatorV2;
import org.gnuhpc.fluss.cape.kafka.protocol.KafkaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class OffsetCommitHandler {
    private static final Logger LOG = LoggerFactory.getLogger(OffsetCommitHandler.class);

    private final ConsumerGroupCoordinatorV2 coordinator;

    public OffsetCommitHandler(ConsumerGroupCoordinatorV2 coordinator) {
        this.coordinator = coordinator;
    }

    public void handle(KafkaRequest request) {
        coordinator.getInitializationFuture()
            .thenCompose(v -> {
                try {
                    org.apache.kafka.common.requests.OffsetCommitRequest commitRequest = request.request();
                    OffsetCommitRequestData commitRequestData = commitRequest.data();
                    return handleOffsetCommitAsync(commitRequestData, request.apiVersion());
                } catch (Exception e) {
                    LOG.error("Error handling offset commit request", e);
                    throw new RuntimeException(e);
                }
            })
            .thenAccept(request::complete)
            .exceptionally(ex -> {
                LOG.error("Error handling offset commit request", ex);
                request.fail(ex);
                return null;
            });
    }

    private CompletableFuture<OffsetCommitResponse> handleOffsetCommitAsync(
            OffsetCommitRequestData request, 
            short apiVersion) {
        
        String groupId = request.groupId();
        
        LOG.debug("OffsetCommit from group {}, {} topics", 
                groupId, request.topics().size());
        
        UpsertWriter offsetWriter = coordinator.getOffsetWriter();
        List<CompletableFuture<?>> allFutures = new ArrayList<>();
        Map<TopicPartition, Errors> responseMap = new HashMap<>();
        
        for (OffsetCommitRequestData.OffsetCommitRequestTopic topic : request.topics()) {
            String topicName = topic.name();
            
            for (OffsetCommitRequestData.OffsetCommitRequestPartition partition : topic.partitions()) {
                int partitionIndex = partition.partitionIndex();
                long offset = partition.committedOffset();
                String metadata = partition.committedMetadata();
                
                TopicPartition tp = new TopicPartition(topicName, partitionIndex);
                
                try {
                    InternalRow row = buildOffsetRow(
                        groupId, 
                        topicName, 
                        partitionIndex, 
                        offset, 
                        metadata
                    );
                    
                    CompletableFuture<?> upsertFuture = offsetWriter.upsert(row)
                        .thenRun(() -> {
                            responseMap.put(tp, Errors.NONE);
                            LOG.trace("Committed offset {} for {}-{} in group {}", 
                                    offset, topicName, partitionIndex, groupId);
                        })
                        .exceptionally(ex -> {
                            LOG.error("Error committing offset for {}-{} in group {}", 
                                    topicName, partitionIndex, groupId, ex);
                            responseMap.put(tp, Errors.UNKNOWN_SERVER_ERROR);
                            return null;
                        });
                    
                    allFutures.add(upsertFuture);
                    
                } catch (Exception e) {
                    LOG.error("Error building offset row for {}-{}", topicName, partitionIndex, e);
                    responseMap.put(tp, Errors.UNKNOWN_SERVER_ERROR);
                }
            }
        }
        
        return CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                OffsetCommitResponseData responseData = new OffsetCommitResponseData();
                
                for (OffsetCommitRequestData.OffsetCommitRequestTopic topic : request.topics()) {
                    OffsetCommitResponseData.OffsetCommitResponseTopic responseTopic = 
                        new OffsetCommitResponseData.OffsetCommitResponseTopic()
                            .setName(topic.name());
                    
                    for (OffsetCommitRequestData.OffsetCommitRequestPartition partition : topic.partitions()) {
                        TopicPartition tp = new TopicPartition(topic.name(), partition.partitionIndex());
                        Errors error = responseMap.getOrDefault(tp, Errors.UNKNOWN_SERVER_ERROR);
                        
                        responseTopic.partitions().add(
                            new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(partition.partitionIndex())
                                .setErrorCode(error.code())
                        );
                    }
                    
                    responseData.topics().add(responseTopic);
                }
                
                return new OffsetCommitResponse(responseData);
            })
            .exceptionally(ex -> {
                LOG.error("Error handling offset commit request", ex);
                return new OffsetCommitResponse(new OffsetCommitResponseData());
            });
    }
    
    private InternalRow buildOffsetRow(
            String groupId, 
            String topic, 
            int partition, 
            long offset, 
            String metadata) {
        
        return GenericRow.of(
            BinaryString.fromString(groupId),
            BinaryString.fromString(topic),
            partition,
            offset,
            metadata != null ? BinaryString.fromString(metadata) : null,
            System.currentTimeMillis()
        );
    }

    public void close() {
    }
}
