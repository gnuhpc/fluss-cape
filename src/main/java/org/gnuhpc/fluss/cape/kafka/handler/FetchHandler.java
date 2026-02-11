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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.metadata.TablePath;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.message.FetchResponseData;
import org.gnuhpc.fluss.cape.kafka.config.KafkaCompatConfig;
import org.gnuhpc.fluss.cape.kafka.converter.RecordConverter;
import org.gnuhpc.fluss.cape.kafka.converter.TablePathResolver;
import org.gnuhpc.fluss.cape.kafka.pool.ScannerPool;
import org.gnuhpc.fluss.cape.kafka.protocol.KafkaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class FetchHandler {
    private static final Logger LOG = LoggerFactory.getLogger(FetchHandler.class);

    private final Connection flussConnection;
    private final ScannerPool scannerPool;
    private final TablePathResolver tablePathResolver;
    private final KafkaCompatConfig config;
    private final ExecutorService executorService;

    public FetchHandler(
            Connection flussConnection,
            ScannerPool scannerPool,
            KafkaCompatConfig config,
            TablePathResolver tablePathResolver,
            ExecutorService executorService) {
        this.flussConnection = flussConnection;
        this.scannerPool = scannerPool;
        this.tablePathResolver = tablePathResolver;
        this.config = config;
        this.executorService = executorService;
    }

    public void handle(KafkaRequest request) {
        try {
            FetchRequest fetchRequest = request.request();
            LOG.info("FetchHandler.handle() called with {} topics", fetchRequest.data().topics().size());
            handleFetchAsync(fetchRequest)
                .thenAccept(response -> {
                    LOG.info("FetchHandler.handle() completing request with response");
                    request.complete(response);
                })
                .exceptionally(ex -> {
                    LOG.error("Error handling fetch request", ex);
                    request.fail(ex);
                    return null;
                });
        } catch (Exception e) {
            LOG.error("Error handling fetch request", e);
            request.fail(e);
        }
    }

    private CompletableFuture<FetchResponse> handleFetchAsync(FetchRequest fetchRequest) {
        LOG.info("handleFetchAsync() starting for {} topics", fetchRequest.data().topics().size());
        List<CompletableFuture<FetchResponseData.FetchableTopicResponse>> topicFutures = new ArrayList<>();
        
        for (var topicData : fetchRequest.data().topics()) {
            String topicName = topicData.topic();
            LOG.info("Processing topic: {}, partitions: {}", topicName, topicData.partitions().size());
            
            CompletableFuture<FetchResponseData.FetchableTopicResponse> topicFuture = 
                CompletableFuture.supplyAsync(() -> {
                    FetchResponseData.FetchableTopicResponse topicResponse = 
                        new FetchResponseData.FetchableTopicResponse().setTopic(topicName);
                    
                    try {
                        TablePath tablePath = tablePathResolver.resolve(topicName);
                        Table table = flussConnection.getTable(tablePath);
                        org.apache.fluss.types.RowType schema = 
                            table.getTableInfo().getSchema().getRowType();
                        
                        List<CompletableFuture<FetchResponseData.PartitionData>> partitionFutures = new ArrayList<>();
                        
                        for (var partitionData : topicData.partitions()) {
                            int partition = partitionData.partition();
                            long fetchOffset = partitionData.fetchOffset();
                            
                            CompletableFuture<FetchResponseData.PartitionData> partitionFuture = 
                                CompletableFuture.supplyAsync(() -> {
                                    FetchResponseData.PartitionData partResponse = 
                                        new FetchResponseData.PartitionData()
                                            .setPartitionIndex(partition);
                                    
                                    try (ScannerPool.Lease lease = scannerPool.acquire(topicName, partition)) {
                                        try {
                                            LOG.info("Fetching from {}-{} at offset {}", topicName, partition, fetchOffset);
                                            LogScanner scanner = lease.scanner();
                                            scanner.subscribe(partition, fetchOffset);
                                            LOG.info("Scanner subscribed to {}-{} at offset {}", topicName, partition, fetchOffset);
                                            
                                            MemoryRecordsBuilder recordsBuilder = MemoryRecords.builder(
                                                ByteBuffer.allocate(1024 * 1024),
                                                org.apache.kafka.common.compress.Compression.NONE,
                                                TimestampType.CREATE_TIME,
                                                fetchOffset
                                            );
                                            
                                            ScanRecords scanRecords = scanner.poll(Duration.ofMillis(5));
                                            LOG.info("Poll returned from {}-{}, hasRecords: {}", topicName, partition, scanRecords.iterator().hasNext());
                                            long highWatermark = fetchOffset;
                                            int recordCount = 0;
                                            
                                            for (ScanRecord scanRecord : scanRecords) {
                                                RecordConverter.KafkaRecordBuilder builder = 
                                                    RecordConverter.flussRowToKafkaRecordBuilder(
                                                        scanRecord.getRow(), 
                                                        schema
                                                    );
                                                
                                                recordsBuilder.append(
                                                    builder.getTimestamp(),
                                                    builder.getKey() != null ? ByteBuffer.wrap(builder.getKey()) : null,
                                                    builder.getValue() != null ? ByteBuffer.wrap(builder.getValue()) : null,
                                                    new org.apache.kafka.common.header.Header[0]
                                                );
                                                
                                                highWatermark = scanRecord.logOffset() + 1;
                                                recordCount++;
                                            }
                                            
                                            partResponse
                                                .setErrorCode(Errors.NONE.code())
                                                .setHighWatermark(highWatermark)
                                                .setLastStableOffset(highWatermark)
                                                .setLogStartOffset(0L)
                                                .setRecords(recordsBuilder.build());
                                            
                                            LOG.info("Fetched {} records from {}-{} at offset {}, highWatermark: {}", 
                                                     recordCount, topicName, partition, fetchOffset, highWatermark);
                                        } catch (Exception e) {
                                            lease.invalidate();
                                            throw e;
                                        }
                                    } catch (Exception e) {
                                        LOG.error("Error fetching from {}-{}", topicName, partition, e);
                                        partResponse.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
                                    }
                                    
                                    return partResponse;
                                }, executorService);
                            
                            partitionFutures.add(partitionFuture);
                        }
                        
                        CompletableFuture<FetchResponseData.FetchableTopicResponse> partitionsFuture = 
                            CompletableFuture.allOf(partitionFutures.toArray(new CompletableFuture[0]))
                                .thenApply(v -> {
                                    for (CompletableFuture<FetchResponseData.PartitionData> pf : partitionFutures) {
                                        topicResponse.partitions().add(pf.join());
                                    }
                                    return topicResponse;
                                })
                                .exceptionally(ex -> {
                                    LOG.error("Error waiting for partition futures for topic {}", topicName, ex);
                                    for (var partitionData : topicData.partitions()) {
                                        FetchResponseData.PartitionData partResponse = 
                                            new FetchResponseData.PartitionData()
                                                .setPartitionIndex(partitionData.partition())
                                                .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
                                        topicResponse.partitions().add(partResponse);
                                    }
                                    return topicResponse;
                                });
                        
                        return partitionsFuture;
                        
                    } catch (Exception e) {
                        LOG.error("Error handling fetch for topic {}", topicName, e);
                        for (var partitionData : topicData.partitions()) {
                            FetchResponseData.PartitionData partResponse = 
                                new FetchResponseData.PartitionData()
                                    .setPartitionIndex(partitionData.partition())
                                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
                            topicResponse.partitions().add(partResponse);
                        }
                        return CompletableFuture.completedFuture(topicResponse);
                    }
                }, executorService).thenCompose(future -> future);
            
            topicFutures.add(topicFuture);
        }
        
        return CompletableFuture.allOf(topicFutures.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                FetchResponseData responseData = new FetchResponseData();
                for (CompletableFuture<FetchResponseData.FetchableTopicResponse> future : topicFutures) {
                    responseData.responses().add(future.join());
                }
                return new FetchResponse(responseData);
            });
    }
}
