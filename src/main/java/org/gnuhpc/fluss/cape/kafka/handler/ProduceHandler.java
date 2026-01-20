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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.gnuhpc.fluss.cape.kafka.config.KafkaCompatConfig;
import org.gnuhpc.fluss.cape.kafka.converter.RecordConverter;
import org.gnuhpc.fluss.cape.kafka.converter.TablePathResolver;
import org.gnuhpc.fluss.cape.kafka.pool.WriterPool;
import org.gnuhpc.fluss.cape.kafka.protocol.KafkaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ProduceHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ProduceHandler.class);

    private final Connection flussConnection;
    private final WriterPool writerPool;
    private final TablePathResolver tablePathResolver;
    private final KafkaCompatConfig config;

    public ProduceHandler(
            Connection flussConnection,
            WriterPool writerPool,
            KafkaCompatConfig config,
            TablePathResolver tablePathResolver) {
        this.flussConnection = flussConnection;
        this.writerPool = writerPool;
        this.tablePathResolver = tablePathResolver;
        this.config = config;
    }

    public void handle(KafkaRequest request) {
        try {
            ProduceRequest produceRequest = request.request();
            handleProduceAsync(produceRequest)
                .thenAccept(request::complete)
                .exceptionally(ex -> {
                    LOG.error("Error handling produce request", ex);
                    request.fail(ex);
                    return null;
                });
        } catch (Exception e) {
            LOG.error("Error handling produce request", e);
            request.fail(e);
        }
    }

    private CompletableFuture<ProduceResponse> handleProduceAsync(ProduceRequest produceRequest) {
        LOG.info("Handling PRODUCE request for {} topics", produceRequest.data().topicData().size());
        
        Map<org.apache.kafka.common.TopicPartition, ProduceResponse.PartitionResponse> responses = 
            new HashMap<>();
        
        List<CompletableFuture<Void>> allFutures = new ArrayList<>();
        
        for (var topicData : produceRequest.data().topicData()) {
            String topicName = topicData.name();
            TablePath tablePath = tablePathResolver.resolve(topicName);
            
            try {
                ensureTableExists(tablePath);
                LOG.info("Getting table for: {}", tablePath);
                Table table = flussConnection.getTable(tablePath);
                LOG.info("Getting table info/schema for: {}", tablePath);
                org.apache.fluss.types.RowType schema = table.getTableInfo().getSchema().getRowType();
                LOG.info("Getting writer from pool for: {}", topicName);
                AppendWriter writer = writerPool.getOrCreate(topicName);
                
                for (var partitionData : topicData.partitionData()) {
                    int partition = partitionData.index();
                    LOG.info("Processing partition {} for topic {}", partition, topicName);
                    org.apache.kafka.common.TopicPartition tp = 
                        new org.apache.kafka.common.TopicPartition(topicName, partition);
                    
                    List<CompletableFuture<?>> partitionFutures = new ArrayList<>();
                    
                    try {
                        MemoryRecords memRecords = (MemoryRecords) partitionData.records();
                        long baseOffset = 0L;
                        int recordCount = 0;
                        
                        LOG.info("Iterating through record batches for {}-{}", topicName, partition);
                        for (RecordBatch batch : memRecords.batches()) {
                            LOG.info("Processing batch with {} records", batch.countOrNull());
                            for (Record kafkaRecord : batch) {
                                LOG.info("Converting Kafka record to Fluss row");
                                InternalRow flussRow = RecordConverter.kafkaRecordToFlussRow(
                                    kafkaRecord, 
                                    schema
                                );
                                LOG.info("Appending row to writer");
                                partitionFutures.add(writer.append(flussRow));
                                recordCount++;
                                LOG.info("Record count: {}", recordCount);
                            }
                            LOG.info("Finished processing batch");
                        }
                        LOG.info("Finished all batches, total records: {}", recordCount);
                        
                        final long finalBaseOffset = baseOffset;
                        final int finalRecordCount = recordCount;
                        
                        LOG.info("Waiting for all append futures to complete for {}-{}", topicName, partition);
                        CompletableFuture<Void> partitionComplete = 
                            CompletableFuture.allOf(partitionFutures.toArray(new CompletableFuture[0]))
                                .thenRunAsync(() -> {
                                    try {
                                        LOG.info("All appends complete, flushing writer for {}-{}", topicName, partition);
                                        long flushStart = System.currentTimeMillis();
                                        writer.flush();
                                        long flushTime = System.currentTimeMillis() - flushStart;
                                        LOG.info("Flush completed in {}ms for {}-{}", flushTime, topicName, partition);
                                        responses.put(tp, new ProduceResponse.PartitionResponse(
                                            Errors.NONE,
                                            finalBaseOffset,
                                            System.currentTimeMillis(),
                                            finalBaseOffset
                                        ));
                                        LOG.info("Produced {} records to {}-{}", 
                                                 finalRecordCount, topicName, partition);
                                    } catch (Exception e) {
                                        LOG.error("Error flushing writer for {}-{}", topicName, partition, e);
                                        responses.put(tp, new ProduceResponse.PartitionResponse(
                                            Errors.UNKNOWN_SERVER_ERROR
                                        ));
                                    }
                                });
                        
                        allFutures.add(partitionComplete);
                        
                    } catch (Exception e) {
                        LOG.error("Error processing partition {} of topic {}", partition, topicName, e);
                        responses.put(tp, new ProduceResponse.PartitionResponse(
                            Errors.UNKNOWN_SERVER_ERROR
                        ));
                    }
                }
                
            } catch (Exception e) {
                LOG.error("Error handling produce for topic {}", topicName, e);
                for (var partitionData : topicData.partitionData()) {
                    org.apache.kafka.common.TopicPartition tp = 
                        new org.apache.kafka.common.TopicPartition(topicName, partitionData.index());
                    responses.put(tp, new ProduceResponse.PartitionResponse(
                        Errors.UNKNOWN_TOPIC_OR_PARTITION
                    ));
                }
            }
        }
        
        LOG.info("Waiting for all partition futures to complete");
        return CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                LOG.info("All partitions complete, creating ProduceResponse with {} responses", responses.size());
                return new ProduceResponse(responses);
            });
    }
    
    private void ensureTableExists(TablePath tablePath) {
        try {
            LOG.info("Checking if table exists: {}", tablePath);
            Admin admin = flussConnection.getAdmin();
            boolean exists = admin.tableExists(tablePath).get();
            LOG.info("Table {} exists: {}", tablePath, exists);
            if (!exists) {
                LOG.info("Auto-creating Kafka table: {}", tablePath);
                
                Schema schema = Schema.newBuilder()
                    .column("key", DataTypes.BYTES())
                    .column("value", DataTypes.BYTES())
                    .column("timestamp", DataTypes.BIGINT())
                    .build();
                
                TableDescriptor tableDescriptor = TableDescriptor.builder()
                    .schema(schema)
                    .distributedBy(3)
                    .build();
                
                admin.createTable(tablePath, tableDescriptor, false).get();
                LOG.info("Successfully created Kafka table: {}", tablePath);
            }
        } catch (Exception e) {
            LOG.error("Failed to create Kafka table: {}", tablePath, e);
            throw new RuntimeException("Failed to ensure table exists: " + tablePath, e);
        }
    }

    public void close() {
    }
}
