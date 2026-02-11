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

package org.gnuhpc.fluss.cape.hbase.storage;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Adapter for HBase atomic increment operations using Fluss aggregation engine.
 * 
 * <p>Uses the same pattern as Redis's RedisSingleTableAdapter for atomic INCR operations.
 */
public class HBaseIncrementAdapter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseIncrementAdapter.class);

    private final Connection connection;
    private final TablePath counterTablePath;
    private final Table counterTable;
    private final ThreadLocal<UpsertWriter> counterWriter;
    private final ThreadLocal<Lookuper> counterLookuper;
    private final ConcurrentLinkedQueue<UpsertWriter> allWriters = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Lookuper> allLookupers = new ConcurrentLinkedQueue<>();

    public HBaseIncrementAdapter(Connection connection, String database, String tableName) 
            throws Exception {
        this.connection = connection;
        this.counterTablePath = HBaseCounterTableManager.getCounterTablePath(database, tableName);
        this.counterTable = connection.getTable(counterTablePath);
        this.counterWriter = ThreadLocal.withInitial(this::createCounterWriter);
        this.counterLookuper = ThreadLocal.withInitial(this::createCounterLookuper);
        
        LOG.info("Initialized HBaseIncrementAdapter for counter table: {}", counterTablePath);
    }

    private UpsertWriter createCounterWriter() {
        try {
            UpsertWriter writer = counterTable.newUpsert().createWriter();
            allWriters.add(writer);
            return writer;
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create counter writer for " + counterTablePath, e);
        }
    }

    private Lookuper createCounterLookuper() {
        try {
            Lookuper lookuper = counterTable.newLookup().createLookuper();
            allLookupers.add(lookuper);
            return lookuper;
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create counter lookuper for " + counterTablePath, e);
        }
    }

    /**
     * Atomically increment a column value using aggregation engine.
     *
     * @param rowKey the HBase row key
     * @param family column family
     * @param qualifier column qualifier
     * @param delta increment amount
     * @return CompletableFuture with the new aggregated value
     */
    public CompletableFuture<Long> incrementColumn(
            byte[] rowKey, String family, String qualifier, long delta) {
        
        String columnKey = HBaseCounterTableManager.buildColumnKey(family, qualifier);
        
        GenericRow deltaRow = GenericRow.of(rowKey, BinaryString.fromString(columnKey), delta);
        
        return counterWriter.get().upsert(deltaRow)
                .thenCompose(v -> {
                    GenericRow keyRow = GenericRow.of(rowKey, BinaryString.fromString(columnKey));
                    return counterLookuper.get().lookup(keyRow);
                })
                .thenApply(lookupResult -> {
                    long aggregatedValue = 0L;
                    if (lookupResult != null) {
                        List<InternalRow> rows = lookupResult.getRowList();
                        if (rows != null && !rows.isEmpty()) {
                            aggregatedValue = rows.get(0).getLong(2);
                        }
                    }
                    return aggregatedValue;
                });
    }

    /**
     * Atomically increment multiple columns in a single row.
     *
     * @param rowKey the HBase row key
     * @param increments map of "family:qualifier" -> delta
     * @return CompletableFuture with map of "family:qualifier" -> new value
     */
    public CompletableFuture<Map<String, Long>> incrementColumns(
            byte[] rowKey, Map<String, Long> increments) {
        
        if (increments.isEmpty()) {
            return CompletableFuture.completedFuture(new HashMap<>());
        }

        @SuppressWarnings("unchecked")
        CompletableFuture<Void>[] upsertFutures = increments.entrySet().stream()
                .map(entry -> {
                    GenericRow deltaRow = GenericRow.of(
                            rowKey, 
                            BinaryString.fromString(entry.getKey()), 
                            entry.getValue());
                    return counterWriter.get().upsert(deltaRow);
                })
                .toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(upsertFutures)
                .thenCompose(v -> {
                    @SuppressWarnings("unchecked")
                    CompletableFuture<Long>[] lookupFutures = increments.keySet().stream()
                            .map(columnKey -> {
                                GenericRow keyRow = GenericRow.of(
                                        rowKey, 
                                        BinaryString.fromString(columnKey));
                                return counterLookuper.get().lookup(keyRow)
                                        .thenApply(result -> {
                                            if (result != null) {
                                                List<InternalRow> rows = result.getRowList();
                                                if (rows != null && !rows.isEmpty()) {
                                                    return rows.get(0).getLong(2);
                                                }
                                            }
                                            return 0L;
                                        });
                            })
                            .toArray(CompletableFuture[]::new);
                    
                    return CompletableFuture.allOf(lookupFutures)
                            .thenApply(v2 -> {
                                Map<String, Long> results = new HashMap<>();
                                String[] keys = increments.keySet().toArray(new String[0]);
                                for (int i = 0; i < keys.length; i++) {
                                    results.put(keys[i], lookupFutures[i].join());
                                }
                                return results;
                            });
                });
    }

    /**
     * Clear all counter entries for a row (called on DELETE).
     *
     * @param rowKey the HBase row key
     * @param columnKeys list of "family:qualifier" keys to clear
     * @return CompletableFuture that completes when all deletes are done
     */
    public CompletableFuture<Void> clearCounters(byte[] rowKey, List<String> columnKeys) {
        if (columnKeys.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        @SuppressWarnings("unchecked")
        CompletableFuture<Void>[] deleteFutures = columnKeys.stream()
                .map(columnKey -> {
                    GenericRow keyRow = GenericRow.of(rowKey, BinaryString.fromString(columnKey));
                    return counterWriter.get().delete(keyRow);
                })
                .toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(deleteFutures);
    }

    /**
     * Seed a counter with an initial value (called on PUT to sync counter state).
     *
     * @param rowKey the HBase row key
     * @param columnKey "family:qualifier"
     * @param value the value to seed (will overwrite any existing aggregated value)
     * @return CompletableFuture that completes when seeding is done
     */
    public CompletableFuture<Void> seedCounter(byte[] rowKey, String columnKey, long value) {
        return counterLookuper.get().lookup(GenericRow.of(rowKey, BinaryString.fromString(columnKey)))
                .thenCompose(result -> {
                    long currentValue = 0L;
                    if (result != null) {
                        List<InternalRow> rows = result.getRowList();
                        if (rows != null && !rows.isEmpty()) {
                            currentValue = rows.get(0).getLong(2);
                        }
                    }
                    
                    long delta = value - currentValue;
                    if (delta == 0) {
                        return CompletableFuture.<Void>completedFuture(null);
                    }
                    
                    GenericRow deltaRow = GenericRow.of(
                            rowKey, 
                            BinaryString.fromString(columnKey), 
                            delta);
                    return counterWriter.get().upsert(deltaRow).thenApply(r -> null);
                });
    }

    @Override
    public void close() {
        for (UpsertWriter writer : allWriters) {
            try {
                writer.flush();
            } catch (Exception e) {
                LOG.warn("Failed to flush counter writer during close", e);
            }
        }
        allWriters.clear();
        allLookupers.clear();
        LOG.info("Closed HBaseIncrementAdapter for {}", counterTablePath);
    }
}
