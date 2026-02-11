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

package org.gnuhpc.fluss.cape.redis.storage;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RedisSingleTableAdapter implements RedisStorageAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSingleTableAdapter.class);

    private final Connection connection;
    private final TablePath tablePath;
    private final Table table;
    private final ThreadLocal<UpsertWriter> upsertWriter;
    private final ThreadLocal<Lookuper> lookuper;

    private final Table subKeyIndexTable;
    private final ThreadLocal<UpsertWriter> subKeyIndexWriter;
    private final ThreadLocal<Lookuper> subKeyIndexLookuper;

    private final Table stringCounterTable;
    private final ThreadLocal<UpsertWriter> stringCounterWriter;
    private final ThreadLocal<Lookuper> stringCounterLookuper;

    private final Table hashCounterTable;
    private final ThreadLocal<UpsertWriter> hashCounterWriter;
    private final ThreadLocal<Lookuper> hashCounterLookuper;

    private final Table zsetCounterTable;
    private final ThreadLocal<UpsertWriter> zsetCounterWriter;
    private final ThreadLocal<Lookuper> zsetCounterLookuper;

    private final ConcurrentLinkedQueue<UpsertWriter> allWriters = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Lookuper> allLookupers = new ConcurrentLinkedQueue<>();

    public RedisSingleTableAdapter(Connection connection, String database) throws Exception {
        this.connection = connection;
        
        this.tablePath = RedisDynamicTableManager.getMainTablePath(database);
        this.table = connection.getTable(tablePath);
        this.upsertWriter = ThreadLocal.withInitial(this::createUpsertWriter);
        this.lookuper = ThreadLocal.withInitial(this::createLookuper);
        
        TablePath subKeyIndexPath = RedisDynamicTableManager.getSubkeyIndexTablePath(database);
        this.subKeyIndexTable = connection.getTable(subKeyIndexPath);
        this.subKeyIndexWriter = ThreadLocal.withInitial(this::createSubKeyIndexWriter);
        this.subKeyIndexLookuper = ThreadLocal.withInitial(this::createSubKeyIndexLookuper);

        TablePath stringCounterPath = RedisDynamicTableManager.getStringCounterTablePath(database);
        this.stringCounterTable = connection.getTable(stringCounterPath);
        this.stringCounterWriter = ThreadLocal.withInitial(this::createStringCounterWriter);
        this.stringCounterLookuper = ThreadLocal.withInitial(this::createStringCounterLookuper);

        TablePath hashCounterPath = RedisDynamicTableManager.getHashCounterTablePath(database);
        this.hashCounterTable = connection.getTable(hashCounterPath);
        this.hashCounterWriter = ThreadLocal.withInitial(this::createHashCounterWriter);
        this.hashCounterLookuper = ThreadLocal.withInitial(this::createHashCounterLookuper);

        TablePath zsetCounterPath = RedisDynamicTableManager.getZsetCounterTablePath(database);
        this.zsetCounterTable = connection.getTable(zsetCounterPath);
        this.zsetCounterWriter = ThreadLocal.withInitial(this::createZsetCounterWriter);
        this.zsetCounterLookuper = ThreadLocal.withInitial(this::createZsetCounterLookuper);
        
        LOG.info("Initialized RedisSingleTableAdapter with dynamic tables: main={}, index={}, stringCounter={}, hashCounter={}, zsetCounter={}", 
                 tablePath, subKeyIndexPath, stringCounterPath, hashCounterPath, zsetCounterPath);
    }

    public byte[] get(byte[] key) throws Exception {
        return getByCompositeKey(new String(key, StandardCharsets.UTF_8), "");
    }

    public void set(byte[] key, byte[] value) throws Exception {
        String redisKey = new String(key, StandardCharsets.UTF_8);
        setByCompositeKey(redisKey, "string", "", value, null);
    }

    public void delete(byte[] key) throws Exception {
        String redisKey = new String(key, StandardCharsets.UTF_8);
        deleteByKey(redisKey);
        
        GenericRow indexKeyRow = GenericRow.of(
                BinaryString.fromString(redisKey),
                null);
        subKeyIndexWriter.get().delete(indexKeyRow).get();
    }

    public boolean exists(byte[] key) throws Exception {
        return get(key) != null;
    }

    public boolean keyExists(String key) throws Exception {
        return exists(key.getBytes(StandardCharsets.UTF_8));
    }

    public List<String> keys(String pattern) throws Exception {
        return new ArrayList<>();
    }

    public long incr(byte[] key) throws Exception {
        return incrBy(key, 1L);
    }

    public long decr(byte[] key) throws Exception {
        return incrBy(key, -1L);
    }

    public long decrBy(byte[] key, long decrement) throws Exception {
        return incrBy(key, -decrement);
    }

    public long incrBy(byte[] key, long increment) throws Exception {
        String redisKey = new String(key, StandardCharsets.UTF_8);
        
        String existingType = getType(redisKey);
        if (existingType != null && !"string".equals(existingType)) {
            throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        
        GenericRow deltaRow = GenericRow.of(
                BinaryString.fromString(redisKey),
                increment);
        stringCounterWriter.get().upsert(deltaRow).get();
        
        GenericRow keyRow = GenericRow.of(BinaryString.fromString(redisKey));
        List<InternalRow> results = stringCounterLookuper.get().lookup(keyRow).get().getRowList();
        
        long aggregatedValue = 0;
        if (results != null && !results.isEmpty()) {
            aggregatedValue = results.get(0).getLong(1);
        }
        
        setByCompositeKey(redisKey, "string", "", String.valueOf(aggregatedValue).getBytes(StandardCharsets.UTF_8), null);
        
        return aggregatedValue;
    }

    public double incrByFloat(byte[] key, double increment) throws Exception {
        String redisKey = new String(key, StandardCharsets.UTF_8);
        
        String existingType = getType(redisKey);
        if (existingType != null && !"string".equals(existingType)) {
            throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        
        byte[] currentBytes = getByCompositeKey(redisKey, "");
        double currentValue = 0.0;
        
        if (currentBytes != null) {
            try {
                currentValue = Double.parseDouble(new String(currentBytes, StandardCharsets.UTF_8));
            } catch (NumberFormatException e) {
                throw new RuntimeException("ERR value is not a valid float");
            }
        }
        
        double newValue = currentValue + increment;
        
        if (Double.isInfinite(newValue) || Double.isNaN(newValue)) {
            throw new RuntimeException("ERR increment would produce NaN or Infinity");
        }
        
        setByCompositeKey(redisKey, "string", "", String.valueOf(newValue).getBytes(StandardCharsets.UTF_8), null);
        
        return newValue;
    }

    public long hincrBy(String redisKey, String field, long increment) throws Exception {
        GenericRow deltaRow = GenericRow.of(
                BinaryString.fromString(redisKey),
                BinaryString.fromString(field),
                increment);
        hashCounterWriter.get().upsert(deltaRow).get();
        
        GenericRow keyRow = GenericRow.of(
                BinaryString.fromString(redisKey),
                BinaryString.fromString(field));
        List<InternalRow> results = hashCounterLookuper.get().lookup(keyRow).get().getRowList();
        
        long aggregatedValue = 0;
        if (results != null && !results.isEmpty()) {
            aggregatedValue = results.get(0).getLong(2);
        }
        
        setByCompositeKey(redisKey, "hash", field, String.valueOf(aggregatedValue).getBytes(StandardCharsets.UTF_8), null);
        
        return aggregatedValue;
    }

    public double zincrBy(String redisKey, String member, double increment) throws Exception {
        GenericRow deltaRow = GenericRow.of(
                BinaryString.fromString(redisKey),
                BinaryString.fromString(member),
                increment);
        zsetCounterWriter.get().upsert(deltaRow).get();
        
        GenericRow keyRow = GenericRow.of(
                BinaryString.fromString(redisKey),
                BinaryString.fromString(member));
        List<InternalRow> results = zsetCounterLookuper.get().lookup(keyRow).get().getRowList();
        
        double aggregatedValue = 0.0;
        if (results != null && !results.isEmpty()) {
            aggregatedValue = results.get(0).getDouble(2);
        }
        
        setByCompositeKey(redisKey, "zset", member, null, aggregatedValue);
        
        return aggregatedValue;
    }

    public void close() {
        for (UpsertWriter writer : allWriters) {
            try {
                writer.flush();
            } catch (Exception e) {
                LOG.warn("Failed to flush writer during close", e);
            }
        }
        allWriters.clear();
        allLookupers.clear();
    }

    private UpsertWriter createUpsertWriter() {
        try {
            UpsertWriter writer = table.newUpsert().createWriter();
            allWriters.add(writer);
            return writer;
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create upsert writer for " + tablePath, e);
        }
    }

    private Lookuper createLookuper() {
        try {
            Lookuper lookuper = table.newLookup().createLookuper();
            allLookupers.add(lookuper);
            return lookuper;
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create lookuper for " + tablePath, e);
        }
    }

    private UpsertWriter createSubKeyIndexWriter() {
        try {
            UpsertWriter writer = subKeyIndexTable.newUpsert().createWriter();
            allWriters.add(writer);
            return writer;
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create subkey index writer", e);
        }
    }

    private Lookuper createSubKeyIndexLookuper() {
        try {
            Lookuper lookuper = subKeyIndexTable.newLookup().createLookuper();
            allLookupers.add(lookuper);
            return lookuper;
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create subkey index lookuper", e);
        }
    }

    private UpsertWriter createStringCounterWriter() {
        try {
            UpsertWriter writer = stringCounterTable.newUpsert().createWriter();
            allWriters.add(writer);
            return writer;
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create string counter writer", e);
        }
    }

    private Lookuper createStringCounterLookuper() {
        try {
            Lookuper lookuper = stringCounterTable.newLookup().createLookuper();
            allLookupers.add(lookuper);
            return lookuper;
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create string counter lookuper", e);
        }
    }

    private UpsertWriter createHashCounterWriter() {
        try {
            UpsertWriter writer = hashCounterTable.newUpsert().createWriter();
            allWriters.add(writer);
            return writer;
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create hash counter writer", e);
        }
    }

    private Lookuper createHashCounterLookuper() {
        try {
            Lookuper lookuper = hashCounterTable.newLookup().createLookuper();
            allLookupers.add(lookuper);
            return lookuper;
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create hash counter lookuper", e);
        }
    }

    private UpsertWriter createZsetCounterWriter() {
        try {
            UpsertWriter writer = zsetCounterTable.newUpsert().createWriter();
            allWriters.add(writer);
            return writer;
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create zset counter writer", e);
        }
    }

    private Lookuper createZsetCounterLookuper() {
        try {
            Lookuper lookuper = zsetCounterTable.newLookup().createLookuper();
            allLookupers.add(lookuper);
            return lookuper;
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create zset counter lookuper", e);
        }
    }

    // ============================================================
    // Phase 2: Multi-row operations for Hash, List, Set, Sorted Set
    // ============================================================

    /**
     * Inner class for key-value pairs returned by scan operations.
     */
    public static class KeyValue {
        public String subKey;
        public byte[] value;
        public Double score;
        public String redisType;

        public KeyValue(String subKey, byte[] value, Double score, String redisType) {
            this.subKey = subKey;
            this.value = value;
            this.score = score;
            this.redisType = redisType;
        }
    }

    public static class FullKeyValue {
        public String redisKey;
        public String subKey;
        public byte[] value;
        public Double score;
        public String redisType;

        public FullKeyValue(
                String redisKey, String subKey, byte[] value, Double score, String redisType) {
            this.redisKey = redisKey;
            this.subKey = subKey;
            this.value = value;
            this.score = score;
            this.redisType = redisType;
        }
    }

    /**
     * Scan all sub-keys for a given redis_key.
     * Returns list of (sub_key, value, score) tuples.
     * Uses Fluss subkey index table to avoid snapshot dependency.
     */
    public List<KeyValue> scanByKey(String redisKey) throws Exception {
        List<KeyValue> results = new ArrayList<>();
        
        Set<String> subKeys = getSubKeysFromIndex(redisKey);
        if (subKeys.isEmpty()) {
            LOG.debug("No indexed sub-keys for redis_key: {}", redisKey);
            return results;
        }
        
        for (String subKey : subKeys) {
            InternalRow row = lookupRow(redisKey, subKey);
            if (row != null) {
                String redisType = row.isNullAt(1) ? null : row.getString(1).toString();
                Double score = row.isNullAt(3) ? null : row.getDouble(3);
                byte[] value = row.isNullAt(4) ? null : row.getBytes(4);
                
                results.add(new KeyValue(subKey, value, score, redisType));
            }
        }
        
        return results;
    }
    
    private Set<String> getSubKeysFromIndex(String redisKey) throws Exception {
        Set<String> subKeys = new HashSet<>();
        
        GenericRow keyRow = GenericRow.of(BinaryString.fromString(redisKey));
        List<InternalRow> results = subKeyIndexLookuper.get().lookup(keyRow).get().getRowList();        
        if (results != null && !results.isEmpty()) {
            InternalRow row = results.get(0);
            if (!row.isNullAt(1)) {
                String subKeysJson = row.getString(1).toString();
                if (subKeysJson.isEmpty()) {
                    // String type with empty subKey
                    subKeys.add("");
                } else {
                    String[] parts = subKeysJson.split("\u0000");
                    for (String part : parts) {
                        subKeys.add(part);
                    }
                }
            }
        }
        
        return subKeys;
    }

    /**
     * Scan sub-keys with a specific prefix.
     */
    public List<KeyValue> scanByPrefix(String redisKey, String subKeyPrefix) throws Exception {
        List<KeyValue> allResults = scanByKey(redisKey);
        List<KeyValue> filtered = new ArrayList<>();
        
        for (KeyValue kv : allResults) {
            if (kv.subKey.startsWith(subKeyPrefix)) {
                filtered.add(kv);
            }
        }
        
        return filtered;
    }

    /**
     * Count rows for a given redis_key.
     */
    public long countByKey(String redisKey) throws Exception {
        return scanByKey(redisKey).size();
    }

    /**
     * Get all Redis keys from cache (lightweight, no data lookup).
     */
    public Set<String> getAllKeys() {
        try {
            return getAllRedisKeysFromIndex();
        } catch (Exception e) {
            LOG.warn("Failed to fetch keys from index", e);
            return Collections.emptySet();
        }
    }
    
    /**
     * Scan all rows in the table (for KEYS/SCAN commands).
     */
    public List<FullKeyValue> scanAll() throws Exception {
        LOG.info("scanAll() called - scanning index from storage");
        List<FullKeyValue> results = new ArrayList<>();
        
        Set<String> allRedisKeys = getAllRedisKeysFromIndex();
        
        for (String redisKey : allRedisKeys) {
            Set<String> subKeys = getSubKeysFromIndex(redisKey);
            for (String subKey : subKeys) {
                InternalRow row = lookupRow(redisKey, subKey);
                if (row != null) {
                    String redisType = row.isNullAt(1) ? null : row.getString(1).toString();
                    Double score = row.isNullAt(3) ? null : row.getDouble(3);
                    byte[] value = row.isNullAt(4) ? null : row.getBytes(4);
                    
                    results.add(new FullKeyValue(redisKey, subKey, value, score, redisType));
                }
            }
        }
        
        return results;
    }
    
    private Set<String> getAllRedisKeysFromIndex() throws Exception {
        Set<String> redisKeys = new HashSet<>();
        
        TableInfo tableInfo = subKeyIndexTable.getTableInfo();
        long tableId = tableInfo.getTableId();
        
        Admin admin = connection.getAdmin();
        String database = tablePath.getDatabaseName();
        TablePath subKeyIndexPath = RedisDynamicTableManager.getSubkeyIndexTablePath(database);
        KvSnapshots kvSnapshots = admin.getLatestKvSnapshots(subKeyIndexPath).get();
        
        LOG.info("Scanning redis_subkey_index: tableId={}, available snapshots={}", 
                 tableId, kvSnapshots.getBucketIds().size());
        
        for (int bucketId : kvSnapshots.getBucketIds()) {
            LOG.debug("Checking bucket {}: hasSnapshot={}", 
                     bucketId, kvSnapshots.getSnapshotId(bucketId).isPresent());
            
            if (!kvSnapshots.getSnapshotId(bucketId).isPresent()) {
                LOG.warn("Bucket {} has no snapshot yet, skipping", bucketId);
                continue;
            }
            
            long snapshotId = kvSnapshots.getSnapshotId(bucketId).getAsLong();
            TableBucket tableBucket = new TableBucket(tableId, bucketId);
            int rowsInBucket = 0;
            int batchCount = 0;
            
            LOG.info("Scanning bucket {} with snapshotId {}", bucketId, snapshotId);
            
            try (BatchScanner scanner = subKeyIndexTable.newScan().createBatchScanner(tableBucket, snapshotId)) {
                CloseableIterator<InternalRow> iterator;
                while ((iterator = scanner.pollBatch(Duration.ofMillis(100))) != null) {
                    batchCount++;
                    try {
                        while (iterator.hasNext()) {
                            InternalRow row = iterator.next();
                            rowsInBucket++;
                            if (!row.isNullAt(0)) {
                                String redisKey = row.getString(0).toString();
                                redisKeys.add(redisKey);
                                LOG.debug("Found redis_key: {}", redisKey);
                            } else {
                                LOG.warn("Row has null redis_key at position 0");
                            }
                        }
                    } finally {
                        iterator.close();
                    }
                }
            } catch (Exception e) {
                LOG.error("Error scanning bucket {}", bucketId, e);
            }
            
            LOG.info("Bucket {} scanned: {} batches, {} rows", bucketId, batchCount, rowsInBucket);
        }
        
        LOG.info("Found {} unique redis_keys from index", redisKeys.size());
        return redisKeys;
    }

    /**
     * Get value by composite key (redis_key + sub_key).
     */
    public byte[] getByCompositeKey(String redisKey, String subKey) throws Exception {
        InternalRow row = lookupRow(redisKey, subKey);
        if (row == null || row.isNullAt(4)) {
            return null;
        }
        return row.getBytes(4);
    }
    
    private InternalRow lookupRow(String redisKey, String subKey) throws Exception {
        GenericRow keyRow = GenericRow.of(
                BinaryString.fromString(redisKey), 
                BinaryString.fromString(subKey));
        
        List<InternalRow> results = lookuper.get().lookup(keyRow).get().getRowList();        
        if (results == null || results.isEmpty()) {
            return null;
        }
        
        return results.get(0);
    }

    /**
     * Get full KeyValue by composite key (includes score and type).
     */
    public KeyValue getKeyValueByCompositeKey(String redisKey, String subKey) throws Exception {
        InternalRow row = lookupRow(redisKey, subKey);
        if (row == null) {
            return null;
        }
        
        String redisType = row.isNullAt(1) ? null : row.getString(1).toString();
        Double score = row.isNullAt(3) ? null : row.getDouble(3);
        byte[] value = row.isNullAt(4) ? null : row.getBytes(4);
        
        return new KeyValue(subKey, value, score, redisType);
    }

    /**
     * Set value by composite key with redis_type and optional score.
     */
    /**
     * Set value by composite key (redisKey + subKey).
     *
     * <p><b>CRITICAL - Secondary Index Consistency Risk:</b>
     * <p>This operation is NOT atomic due to Fluss storage limitations. It performs:
     * <ol>
     *   <li>WRITE main data to primary table
     *   <li>READ current subkeys from index
     *   <li>WRITE updated subkeys to index table
     * </ol>
     *
     * <p><b>Crash Recovery Risk:</b>
     * If a crash occurs between steps 1 and 3:
     * <ul>
     *   <li><b>Orphan data</b> - Main data exists but index doesn't reference it
     *   <li><b>Stale index</b> - Index references data that was deleted
     *   <li><b>Incomplete HGETALL</b> - HGETALL may miss newly added fields
     * </ul>
     *
     * <p><b>Impact on Redis Commands:</b>
     * <table>
     *   <tr><th>Command</th><th>Risk</th><th>Symptom</th></tr>
     *   <tr><td>HGETALL</td><td>High</td><td>May return incomplete field list</td></tr>
     *   <tr><td>HKEYS</td><td>High</td><td>May miss keys or show deleted keys</td></tr>
     *   <tr><td>HLEN</td><td>High</td><td>May return incorrect count</td></tr>
     *   <tr><td>HGET</td><td>Low</td><td>Still works (queries main table directly)</td></tr>
     * </table>
     *
     * <p><b>Why This Happens:</b>
     * Fluss KV storage does not support:
     * <ul>
     *   <li>Multi-table transactions
     *   <li>Two-phase commit (2PC)
     *   <li>Atomic cross-table operations
     * </ul>
     *
     * <p><b>Mitigation Strategies:</b>
     * <ol>
     *   <li><b>Write-ahead pattern</b> - Write index before data (current approach minimizes window)
     *   <li><b>Reconciliation job</b> - Periodic background job to fix inconsistencies
     *   <li><b>Accept eventual consistency</b> - Index catches up on next write
     *   <li><b>Client retry logic</b> - Retry failed operations on crash recovery
     * </ol>
     *
     * <p><b>Production Recommendations:</b>
     * <ul>
     *   <li>✅ Monitor index inconsistency metrics
     *   <li>✅ Run periodic reconciliation (e.g., nightly scan + repair)
     *   <li>✅ Use application-level retries for critical operations
     *   <li>⚠️ Understand that HGETALL results may be eventually consistent
     * </ul>
     *
     * @param redisKey Redis key
     * @param redisType Redis data type (e.g., "hash", "zset")
     * @param subKey Sub-key (field name for hash, member for zset)
     * @param value Value bytes
     * @param score Score (for sorted sets, null for hashes)
     * @throws Exception if write fails
     * @see <a href="https://github.com/gnuhpc/fluss-cape/blob/main/docs/REDIS-GUIDE.md#index-consistency">
     *     Redis Guide - Index Consistency</a>
     */
    public void setByCompositeKey(String redisKey, String redisType, String subKey, byte[] value, Double score) throws Exception {
        GenericRow row = GenericRow.of(
                BinaryString.fromString(redisKey), 
                redisType != null ? BinaryString.fromString(redisType) : null,
                BinaryString.fromString(subKey), 
                score, 
                value);
        upsertWriter.get().upsert(row).get();
        LOG.debug("Wrote main data for redis_key={}, sub_key={}", redisKey, subKey);
        
        Set<String> currentSubKeys = getSubKeysFromIndex(redisKey);
        currentSubKeys.add(subKey);
        String subKeysJson = String.join("\u0000", currentSubKeys);
        
        LOG.info("Writing to index: redis_key={}, sub_keys_count={}, sub_keys={}", 
                redisKey, currentSubKeys.size(), subKeysJson);
        GenericRow indexRow = GenericRow.of(
                BinaryString.fromString(redisKey),
                BinaryString.fromString(subKeysJson));
        subKeyIndexWriter.get().upsert(indexRow).get();
        LOG.info("Index write succeeded for redis_key={}", redisKey);
    }

    /**
     * Delete by composite key.
     */
    public void deleteByCompositeKey(String redisKey, String subKey) throws Exception {
        GenericRow keyRow = GenericRow.of(
                BinaryString.fromString(redisKey),
                null,
                BinaryString.fromString(subKey),
                null,
                null);
        upsertWriter.get().delete(keyRow).get();        
        Set<String> currentSubKeys = getSubKeysFromIndex(redisKey);
        currentSubKeys.remove(subKey);
        
        if (currentSubKeys.isEmpty()) {
            GenericRow indexKeyRow = GenericRow.of(
                    BinaryString.fromString(redisKey),
                    null);
            subKeyIndexWriter.get().delete(indexKeyRow).get();
        } else {
            String subKeysJson = String.join("\u0000", currentSubKeys);
            GenericRow indexRow = GenericRow.of(
                    BinaryString.fromString(redisKey),
                    BinaryString.fromString(subKeysJson));
            subKeyIndexWriter.get().upsert(indexRow).get();
        }

    }

    /**
     * Delete all sub-keys for a given redis_key.
     */
    public void deleteByKey(String redisKey) throws Exception {
        Set<String> subKeys = getSubKeysFromIndex(redisKey);
        
        for (String subKey : subKeys) {
            deleteByCompositeKey(redisKey, subKey);
        }
        
        GenericRow indexKeyRow = GenericRow.of(
                BinaryString.fromString(redisKey),
                null);
        subKeyIndexWriter.get().delete(indexKeyRow).get();        
    }

    /**
     * Get the redis_type for a given key.
     */
    public String getType(String redisKey) throws Exception {
        List<KeyValue> results = scanByKey(redisKey);
        if (results.isEmpty()) {
            return null;
        }
        return results.get(0).redisType;
    }

    /**
     * Batch get multiple keys (for MGET command).
     * Returns list of values in the same order as keys.
     * Returns null for non-existent keys.
     */
    public List<byte[]> multiGet(List<String> keys) throws Exception {
        List<byte[]> results = new ArrayList<>();
        
        for (String key : keys) {
            byte[] value = get(key.getBytes());
            results.add(value);
        }
        
        return results;
    }

    /**
     * Batch set multiple key-value pairs (for MSET command).
     */
    public void multiSet(List<String> keys, List<byte[]> values) throws Exception {
        if (keys.size() != values.size()) {
            throw new IllegalArgumentException("Keys and values size mismatch");
        }
        
        for (int i = 0; i < keys.size(); i++) {
            set(keys.get(i).getBytes(), values.get(i));
        }
    }

    /**
     * Check if multiple keys exist (for MSETNX command).
     * Returns list of booleans indicating existence.
     */
    public List<Boolean> multiExists(List<String> keys) throws Exception {
        List<Boolean> results = new ArrayList<>();
        
        for (String key : keys) {
            results.add(exists(key.getBytes()));
        }
        
        return results;
    }
}
