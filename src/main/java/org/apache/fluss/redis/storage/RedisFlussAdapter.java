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

package org.apache.fluss.redis.storage;

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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class RedisFlussAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(RedisFlussAdapter.class);

    private final Connection connection;
    private final TablePath tablePath;
    private final Table table;
    private final UpsertWriter upsertWriter;
    private final Lookuper lookuper;

    public RedisFlussAdapter(Connection connection, TablePath tablePath) throws Exception {
        this.connection = connection;
        this.tablePath = tablePath;
        this.table = connection.getTable(tablePath);
        this.upsertWriter = table.newUpsert().createWriter();
        this.lookuper = table.newLookup().createLookuper();
    }

    public byte[] get(byte[] key) throws Exception {
        return getByCompositeKey(new String(key), "");
    }

    public void set(byte[] key, byte[] value) throws Exception {
        setByCompositeKey(new String(key), "string", "", value, null);
    }

    public void delete(byte[] key) throws Exception {
        deleteByCompositeKey(new String(key), "");
    }

    public boolean exists(byte[] key) throws Exception {
        return get(key) != null;
    }

    public boolean keyExists(String key) throws Exception {
        return exists(key.getBytes());
    }

    public List<String> keys(String pattern) throws Exception {
        return new ArrayList<>();
    }

    public long incr(byte[] key) throws Exception {
        byte[] current = get(key);
        long value = 0;
        if (current != null) {
            value = Long.parseLong(new String(current));
        }
        value++;
        set(key, String.valueOf(value).getBytes());
        return value;
    }

    public long incrBy(byte[] key, long increment) throws Exception {
        byte[] current = get(key);
        long value = 0;
        if (current != null) {
            value = Long.parseLong(new String(current));
        }
        value += increment;
        set(key, String.valueOf(value).getBytes());
        return value;
    }

    public void close() {
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

    /**
     * Scan all sub-keys for a given redis_key.
     * Returns list of (sub_key, value, score) tuples.
     */
    public List<KeyValue> scanByKey(String redisKey) throws Exception {
        List<KeyValue> results = new ArrayList<>();
        
        TableInfo tableInfo = table.getTableInfo();
        long tableId = tableInfo.getTableId();
        
        Admin admin = connection.getAdmin();
        KvSnapshots kvSnapshots = admin.getLatestKvSnapshots(tablePath).get();
        
        for (int bucketId : kvSnapshots.getBucketIds()) {
            if (!kvSnapshots.getSnapshotId(bucketId).isPresent()) {
                LOG.warn("Bucket {} has no snapshot yet, skipping", bucketId);
                continue;
            }
            
            long snapshotId = kvSnapshots.getSnapshotId(bucketId).getAsLong();
            TableBucket tableBucket = new TableBucket(tableId, bucketId);
            
            BatchScanner scanner = table.newScan().createBatchScanner(tableBucket, snapshotId);
            
            try {
                while (true) {
                    CloseableIterator<InternalRow> iterator = 
                        scanner.pollBatch(Duration.ofMillis(100));
                    if (iterator == null) {
                        break;
                    }
                    
                    try {
                        while (iterator.hasNext()) {
                            InternalRow row = iterator.next();
                            
                            // Column 0: redis_key
                            String rowRedisKey = row.getString(0).toString();
                            
                            // Only include rows matching the requested redis_key
                            if (!rowRedisKey.equals(redisKey)) {
                                continue;
                            }
                            
                            // Column 1: redis_type
                            String redisType = row.isNullAt(1) ? null : row.getString(1).toString();
                            
                            // Column 2: sub_key
                            String subKey = row.isNullAt(2) ? "" : row.getString(2).toString();
                            
                            // Column 3: score
                            Double score = row.isNullAt(3) ? null : row.getDouble(3);
                            
                            // Column 4: value
                            byte[] value = row.isNullAt(4) ? null : row.getBytes(4);
                            
                            results.add(new KeyValue(subKey, value, score, redisType));
                        }
                    } finally {
                        iterator.close();
                    }
                }
            } finally {
                scanner.close();
            }
        }
        
        return results;
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
     * Get value by composite key (redis_key + sub_key).
     */
    public byte[] getByCompositeKey(String redisKey, String subKey) throws Exception {
        GenericRow keyRow = GenericRow.of(
                BinaryString.fromString(redisKey), 
                BinaryString.fromString(subKey));
        
        List<InternalRow> results = lookuper.lookup(keyRow).get().getRowList();
        
        if (results == null || results.isEmpty()) {
            return null;
        }
        
        InternalRow row = results.get(0);
        if (row.isNullAt(4)) {  // Column 4 is "value"
            return null;
        }
        
        return row.getBytes(4);
    }

    /**
     * Get full KeyValue by composite key (includes score and type).
     */
    public KeyValue getKeyValueByCompositeKey(String redisKey, String subKey) throws Exception {
        GenericRow keyRow = GenericRow.of(
                BinaryString.fromString(redisKey), 
                BinaryString.fromString(subKey));
        
        List<InternalRow> results = lookuper.lookup(keyRow).get().getRowList();
        
        if (results == null || results.isEmpty()) {
            return null;
        }
        
        InternalRow row = results.get(0);
        
        // Column 1: redis_type
        String redisType = row.isNullAt(1) ? null : row.getString(1).toString();
        
        // Column 3: score
        Double score = row.isNullAt(3) ? null : row.getDouble(3);
        
        // Column 4: value
        byte[] value = row.isNullAt(4) ? null : row.getBytes(4);
        
        return new KeyValue(subKey, value, score, redisType);
    }

    /**
     * Set value by composite key with redis_type and optional score.
     */
    public void setByCompositeKey(String redisKey, String redisType, String subKey, byte[] value, Double score) throws Exception {
        GenericRow row = GenericRow.of(
                BinaryString.fromString(redisKey), 
                redisType != null ? BinaryString.fromString(redisType) : null,
                BinaryString.fromString(subKey), 
                score, 
                value);
        upsertWriter.upsert(row).get();
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
        upsertWriter.delete(keyRow).get();
    }

    /**
     * Delete all sub-keys for a given redis_key.
     */
    public void deleteByKey(String redisKey) throws Exception {
        List<KeyValue> allKeys = scanByKey(redisKey);
        for (KeyValue kv : allKeys) {
            deleteByCompositeKey(redisKey, kv.subKey);
        }
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
