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
import org.gnuhpc.fluss.cape.redis.sharding.RedisSlotRouter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RedisShardedAdapter implements RedisStorageAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(RedisShardedAdapter.class);

    private final Connection connection;
    private final RedisSlotRouter router;
    private final String database;

    private final Map<TablePath, Table> tableCache;
    private final Map<TablePath, ThreadLocal<UpsertWriter>> writerCache;
    private final Map<TablePath, ThreadLocal<Lookuper>> lookuperCache;

    private final Table subKeyIndexTable;
    private final ThreadLocal<UpsertWriter> subKeyIndexWriter;
    private final ThreadLocal<Lookuper> subKeyIndexLookuper;

    private final Set<String> keyCache;

    public RedisShardedAdapter(Connection connection, String database, int numberOfShards)
            throws Exception {
        this.connection = connection;
        this.database = database;
        this.router = new RedisSlotRouter(database, numberOfShards, true);

        this.tableCache = new ConcurrentHashMap<>();
        this.writerCache = new ConcurrentHashMap<>();
        this.lookuperCache = new ConcurrentHashMap<>();

        TablePath subKeyIndexPath = RedisDynamicTableManager.getSubkeyIndexTablePath(database);
        this.subKeyIndexTable = connection.getTable(subKeyIndexPath);
        this.subKeyIndexWriter = ThreadLocal.withInitial(this::createSubKeyIndexWriter);
        this.subKeyIndexLookuper = ThreadLocal.withInitial(this::createSubKeyIndexLookuper);

        this.keyCache = Collections.synchronizedSet(new HashSet<>());

        LOG.info(
                "Initialized RedisShardedAdapter: database={}, shards={}, hashTag={}",
                database,
                numberOfShards,
                router.isHashTagEnabled());
    }

    private Table getTable(String redisKey) throws Exception {
        TablePath tablePath = router.route(redisKey);
        return tableCache.computeIfAbsent(
                tablePath,
                path -> {
                    try {
                        return connection.getTable(path);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to get table: " + path, e);
                    }
                });
    }

    private UpsertWriter getWriter(String redisKey) throws Exception {
        TablePath tablePath = router.route(redisKey);
        return writerCache.computeIfAbsent(
                tablePath,
                path -> ThreadLocal.withInitial(() -> {
                    try {
                        Table table = getTable(redisKey);
                        return table.newUpsert().createWriter();
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to create writer for: " + path, e);
                    }
                })).get();
    }

    private Lookuper getLookuper(String redisKey) throws Exception {
        TablePath tablePath = router.route(redisKey);
        return lookuperCache.computeIfAbsent(
                tablePath,
                path -> ThreadLocal.withInitial(() -> {
                    try {
                        Table table = getTable(redisKey);
                        return table.newLookup().createLookuper();
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to create lookuper for: " + path, e);
                    }
                })).get();
    }

    public byte[] get(byte[] key) throws Exception {
        return getByCompositeKey(new String(key), "");
    }

    public void set(byte[] key, byte[] value) throws Exception {
        String redisKey = new String(key);
        setByCompositeKey(redisKey, "string", "", value, null);
        keyCache.add(redisKey);
    }

    public void delete(byte[] key) throws Exception {
        String redisKey = new String(key);
        deleteByKey(redisKey);

        GenericRow indexKeyRow = GenericRow.of(BinaryString.fromString(redisKey), null);
        subKeyIndexWriter.get().delete(indexKeyRow).get();

        keyCache.remove(redisKey);
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
        throw new UnsupportedOperationException("INCR is not supported due to non-atomic semantics");
    }

    public long incrBy(byte[] key, long increment) throws Exception {
        throw new UnsupportedOperationException("INCRBY is not supported due to non-atomic semantics");
    }

    public void close() {
        for (ThreadLocal<UpsertWriter> writer : writerCache.values()) {
            UpsertWriter localWriter = writer.get();
            if (localWriter != null) {
                try {
                    localWriter.flush();
                } catch (RuntimeException e) {
                    LOG.warn("Failed to flush shard writer", e);
                }
            }
        }
        UpsertWriter indexWriter = subKeyIndexWriter.get();
        if (indexWriter != null) {
            try {
                indexWriter.flush();
            } catch (RuntimeException e) {
                LOG.warn("Failed to flush subkey index writer", e);
            }
        }
    }

    private UpsertWriter createSubKeyIndexWriter() {
        try {
            return subKeyIndexTable.newUpsert().createWriter();
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create subkey index writer", e);
        }
    }

    private Lookuper createSubKeyIndexLookuper() {
        try {
            return subKeyIndexTable.newLookup().createLookuper();
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create subkey index lookuper", e);
        }
    }

    public List<RedisSingleTableAdapter.KeyValue> scanByKey(String redisKey) throws Exception {
        List<RedisSingleTableAdapter.KeyValue> results = new ArrayList<>();

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

                results.add(new RedisSingleTableAdapter.KeyValue(subKey, value, score, redisType));
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

    public List<RedisSingleTableAdapter.KeyValue> scanByPrefix(String redisKey, String subKeyPrefix) throws Exception {
        List<RedisSingleTableAdapter.KeyValue> allResults = scanByKey(redisKey);
        List<RedisSingleTableAdapter.KeyValue> filtered = new ArrayList<>();

        for (RedisSingleTableAdapter.KeyValue kv : allResults) {
            if (kv.subKey.startsWith(subKeyPrefix)) {
                filtered.add(kv);
            }
        }

        return filtered;
    }

    public long countByKey(String redisKey) throws Exception {
        return scanByKey(redisKey).size();
    }

    public Set<String> getAllKeys() {
        LOG.info("getAllKeys() called - returning {} keys from cache", keyCache.size());
        if (keyCache.isEmpty()) {
            try {
                return getAllRedisKeysFromIndex();
            } catch (Exception e) {
                LOG.warn("Failed to fetch keys from index, returning cache", e);
            }
        }
        return new HashSet<>(keyCache);
    }

    public List<RedisSingleTableAdapter.FullKeyValue> scanAll() throws Exception {
        LOG.info("scanAll() called - using in-memory key cache with {} keys", keyCache.size());
        List<RedisSingleTableAdapter.FullKeyValue> results = new ArrayList<>();

        Set<String> allRedisKeys = new HashSet<>(keyCache);
        if (allRedisKeys.isEmpty()) {
            allRedisKeys = getAllRedisKeysFromIndex();
        }

        for (String redisKey : allRedisKeys) {
            Set<String> subKeys = getSubKeysFromIndex(redisKey);
            for (String subKey : subKeys) {
                InternalRow row = lookupRow(redisKey, subKey);
                if (row != null) {
                    String redisType = row.isNullAt(1) ? null : row.getString(1).toString();
                    Double score = row.isNullAt(3) ? null : row.getDouble(3);
                    byte[] value = row.isNullAt(4) ? null : row.getBytes(4);

                    results.add(new RedisSingleTableAdapter.FullKeyValue(redisKey, subKey, value, score, redisType));
                }
            }
        }

        return results;
    }

    public byte[] getByCompositeKey(String redisKey, String subKey) throws Exception {
        InternalRow row = lookupRow(redisKey, subKey);
        if (row == null || row.isNullAt(4)) {
            return null;
        }
        return row.getBytes(4);
    }

    private InternalRow lookupRow(String redisKey, String subKey) throws Exception {
        Lookuper lookuper = getLookuper(redisKey);
        GenericRow keyRow =
                GenericRow.of(BinaryString.fromString(redisKey), BinaryString.fromString(subKey));

        List<InternalRow> results = lookuper.lookup(keyRow).get().getRowList();

        if (results == null || results.isEmpty()) {
            return null;
        }

        return results.get(0);
    }

    public RedisSingleTableAdapter.KeyValue getKeyValueByCompositeKey(String redisKey, String subKey) throws Exception {
        InternalRow row = lookupRow(redisKey, subKey);
        if (row == null) {
            return null;
        }

        String redisType = row.isNullAt(1) ? null : row.getString(1).toString();
        Double score = row.isNullAt(3) ? null : row.getDouble(3);
        byte[] value = row.isNullAt(4) ? null : row.getBytes(4);

        return new RedisSingleTableAdapter.KeyValue(subKey, value, score, redisType);
    }

    public void setByCompositeKey(
            String redisKey, String redisType, String subKey, byte[] value, Double score)
            throws Exception {
        UpsertWriter writer = getWriter(redisKey);

        GenericRow row =
                GenericRow.of(
                        BinaryString.fromString(redisKey),
                        redisType != null ? BinaryString.fromString(redisType) : null,
                        BinaryString.fromString(subKey),
                        score,
                        value);
        writer.upsert(row).get();
        LOG.debug("Wrote main data for redis_key={}, sub_key={}", redisKey, subKey);

        Set<String> currentSubKeys = getSubKeysFromIndex(redisKey);
        currentSubKeys.add(subKey);
        String subKeysJson = String.join("\u0000", currentSubKeys);

        LOG.debug(
                "Writing to index: redis_key={}, sub_keys_count={}, sub_keys={}",
                redisKey,
                currentSubKeys.size(),
                subKeysJson);
        GenericRow indexRow =
                GenericRow.of(
                        BinaryString.fromString(redisKey), BinaryString.fromString(subKeysJson));
        subKeyIndexWriter.get().upsert(indexRow).get();
        LOG.debug("Index write succeeded for redis_key={}", redisKey);

        keyCache.add(redisKey);
    }

    public void deleteByCompositeKey(String redisKey, String subKey) throws Exception {
        UpsertWriter writer = getWriter(redisKey);

        GenericRow keyRow =
                GenericRow.of(
                        BinaryString.fromString(redisKey),
                        null,
                        BinaryString.fromString(subKey),
                        null,
                        null);
        writer.delete(keyRow).get();

        Set<String> currentSubKeys = getSubKeysFromIndex(redisKey);
        currentSubKeys.remove(subKey);

        if (currentSubKeys.isEmpty()) {
            GenericRow indexKeyRow = GenericRow.of(BinaryString.fromString(redisKey), null);
            subKeyIndexWriter.get().delete(indexKeyRow).get();
        } else {
            String subKeysJson = String.join("\u0000", currentSubKeys);
            GenericRow indexRow =
                    GenericRow.of(
                            BinaryString.fromString(redisKey),
                            BinaryString.fromString(subKeysJson));
            subKeyIndexWriter.get().upsert(indexRow).get();
        }
    }

    public void deleteByKey(String redisKey) throws Exception {
        Set<String> subKeys = getSubKeysFromIndex(redisKey);

        for (String subKey : subKeys) {
            deleteByCompositeKey(redisKey, subKey);
        }

        GenericRow indexKeyRow = GenericRow.of(BinaryString.fromString(redisKey), null);
        subKeyIndexWriter.get().delete(indexKeyRow).get();
        keyCache.remove(redisKey);
    }

    public String getType(String redisKey) throws Exception {
        List<RedisSingleTableAdapter.KeyValue> results = scanByKey(redisKey);
        if (results.isEmpty()) {
            return null;
        }
        return results.get(0).redisType;
    }

    public List<byte[]> multiGet(List<String> keys) throws Exception {
        List<byte[]> results = new ArrayList<>();

        for (String key : keys) {
            byte[] value = get(key.getBytes());
            results.add(value);
        }

        return results;
    }

    public void multiSet(List<String> keys, List<byte[]> values) throws Exception {
        if (keys.size() != values.size()) {
            throw new IllegalArgumentException("Keys and values size mismatch");
        }

        for (int i = 0; i < keys.size(); i++) {
            set(keys.get(i).getBytes(), values.get(i));
        }
    }

    public List<Boolean> multiExists(List<String> keys) throws Exception {
        List<Boolean> results = new ArrayList<>();

        for (String key : keys) {
            results.add(exists(key.getBytes()));
        }

        return results;
    }

    public RedisSlotRouter getRouter() {
        return router;
    }

    public int getNumberOfShards() {
        return router.getNumberOfShards();
    }

    private Set<String> getAllRedisKeysFromIndex() throws Exception {
        Set<String> redisKeys = new HashSet<>();

        Admin admin = connection.getAdmin();
        String databaseName = database;
        TablePath subKeyIndexPath = RedisDynamicTableManager.getSubkeyIndexTablePath(databaseName);
        KvSnapshots kvSnapshots = admin.getLatestKvSnapshots(subKeyIndexPath).get();

        LOG.info(
                "Scanning redis_subkey_index: available snapshots={}",
                kvSnapshots.getBucketIds().size());

        for (int bucketId : kvSnapshots.getBucketIds()) {
            if (!kvSnapshots.getSnapshotId(bucketId).isPresent()) {
                LOG.debug("Bucket {} has no snapshot yet, skipping", bucketId);
                continue;
            }

            long snapshotId = kvSnapshots.getSnapshotId(bucketId).getAsLong();
            TableInfo tableInfo = subKeyIndexTable.getTableInfo();
            TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), bucketId);

            try (BatchScanner scanner = subKeyIndexTable.newScan().createBatchScanner(tableBucket, snapshotId)) {
                CloseableIterator<InternalRow> iterator;
                while ((iterator = scanner.pollBatch(Duration.ofMillis(100))) != null) {
                    try {
                        while (iterator.hasNext()) {
                            InternalRow row = iterator.next();
                            if (!row.isNullAt(0)) {
                                redisKeys.add(row.getString(0).toString());
                            }
                        }
                    } finally {
                        iterator.close();
                    }
                }
            }
        }

        return redisKeys;
    }
}
