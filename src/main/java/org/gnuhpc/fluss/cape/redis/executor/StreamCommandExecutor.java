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

package org.gnuhpc.fluss.cape.redis.executor;
import java.nio.charset.StandardCharsets;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.gnuhpc.fluss.cape.redis.protocol.RedisCommand;
import org.gnuhpc.fluss.cape.redis.protocol.RedisResponse;
import org.gnuhpc.fluss.cape.redis.storage.RedisSingleTableAdapter;
import org.gnuhpc.fluss.cape.redis.storage.RedisStorageAdapter;
import org.gnuhpc.fluss.cape.redis.util.RedisErrorSanitizer;
import org.gnuhpc.fluss.cape.redis.util.SimpleLRUCache;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class StreamCommandExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(StreamCommandExecutor.class);

    private final RedisStorageAdapter adapter;
    private final Connection flussConnection;
    private final String streamDataTableName;
    private final String tombstoneTableName;
    private final SimpleLRUCache<String, StreamMetadata> streamMetadataCache;
    private final SimpleLRUCache<String, AtomicLong> streamSequenceCounters;
    private final SimpleLRUCache<String, Set<String>> deletedEntriesCache;
    private final Map<String, Map<String, ConsumerGroupMetadata>> consumerGroups;
    private final String consumerGroupTableName;
    private final String pendingEntriesTableName;

    public StreamCommandExecutor(RedisStorageAdapter adapter, String flussBootstrapServers, String streamDataTableName) {
        this.adapter = adapter;
        this.streamDataTableName = streamDataTableName;
        this.tombstoneTableName = "default.redis_stream_tombstones";
        this.consumerGroupTableName = "default.redis_consumer_groups";
        this.pendingEntriesTableName = "default.redis_pending_entries";
        this.streamMetadataCache = new SimpleLRUCache<>(10000);
        this.streamSequenceCounters = new SimpleLRUCache<>(5000);
        this.deletedEntriesCache = new SimpleLRUCache<>(5000);
        this.consumerGroups = new ConcurrentHashMap<>();

        try {
            Configuration config = new Configuration();
            config.setString("bootstrap.servers", flussBootstrapServers);
            this.flussConnection = ConnectionFactory.createConnection(config);
            
            ensureTombstoneTableExists();
            loadTombstonesCache();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Fluss connection for Stream executor", e);
        }
    }

    @Override
    public RedisMessage execute(RedisCommand command) {
        String cmd = command.getCommand();

        try {
            switch (cmd) {
                case "XADD":
                    return executeXAdd(command);
                case "XLEN":
                    return executeXLen(command);
                case "XRANGE":
                    return executeXRange(command);
                case "XREVRANGE":
                    return executeXRevRange(command);
                case "XREAD":
                    return executeXRead(command);
                case "XDEL":
                    return executeXDel(command);
                case "XTRIM":
                    return executeXTrim(command);
                case "XGROUP":
                    return executeXGroup(command);
                case "XREADGROUP":
                    return executeXReadGroup(command);
                case "XACK":
                    return executeXAck(command);
                case "XPENDING":
                    return executeXPending(command);
                default:
                    return RedisResponse.error("ERR unknown stream command '" + cmd + "'");
            }
        } catch (Exception e) {
            LOG.error("Error executing stream command: {}", cmd, e);
            return RedisErrorSanitizer.sanitizeError(e, cmd);
        }
    }

    private RedisMessage executeXAdd(RedisCommand command) {
        List<byte[]> args = command.getArgs();
        if (args.size() < 4 || args.size() % 2 != 0) {
            return RedisResponse.error("ERR wrong number of arguments for 'xadd' command");
        }

        String streamKey = new String(args.get(0));
        String entryIdArg = new String(args.get(1));

        String entryId;
        if ("*".equals(entryIdArg)) {
            entryId = generateStreamId(streamKey);
        } else {
            entryId = entryIdArg;
        }

        try {
            Map<String, byte[]> fieldsMap = new HashMap<>();
            for (int i = 2; i < args.size(); i += 2) {
                String field = new String(args.get(i));
                byte[] value = args.get(i + 1);
                fieldsMap.put(field, value);
            }

            byte[] fieldsBytes = serializeFields(fieldsMap);
            
            adapter.setByCompositeKey(streamKey, "stream", entryId, fieldsBytes, null);

            updateStreamMetadata(streamKey, entryId);

            return RedisResponse.bulkString(entryId);
        } catch (Exception e) {
            LOG.error("Failed to execute XADD", e);
            return RedisErrorSanitizer.sanitizeError(e, "XADD");
        }
    }

    private RedisMessage executeXLen(RedisCommand command) {
        List<byte[]> args = command.getArgs();
        if (args.size() != 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'xlen' command");
        }

        String streamKey = new String(args.get(0));
        StreamMetadata metadata = streamMetadataCache.get(streamKey);
        
        return RedisResponse.integer(metadata != null ? metadata.length : 0);
    }

    private RedisMessage executeXRange(RedisCommand command) {
        List<byte[]> args = command.getArgs();
        if (args.size() < 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'xrange' command");
        }

        String streamKey = new String(args.get(0));
        String start = new String(args.get(1));
        String end = new String(args.get(2));
        long count = -1;

        if (args.size() >= 5 && "COUNT".equalsIgnoreCase(new String(args.get(3)))) {
            try {
                count = Long.parseLong(new String(args.get(4)));
            } catch (NumberFormatException e) {
                return RedisResponse.error("ERR value is not an integer or out of range");
            }
        }

        return executeRangeQuery(streamKey, start, end, (int) count, false);
    }

    private RedisMessage executeXRevRange(RedisCommand command) {
        List<byte[]> args = command.getArgs();
        if (args.size() < 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'xrevrange' command");
        }

        String streamKey = new String(args.get(0));
        String end = new String(args.get(1));
        String start = new String(args.get(2));
        long count = -1;

        if (args.size() >= 5 && "COUNT".equalsIgnoreCase(new String(args.get(3)))) {
            try {
                count = Long.parseLong(new String(args.get(4)));
            } catch (NumberFormatException e) {
                return RedisResponse.error("ERR value is not an integer or out of range");
            }
        }

        return executeRangeQuery(streamKey, start, end, (int) count, true);
    }

    private RedisMessage executeXRead(RedisCommand command) {
        List<byte[]> args = command.getArgs();
        if (args.size() < 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'xread' command");
        }

        long count = -1;
        long blockMs = 0;
        int streamsIndex = 0;

        for (int i = 0; i < args.size(); i++) {
            String arg = new String(args.get(i)).toUpperCase();
            if ("COUNT".equals(arg) && i + 1 < args.size()) {
                try {
                    count = Long.parseLong(new String(args.get(i + 1)));
                    i++;
                } catch (NumberFormatException e) {
                    return RedisResponse.error("ERR value is not an integer or out of range");
                }
            } else if ("BLOCK".equals(arg) && i + 1 < args.size()) {
                try {
                    blockMs = Long.parseLong(new String(args.get(i + 1)));
                    i++;
                } catch (NumberFormatException e) {
                    return RedisResponse.error("ERR value is not an integer or out of range");
                }
            } else if ("STREAMS".equals(arg)) {
                streamsIndex = i + 1;
                break;
            }
        }

        if (streamsIndex == 0 || (args.size() - streamsIndex) % 2 != 0) {
            return RedisResponse.error("ERR wrong number of arguments for 'xread' command");
        }

        int streamCount = (args.size() - streamsIndex) / 2;
        List<String> streamKeys = new ArrayList<>();
        List<String> startIds = new ArrayList<>();

        for (int i = 0; i < streamCount; i++) {
            streamKeys.add(new String(args.get(streamsIndex + i)));
            startIds.add(new String(args.get(streamsIndex + streamCount + i)));
        }

        return executeMultiStreamRead(streamKeys, startIds, (int) count, blockMs);
    }

    private RedisMessage executeXDel(RedisCommand command) {
        List<byte[]> args = command.getArgs();
        if (args.size() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'xdel' command");
        }

        String streamKey = new String(args.get(0));
        List<String> entryIds = new ArrayList<>();
        for (int i = 1; i < args.size(); i++) {
            entryIds.add(new String(args.get(i)));
        }

        try {
            Set<String> deleted = deletedEntriesCache.computeIfAbsent(
                streamKey, k -> ConcurrentHashMap.newKeySet()
            );
            deleted.addAll(entryIds);
            
            CompletableFuture.runAsync(() -> {
                try {
                    persistTombstones(streamKey, entryIds);
                } catch (Exception e) {
                    LOG.error("Failed to persist tombstones for stream: {}", streamKey, e);
                }
            });
            
            return RedisResponse.integer(entryIds.size());
        } catch (Exception e) {
            LOG.error("Failed to execute XDEL", e);
            return RedisErrorSanitizer.sanitizeError(e, "XDEL");
        }
    }

    private RedisMessage executeXTrim(RedisCommand command) {
        List<byte[]> args = command.getArgs();
        if (args.size() < 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'xtrim' command");
        }

        String streamKey = new String(args.get(0));
        String strategy = new String(args.get(1)).toUpperCase();
        
        if (!"MAXLEN".equals(strategy) && !"MINID".equals(strategy)) {
            return RedisResponse.error("ERR unsupported XTRIM strategy: " + strategy);
        }

        int argIndex = 2;
        boolean approximate = false;
        
        if (argIndex < args.size() && "~".equals(new String(args.get(argIndex)))) {
            approximate = true;
            argIndex++;
        }
        
        if (argIndex >= args.size()) {
            return RedisResponse.error("ERR syntax error");
        }

        String thresholdStr = new String(args.get(argIndex));
        
        try {
            long deletedCount = 0;
            
            if ("MAXLEN".equals(strategy)) {
                long maxLen = Long.parseLong(thresholdStr);
                deletedCount = trimByMaxLen(streamKey, maxLen, approximate);
            } else if ("MINID".equals(strategy)) {
                deletedCount = trimByMinId(streamKey, thresholdStr, approximate);
            }
            
            return RedisResponse.integer(deletedCount);
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not an integer or out of range");
        } catch (Exception e) {
            LOG.error("Failed to execute XTRIM", e);
            return RedisErrorSanitizer.sanitizeError(e, "XTRIM");
        }
    }

    private long trimByMaxLen(String streamKey, long maxLen, boolean approximate) throws Exception {
        StreamMetadata metadata = streamMetadataCache.get(streamKey);
        if (metadata == null || metadata.length <= maxLen) {
            return 0;
        }
        
        long toDelete = metadata.length - maxLen;
        if (approximate && toDelete > 0) {
            toDelete = ((toDelete + 99) / 100) * 100;
        }
        
        List<String> entriesToDelete = scanAndCollectOldestEntries(streamKey, toDelete);
        
        if (entriesToDelete.isEmpty()) {
            return 0;
        }
        
        markEntriesAsDeleted(streamKey, entriesToDelete);
        updateStreamLengthAfterTrim(streamKey, entriesToDelete.size());
        
        return entriesToDelete.size();
    }

    private long trimByMinId(String streamKey, String minId, boolean approximate) throws Exception {
        List<String> entriesToDelete = scanAndCollectEntriesBeforeId(streamKey, minId, approximate);
        
        if (entriesToDelete.isEmpty()) {
            return 0;
        }
        
        markEntriesAsDeleted(streamKey, entriesToDelete);
        updateStreamLengthAfterTrim(streamKey, entriesToDelete.size());
        
        return entriesToDelete.size();
    }

    private List<String> scanAndCollectOldestEntries(String streamKey, long count) throws Exception {
        List<String> entryIds = new ArrayList<>();
        
        String[] parts = streamDataTableName.split("\\.");
        String database = parts.length > 1 ? parts[0] : "default";
        String tableName = parts.length > 1 ? parts[1] : parts[0];
        
        Table table = flussConnection.getTable(TablePath.of(database, tableName));
        LogScanner scanner = table.newScan().createLogScanner();
        
        int numBuckets = table.getTableInfo().getNumBuckets();
        for (int bucket = 0; bucket < numBuckets; bucket++) {
            scanner.subscribeFromBeginning(bucket);
        }
        
        ScanRecords records = scanner.poll(Duration.ofSeconds(1));
        
        for (ScanRecord record : records) {
            if (entryIds.size() >= count) break;
            
            InternalRow row = record.getRow();
            String scannedStreamKey = row.getString(0).toString();
            if (!streamKey.equals(scannedStreamKey)) continue;
            
            String entryId = row.getString(1).toString();
            if (!isEntryDeleted(streamKey, entryId)) {
                entryIds.add(entryId);
            }
        }
        
        scanner.close();
        
        entryIds.sort(this::compareStreamIds);
        
        return entryIds.size() > count ? entryIds.subList(0, (int) count) : entryIds;
    }

    private List<String> scanAndCollectEntriesBeforeId(String streamKey, String minId, boolean approximate) throws Exception {
        List<String> entryIds = new ArrayList<>();
        
        String[] parts = streamDataTableName.split("\\.");
        String database = parts.length > 1 ? parts[0] : "default";
        String tableName = parts.length > 1 ? parts[1] : parts[0];
        
        Table table = flussConnection.getTable(TablePath.of(database, tableName));
        LogScanner scanner = table.newScan().createLogScanner();
        
        int numBuckets = table.getTableInfo().getNumBuckets();
        for (int bucket = 0; bucket < numBuckets; bucket++) {
            scanner.subscribeFromBeginning(bucket);
        }
        
        ScanRecords records = scanner.poll(Duration.ofSeconds(1));
        
        for (ScanRecord record : records) {
            InternalRow row = record.getRow();
            String scannedStreamKey = row.getString(0).toString();
            if (!streamKey.equals(scannedStreamKey)) continue;
            
            String entryId = row.getString(1).toString();
            if (compareStreamIds(entryId, minId) < 0 && !isEntryDeleted(streamKey, entryId)) {
                entryIds.add(entryId);
            }
        }
        
        scanner.close();
        
        return entryIds;
    }

    private void markEntriesAsDeleted(String streamKey, List<String> entryIds) {
        Set<String> deleted = deletedEntriesCache.computeIfAbsent(
            streamKey, k -> ConcurrentHashMap.newKeySet()
        );
        deleted.addAll(entryIds);
        
        CompletableFuture.runAsync(() -> {
            try {
                persistTombstones(streamKey, entryIds);
            } catch (Exception e) {
                LOG.error("Failed to persist tombstones during XTRIM", e);
            }
        });
    }

    private void updateStreamLengthAfterTrim(String streamKey, long deletedCount) {
        streamMetadataCache.computeIfPresent(streamKey, (k, metadata) ->
            new StreamMetadata(metadata.lastEntryId, Math.max(0, metadata.length - deletedCount))
        );
    }

    private int compareStreamIds(String id1, String id2) {
        String[] parts1 = id1.split("-");
        String[] parts2 = id2.split("-");
        
        long ts1 = Long.parseLong(parts1[0]);
        long ts2 = Long.parseLong(parts2[0]);
        
        if (ts1 != ts2) {
            return Long.compare(ts1, ts2);
        }
        
        long seq1 = parts1.length > 1 ? Long.parseLong(parts1[1]) : 0;
        long seq2 = parts2.length > 1 ? Long.parseLong(parts2[1]) : 0;
        
        return Long.compare(seq1, seq2);
    }
    
    private void ensureTombstoneTableExists() {
        try {
            String[] parts = tombstoneTableName.split("\\.");
            String database = parts.length > 1 ? parts[0] : "default";
            String tableName = parts.length > 1 ? parts[1] : parts[0];
            TablePath tablePath = TablePath.of(database, tableName);
            
            if (flussConnection.getAdmin().tableExists(tablePath).get()) {
                LOG.info("Tombstone table already exists: {}", tombstoneTableName);
                return;
            }
            
            LOG.info("Creating tombstone table: {}", tombstoneTableName);
            
            org.apache.fluss.metadata.Schema schema = org.apache.fluss.metadata.Schema.newBuilder()
                    .column("stream_key", DataTypes.STRING())
                    .column("entry_id", DataTypes.STRING())
                    .column("deleted", DataTypes.BOOLEAN())
                    .column("delete_timestamp", DataTypes.BIGINT())
                    .primaryKey("stream_key", "entry_id")
                    .build();

            org.apache.fluss.metadata.TableDescriptor descriptor = 
                org.apache.fluss.metadata.TableDescriptor.builder()
                    .schema(schema)
                    .distributedBy(10, "stream_key")
                    .build();

            flussConnection.getAdmin().createTable(tablePath, descriptor, false).get();
            LOG.info("Tombstone table created successfully");
        } catch (Exception e) {
            LOG.warn("Failed to create tombstone table (may already exist)", e);
        }
    }
    
    private void loadTombstonesCache() {
        LOG.info("Tombstone cache will be loaded lazily on first XDEL/XRANGE operation");
    }
    
    private void persistTombstones(String streamKey, List<String> entryIds) throws Exception {
        String[] parts = tombstoneTableName.split("\\.");
        String database = parts.length > 1 ? parts[0] : "default";
        String tableName = parts.length > 1 ? parts[1] : parts[0];
        
        Table tombstoneTable = flussConnection.getTable(TablePath.of(database, tableName));
        org.apache.fluss.client.table.writer.UpsertWriter writer = tombstoneTable.newUpsert().createWriter();
        
        RowType rowType = RowType.builder()
                .field("stream_key", DataTypes.STRING())
                .field("entry_id", DataTypes.STRING())
                .field("deleted", DataTypes.BOOLEAN())
                .field("delete_timestamp", DataTypes.BIGINT())
                .build();
        
        RowEncoder encoder = RowEncoder.create(KvFormat.COMPACTED, rowType);
        
        for (String entryId : entryIds) {
            encoder.startNewRow();
            encoder.encodeField(0, BinaryString.fromString(streamKey));
            encoder.encodeField(1, BinaryString.fromString(entryId));
            encoder.encodeField(2, true);
            encoder.encodeField(3, System.currentTimeMillis());
            
            InternalRow row = encoder.finishRow();
            writer.upsert(row);
        }
        
        writer.flush();
    }
    
    private Set<String> loadDeletedEntriesForStream(String streamKey) {
        return ConcurrentHashMap.newKeySet();
    }
    
    private boolean isEntryDeleted(String streamKey, String entryId) {
        Set<String> cached = deletedEntriesCache.get(streamKey);
        if (cached != null && cached.contains(entryId)) {
            return true;
        }
        
        try {
            String[] parts = tombstoneTableName.split("\\.");
            String database = parts.length > 1 ? parts[0] : "default";
            String tableName = parts.length > 1 ? parts[1] : parts[0];
            
            Table tombstoneTable = flussConnection.getTable(TablePath.of(database, tableName));
            Lookuper lookuper = tombstoneTable.newLookup().createLookuper();
            
            GenericRow keyRow = GenericRow.of(
                BinaryString.fromString(streamKey),
                BinaryString.fromString(entryId)
            );
            
            List<InternalRow> results = lookuper.lookup(keyRow).get().getRowList();
            if (results != null && !results.isEmpty()) {
                InternalRow result = results.get(0);
                boolean deleted = result.getBoolean(2);
                if (deleted) {
                    deletedEntriesCache.computeIfAbsent(streamKey, k -> ConcurrentHashMap.newKeySet()).add(entryId);
                    return true;
                }
            }
        } catch (Exception e) {
            LOG.debug("Failed to check if entry is deleted", e);
        }
        return false;
    }

    private RedisMessage executeRangeQuery(String streamKey, String startId, String endId, int count, boolean reverse) {
        try {
            // Extract database and table name (e.g., "default.redis_stream_data")
            String[] parts = streamDataTableName.split("\\.");
            String database = parts.length > 1 ? parts[0] : "default";
            String tableName = parts.length > 1 ? parts[1] : parts[0];
            
            Table table = flussConnection.getTable(TablePath.of(database, tableName));
            LogScanner scanner = table.newScan().createLogScanner();

            // Subscribe to all buckets (data is distributed across buckets by stream_key hash)
            int numBuckets = table.getTableInfo().getNumBuckets();
            for (int bucket = 0; bucket < numBuckets; bucket++) {
                scanner.subscribeFromBeginning(bucket);
            }

            long startOffset = parseOffsetFromStreamId(startId);

            List<RedisMessage> entries = new ArrayList<>();
            Map<String, Map<String, byte[]>> entryFieldsMap = new HashMap<>();

            ScanRecords records = scanner.poll(Duration.ofSeconds(1));
            int collected = 0;

            for (ScanRecord record : records) {
                if (count > 0 && collected >= count) break;

                InternalRow row = record.getRow();
                String scannedStreamKey = row.getString(0).toString();
                if (!streamKey.equals(scannedStreamKey)) continue;

                String scannedEntryId = row.getString(1).toString();
                
                if (isEntryDeleted(streamKey, scannedEntryId)) {
                    continue;
                }
                
                if (!isInRange(scannedEntryId, startId, endId)) continue;

                byte[] serializedFields;
                if (row.isNullAt(2)) {
                    serializedFields = new byte[0];
                } else {
                    try {
                        serializedFields = row.getBytes(2);
                    } catch (ClassCastException e) {
                        LOG.warn("Failed to get bytes from row, trying getString fallback", e);
                        serializedFields = row.getString(2).toBytes();
                    }
                }
                
                Map<String, byte[]> fields = deserializeFields(serializedFields);

                entryFieldsMap.put(scannedEntryId, fields);
                collected++;
            }

            List<String> sortedEntryIds = new ArrayList<>(entryFieldsMap.keySet());
            sortedEntryIds.sort(this::compareStreamIds);
            
            int resultCount = 0;
            for (String entryId : sortedEntryIds) {
                if (count > 0 && resultCount >= count) break;
                entries.add(formatStreamEntry(entryId, entryFieldsMap.get(entryId)));
                resultCount++;
            }

            scanner.close();

            if (reverse) {
                java.util.Collections.reverse(entries);
            }

            return new ArrayRedisMessage(entries);
        } catch (Exception e) {
            LOG.error("Failed to execute range query", e);
            return RedisErrorSanitizer.sanitizeError(e, "XREAD");
        }
    }

    private RedisMessage executeMultiStreamRead(List<String> streamKeys, List<String> startIds, int count, long blockMs) {
        try {
            List<RedisMessage> results = new ArrayList<>();

            for (int i = 0; i < streamKeys.size(); i++) {
                String streamKey = streamKeys.get(i);
                String startId = startIds.get(i);

                List<RedisMessage> streamEntries = new ArrayList<>();
                streamEntries.add(RedisResponse.bulkString(streamKey));

                RedisMessage rangeResult = executeRangeQuery(streamKey, incrementStreamId(startId), "+", count, false);
                streamEntries.add(rangeResult);

                results.add(new ArrayRedisMessage(streamEntries));
            }

            return new ArrayRedisMessage(results);
        } catch (Exception e) {
            LOG.error("Failed to execute XREAD", e);
            return RedisErrorSanitizer.sanitizeError(e, "XREAD");
        }
    }

    private String generateStreamId(String streamKey) {
        long timestampMs = System.currentTimeMillis();
        AtomicLong counter = streamSequenceCounters.computeIfAbsent(streamKey, k -> new AtomicLong(0));
        long sequence = counter.incrementAndGet();
        return timestampMs + "-" + sequence;
    }

    private long parseOffsetFromStreamId(String streamId) {
        if ("-".equals(streamId)) return 0L;
        if ("+".equals(streamId)) return Long.MAX_VALUE;

        try {
            String[] parts = streamId.split("-");
            return Long.parseLong(parts[0]);
        } catch (Exception e) {
            return 0L;
        }
    }

    private String incrementStreamId(String streamId) {
        if ("$".equals(streamId)) {
            return System.currentTimeMillis() + "-0";
        }
        try {
            String[] parts = streamId.split("-");
            long timestamp = Long.parseLong(parts[0]);
            long sequence = parts.length > 1 ? Long.parseLong(parts[1]) : 0;
            return timestamp + "-" + (sequence + 1);
        } catch (Exception e) {
            return "0-1";
        }
    }

    private boolean isInRange(String entryId, String startId, String endId) {
        if ("-".equals(startId) && "+".equals(endId)) return true;

        if (!"-".equals(startId)) {
            int cmpStart = compareStreamIds(entryId, startId);
            if (cmpStart < 0) return false;
        }
        
        if (!"+".equals(endId)) {
            int cmpEnd = compareStreamIds(entryId, endId);
            if (cmpEnd > 0) return false;
        }
        
        return true;
    }

    private InternalRow createStreamEntryRow(String streamKey, String entryId, String fieldName, byte[] fieldValue) {
        RowType rowType = RowType.builder()
                .field("stream_key", DataTypes.STRING())
                .field("entry_id", DataTypes.STRING())
                .field("field_name", DataTypes.STRING())
                .field("field_value", DataTypes.BYTES())
                .build();

        RowEncoder encoder = RowEncoder.create(KvFormat.COMPACTED, rowType);
        encoder.startNewRow();
        encoder.encodeField(0, BinaryString.fromString(streamKey));
        encoder.encodeField(1, BinaryString.fromString(entryId));
        encoder.encodeField(2, BinaryString.fromString(fieldName));
        encoder.encodeField(3, fieldValue);

        return encoder.finishRow();
    }

    private byte[] serializeFields(Map<String, byte[]> fields) throws Exception {
        // Simple length-prefixed serialization: 
        // [field_count:4][field1_name_len:4][field1_name][field1_value_len:4][field1_value]...
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        java.io.DataOutputStream dos = new java.io.DataOutputStream(baos);
        
        dos.writeInt(fields.size());
        for (Map.Entry<String, byte[]> entry : fields.entrySet()) {
            byte[] fieldName = entry.getKey().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] fieldValue = entry.getValue();
            
            dos.writeInt(fieldName.length);
            dos.write(fieldName);
            dos.writeInt(fieldValue.length);
            dos.write(fieldValue);
        }
        
        return baos.toByteArray();
    }
    
    private Map<String, byte[]> deserializeFields(byte[] data) throws Exception {
        Map<String, byte[]> fields = new HashMap<>();
        java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data);
        java.io.DataInputStream dis = new java.io.DataInputStream(bais);
        
        int fieldCount = dis.readInt();
        for (int i = 0; i < fieldCount; i++) {
            int nameLen = dis.readInt();
            byte[] nameBytes = new byte[nameLen];
            dis.readFully(nameBytes);
            String fieldName = new String(nameBytes, java.nio.charset.StandardCharsets.UTF_8);
            
            int valueLen = dis.readInt();
            byte[] fieldValue = new byte[valueLen];
            dis.readFully(fieldValue);
            
            fields.put(fieldName, fieldValue);
        }
        
        return fields;
    }

    private RedisMessage formatStreamEntry(String entryId, Map<String, byte[]> fields) {
        List<RedisMessage> entryData = new ArrayList<>();
        entryData.add(RedisResponse.bulkString(entryId));

        List<RedisMessage> fieldList = new ArrayList<>();
        for (Map.Entry<String, byte[]> field : fields.entrySet()) {
            fieldList.add(RedisResponse.bulkString(field.getKey()));
            fieldList.add(RedisResponse.bulkString(field.getValue()));
        }
        entryData.add(new ArrayRedisMessage(fieldList));

        return new ArrayRedisMessage(entryData);
    }

    private void updateStreamMetadata(String streamKey, String lastEntryId) {
        streamMetadataCache.compute(streamKey, (k, metadata) -> {
            if (metadata == null) {
                return new StreamMetadata(lastEntryId, 1L);
            } else {
                return new StreamMetadata(lastEntryId, metadata.length + 1);
            }
        });
    }

    private RedisMessage executeXGroup(RedisCommand command) {
        List<byte[]> args = command.getArgs();
        if (args.isEmpty()) {
            return RedisResponse.error("ERR wrong number of arguments for 'xgroup' command");
        }
        
        String subcommand = new String(args.get(0)).toUpperCase();
        switch (subcommand) {
            case "CREATE":
                return executeXGroupCreate(args);
            case "DESTROY":
                return executeXGroupDestroy(args);
            case "SETID":
                return executeXGroupSetId(args);
            default:
                return RedisResponse.error("ERR Unknown XGROUP subcommand: " + subcommand);
        }
    }

    private RedisMessage executeXGroupCreate(List<byte[]> args) {
        if (args.size() < 4) {
            return RedisResponse.error("ERR wrong number of arguments for 'xgroup create' command");
        }
        
        String streamKey = new String(args.get(1));
        String groupName = new String(args.get(2));
        String startId = new String(args.get(3));
        boolean mkstream = false;
        
        if (args.size() >= 5 && "MKSTREAM".equalsIgnoreCase(new String(args.get(4)))) {
            mkstream = true;
        }
        
        try {
            if (!mkstream) {
                StreamMetadata metadata = streamMetadataCache.get(streamKey);
                if (metadata == null) {
                    return RedisResponse.error("ERR The XGROUP subcommand requires the key to exist. " +
                            "Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.");
                }
            }
            
            String normalizedStartId = "$".equals(startId) ? 
                (streamMetadataCache.containsKey(streamKey) ? streamMetadataCache.get(streamKey).lastEntryId : "0-0") : 
                startId;
            
            String[] parts = consumerGroupTableName.split("\\.");
            String database = parts.length > 1 ? parts[0] : "default";
            String tableName = parts.length > 1 ? parts[1] : parts[0];
            
            Table table = flussConnection.getTable(TablePath.of(database, tableName));
            UpsertWriter writer = table.newUpsert().createWriter();
            
            GenericRow row = GenericRow.of(
                BinaryString.fromString(streamKey),
                BinaryString.fromString(groupName),
                BinaryString.fromString(normalizedStartId),
                System.currentTimeMillis()
            );
            
            writer.upsert(row);
            writer.flush();
            
            consumerGroups.computeIfAbsent(streamKey, k -> new ConcurrentHashMap<>())
                .put(groupName, new ConsumerGroupMetadata(normalizedStartId, System.currentTimeMillis()));
            
            return RedisResponse.ok();
        } catch (Exception e) {
            LOG.error("Failed to create consumer group", e);
            return RedisErrorSanitizer.sanitizeError(e, "XGROUP");
        }
    }

    private RedisMessage executeXGroupDestroy(List<byte[]> args) {
        if (args.size() < 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'xgroup destroy' command");
        }
        
        String streamKey = new String(args.get(1));
        String groupName = new String(args.get(2));
        
        try {
            String[] parts = consumerGroupTableName.split("\\.");
            String database = parts.length > 1 ? parts[0] : "default";
            String tableName = parts.length > 1 ? parts[1] : parts[0];
            
            Table table = flussConnection.getTable(TablePath.of(database, tableName));
            UpsertWriter writer = table.newUpsert().createWriter();
            
            GenericRow deleteRow = GenericRow.of(
                BinaryString.fromString(streamKey),
                BinaryString.fromString(groupName),
                null,
                null
            );
            
            writer.delete(deleteRow);
            writer.flush();
            
            Map<String, ConsumerGroupMetadata> groups = consumerGroups.get(streamKey);
            if (groups != null) {
                groups.remove(groupName);
            }
            
            return RedisResponse.integer(1);
        } catch (Exception e) {
            LOG.error("Failed to destroy consumer group", e);
            return RedisErrorSanitizer.sanitizeError(e, "XGROUP");
        }
    }

    private RedisMessage executeXGroupSetId(List<byte[]> args) {
        if (args.size() < 4) {
            return RedisResponse.error("ERR wrong number of arguments for 'xgroup setid' command");
        }
        
        String streamKey = new String(args.get(1));
        String groupName = new String(args.get(2));
        String newId = new String(args.get(3));
        
        try {
            String normalizedNewId = "$".equals(newId) ? 
                (streamMetadataCache.containsKey(streamKey) ? streamMetadataCache.get(streamKey).lastEntryId : "0-0") : 
                newId;
            
            String[] parts = consumerGroupTableName.split("\\.");
            String database = parts.length > 1 ? parts[0] : "default";
            String tableName = parts.length > 1 ? parts[1] : parts[0];
            
            Table table = flussConnection.getTable(TablePath.of(database, tableName));
            UpsertWriter writer = table.newUpsert().createWriter();
            
            GenericRow row = GenericRow.of(
                BinaryString.fromString(streamKey),
                BinaryString.fromString(groupName),
                BinaryString.fromString(normalizedNewId),
                System.currentTimeMillis()
            );
            
            writer.upsert(row);
            writer.flush();
            
            Map<String, ConsumerGroupMetadata> groups = consumerGroups.get(streamKey);
            if (groups != null && groups.containsKey(groupName)) {
                ConsumerGroupMetadata oldMetadata = groups.get(groupName);
                groups.put(groupName, new ConsumerGroupMetadata(normalizedNewId, oldMetadata.createdTimestamp));
            }
            
            return RedisResponse.ok();
        } catch (Exception e) {
            LOG.error("Failed to set consumer group ID", e);
            return RedisErrorSanitizer.sanitizeError(e, "XGROUP");
        }
    }

    private RedisMessage executeXReadGroup(RedisCommand command) {
        List<byte[]> args = command.getArgs();
        if (args.size() < 6) {
            return RedisResponse.error("ERR wrong number of arguments for 'xreadgroup' command");
        }
        
        if (!"GROUP".equalsIgnoreCase(new String(args.get(0)))) {
            return RedisResponse.error("ERR syntax error");
        }
        
        String groupName = new String(args.get(1));
        String consumerName = new String(args.get(2));
        long count = -1;
        int streamsIndex = -1;
        
        for (int i = 3; i < args.size(); i++) {
            String arg = new String(args.get(i)).toUpperCase();
            if ("COUNT".equals(arg) && i + 1 < args.size()) {
                count = Long.parseLong(new String(args.get(i + 1)));
                i++;
            } else if ("STREAMS".equals(arg)) {
                streamsIndex = i + 1;
                break;
            }
        }
        
        if (streamsIndex < 0 || (args.size() - streamsIndex) % 2 != 0) {
            return RedisResponse.error("ERR syntax error");
        }
        
        int numStreams = (args.size() - streamsIndex) / 2;
        List<RedisMessage> results = new ArrayList<>();
        
        try {
            for (int i = 0; i < numStreams; i++) {
                String streamKey = new String(args.get(streamsIndex + i));
                String startId = new String(args.get(streamsIndex + numStreams + i));
                
                ConsumerGroupMetadata groupMetadata = getOrLoadConsumerGroup(streamKey, groupName);
                if (groupMetadata == null) {
                    return RedisResponse.error("NOGROUP No such key '" + streamKey + "' or consumer group '" + groupName + "' in XREADGROUP with GROUP option");
                }
                
                String actualStartId;
                if (">".equals(startId)) {
                    String lastId = groupMetadata.lastDeliveredId;
                    if ("0-0".equals(lastId) || "0".equals(lastId)) {
                        actualStartId = "0-0";
                    } else {
                        String[] parts = lastId.split("-");
                        long timestamp = Long.parseLong(parts[0]);
                        int sequence = Integer.parseInt(parts[1]);
                        actualStartId = timestamp + "-" + (sequence + 1);
                    }
                } else {
                    actualStartId = startId;
                }
                
                RedisMessage rangeResult = executeRangeQuery(streamKey, actualStartId, "+", (int) count, false);
                
                if (rangeResult instanceof ArrayRedisMessage) {
                    List<RedisMessage> entries = ((ArrayRedisMessage) rangeResult).children();
                    
                    if (!entries.isEmpty() && ">".equals(startId)) {
                        List<String> entryIds = new ArrayList<>();
                        for (RedisMessage entry : entries) {
                            if (entry instanceof ArrayRedisMessage) {
                                List<RedisMessage> entryParts = ((ArrayRedisMessage) entry).children();
                                if (!entryParts.isEmpty() && entryParts.get(0) instanceof io.netty.handler.codec.redis.FullBulkStringRedisMessage) {
                                    io.netty.handler.codec.redis.FullBulkStringRedisMessage idMsg = 
                                        (io.netty.handler.codec.redis.FullBulkStringRedisMessage) entryParts.get(0);
                                    byte[] idBytes = new byte[idMsg.content().readableBytes()];
                                    idMsg.content().getBytes(idMsg.content().readerIndex(), idBytes);
                                    String entryId = new String(idBytes);
                                    entryIds.add(entryId);
                                }
                            }
                        }
                        
                        if (!entryIds.isEmpty()) {
                            try {
                                addToPendingList(streamKey, groupName, consumerName, entryIds);
                            } catch (Exception e) {
                                LOG.error("Failed to add entries to pending list", e);
                                return RedisErrorSanitizer.sanitizeError(e, "XREADGROUP");
                            }
                            
                            String maxEntryId = entryIds.get(entryIds.size() - 1);
                            updateConsumerGroupLastDelivered(streamKey, groupName, maxEntryId);
                        }
                    }
                    
                    if (!entries.isEmpty()) {
                        List<RedisMessage> streamResult = new ArrayList<>();
                        streamResult.add(RedisResponse.bulkString(streamKey));
                        streamResult.add(new ArrayRedisMessage(entries));
                        results.add(new ArrayRedisMessage(streamResult));
                    }
                }
            }
            
            return results.isEmpty() ? RedisResponse.bulkString((String) null) : new ArrayRedisMessage(results);
        } catch (Exception e) {
            LOG.error("Failed to execute XREADGROUP", e);
            return RedisErrorSanitizer.sanitizeError(e, "XREADGROUP");
        }
    }

    private RedisMessage executeXAck(RedisCommand command) {
        List<byte[]> args = command.getArgs();
        if (args.size() < 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'xack' command");
        }
        
        String streamKey = new String(args.get(0));
        String groupName = new String(args.get(1));
        List<String> entryIds = new ArrayList<>();
        
        for (int i = 2; i < args.size(); i++) {
            entryIds.add(new String(args.get(i)));
        }
        
        try {
            int acknowledgedCount = removeFromPendingList(streamKey, groupName, entryIds);
            return RedisResponse.integer(acknowledgedCount);
        } catch (Exception e) {
            LOG.error("Failed to execute XACK", e);
            return RedisErrorSanitizer.sanitizeError(e, "XACK");
        }
    }

    private RedisMessage executeXPending(RedisCommand command) {
        List<byte[]> args = command.getArgs();
        if (args.size() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'xpending' command");
        }
        
        String streamKey = new String(args.get(0));
        String groupName = new String(args.get(1));
        
        try {
            List<PendingEntry> pendingEntries = getPendingEntries(streamKey, groupName);
            
            if (pendingEntries.isEmpty()) {
                return new ArrayRedisMessage(List.of(
                    RedisResponse.integer(0),
                    RedisResponse.bulkString((String) null),
                    RedisResponse.bulkString((String) null),
                    RedisResponse.bulkString((String) null)
                ));
            }
            
            String minId = pendingEntries.get(0).entryId;
            String maxId = pendingEntries.get(pendingEntries.size() - 1).entryId;
            
            Map<String, Long> consumerCounts = new HashMap<>();
            for (PendingEntry entry : pendingEntries) {
                consumerCounts.merge(entry.consumerName, 1L, Long::sum);
            }
            
            List<RedisMessage> consumerList = new ArrayList<>();
            for (Map.Entry<String, Long> consumerEntry : consumerCounts.entrySet()) {
                consumerList.add(new ArrayRedisMessage(List.of(
                    RedisResponse.bulkString(consumerEntry.getKey()),
                    RedisResponse.bulkString(String.valueOf(consumerEntry.getValue()))
                )));
            }
            
            return new ArrayRedisMessage(List.of(
                RedisResponse.integer(pendingEntries.size()),
                RedisResponse.bulkString(minId),
                RedisResponse.bulkString(maxId),
                new ArrayRedisMessage(consumerList)
            ));
        } catch (Exception e) {
            LOG.error("Failed to execute XPENDING", e);
            return RedisErrorSanitizer.sanitizeError(e, "XPENDING");
        }
    }

    private ConsumerGroupMetadata getOrLoadConsumerGroup(String streamKey, String groupName) throws Exception {
        Map<String, ConsumerGroupMetadata> groups = consumerGroups.get(streamKey);
        if (groups != null && groups.containsKey(groupName)) {
            return groups.get(groupName);
        }
        
        String[] parts = consumerGroupTableName.split("\\.");
        String database = parts.length > 1 ? parts[0] : "default";
        String tableName = parts.length > 1 ? parts[1] : parts[0];
        
            Table table = flussConnection.getTable(TablePath.of(database, tableName));
            Lookuper lookuper = table.newLookup().createLookuper();
            
            GenericRow keyRow = GenericRow.of(
                BinaryString.fromString(streamKey),
                BinaryString.fromString(groupName)
            );
            
            List<InternalRow> results = lookuper.lookup(keyRow).get().getRowList();
            InternalRow row = (results != null && !results.isEmpty()) ? results.get(0) : null;
        
        if (row == null) {
            return null;
        }
        
        String lastDeliveredId = row.getString(2).toString();
        long createdTimestamp = row.getLong(3);
        
        ConsumerGroupMetadata metadata = new ConsumerGroupMetadata(lastDeliveredId, createdTimestamp);
        consumerGroups.computeIfAbsent(streamKey, k -> new ConcurrentHashMap<>()).put(groupName, metadata);
        
        return metadata;
    }

    private void updateConsumerGroupLastDelivered(String streamKey, String groupName, String lastEntryId) {
        try {
            String[] parts = consumerGroupTableName.split("\\.");
            String database = parts.length > 1 ? parts[0] : "default";
            String tableName = parts.length > 1 ? parts[1] : parts[0];
            
            Table table = flussConnection.getTable(TablePath.of(database, tableName));
            UpsertWriter writer = table.newUpsert().createWriter();
            
            ConsumerGroupMetadata metadata = consumerGroups.get(streamKey).get(groupName);
            GenericRow row = GenericRow.of(
                BinaryString.fromString(streamKey),
                BinaryString.fromString(groupName),
                BinaryString.fromString(lastEntryId),
                metadata.createdTimestamp
            );
            
            writer.upsert(row);
            writer.flush();
            
            consumerGroups.get(streamKey).put(groupName, new ConsumerGroupMetadata(lastEntryId, metadata.createdTimestamp));
        } catch (Exception e) {
            LOG.error("Failed to update consumer group last delivered ID", e);
        }
    }

    private void addToPendingList(String streamKey, String groupName, String consumerName, List<String> entryIds) throws Exception {
        String[] parts = pendingEntriesTableName.split("\\.");
        String database = parts.length > 1 ? parts[0] : "default";
        String tableName = parts.length > 1 ? parts[1] : parts[0];
        
        Table table = flussConnection.getTable(TablePath.of(database, tableName));
        UpsertWriter writer = table.newUpsert().createWriter();
        
        long timestamp = System.currentTimeMillis();
        for (String entryId : entryIds) {
            GenericRow row = GenericRow.of(
                BinaryString.fromString(streamKey),
                BinaryString.fromString(groupName),
                BinaryString.fromString(entryId),
                BinaryString.fromString(consumerName),
                timestamp,
                1
            );
            writer.upsert(row);
        }
        writer.flush();
    }

    private int removeFromPendingList(String streamKey, String groupName, List<String> entryIds) throws Exception {
        String[] parts = pendingEntriesTableName.split("\\.");
        String database = parts.length > 1 ? parts[0] : "default";
        String tableName = parts.length > 1 ? parts[1] : parts[0];
        
        Table table = flussConnection.getTable(TablePath.of(database, tableName));
        Lookuper lookuper = table.newLookup().createLookuper();
        UpsertWriter writer = table.newUpsert().createWriter();
        
        int count = 0;
        for (String entryId : entryIds) {
            GenericRow keyRow = GenericRow.of(
                BinaryString.fromString(streamKey),
                BinaryString.fromString(groupName),
                BinaryString.fromString(entryId)
            );
            
            List<InternalRow> results = lookuper.lookup(keyRow).get().getRowList();
            if (!results.isEmpty()) {
                InternalRow row = results.get(0);
                GenericRow deleteRow = GenericRow.of(
                    BinaryString.fromString(streamKey),
                    BinaryString.fromString(groupName),
                    BinaryString.fromString(entryId),
                    row.getString(3),
                    row.getLong(4),
                    row.getInt(5)
                );
                writer.delete(deleteRow);
                count++;
            }
        }
        writer.flush();
        
        return count;
    }

    private List<PendingEntry> getPendingEntries(String streamKey, String groupName) throws Exception {
        String[] parts = pendingEntriesTableName.split("\\.");
        String database = parts.length > 1 ? parts[0] : "default";
        String tableName = parts.length > 1 ? parts[1] : parts[0];
        
        Table table = flussConnection.getTable(TablePath.of(database, tableName));
        LogScanner scanner = table.newScan().createLogScanner();
        Lookuper lookuper = table.newLookup().createLookuper();
        
        List<PendingEntry> result = new ArrayList<>();
        Set<String> seenEntryIds = new HashSet<>();
        
        int numBuckets = table.getTableInfo().getNumBuckets();
        for (int bucket = 0; bucket < numBuckets; bucket++) {
            try {
                scanner.subscribeFromBeginning(bucket);
            } catch (Exception e) {
                LOG.debug("Bucket {} may not exist yet", bucket);
            }
        }
        
        try {
            while (true) {
                ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    break;
                }
                
                for (ScanRecord record : records) {
                    InternalRow row = record.getRow();
                    String rowStreamKey = row.getString(0).toString();
                    String rowGroupName = row.getString(1).toString();
                    
                    if (rowStreamKey.equals(streamKey) && rowGroupName.equals(groupName)) {
                        String entryId = row.getString(2).toString();
                        
                        if (!seenEntryIds.contains(entryId)) {
                            seenEntryIds.add(entryId);
                            
                            GenericRow keyRow = GenericRow.of(
                                BinaryString.fromString(streamKey),
                                BinaryString.fromString(groupName),
                                BinaryString.fromString(entryId)
                            );
                            List<InternalRow> lookupResults = lookuper.lookup(keyRow).get().getRowList();
                            
                            if (!lookupResults.isEmpty()) {
                                InternalRow currentRow = lookupResults.get(0);
                                String consumerName = currentRow.getString(3).toString();
                                long deliveredTimestamp = currentRow.getLong(4);
                                int deliveryCount = currentRow.getInt(5);
                                
                                result.add(new PendingEntry(entryId, consumerName, deliveredTimestamp, deliveryCount));
                            }
                        }
                    }
                }
            }
        } finally {
            scanner.close();
        }
        
        result.sort((a, b) -> compareStreamIds(a.entryId, b.entryId));
        return result;
    }

    private static class ConsumerGroupMetadata {
        final String lastDeliveredId;
        final long createdTimestamp;
        
        ConsumerGroupMetadata(String lastDeliveredId, long createdTimestamp) {
            this.lastDeliveredId = lastDeliveredId;
            this.createdTimestamp = createdTimestamp;
        }
    }

    private static class PendingEntry {
        final String entryId;
        final String consumerName;
        final long deliveredTimestamp;
        final int deliveryCount;
        
        PendingEntry(String entryId, String consumerName, long deliveredTimestamp, int deliveryCount) {
            this.entryId = entryId;
            this.consumerName = consumerName;
            this.deliveredTimestamp = deliveredTimestamp;
            this.deliveryCount = deliveryCount;
        }
    }

    private static class StreamMetadata {
        final String lastEntryId;
        final long length;

        StreamMetadata(String lastEntryId, long length) {
            this.lastEntryId = lastEntryId;
            this.length = length;
        }
    }
}
