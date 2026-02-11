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
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.gnuhpc.fluss.cape.redis.protocol.RedisCommand;
import org.gnuhpc.fluss.cape.redis.protocol.RedisResponse;
import org.gnuhpc.fluss.cape.redis.sharding.RedisSlotRouter;
import org.gnuhpc.fluss.cape.redis.storage.RedisStorageAdapter;
import org.gnuhpc.fluss.cape.redis.util.RedisErrorSanitizer;

import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class StreamCommandExecutorSharded implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(StreamCommandExecutorSharded.class);

    private final Connection flussConnection;
    private final String database;
    private final RedisSlotRouter router;
    
    private final Map<TablePath, Table> streamTableCache;
    private final Map<TablePath, UpsertWriter> streamWriterCache;
    private final Map<TablePath, Lookuper> streamLookuperCache;
    
    private final Table consumerGroupTable;
    private final UpsertWriter consumerGroupWriter;
    private final Lookuper consumerGroupLookuper;
    
    private final Table tombstoneTable;
    private final UpsertWriter tombstoneWriter;
    private final Lookuper tombstoneLookuper;
    
    private final Map<String, StreamMetadata> streamMetadataCache;
    private final Map<String, AtomicLong> streamSequenceCounters;
    private final Map<String, Set<String>> deletedEntriesCache;

    public StreamCommandExecutorSharded(
            Connection flussConnection,
            String database,
            int numberOfShards) throws Exception {
        this.flussConnection = flussConnection;
        this.database = database;
        this.router = new RedisSlotRouter(database, numberOfShards, true);
        
        this.streamTableCache = new ConcurrentHashMap<>();
        this.streamWriterCache = new ConcurrentHashMap<>();
        this.streamLookuperCache = new ConcurrentHashMap<>();
        
        TablePath consumerGroupPath = TablePath.of(database, "redis_stream_consumer_groups");
        this.consumerGroupTable = flussConnection.getTable(consumerGroupPath);
        this.consumerGroupWriter = consumerGroupTable.newUpsert().createWriter();
        this.consumerGroupLookuper = consumerGroupTable.newLookup().createLookuper();
        
        TablePath tombstonePath = TablePath.of(database, "redis_stream_tombstones");
        this.tombstoneTable = flussConnection.getTable(tombstonePath);
        this.tombstoneWriter = tombstoneTable.newUpsert().createWriter();
        this.tombstoneLookuper = tombstoneTable.newLookup().createLookuper();
        
        this.streamMetadataCache = new ConcurrentHashMap<>();
        this.streamSequenceCounters = new ConcurrentHashMap<>();
        this.deletedEntriesCache = new ConcurrentHashMap<>();
        
        LOG.info("Initialized StreamCommandExecutorSharded: database={}, shards={}", 
                 database, numberOfShards);
    }

    private Table getStreamTable(String streamKey) {
        TablePath tablePath = getStreamTablePath(streamKey);
        return streamTableCache.computeIfAbsent(tablePath, path -> {
            try {
                return flussConnection.getTable(path);
            } catch (Exception e) {
                throw new RuntimeException("Failed to get stream table: " + path, e);
            }
        });
    }

    private UpsertWriter getStreamWriter(String streamKey) {
        TablePath tablePath = getStreamTablePath(streamKey);
        return streamWriterCache.computeIfAbsent(tablePath, path -> {
            try {
                Table table = getStreamTable(streamKey);
                return table.newUpsert().createWriter();
            } catch (Exception e) {
                throw new RuntimeException("Failed to create stream writer: " + path, e);
            }
        });
    }

    private Lookuper getStreamLookuper(String streamKey) {
        TablePath tablePath = getStreamTablePath(streamKey);
        return streamLookuperCache.computeIfAbsent(tablePath, path -> {
            try {
                Table table = getStreamTable(streamKey);
                return table.newLookup().createLookuper();
            } catch (Exception e) {
                throw new RuntimeException("Failed to create stream lookuper: " + path, e);
            }
        });
    }

    private TablePath getStreamTablePath(String streamKey) {
        int slot = router.calculateSlot(streamKey);
        int shardIndex = slot / (16384 / router.getNumberOfShards());
        String tableName = String.format("redis_stream_shard_%02d", shardIndex);
        return TablePath.of(database, tableName);
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
            
            UpsertWriter writer = getStreamWriter(streamKey);
            GenericRow row = GenericRow.of(
                BinaryString.fromString(streamKey),
                BinaryString.fromString(entryId),
                fieldsBytes
            );
            writer.upsert(row).get();

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

        return executeMultiStreamRead(streamKeys, startIds, (int) count);
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
            
            for (String entryId : entryIds) {
                GenericRow row = GenericRow.of(
                    BinaryString.fromString(streamKey),
                    BinaryString.fromString(entryId),
                    true,
                    System.currentTimeMillis()
                );
                tombstoneWriter.upsert(row);
            }
            tombstoneWriter.flush();
            
            return RedisResponse.integer(entryIds.size());
        } catch (Exception e) {
            LOG.error("Failed to execute XDEL", e);
            return RedisErrorSanitizer.sanitizeError(e, "XDEL");
        }
    }

    private RedisMessage executeRangeQuery(String streamKey, String startId, String endId, int count, boolean reverse) {
        try {
            Table shardTable = getStreamTable(streamKey);
            LogScanner scanner = shardTable.newScan().createLogScanner();
            
            int numBuckets = shardTable.getTableInfo().getNumBuckets();
            for (int bucket = 0; bucket < numBuckets; bucket++) {
                scanner.subscribeFromBeginning(bucket);
            }

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
            return RedisErrorSanitizer.sanitizeError(e, "XRANGE");
        }
    }

    private RedisMessage executeMultiStreamRead(List<String> streamKeys, List<String> startIds, int count) {
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

    private boolean isEntryDeleted(String streamKey, String entryId) {
        Set<String> cached = deletedEntriesCache.get(streamKey);
        if (cached != null && cached.contains(entryId)) {
            return true;
        }
        
        try {
            GenericRow keyRow = GenericRow.of(
                BinaryString.fromString(streamKey),
                BinaryString.fromString(entryId)
            );
            
            List<InternalRow> results = tombstoneLookuper.lookup(keyRow).get().getRowList();
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

    private byte[] serializeFields(Map<String, byte[]> fields) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
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
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        
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

    private static class StreamMetadata {
        public final String lastEntryId;
        public final long length;

        public StreamMetadata(String lastEntryId, long length) {
            this.lastEntryId = lastEntryId;
            this.length = length;
        }
    }
}
