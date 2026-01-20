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

import org.apache.fluss.client.Connection;
import org.apache.fluss.metadata.TablePath;
import org.gnuhpc.fluss.cape.redis.expiration.ExpirationManager;
import org.gnuhpc.fluss.cape.redis.metadata.MetadataManager;
import org.gnuhpc.fluss.cape.redis.metadata.ZSetMetadata;
import org.gnuhpc.fluss.cape.redis.protocol.RedisCommand;
import org.gnuhpc.fluss.cape.redis.protocol.RedisResponse;
import org.gnuhpc.fluss.cape.redis.storage.RedisSingleTableAdapter;
import org.gnuhpc.fluss.cape.redis.storage.RedisStorageAdapter;
import org.gnuhpc.fluss.cape.redis.storage.ZSetReverseIndex;
import org.gnuhpc.fluss.cape.redis.util.BlockingOperationQueue;
import org.gnuhpc.fluss.cape.redis.util.ScanCursorManager;
import org.gnuhpc.fluss.cape.redis.util.ScoreEncoder;
import org.gnuhpc.fluss.cape.redis.util.RedisErrorSanitizer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class SortedSetCommandExecutor implements BlockingAwareExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(SortedSetCommandExecutor.class);
    private static final String REDIS_TYPE = "zset";
    private static final String METADATA_SUB_KEY = "__meta__";
    private static final byte[] EMPTY_VALUE = new byte[0];

    private final RedisStorageAdapter adapter;
    private final MetadataManager metadataManager;
    private final ZSetReverseIndex reverseIndex;
    private final ExpirationManager expirationManager;
    private final ScanCursorManager cursorManager;
    private final Random random;
    private BlockingOperationQueue currentBlockingQueue;

    public SortedSetCommandExecutor(
            RedisStorageAdapter adapter, Connection connection, String reverseIndexTableName)
            throws Exception {
        this.adapter = adapter;
        this.metadataManager = new MetadataManager(adapter);
        this.reverseIndex =
                new ZSetReverseIndex(connection, TablePath.of("default", reverseIndexTableName));
        this.expirationManager = new ExpirationManager(adapter);
        this.cursorManager = new ScanCursorManager();
        this.random = new Random();
        LOG.info("Initialized SortedSetCommandExecutor with reverse index: {}", reverseIndexTableName);
    }

    @Override
    public RedisMessage execute(RedisCommand command) {
        return execute(command, null, null);
    }

    @Override
    public RedisMessage execute(RedisCommand command, ChannelHandlerContext ctx, BlockingOperationQueue blockingQueue) {
        String cmd = command.getCommand();
        this.currentBlockingQueue = blockingQueue;

        try {
            switch (cmd) {
                case "ZADD":
                    return executeZAdd(command);
                case "ZREM":
                    return executeZRem(command);
                case "ZRANGE":
                    return executeZRange(command);
                case "ZSCORE":
                    return executeZScore(command);
                case "ZCARD":
                    return executeZCard(command);
                case "ZRANGEBYSCORE":
                    return executeZRangeByScore(command);
                case "ZINCRBY":
                    return executeZIncrBy(command);
                case "ZCOUNT":
                    return executeZCount(command);
                case "ZRANK":
                    return executeZRank(command);
                case "ZREVRANK":
                    return executeZRevRank(command);
                case "ZPOPMIN":
                    return executeZPopMin(command);
                case "ZPOPMAX":
                    return executeZPopMax(command);
                case "ZREVRANGE":
                    return executeZRevRange(command);
                case "ZREVRANGEBYSCORE":
                    return executeZRevRangeByScore(command);
                case "ZREMRANGEBYRANK":
                    return executeZRemRangeByRank(command);
                case "ZREMRANGEBYSCORE":
                    return executeZRemRangeByScore(command);
                case "ZINTER":
                    return executeZInter(command);
                case "ZINTERSTORE":
                    return executeZInterStore(command);
                case "ZUNION":
                    return executeZUnion(command);
                case "ZUNIONSTORE":
                    return executeZUnionStore(command);
                case "ZDIFF":
                    return executeZDiff(command);
                case "ZDIFFSTORE":
                    return executeZDiffStore(command);
                case "ZRANDMEMBER":
                    return executeZRandMember(command);
                case "ZSCAN":
                    return executeZScan(command);
                case "BZPOPMIN":
                    return executeBZPopMin(command, ctx, blockingQueue);
                case "BZPOPMAX":
                    return executeBZPopMax(command, ctx, blockingQueue);
                default:
                    return RedisResponse.error("ERR unknown sorted set command '" + cmd + "'");
            }
        } catch (Exception e) {
            LOG.error("Error executing sorted set command: {}", cmd, e);
            return RedisErrorSanitizer.sanitizeError(e, cmd);
        } finally {
            this.currentBlockingQueue = null;
        }
    }

    private RedisMessage executeZAdd(RedisCommand command) throws Exception {
        if (command.getArgCount() < 3 || command.getArgCount() % 2 != 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'ZADD' command");
        }

        String key = command.getArgAsString(0);

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        ZSetMetadata metadata = metadataManager.getOrCreateZSetMetadata(key);

        int addedCount = 0;

        for (int i = 1; i < command.getArgCount(); i += 2) {
            double score;
            try {
                score = Double.parseDouble(command.getArgAsString(i));
            } catch (NumberFormatException e) {
                return RedisResponse.error("ERR value is not a valid float");
            }

            String member = command.getArgAsString(i + 1);

            Double existingScore = reverseIndex.getMemberScore(key, member);

            if (existingScore != null) {
                String oldSubKey = ScoreEncoder.encodeWithMember(existingScore, member);
                adapter.deleteByCompositeKey(key, oldSubKey);
            } else {
                addedCount++;
                metadata.incrementCount();
            }

            String newSubKey = ScoreEncoder.encodeWithMember(score, member);
            adapter.setByCompositeKey(key, REDIS_TYPE, newSubKey, EMPTY_VALUE, score);

            reverseIndex.setMemberScore(key, member, score);
        }

        metadataManager.saveZSetMetadata(key, REDIS_TYPE, metadata);

        if (currentBlockingQueue != null && addedCount > 0) {
            currentBlockingQueue.notifyDataAvailable(key, () -> {
                try {
                    return executePopMinForBlocking(key);
                } catch (Exception e) {
                    LOG.error("Error in blocking pop", e);
                    return RedisResponse.nullBulkString();
                }
            });
        }

        return RedisResponse.integer(addedCount);
    }

    private RedisMessage executeZRem(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'ZREM' command");
        }

        String key = command.getArgAsString(0);

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.integer(0);
        }

        int removedCount = 0;

        for (int i = 1; i < command.getArgCount(); i++) {
            String memberToRemove = command.getArgAsString(i);

            Double existingScore = reverseIndex.getMemberScore(key, memberToRemove);

            if (existingScore != null) {
                String subKey = ScoreEncoder.encodeWithMember(existingScore, memberToRemove);
                adapter.deleteByCompositeKey(key, subKey);

                reverseIndex.deleteMember(key, memberToRemove);

                removedCount++;
            }
        }

        if (removedCount > 0) {
            ZSetMetadata metadata = metadataManager.getOrCreateZSetMetadata(key);
            metadata.decrementCount(removedCount);
            metadataManager.saveZSetMetadata(key, REDIS_TYPE, metadata);
        }

        return RedisResponse.integer(removedCount);
    }

    private RedisMessage executeZRange(RedisCommand command) throws Exception {
        if (command.getArgCount() < 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'ZRANGE' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }
        
        long start;
        long stop;

        try {
            start = Long.parseLong(command.getArgAsString(1));
            stop = Long.parseLong(command.getArgAsString(2));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not an integer or out of range");
        }

        boolean withScores = false;
        if (command.getArgCount() > 3) {
            String option = command.getArgAsString(3).toUpperCase();
            if ("WITHSCORES".equals(option)) {
                withScores = true;
            }
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }

        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

        if (items.isEmpty()) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }

        items.sort((a, b) -> a.subKey.compareTo(b.subKey));

        long totalCount = items.size();

        if (start < 0) {
            start = totalCount + start;
        }
        if (stop < 0) {
            stop = totalCount + stop;
        }

        start = Math.max(0, start);
        stop = Math.min(totalCount - 1, stop);

        if (start > stop || start >= totalCount) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }

        List<byte[]> results = new ArrayList<>();
        for (long i = start; i <= stop && i < totalCount; i++) {
            RedisSingleTableAdapter.KeyValue kv = items.get((int) i);
            ScoreEncoder.ScoreMember sm = ScoreEncoder.decodeWithMember(kv.subKey);
            results.add(sm.member.getBytes());
            if (withScores) {
                results.add(String.valueOf(sm.score).getBytes());
            }
        }

        return RedisResponse.bytesArray(results);
    }

    private RedisMessage executeZScore(RedisCommand command) throws Exception {
        if (command.getArgCount() != 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'ZSCORE' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.nullBulkString();
        }
        
        String member = command.getArgAsString(1);

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.nullBulkString();
        }

        Double score = reverseIndex.getMemberScore(key, member);

        if (score == null) {
            return RedisResponse.nullBulkString();
        }

        return RedisResponse.bulkString(String.valueOf(score).getBytes());
    }

    private RedisMessage executeZCard(RedisCommand command) throws Exception {
        if (command.getArgCount() != 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'ZCARD' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.integer(0);
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.integer(0);
        }

        ZSetMetadata metadata = metadataManager.getZSetMetadata(key);
        if (metadata == null) {
            return RedisResponse.integer(0);
        }

        return RedisResponse.integer(metadata.getCount());
    }

    private RedisMessage executeZRangeByScore(RedisCommand command) throws Exception {
        if (command.getArgCount() < 3) {
            return RedisResponse.error(
                    "ERR wrong number of arguments for 'ZRANGEBYSCORE' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }
        
        double minScore;
        double maxScore;

        try {
            String minStr = command.getArgAsString(1);
            String maxStr = command.getArgAsString(2);

            if ("-inf".equalsIgnoreCase(minStr)) {
                minScore = Double.NEGATIVE_INFINITY;
            } else {
                minScore = Double.parseDouble(minStr);
            }

            if ("+inf".equalsIgnoreCase(maxStr) || "inf".equalsIgnoreCase(maxStr)) {
                maxScore = Double.POSITIVE_INFINITY;
            } else {
                maxScore = Double.parseDouble(maxStr);
            }
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR min or max is not a float");
        }

        boolean withScores = false;
        if (command.getArgCount() > 3) {
            String option = command.getArgAsString(3).toUpperCase();
            if ("WITHSCORES".equals(option)) {
                withScores = true;
            }
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }

        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

        if (items.isEmpty()) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }

        items.sort((a, b) -> a.subKey.compareTo(b.subKey));

        List<byte[]> results = new ArrayList<>();
        for (RedisSingleTableAdapter.KeyValue kv : items) {
            try {
                ScoreEncoder.ScoreMember sm = ScoreEncoder.decodeWithMember(kv.subKey);
                if (sm.score >= minScore && sm.score <= maxScore) {
                    results.add(sm.member.getBytes());
                    if (withScores) {
                        results.add(String.valueOf(sm.score).getBytes());
                    }
                }
            } catch (Exception e) {
            }
        }

        return RedisResponse.bytesArray(results);
    }

    private RedisMessage executeZIncrBy(RedisCommand command) throws Exception {
        if (command.getArgCount() != 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'ZINCRBY' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            // Expired key treated as non-existent
        }
        
        double increment;
        try {
            increment = Double.parseDouble(command.getArgAsString(1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not a valid float");
        }
        
        String member = command.getArgAsString(2);

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Double currentScore = reverseIndex.getMemberScore(key, member);
        double newScore = (currentScore != null) ? currentScore + increment : increment;

        if (currentScore != null) {
            // Delete old entry
            String oldSubKey = ScoreEncoder.encodeWithMember(currentScore, member);
            adapter.deleteByCompositeKey(key, oldSubKey);
        } else {
            // New member, update count
            ZSetMetadata metadata = metadataManager.getOrCreateZSetMetadata(key);
            metadata.incrementCount();
            metadataManager.saveZSetMetadata(key, REDIS_TYPE, metadata);
        }

        // Add new entry
        String newSubKey = ScoreEncoder.encodeWithMember(newScore, member);
        adapter.setByCompositeKey(key, REDIS_TYPE, newSubKey, EMPTY_VALUE, newScore);
        reverseIndex.setMemberScore(key, member, newScore);

        return RedisResponse.bulkString(String.valueOf(newScore).getBytes());
    }

    private RedisMessage executeZCount(RedisCommand command) throws Exception {
        if (command.getArgCount() != 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'ZCOUNT' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.integer(0);
        }

        double minScore;
        double maxScore;

        try {
            String minStr = command.getArgAsString(1);
            String maxStr = command.getArgAsString(2);

            if ("-inf".equalsIgnoreCase(minStr)) {
                minScore = Double.NEGATIVE_INFINITY;
            } else {
                minScore = Double.parseDouble(minStr);
            }

            if ("+inf".equalsIgnoreCase(maxStr) || "inf".equalsIgnoreCase(maxStr)) {
                maxScore = Double.POSITIVE_INFINITY;
            } else {
                maxScore = Double.parseDouble(maxStr);
            }
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR min or max is not a float");
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.integer(0);
        }

        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

        long count = 0;
        for (RedisSingleTableAdapter.KeyValue kv : items) {
            try {
                ScoreEncoder.ScoreMember sm = ScoreEncoder.decodeWithMember(kv.subKey);
                if (sm.score >= minScore && sm.score <= maxScore) {
                    count++;
                }
            } catch (Exception e) {
                // Skip invalid entries
            }
        }

        return RedisResponse.integer(count);
    }

    private RedisMessage executeZRank(RedisCommand command) throws Exception {
        if (command.getArgCount() != 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'ZRANK' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.nullBulkString();
        }
        
        String member = command.getArgAsString(1);

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.nullBulkString();
        }

        // Check if member exists
        Double score = reverseIndex.getMemberScore(key, member);
        if (score == null) {
            return RedisResponse.nullBulkString();
        }

        // Load all members and sort ascending
        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

        List<ScoreEncoder.ScoreMember> members = new ArrayList<>();
        for (RedisSingleTableAdapter.KeyValue kv : items) {
            try {
                members.add(ScoreEncoder.decodeWithMember(kv.subKey));
            } catch (Exception e) {
                // Skip invalid entries
            }
        }

        // Sort by score ascending, then by member lexicographically
        members.sort(Comparator.comparingDouble((ScoreEncoder.ScoreMember sm) -> sm.score)
                .thenComparing(sm -> sm.member));

        // Find member rank
        for (int i = 0; i < members.size(); i++) {
            if (members.get(i).member.equals(member)) {
                return RedisResponse.integer(i);
            }
        }

        return RedisResponse.nullBulkString();
    }

    private RedisMessage executeZRevRank(RedisCommand command) throws Exception {
        if (command.getArgCount() != 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'ZREVRANK' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.nullBulkString();
        }
        
        String member = command.getArgAsString(1);

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.nullBulkString();
        }

        // Check if member exists
        Double score = reverseIndex.getMemberScore(key, member);
        if (score == null) {
            return RedisResponse.nullBulkString();
        }

        // Load all members and sort descending
        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

        List<ScoreEncoder.ScoreMember> members = new ArrayList<>();
        for (RedisSingleTableAdapter.KeyValue kv : items) {
            try {
                members.add(ScoreEncoder.decodeWithMember(kv.subKey));
            } catch (Exception e) {
                // Skip invalid entries
            }
        }

        // Sort by score descending, then by member lexicographically descending
        members.sort(Comparator.comparingDouble((ScoreEncoder.ScoreMember sm) -> sm.score)
                .thenComparing(sm -> sm.member)
                .reversed());

        // Find member rank
        for (int i = 0; i < members.size(); i++) {
            if (members.get(i).member.equals(member)) {
                return RedisResponse.integer(i);
            }
        }

        return RedisResponse.nullBulkString();
    }

    private RedisMessage executeZPopMin(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1 || command.getArgCount() > 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'ZPOPMIN' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }

        long count = 1;
        if (command.getArgCount() == 2) {
            try {
                count = Long.parseLong(command.getArgAsString(1));
                if (count < 1) {
                    return RedisResponse.error("ERR count must be positive");
                }
            } catch (NumberFormatException e) {
                return RedisResponse.error("ERR value is not an integer or out of range");
            }
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }

        // Load all members and sort ascending
        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

        if (items.isEmpty()) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }

        items.sort(Comparator.comparing((RedisSingleTableAdapter.KeyValue kv) -> kv.subKey));

        // Pop first N members
        List<byte[]> results = new ArrayList<>();
        int removed = 0;
        for (int i = 0; i < Math.min(count, items.size()); i++) {
            RedisSingleTableAdapter.KeyValue kv = items.get(i);
            try {
                ScoreEncoder.ScoreMember sm = ScoreEncoder.decodeWithMember(kv.subKey);
                results.add(sm.member.getBytes());
                results.add(String.valueOf(sm.score).getBytes());

                // Remove from storage
                adapter.deleteByCompositeKey(key, kv.subKey);
                reverseIndex.deleteMember(key, sm.member);
                removed++;
            } catch (Exception e) {
                // Skip invalid entries
            }
        }

        // Update metadata count
        if (removed > 0) {
            ZSetMetadata metadata = metadataManager.getOrCreateZSetMetadata(key);
            metadata.decrementCount(removed);
            metadataManager.saveZSetMetadata(key, REDIS_TYPE, metadata);
        }

        return RedisResponse.bytesArray(results);
    }

    private RedisMessage executeZPopMax(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1 || command.getArgCount() > 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'ZPOPMAX' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }

        long count = 1;
        if (command.getArgCount() == 2) {
            try {
                count = Long.parseLong(command.getArgAsString(1));
                if (count < 1) {
                    return RedisResponse.error("ERR count must be positive");
                }
            } catch (NumberFormatException e) {
                return RedisResponse.error("ERR value is not an integer or out of range");
            }
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }

        // Load all members and sort descending
        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

        if (items.isEmpty()) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }

        items.sort(Comparator.comparing((RedisSingleTableAdapter.KeyValue kv) -> kv.subKey).reversed());

        // Pop first N members
        List<byte[]> results = new ArrayList<>();
        int removed = 0;
        for (int i = 0; i < Math.min(count, items.size()); i++) {
            RedisSingleTableAdapter.KeyValue kv = items.get(i);
            try {
                ScoreEncoder.ScoreMember sm = ScoreEncoder.decodeWithMember(kv.subKey);
                results.add(sm.member.getBytes());
                results.add(String.valueOf(sm.score).getBytes());

                // Remove from storage
                adapter.deleteByCompositeKey(key, kv.subKey);
                reverseIndex.deleteMember(key, sm.member);
                removed++;
            } catch (Exception e) {
                // Skip invalid entries
            }
        }

        // Update metadata count
        if (removed > 0) {
            ZSetMetadata metadata = metadataManager.getOrCreateZSetMetadata(key);
            metadata.decrementCount(removed);
            metadataManager.saveZSetMetadata(key, REDIS_TYPE, metadata);
        }

        return RedisResponse.bytesArray(results);
    }

    private RedisMessage executeZRevRange(RedisCommand command) throws Exception {
        if (command.getArgCount() < 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'ZREVRANGE' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }
        
        long start;
        long stop;

        try {
            start = Long.parseLong(command.getArgAsString(1));
            stop = Long.parseLong(command.getArgAsString(2));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not an integer or out of range");
        }

        boolean withScores = false;
        if (command.getArgCount() > 3) {
            String option = command.getArgAsString(3).toUpperCase();
            if ("WITHSCORES".equals(option)) {
                withScores = true;
            }
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }

        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

        if (items.isEmpty()) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }

        // Sort descending
        items.sort(Comparator.comparing((RedisSingleTableAdapter.KeyValue kv) -> kv.subKey).reversed());

        long totalCount = items.size();

        if (start < 0) {
            start = totalCount + start;
        }
        if (stop < 0) {
            stop = totalCount + stop;
        }

        start = Math.max(0, start);
        stop = Math.min(totalCount - 1, stop);

        if (start > stop || start >= totalCount) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }

        List<byte[]> results = new ArrayList<>();
        for (long i = start; i <= stop && i < totalCount; i++) {
            RedisSingleTableAdapter.KeyValue kv = items.get((int) i);
            try {
                ScoreEncoder.ScoreMember sm = ScoreEncoder.decodeWithMember(kv.subKey);
                results.add(sm.member.getBytes());
                if (withScores) {
                    results.add(String.valueOf(sm.score).getBytes());
                }
            } catch (Exception e) {
                // Skip invalid entries
            }
        }

        return RedisResponse.bytesArray(results);
    }

    private RedisMessage executeZRevRangeByScore(RedisCommand command) throws Exception {
        if (command.getArgCount() < 3) {
            return RedisResponse.error(
                    "ERR wrong number of arguments for 'ZREVRANGEBYSCORE' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }
        
        double maxScore;
        double minScore;

        try {
            String maxStr = command.getArgAsString(1);
            String minStr = command.getArgAsString(2);

            if ("+inf".equalsIgnoreCase(maxStr) || "inf".equalsIgnoreCase(maxStr)) {
                maxScore = Double.POSITIVE_INFINITY;
            } else {
                maxScore = Double.parseDouble(maxStr);
            }

            if ("-inf".equalsIgnoreCase(minStr)) {
                minScore = Double.NEGATIVE_INFINITY;
            } else {
                minScore = Double.parseDouble(minStr);
            }
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR min or max is not a float");
        }

        boolean withScores = false;
        if (command.getArgCount() > 3) {
            String option = command.getArgAsString(3).toUpperCase();
            if ("WITHSCORES".equals(option)) {
                withScores = true;
            }
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }

        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

        if (items.isEmpty()) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }

        // Sort descending
        items.sort(Comparator.comparing((RedisSingleTableAdapter.KeyValue kv) -> kv.subKey).reversed());

        List<byte[]> results = new ArrayList<>();
        for (RedisSingleTableAdapter.KeyValue kv : items) {
            try {
                ScoreEncoder.ScoreMember sm = ScoreEncoder.decodeWithMember(kv.subKey);
                if (sm.score >= minScore && sm.score <= maxScore) {
                    results.add(sm.member.getBytes());
                    if (withScores) {
                        results.add(String.valueOf(sm.score).getBytes());
                    }
                }
            } catch (Exception e) {
                // Skip invalid entries
            }
        }

        return RedisResponse.bytesArray(results);
    }

    private RedisMessage executeZRemRangeByRank(RedisCommand command) throws Exception {
        if (command.getArgCount() != 3) {
            return RedisResponse.error(
                    "ERR wrong number of arguments for 'ZREMRANGEBYRANK' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.integer(0);
        }

        long start;
        long stop;

        try {
            start = Long.parseLong(command.getArgAsString(1));
            stop = Long.parseLong(command.getArgAsString(2));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not an integer or out of range");
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.integer(0);
        }

        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

        if (items.isEmpty()) {
            return RedisResponse.integer(0);
        }

        items.sort(Comparator.comparing((RedisSingleTableAdapter.KeyValue kv) -> kv.subKey));

        long totalCount = items.size();

        if (start < 0) {
            start = totalCount + start;
        }
        if (stop < 0) {
            stop = totalCount + stop;
        }

        start = Math.max(0, start);
        stop = Math.min(totalCount - 1, stop);

        if (start > stop || start >= totalCount) {
            return RedisResponse.integer(0);
        }

        int removed = 0;
        for (long i = start; i <= stop && i < totalCount; i++) {
            RedisSingleTableAdapter.KeyValue kv = items.get((int) i);
            try {
                ScoreEncoder.ScoreMember sm = ScoreEncoder.decodeWithMember(kv.subKey);
                adapter.deleteByCompositeKey(key, kv.subKey);
                reverseIndex.deleteMember(key, sm.member);
                removed++;
            } catch (Exception e) {
                // Skip invalid entries
            }
        }

        if (removed > 0) {
            ZSetMetadata metadata = metadataManager.getOrCreateZSetMetadata(key);
            metadata.decrementCount(removed);
            metadataManager.saveZSetMetadata(key, REDIS_TYPE, metadata);
        }

        return RedisResponse.integer(removed);
    }

    private RedisMessage executeZRemRangeByScore(RedisCommand command) throws Exception {
        if (command.getArgCount() != 3) {
            return RedisResponse.error(
                    "ERR wrong number of arguments for 'ZREMRANGEBYSCORE' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.integer(0);
        }

        double minScore;
        double maxScore;

        try {
            String minStr = command.getArgAsString(1);
            String maxStr = command.getArgAsString(2);

            if ("-inf".equalsIgnoreCase(minStr)) {
                minScore = Double.NEGATIVE_INFINITY;
            } else {
                minScore = Double.parseDouble(minStr);
            }

            if ("+inf".equalsIgnoreCase(maxStr) || "inf".equalsIgnoreCase(maxStr)) {
                maxScore = Double.POSITIVE_INFINITY;
            } else {
                maxScore = Double.parseDouble(maxStr);
            }
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR min or max is not a float");
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.integer(0);
        }

        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

        if (items.isEmpty()) {
            return RedisResponse.integer(0);
        }

        int removed = 0;
        for (RedisSingleTableAdapter.KeyValue kv : items) {
            try {
                ScoreEncoder.ScoreMember sm = ScoreEncoder.decodeWithMember(kv.subKey);
                if (sm.score >= minScore && sm.score <= maxScore) {
                    adapter.deleteByCompositeKey(key, kv.subKey);
                    reverseIndex.deleteMember(key, sm.member);
                    removed++;
                }
            } catch (Exception e) {
                // Skip invalid entries
            }
        }

        if (removed > 0) {
            ZSetMetadata metadata = metadataManager.getOrCreateZSetMetadata(key);
            metadata.decrementCount(removed);
            metadataManager.saveZSetMetadata(key, REDIS_TYPE, metadata);
        }

        return RedisResponse.integer(removed);
    }

    private RedisMessage executeZRandMember(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1 || command.getArgCount() > 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'ZRANDMEMBER' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.nullBulkString();
        }

        long count = 1;
        boolean withScores = false;
        boolean allowDuplicates = false;

        if (command.getArgCount() >= 2) {
            try {
                count = Long.parseLong(command.getArgAsString(1));
                allowDuplicates = count < 0;
                count = Math.abs(count);
            } catch (NumberFormatException e) {
                return RedisResponse.error("ERR value is not an integer or out of range");
            }
        }

        if (command.getArgCount() == 3) {
            String option = command.getArgAsString(2).toUpperCase();
            if ("WITHSCORES".equals(option)) {
                withScores = true;
            }
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            if (command.getArgCount() == 1) {
                return RedisResponse.nullBulkString();
            } else {
                return RedisResponse.bytesArray(Collections.emptyList());
            }
        }

        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

        if (items.isEmpty()) {
            if (command.getArgCount() == 1) {
                return RedisResponse.nullBulkString();
            } else {
                return RedisResponse.bytesArray(Collections.emptyList());
            }
        }

        List<ScoreEncoder.ScoreMember> members = new ArrayList<>();
        for (RedisSingleTableAdapter.KeyValue kv : items) {
            try {
                members.add(ScoreEncoder.decodeWithMember(kv.subKey));
            } catch (Exception e) {
                // Skip invalid entries
            }
        }

        // Single member without count
        if (command.getArgCount() == 1) {
            ScoreEncoder.ScoreMember sm = members.get(random.nextInt(members.size()));
            return RedisResponse.bulkString(sm.member.getBytes());
        }

        // Multiple members
        List<byte[]> results = new ArrayList<>();
        if (allowDuplicates) {
            // Allow duplicates - simple random selection with replacement
            for (long i = 0; i < count; i++) {
                ScoreEncoder.ScoreMember sm = members.get(random.nextInt(members.size()));
                results.add(sm.member.getBytes());
                if (withScores) {
                    results.add(String.valueOf(sm.score).getBytes());
                }
            }
        } else {
            // No duplicates - shuffle and take first N
            Collections.shuffle(members, random);
            long limit = Math.min(count, members.size());
            for (int i = 0; i < limit; i++) {
                ScoreEncoder.ScoreMember sm = members.get(i);
                results.add(sm.member.getBytes());
                if (withScores) {
                    results.add(String.valueOf(sm.score).getBytes());
                }
            }
        }

        return RedisResponse.bytesArray(results);
    }

    private RedisMessage executeZInter(RedisCommand command) throws Exception {
        return executeZSetOperation(command, "ZINTER", false, SetOperation.INTERSECT);
    }

    private RedisMessage executeZInterStore(RedisCommand command) throws Exception {
        return executeZSetOperation(command, "ZINTERSTORE", true, SetOperation.INTERSECT);
    }

    private RedisMessage executeZUnion(RedisCommand command) throws Exception {
        return executeZSetOperation(command, "ZUNION", false, SetOperation.UNION);
    }

    private RedisMessage executeZUnionStore(RedisCommand command) throws Exception {
        return executeZSetOperation(command, "ZUNIONSTORE", true, SetOperation.UNION);
    }

    private RedisMessage executeZDiff(RedisCommand command) throws Exception {
        return executeZSetOperation(command, "ZDIFF", false, SetOperation.DIFFERENCE);
    }

    private RedisMessage executeZDiffStore(RedisCommand command) throws Exception {
        return executeZSetOperation(command, "ZDIFFSTORE", true, SetOperation.DIFFERENCE);
    }

    private enum SetOperation {
        INTERSECT,
        UNION,
        DIFFERENCE
    }

    private enum AggregateMode {
        SUM,
        MIN,
        MAX
    }

    private RedisMessage executeZSetOperation(
            RedisCommand command, String cmdName, boolean isStore, SetOperation operation)
            throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for '" + cmdName + "' command");
        }

        int argIndex = 0;
        String destinationKey = null;

        if (isStore) {
            destinationKey = command.getArgAsString(argIndex++);
        }

        long numKeys;
        try {
            numKeys = Long.parseLong(command.getArgAsString(argIndex++));
            if (numKeys < 1) {
                return RedisResponse.error("ERR at least 1 input key is needed");
            }
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not an integer or out of range");
        }

        if (argIndex + numKeys > command.getArgCount()) {
            return RedisResponse.error("ERR syntax error");
        }

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            keys.add(command.getArgAsString(argIndex++));
        }

        // Parse WEIGHTS
        Map<String, Double> weights = new HashMap<>();
        AggregateMode aggregateMode = AggregateMode.SUM;

        while (argIndex < command.getArgCount()) {
            String option = command.getArgAsString(argIndex++).toUpperCase();

            if ("WEIGHTS".equals(option)) {
                if (argIndex + numKeys > command.getArgCount()) {
                    return RedisResponse.error("ERR syntax error");
                }
                for (int i = 0; i < numKeys; i++) {
                    try {
                        double weight = Double.parseDouble(command.getArgAsString(argIndex++));
                        weights.put(keys.get(i), weight);
                    } catch (NumberFormatException e) {
                        return RedisResponse.error("ERR weight value is not a float");
                    }
                }
            } else if ("AGGREGATE".equals(option)) {
                if (argIndex >= command.getArgCount()) {
                    return RedisResponse.error("ERR syntax error");
                }
                String aggStr = command.getArgAsString(argIndex++).toUpperCase();
                switch (aggStr) {
                    case "SUM":
                        aggregateMode = AggregateMode.SUM;
                        break;
                    case "MIN":
                        aggregateMode = AggregateMode.MIN;
                        break;
                    case "MAX":
                        aggregateMode = AggregateMode.MAX;
                        break;
                    default:
                        return RedisResponse.error("ERR syntax error");
                }
            } else if ("WITHSCORES".equals(option)) {
                // WITHSCORES only valid for non-STORE variants
                if (isStore) {
                    return RedisResponse.error("ERR syntax error");
                }
            } else {
                return RedisResponse.error("ERR syntax error");
            }
        }

        // Load all sorted sets
        List<Map<String, Double>> sets = new ArrayList<>();
        for (String k : keys) {
            if (expirationManager.checkAndDeleteIfExpired(k)) {
                sets.add(Collections.emptyMap());
                continue;
            }

            String type = adapter.getType(k);
            if (type != null && !type.equals(REDIS_TYPE)) {
                return RedisResponse.error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            if (type == null) {
                sets.add(Collections.emptyMap());
                continue;
            }

            List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(k);
            items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

            Map<String, Double> memberScores = new HashMap<>();
            for (RedisSingleTableAdapter.KeyValue kv : items) {
                try {
                    ScoreEncoder.ScoreMember sm = ScoreEncoder.decodeWithMember(kv.subKey);
                    double weight = weights.getOrDefault(k, 1.0);
                    memberScores.put(sm.member, sm.score * weight);
                } catch (Exception e) {
                    // Skip invalid entries
                }
            }
            sets.add(memberScores);
        }

        // Compute result based on operation
        Map<String, Double> result = computeSetOperation(sets, operation, aggregateMode);

        if (isStore) {
            // STORE variant - save to destination and return count
            if (!result.isEmpty()) {
                // Clear destination if exists
                String existingType = adapter.getType(destinationKey);
                if (existingType != null) {
                    if (!existingType.equals(REDIS_TYPE)) {
                        return RedisResponse.error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value");
                    }
                    // Delete existing data
                    List<RedisSingleTableAdapter.KeyValue> existing = adapter.scanByKey(destinationKey);
                    for (RedisSingleTableAdapter.KeyValue kv : existing) {
                        adapter.deleteByCompositeKey(destinationKey, kv.subKey);
                    }
                }

                // Store result
                ZSetMetadata metadata = new ZSetMetadata();
                for (Map.Entry<String, Double> entry : result.entrySet()) {
                    String member = entry.getKey();
                    double score = entry.getValue();
                    String subKey = ScoreEncoder.encodeWithMember(score, member);
                    adapter.setByCompositeKey(destinationKey, REDIS_TYPE, subKey, EMPTY_VALUE, score);
                    reverseIndex.setMemberScore(destinationKey, member, score);
                    metadata.incrementCount();
                }
                metadataManager.saveZSetMetadata(destinationKey, REDIS_TYPE, metadata);
            }
            return RedisResponse.integer(result.size());
        } else {
            // Non-STORE variant - return members with scores
            boolean withScores = argIndex > 0 && 
                    command.getArgAsString(command.getArgCount() - 1).equalsIgnoreCase("WITHSCORES");

            List<byte[]> results = new ArrayList<>();
            // Sort by score ascending, then by member lexicographically
            result.entrySet().stream()
                    .sorted(Map.Entry.<String, Double>comparingByValue()
                            .thenComparing(Map.Entry.comparingByKey()))
                    .forEach(entry -> {
                        results.add(entry.getKey().getBytes());
                        if (withScores) {
                            results.add(String.valueOf(entry.getValue()).getBytes());
                        }
                    });

            return RedisResponse.bytesArray(results);
        }
    }

    private Map<String, Double> computeSetOperation(
            List<Map<String, Double>> sets, SetOperation operation, AggregateMode aggregateMode) {
        if (sets.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Double> result = new HashMap<>();

        switch (operation) {
            case INTERSECT:
                // Members must appear in ALL sets
                if (sets.stream().anyMatch(Map::isEmpty)) {
                    return Collections.emptyMap();
                }
                Set<String> commonMembers = new HashSet<>(sets.get(0).keySet());
                for (int i = 1; i < sets.size(); i++) {
                    commonMembers.retainAll(sets.get(i).keySet());
                }
                for (String member : commonMembers) {
                    result.put(member, aggregateScores(sets, member, aggregateMode));
                }
                break;

            case UNION:
                // Members appear in ANY set
                Set<String> allMembers = new HashSet<>();
                for (Map<String, Double> set : sets) {
                    allMembers.addAll(set.keySet());
                }
                for (String member : allMembers) {
                    result.put(member, aggregateScores(sets, member, aggregateMode));
                }
                break;

            case DIFFERENCE:
                // Members in first set NOT in others
                if (sets.get(0).isEmpty()) {
                    return Collections.emptyMap();
                }
                Set<String> firstSetMembers = new HashSet<>(sets.get(0).keySet());
                for (int i = 1; i < sets.size(); i++) {
                    firstSetMembers.removeAll(sets.get(i).keySet());
                }
                for (String member : firstSetMembers) {
                    result.put(member, sets.get(0).get(member));
                }
                break;
        }

        return result;
    }

    private double aggregateScores(
            List<Map<String, Double>> sets, String member, AggregateMode mode) {
        List<Double> scores = new ArrayList<>();
        for (Map<String, Double> set : sets) {
            if (set.containsKey(member)) {
                scores.add(set.get(member));
            }
        }

        if (scores.isEmpty()) {
            return 0.0;
        }

        switch (mode) {
            case SUM:
                return scores.stream().mapToDouble(Double::doubleValue).sum();
            case MIN:
                return scores.stream().mapToDouble(Double::doubleValue).min().orElse(0.0);
            case MAX:
                return scores.stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
            default:
                return 0.0;
        }
    }

    private RedisMessage executeZScan(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'zscan' command");
        }

        String key = command.getArgAsString(0);
        long cursor;
        try {
            cursor = Long.parseLong(command.getArgAsString(1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR invalid cursor");
        }

        if (expirationManager.checkAndDeleteIfExpired(key)) {
            List<RedisMessage> result = new ArrayList<>();
            result.add(RedisResponse.bulkString("0".getBytes()));
            result.add(RedisResponse.bytesArray(Collections.emptyList()));
            return RedisResponse.array(result);
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            List<RedisMessage> result = new ArrayList<>();
            result.add(RedisResponse.bulkString("0".getBytes()));
            result.add(RedisResponse.bytesArray(Collections.emptyList()));
            return RedisResponse.array(result);
        }

        int count = 10;
        for (int i = 2; i < command.getArgCount(); i++) {
            String arg = command.getArgAsString(i).toUpperCase();
            if ("COUNT".equals(arg) && i + 1 < command.getArgCount()) {
                try {
                    count = Integer.parseInt(command.getArgAsString(++i));
                } catch (NumberFormatException e) {
                    return RedisResponse.error("ERR value is not an integer or out of range");
                }
            }
        }

        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

        List<String> memberScorePairs = new ArrayList<>();
        for (RedisSingleTableAdapter.KeyValue kv : items) {
            ScoreEncoder.ScoreMember sm = ScoreEncoder.decodeWithMember(kv.subKey);
            memberScorePairs.add(sm.member);
            memberScorePairs.add(String.valueOf(sm.score));
        }

        ScanCursorManager.ScanResult scanResult = cursorManager.scan(cursor, memberScorePairs, count * 2);

        List<byte[]> resultBytes = new ArrayList<>();
        for (String item : scanResult.items) {
            resultBytes.add(item.getBytes());
        }

        List<RedisMessage> result = new ArrayList<>();
        result.add(RedisResponse.bulkString(String.valueOf(scanResult.nextCursor).getBytes()));
        result.add(RedisResponse.bytesArray(resultBytes));

        return RedisResponse.array(result);
    }

    private RedisMessage executePopMinForBlocking(String key) throws Exception {
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.nullBulkString();
        }

        String existingType = adapter.getType(key);
        if (existingType == null || !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.nullBulkString();
        }

        ZSetMetadata metadata = metadataManager.getZSetMetadata(key);
        if (metadata == null || metadata.getCount() == 0) {
            return RedisResponse.nullBulkString();
        }

        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

        if (items.isEmpty()) {
            return RedisResponse.nullBulkString();
        }

        items.sort(Comparator.comparing(kv -> 
            ScoreEncoder.decodeWithMember(kv.subKey).score));

        RedisSingleTableAdapter.KeyValue minItem = items.get(0);
        ScoreEncoder.ScoreMember sm = ScoreEncoder.decodeWithMember(minItem.subKey);

        adapter.deleteByCompositeKey(key, minItem.subKey);
        reverseIndex.deleteMember(key, sm.member);

        metadata.decrementCount();
        metadataManager.saveZSetMetadata(key, REDIS_TYPE, metadata);

        List<byte[]> response = new ArrayList<>();
        response.add(key.getBytes());
        response.add(sm.member.getBytes());
        response.add(String.valueOf(sm.score).getBytes());

        return RedisResponse.bytesArray(response);
    }

    private RedisMessage executePopMaxForBlocking(String key) throws Exception {
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.nullBulkString();
        }

        String existingType = adapter.getType(key);
        if (existingType == null || !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.nullBulkString();
        }

        ZSetMetadata metadata = metadataManager.getZSetMetadata(key);
        if (metadata == null || metadata.getCount() == 0) {
            return RedisResponse.nullBulkString();
        }

        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

        if (items.isEmpty()) {
            return RedisResponse.nullBulkString();
        }

        Comparator<RedisSingleTableAdapter.KeyValue> byScore = 
            Comparator.comparing(kv -> ScoreEncoder.decodeWithMember(kv.subKey).score);
        items.sort(byScore.reversed());

        RedisSingleTableAdapter.KeyValue maxItem = items.get(0);
        ScoreEncoder.ScoreMember sm = ScoreEncoder.decodeWithMember(maxItem.subKey);

        adapter.deleteByCompositeKey(key, maxItem.subKey);
        reverseIndex.deleteMember(key, sm.member);

        metadata.decrementCount();
        metadataManager.saveZSetMetadata(key, REDIS_TYPE, metadata);

        List<byte[]> response = new ArrayList<>();
        response.add(key.getBytes());
        response.add(sm.member.getBytes());
        response.add(String.valueOf(sm.score).getBytes());

        return RedisResponse.bytesArray(response);
    }

    private RedisMessage executeBZPopMin(RedisCommand command, ChannelHandlerContext ctx, BlockingOperationQueue blockingQueue) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'BZPOPMIN' command");
        }

        int timeout;
        try {
            timeout = Integer.parseInt(command.getArgAsString(command.getArgCount() - 1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR timeout is not an integer or out of range");
        }

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < command.getArgCount() - 1; i++) {
            keys.add(command.getArgAsString(i));
        }

        for (String key : keys) {
            if (expirationManager.checkAndDeleteIfExpired(key)) {
                continue;
            }

            String existingType = adapter.getType(key);
            if (existingType != null && !existingType.equals(REDIS_TYPE)) {
                return RedisResponse.error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            if (existingType != null) {
                ZSetMetadata metadata = metadataManager.getZSetMetadata(key);
                if (metadata != null && metadata.getCount() > 0) {
                    List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
                    items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

                    if (!items.isEmpty()) {
                        items.sort(Comparator.comparing(kv -> 
                            ScoreEncoder.decodeWithMember(kv.subKey).score));

                        RedisSingleTableAdapter.KeyValue minItem = items.get(0);
                        ScoreEncoder.ScoreMember sm = ScoreEncoder.decodeWithMember(minItem.subKey);

                        adapter.deleteByCompositeKey(key, minItem.subKey);
                        reverseIndex.deleteMember(key, sm.member);

                        metadata.decrementCount();
                        metadataManager.saveZSetMetadata(key, REDIS_TYPE, metadata);

                        List<byte[]> response = new ArrayList<>();
                        response.add(key.getBytes());
                        response.add(sm.member.getBytes());
                        response.add(String.valueOf(sm.score).getBytes());

                        return RedisResponse.bytesArray(response);
                    }
                }
            }
        }

        blockingQueue.registerBlockingRequest(ctx, keys, timeout);
        return null;
    }

    private RedisMessage executeBZPopMax(RedisCommand command, ChannelHandlerContext ctx, BlockingOperationQueue blockingQueue) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'BZPOPMAX' command");
        }

        int timeout;
        try {
            timeout = Integer.parseInt(command.getArgAsString(command.getArgCount() - 1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR timeout is not an integer or out of range");
        }

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < command.getArgCount() - 1; i++) {
            keys.add(command.getArgAsString(i));
        }

        for (String key : keys) {
            if (expirationManager.checkAndDeleteIfExpired(key)) {
                continue;
            }

            String existingType = adapter.getType(key);
            if (existingType != null && !existingType.equals(REDIS_TYPE)) {
                return RedisResponse.error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            if (existingType != null) {
                ZSetMetadata metadata = metadataManager.getZSetMetadata(key);
                if (metadata != null && metadata.getCount() > 0) {
                    List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
                    items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

                    if (!items.isEmpty()) {
                        Comparator<RedisSingleTableAdapter.KeyValue> byScore = 
                            Comparator.comparing(kv -> ScoreEncoder.decodeWithMember(kv.subKey).score);
                        items.sort(byScore.reversed());

                        RedisSingleTableAdapter.KeyValue maxItem = items.get(0);
                        ScoreEncoder.ScoreMember sm = ScoreEncoder.decodeWithMember(maxItem.subKey);

                        adapter.deleteByCompositeKey(key, maxItem.subKey);
                        reverseIndex.deleteMember(key, sm.member);

                        metadata.decrementCount();
                        metadataManager.saveZSetMetadata(key, REDIS_TYPE, metadata);

                        List<byte[]> response = new ArrayList<>();
                        response.add(key.getBytes());
                        response.add(sm.member.getBytes());
                        response.add(String.valueOf(sm.score).getBytes());

                        return RedisResponse.bytesArray(response);
                    }
                }
            }
        }

        blockingQueue.registerBlockingRequest(ctx, keys, timeout);
        return null;
    }
}
