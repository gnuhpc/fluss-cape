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

package org.apache.fluss.redis.executor;

import org.apache.fluss.client.Connection;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.redis.expiration.ExpirationManager;
import org.apache.fluss.redis.metadata.MetadataManager;
import org.apache.fluss.redis.metadata.ZSetMetadata;
import org.apache.fluss.redis.protocol.RedisCommand;
import org.apache.fluss.redis.protocol.RedisResponse;
import org.apache.fluss.redis.storage.RedisFlussAdapter;
import org.apache.fluss.redis.storage.ZSetReverseIndex;
import org.apache.fluss.redis.util.ScoreEncoder;

import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SortedSetCommandExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(SortedSetCommandExecutor.class);
    private static final String REDIS_TYPE = "zset";
    private static final String METADATA_SUB_KEY = "__meta__";
    private static final byte[] EMPTY_VALUE = new byte[0];

    private final RedisFlussAdapter adapter;
    private final MetadataManager metadataManager;
    private final ZSetReverseIndex reverseIndex;
    private final ExpirationManager expirationManager;

    public SortedSetCommandExecutor(
            RedisFlussAdapter adapter, Connection connection, String reverseIndexTableName)
            throws Exception {
        this.adapter = adapter;
        this.metadataManager = new MetadataManager(adapter);
        this.reverseIndex =
                new ZSetReverseIndex(connection, TablePath.of("default", reverseIndexTableName));
        this.expirationManager = new ExpirationManager(adapter);
        LOG.info("Initialized SortedSetCommandExecutor with reverse index: {}", reverseIndexTableName);
    }

    @Override
    public RedisMessage execute(RedisCommand command) {
        String cmd = command.getCommand();

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
                default:
                    return RedisResponse.error("ERR unknown sorted set command '" + cmd + "'");
            }
        } catch (Exception e) {
            LOG.error("Error executing sorted set command: {}", cmd, e);
            return RedisResponse.error("ERR " + e.getMessage());
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

        List<RedisFlussAdapter.KeyValue> items = adapter.scanByKey(key);
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
            RedisFlussAdapter.KeyValue kv = items.get((int) i);
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

        List<RedisFlussAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

        if (items.isEmpty()) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }

        items.sort((a, b) -> a.subKey.compareTo(b.subKey));

        List<byte[]> results = new ArrayList<>();
        for (RedisFlussAdapter.KeyValue kv : items) {
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
}
