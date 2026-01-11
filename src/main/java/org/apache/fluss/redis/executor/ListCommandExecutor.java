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

import org.apache.fluss.redis.expiration.ExpirationManager;
import org.apache.fluss.redis.metadata.ListMetadata;
import org.apache.fluss.redis.metadata.MetadataManager;
import org.apache.fluss.redis.protocol.RedisCommand;
import org.apache.fluss.redis.protocol.RedisResponse;
import org.apache.fluss.redis.storage.RedisFlussAdapter;
import org.apache.fluss.redis.util.IndexEncoder;

import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ListCommandExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ListCommandExecutor.class);
    private static final String REDIS_TYPE = "list";
    private static final String METADATA_SUB_KEY = "__meta__";

    private final RedisFlussAdapter adapter;
    private final MetadataManager metadataManager;
    private final ExpirationManager expirationManager;

    public ListCommandExecutor(RedisFlussAdapter adapter) {
        this.adapter = adapter;
        this.metadataManager = new MetadataManager(adapter);
        this.expirationManager = new ExpirationManager(adapter);
    }

    @Override
    public RedisMessage execute(RedisCommand command) {
        String cmd = command.getCommand();

        try {
            switch (cmd) {
                case "LPUSH":
                    return executeLPush(command);
                case "RPUSH":
                    return executeRPush(command);
                case "LPOP":
                    return executeLPop(command);
                case "RPOP":
                    return executeRPop(command);
                case "LRANGE":
                    return executeLRange(command);
                case "LLEN":
                    return executeLLen(command);
                case "LINDEX":
                    return executeLIndex(command);
                default:
                    return RedisResponse.error("ERR unknown list command '" + cmd + "'");
            }
        } catch (Exception e) {
            LOG.error("Error executing list command: {}", cmd, e);
            return RedisResponse.error("ERR " + e.getMessage());
        }
    }

    private RedisMessage executeLPush(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'LPUSH' command");
        }

        String key = command.getArgAsString(0);
        List<byte[]> values = command.getArgs().subList(1, command.getArgs().size());

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        ListMetadata metadata = metadataManager.getOrCreateListMetadata(key);

        for (byte[]  value : values) {
            metadata.decrementHeadIndex();
            String subKey = IndexEncoder.encode(metadata.getHeadIndex());
            adapter.setByCompositeKey(key, REDIS_TYPE, subKey, value, null);
            metadata.incrementCount();
        }

        metadataManager.saveListMetadata(key, REDIS_TYPE, metadata);

        return RedisResponse.integer(metadata.getCount());
    }

    private RedisMessage executeRPush(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'RPUSH' command");
        }

        String key = command.getArgAsString(0);
        List<byte[]> values = command.getArgs().subList(1, command.getArgs().size());

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        ListMetadata metadata = metadataManager.getOrCreateListMetadata(key);

        for (byte[] value : values) {
            metadata.incrementTailIndex();
            String subKey = IndexEncoder.encode(metadata.getTailIndex());
            adapter.setByCompositeKey(key, REDIS_TYPE, subKey, value, null);
            metadata.incrementCount();
        }

        metadataManager.saveListMetadata(key, REDIS_TYPE, metadata);

        return RedisResponse.integer(metadata.getCount());
    }

    private RedisMessage executeLPop(RedisCommand command) throws Exception {
        if (command.getArgCount() != 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'LPOP' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.nullBulkString();
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.nullBulkString();
        }

        ListMetadata metadata = metadataManager.getListMetadata(key);
        if (metadata == null || metadata.isEmpty()) {
            return RedisResponse.nullBulkString();
        }

        String headSubKey = IndexEncoder.encode(metadata.getHeadIndex());
        byte[] value = adapter.getByCompositeKey(key, headSubKey);

        if (value == null) {
            return RedisResponse.nullBulkString();
        }

        adapter.deleteByCompositeKey(key, headSubKey);

        metadata.incrementHeadIndex();
        metadata.decrementCount();
        metadataManager.saveListMetadata(key, REDIS_TYPE, metadata);

        return RedisResponse.bulkString(value);
    }

    private RedisMessage executeRPop(RedisCommand command) throws Exception {
        if (command.getArgCount() != 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'RPOP' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.nullBulkString();
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.nullBulkString();
        }

        ListMetadata metadata = metadataManager.getListMetadata(key);
        if (metadata == null || metadata.isEmpty()) {
            return RedisResponse.nullBulkString();
        }

        String tailSubKey = IndexEncoder.encode(metadata.getTailIndex());
        byte[] value = adapter.getByCompositeKey(key, tailSubKey);

        if (value == null) {
            return RedisResponse.nullBulkString();
        }

        adapter.deleteByCompositeKey(key, tailSubKey);

        metadata.decrementTailIndex();
        metadata.decrementCount();
        metadataManager.saveListMetadata(key, REDIS_TYPE, metadata);

        return RedisResponse.bulkString(value);
    }

    private RedisMessage executeLRange(RedisCommand command) throws Exception {
        if (command.getArgCount() != 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'LRANGE' command");
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
            results.add(items.get((int) i).value);
        }

        return RedisResponse.bytesArray(results);
    }

    private RedisMessage executeLLen(RedisCommand command) throws Exception {
        if (command.getArgCount() != 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'LLEN' command");
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

        ListMetadata metadata = metadataManager.getListMetadata(key);
        if (metadata == null) {
            return RedisResponse.integer(0);
        }

        return RedisResponse.integer(metadata.getCount());
    }

    private RedisMessage executeLIndex(RedisCommand command) throws Exception {
        if (command.getArgCount() != 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'LINDEX' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.nullBulkString();
        }
        
        long index;

        try {
            index = Long.parseLong(command.getArgAsString(1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not an integer or out of range");
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.nullBulkString();
        }

        List<RedisFlussAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

        if (items.isEmpty()) {
            return RedisResponse.nullBulkString();
        }

        items.sort((a, b) -> a.subKey.compareTo(b.subKey));

        long totalCount = items.size();

        if (index < 0) {
            index = totalCount + index;
        }

        if (index < 0 || index >= totalCount) {
            return RedisResponse.nullBulkString();
        }

        return RedisResponse.bulkString(items.get((int) index).value);
    }
}
