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
import org.apache.fluss.redis.protocol.RedisCommand;
import org.apache.fluss.redis.protocol.RedisResponse;
import org.apache.fluss.redis.storage.RedisFlussAdapter;

import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HashCommandExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(HashCommandExecutor.class);
    private static final String REDIS_TYPE = "hash";

    private final RedisFlussAdapter adapter;
    private final ExpirationManager expirationManager;

    public HashCommandExecutor(RedisFlussAdapter adapter) {
        this.adapter = adapter;
        this.expirationManager = new ExpirationManager(adapter);
    }

    @Override
    public RedisMessage execute(RedisCommand command) {
        String cmd = command.getCommand();

        try {
            switch (cmd) {
                case "HGET":
                    return executeHGet(command);
                case "HSET":
                    return executeHSet(command);
                case "HDEL":
                    return executeHDel(command);
                case "HEXISTS":
                    return executeHExists(command);
                case "HGETALL":
                    return executeHGetAll(command);
                case "HKEYS":
                    return executeHKeys(command);
                case "HVALS":
                    return executeHVals(command);
                case "HLEN":
                    return executeHLen(command);
                case "HMGET":
                    return executeHMGet(command);
                case "HMSET":
                    return executeHMSet(command);
                default:
                    return RedisResponse.error("ERR unknown command '" + cmd + "'");
            }
        } catch (Exception e) {
            LOG.error("Error executing command: {}", cmd, e);
            return RedisResponse.error("ERR " + e.getMessage());
        }
    }

    private RedisMessage executeHGet(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'hget' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.nullBulkString();
        }
        
        String field = command.getArgAsString(1);
        byte[] value = adapter.getByCompositeKey(key, field);

        if (value == null) {
            return RedisResponse.nullBulkString();
        }

        return RedisResponse.bulkString(value);
    }

    private RedisMessage executeHSet(RedisCommand command) throws Exception {
        if (command.getArgCount() < 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'hset' command");
        }

        String key = command.getArgAsString(0);
        int fieldsSet = 0;

        for (int i = 1; i < command.getArgCount(); i += 2) {
            if (i + 1 >= command.getArgCount()) {
                return RedisResponse.error("ERR wrong number of arguments for 'hset' command");
            }

            String field = command.getArgAsString(i);
            byte[] value = command.getArg(i + 1);

            boolean exists = adapter.getByCompositeKey(key, field) != null;
            adapter.setByCompositeKey(key, REDIS_TYPE, field, value, null);

            if (!exists) {
                fieldsSet++;
            }
        }

        return RedisResponse.integer(fieldsSet);
    }

    private RedisMessage executeHDel(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'hdel' command");
        }

        String key = command.getArgAsString(0);
        int deleted = 0;

        for (int i = 1; i < command.getArgCount(); i++) {
            String field = command.getArgAsString(i);

            if (adapter.getByCompositeKey(key, field) != null) {
                adapter.deleteByCompositeKey(key, field);
                deleted++;
            }
        }

        return RedisResponse.integer(deleted);
    }

    private RedisMessage executeHExists(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'hexists' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.integer(0);
        }
        
        String field = command.getArgAsString(1);
        byte[] value = adapter.getByCompositeKey(key, field);

        return RedisResponse.integer(value != null ? 1 : 0);
    }

    private RedisMessage executeHGetAll(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'hgetall' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }
        
        List<RedisFlussAdapter.KeyValue> kvList = adapter.scanByKey(key);

        List<byte[]> results = new ArrayList<>();
        for (RedisFlussAdapter.KeyValue kv : kvList) {
            results.add(kv.subKey.getBytes());
            results.add(kv.value);
        }

        return RedisResponse.bytesArray(results);
    }

    private RedisMessage executeHKeys(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'hkeys' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }
        
        List<RedisFlussAdapter.KeyValue> kvList = adapter.scanByKey(key);

        List<byte[]> results = new ArrayList<>();
        for (RedisFlussAdapter.KeyValue kv : kvList) {
            results.add(kv.subKey.getBytes());
        }

        return RedisResponse.bytesArray(results);
    }

    private RedisMessage executeHVals(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'hvals' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }
        
        List<RedisFlussAdapter.KeyValue> kvList = adapter.scanByKey(key);

        List<byte[]> results = new ArrayList<>();
        for (RedisFlussAdapter.KeyValue kv : kvList) {
            results.add(kv.value);
        }

        return RedisResponse.bytesArray(results);
    }

    private RedisMessage executeHLen(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'hlen' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.integer(0);
        }
        
        long count = adapter.countByKey(key);

        return RedisResponse.integer(count);
    }

    private RedisMessage executeHMGet(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'hmget' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            List<byte[]> nulls = new ArrayList<>();
            for (int i = 1; i < command.getArgCount(); i++) {
                nulls.add(null);
            }
            return RedisResponse.bytesArray(nulls);
        }
        
        List<byte[]> results = new ArrayList<>();

        for (int i = 1; i < command.getArgCount(); i++) {
            String field = command.getArgAsString(i);
            byte[] value = adapter.getByCompositeKey(key, field);

            results.add(value);
        }

        return RedisResponse.bytesArray(results);
    }

    private RedisMessage executeHMSet(RedisCommand command) throws Exception {
        if (command.getArgCount() < 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'hmset' command");
        }

        String key = command.getArgAsString(0);

        for (int i = 1; i < command.getArgCount(); i += 2) {
            if (i + 1 >= command.getArgCount()) {
                return RedisResponse.error("ERR wrong number of arguments for 'hmset' command");
            }

            String field = command.getArgAsString(i);
            byte[] value = command.getArg(i + 1);

            adapter.setByCompositeKey(key, REDIS_TYPE, field, value, null);
        }

        return RedisResponse.ok();
    }
}
