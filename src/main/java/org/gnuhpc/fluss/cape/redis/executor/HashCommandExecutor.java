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

import org.gnuhpc.fluss.cape.redis.expiration.ExpirationManager;
import org.gnuhpc.fluss.cape.redis.protocol.RedisCommand;
import org.gnuhpc.fluss.cape.redis.protocol.RedisResponse;
import org.gnuhpc.fluss.cape.redis.storage.RedisSingleTableAdapter;
import org.gnuhpc.fluss.cape.redis.storage.RedisStorageAdapter;
import org.gnuhpc.fluss.cape.redis.util.RedisErrorSanitizer;

import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HashCommandExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(HashCommandExecutor.class);
    private static final String REDIS_TYPE = "hash";

    private final RedisStorageAdapter adapter;
    private final ExpirationManager expirationManager;

    public HashCommandExecutor(RedisStorageAdapter adapter) {
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
                case "HINCRBY":
                    return executeHIncrBy(command);
                case "HINCRBYFLOAT":
                    return executeHIncrByFloat(command);
                case "HRANDFIELD":
                    return executeHRandField(command);
                case "HSCAN":
                    return executeHScan(command);
                case "HSTRLEN":
                    return executeHStrLen(command);
                default:
                    return RedisResponse.error("ERR unknown command '" + cmd + "'");
            }
        } catch (Exception e) {
            LOG.error("Error executing command: {}", cmd, e);
            return RedisErrorSanitizer.sanitizeError(e, cmd);
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
        
        List<RedisSingleTableAdapter.KeyValue> kvList = adapter.scanByKey(key);

        List<byte[]> results = new ArrayList<>();
        for (RedisSingleTableAdapter.KeyValue kv : kvList) {
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
        
        List<RedisSingleTableAdapter.KeyValue> kvList = adapter.scanByKey(key);

        List<byte[]> results = new ArrayList<>();
        for (RedisSingleTableAdapter.KeyValue kv : kvList) {
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
        
        List<RedisSingleTableAdapter.KeyValue> kvList = adapter.scanByKey(key);

        List<byte[]> results = new ArrayList<>();
        for (RedisSingleTableAdapter.KeyValue kv : kvList) {
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

    private RedisMessage executeHIncrBy(RedisCommand command) throws Exception {
        if (command.getArgCount() != 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'hincrby' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
        }
        
        String field = command.getArgAsString(1);
        long increment;
        try {
            increment = Long.parseLong(command.getArgAsString(2));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not an integer or out of range");
        }

        byte[] currentBytes = adapter.getByCompositeKey(key, field);
        long currentValue = 0;
        
        if (currentBytes != null) {
            try {
                currentValue = Long.parseLong(new String(currentBytes));
            } catch (NumberFormatException e) {
                return RedisResponse.error("ERR hash value is not an integer");
            }
        }

        long newValue = currentValue + increment;
        adapter.setByCompositeKey(key, REDIS_TYPE, field, String.valueOf(newValue).getBytes(), null);

        return RedisResponse.integer(newValue);
    }

    private RedisMessage executeHIncrByFloat(RedisCommand command) throws Exception {
        if (command.getArgCount() != 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'hincrbyfloat' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
        }
        
        String field = command.getArgAsString(1);
        double increment;
        try {
            increment = Double.parseDouble(command.getArgAsString(2));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not a valid float");
        }

        byte[] currentBytes = adapter.getByCompositeKey(key, field);
        double currentValue = 0.0;
        
        if (currentBytes != null) {
            try {
                currentValue = Double.parseDouble(new String(currentBytes));
            } catch (NumberFormatException e) {
                return RedisResponse.error("ERR hash value is not a float");
            }
        }

        double newValue = currentValue + increment;
        String result = String.valueOf(newValue);
        adapter.setByCompositeKey(key, REDIS_TYPE, field, result.getBytes(), null);

        return RedisResponse.bulkString(result.getBytes());
    }

    private RedisMessage executeHRandField(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'hrandfield' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.nullBulkString();
        }

        int count = 1;
        boolean withValues = false;

        if (command.getArgCount() >= 2) {
            try {
                count = Integer.parseInt(command.getArgAsString(1));
            } catch (NumberFormatException e) {
                return RedisResponse.error("ERR value is not an integer or out of range");
            }
        }

        if (command.getArgCount() >= 3) {
            String option = command.getArgAsString(2).toUpperCase();
            if (option.equals("WITHVALUES")) {
                withValues = true;
            }
        }

        List<RedisSingleTableAdapter.KeyValue> kvList = adapter.scanByKey(key);

        if (kvList.isEmpty()) {
            if (count == 1) {
                return RedisResponse.nullBulkString();
            } else {
                return RedisResponse.bytesArray(Collections.emptyList());
            }
        }

        java.util.Random random = new java.util.Random();
        List<byte[]> results = new ArrayList<>();

        if (count == 1) {
            RedisSingleTableAdapter.KeyValue randomKv = kvList.get(random.nextInt(kvList.size()));
            if (withValues) {
                results.add(randomKv.subKey.getBytes());
                results.add(randomKv.value);
                return RedisResponse.bytesArray(results);
            } else {
                return RedisResponse.bulkString(randomKv.subKey.getBytes());
            }
        } else {
            int absCount = Math.abs(count);
            for (int i = 0; i < absCount; i++) {
                RedisSingleTableAdapter.KeyValue randomKv = kvList.get(random.nextInt(kvList.size()));
                results.add(randomKv.subKey.getBytes());
                if (withValues) {
                    results.add(randomKv.value);
                }
            }
            return RedisResponse.bytesArray(results);
        }
    }

    private RedisMessage executeHScan(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'hscan' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            List<byte[]> emptyResult = new ArrayList<>();
            emptyResult.add("0".getBytes());
            emptyResult.add(new byte[0]);
            return RedisResponse.bytesArray(emptyResult);
        }

        int cursor;
        try {
            cursor = Integer.parseInt(command.getArgAsString(1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR invalid cursor");
        }

        List<RedisSingleTableAdapter.KeyValue> kvList = adapter.scanByKey(key);

        List<byte[]> fieldsAndValues = new ArrayList<>();
        for (RedisSingleTableAdapter.KeyValue kv : kvList) {
            fieldsAndValues.add(kv.subKey.getBytes());
            fieldsAndValues.add(kv.value);
        }

        List<RedisMessage> result = new ArrayList<>();
        result.add(RedisResponse.bulkString("0".getBytes()));
        result.add(RedisResponse.bytesArray(fieldsAndValues));

        return RedisResponse.array(result);
    }

    private RedisMessage executeHStrLen(RedisCommand command) throws Exception {
        if (command.getArgCount() != 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'hstrlen' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.integer(0);
        }
        
        String field = command.getArgAsString(1);
        byte[] value = adapter.getByCompositeKey(key, field);

        if (value == null) {
            return RedisResponse.integer(0);
        }

        return RedisResponse.integer(value.length);
    }
}
