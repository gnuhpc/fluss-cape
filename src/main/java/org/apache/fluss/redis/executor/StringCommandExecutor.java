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

import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class StringCommandExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(StringCommandExecutor.class);

    private final RedisFlussAdapter adapter;
    private final ExpirationManager expirationManager;

    public StringCommandExecutor(RedisFlussAdapter adapter) {
        this.adapter = adapter;
        this.expirationManager = new ExpirationManager(adapter);
    }

    @Override
    public RedisMessage execute(RedisCommand command) {
        String cmd = command.getCommand();

        try {
            switch (cmd) {
                case "GET":
                    return executeGet(command);
                case "SET":
                    return executeSet(command);
                case "DEL":
                    return executeDel(command);
                case "EXISTS":
                    return executeExists(command);
                case "INCR":
                    return executeIncr(command);
                case "INCRBY":
                    return executeIncrBy(command);
                case "MGET":
                    return executeMGet(command);
                case "MSET":
                    return executeMSet(command);
                case "SETNX":
                    return executeSetnx(command);
                case "SETEX":
                    return executeSetex(command);
                case "PSETEX":
                    return executePsetex(command);
                case "MSETNX":
                    return executeMsetnx(command);
                case "GETSET":
                    return executeGetset(command);
                case "APPEND":
                    return executeAppend(command);
                case "STRLEN":
                    return executeStrlen(command);
                case "DECR":
                    return executeDecr(command);
                case "DECRBY":
                    return executeDecrby(command);
                case "PING":
                    return RedisResponse.bulkString("PONG");
                default:
                    return RedisResponse.error("ERR unknown command '" + cmd + "'");
            }
        } catch (Exception e) {
            LOG.error("Error executing command: {}", cmd, e);
            return RedisResponse.error("ERR " + e.getMessage());
        }
    }

    private RedisMessage executeGet(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'get' command");
        }

        byte[] key = command.getArg(0);
        String keyStr = new String(key);

        if (expirationManager.checkAndDeleteIfExpired(keyStr)) {
            return RedisResponse.nullBulkString();
        }

        byte[] value = adapter.get(key);

        if (value == null) {
            return RedisResponse.nullBulkString();
        }

        return RedisResponse.bulkString(value);
    }

    private RedisMessage executeSet(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'set' command");
        }

        byte[] key = command.getArg(0);
        byte[] value = command.getArg(1);

        adapter.set(key, value);
        return RedisResponse.ok();
    }

    private RedisMessage executeDel(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'del' command");
        }

        int deleted = 0;
        for (int i = 0; i < command.getArgCount(); i++) {
            byte[] key = command.getArg(i);
            if (adapter.exists(key)) {
                adapter.delete(key);
                deleted++;
            }
        }

        return RedisResponse.integer(deleted);
    }

    private RedisMessage executeExists(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'exists' command");
        }

        int count = 0;
        for (int i = 0; i < command.getArgCount(); i++) {
            byte[] key = command.getArg(i);
            String keyStr = new String(key);

            if (expirationManager.checkAndDeleteIfExpired(keyStr)) {
                continue;
            }

            if (adapter.exists(key)) {
                count++;
            }
        }

        return RedisResponse.integer(count);
    }

    private RedisMessage executeIncr(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'incr' command");
        }

        byte[] key = command.getArg(0);
        String keyStr = new String(key);

        expirationManager.checkAndDeleteIfExpired(keyStr);

        long newValue = adapter.incr(key);
        return RedisResponse.integer(newValue);
    }

    private RedisMessage executeIncrBy(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'incrby' command");
        }

        byte[] key = command.getArg(0);
        String keyStr = new String(key);

        expirationManager.checkAndDeleteIfExpired(keyStr);

        long increment = Long.parseLong(command.getArgAsString(1));
        long newValue = adapter.incrBy(key, increment);
        return RedisResponse.integer(newValue);
    }

    private RedisMessage executeMGet(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'mget' command");
        }

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < command.getArgCount(); i++) {
            keys.add(command.getArgAsString(i));
        }

        List<RedisMessage> values = new ArrayList<>();
        for (String key : keys) {
            if (expirationManager.checkAndDeleteIfExpired(key)) {
                values.add(RedisResponse.nullBulkString());
            } else {
                byte[] value = adapter.get(key.getBytes());
                if (value == null) {
                    values.add(RedisResponse.nullBulkString());
                } else {
                    values.add(RedisResponse.bulkString(value));
                }
            }
        }

        return new ArrayRedisMessage(values);
    }

    private RedisMessage executeMSet(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2 || command.getArgCount() % 2 != 0) {
            return RedisResponse.error("ERR wrong number of arguments for 'mset' command");
        }

        List<String> keys = new ArrayList<>();
        List<byte[]> values = new ArrayList<>();

        for (int i = 0; i < command.getArgCount(); i += 2) {
            keys.add(command.getArgAsString(i));
            values.add(command.getArg(i + 1));
        }

        adapter.multiSet(keys, values);
        return RedisResponse.ok();
    }

    private RedisMessage executeSetnx(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'setnx' command");
        }

        byte[] key = command.getArg(0);
        String keyStr = new String(key);
        byte[] value = command.getArg(1);

        if (expirationManager.checkAndDeleteIfExpired(keyStr)) {
            adapter.set(key, value);
            return RedisResponse.integer(1);
        }

        if (adapter.exists(key)) {
            return RedisResponse.integer(0);
        }

        adapter.set(key, value);
        return RedisResponse.integer(1);
    }

    private RedisMessage executeSetex(RedisCommand command) throws Exception {
        if (command.getArgCount() < 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'setex' command");
        }

        byte[] key = command.getArg(0);
        String keyStr = new String(key);
        long seconds = Long.parseLong(command.getArgAsString(1));
        byte[] value = command.getArg(2);

        adapter.set(key, value);
        long expireAtMs = System.currentTimeMillis() + (seconds * 1000);
        expirationManager.setExpiration(keyStr, expireAtMs);

        return RedisResponse.ok();
    }

    private RedisMessage executePsetex(RedisCommand command) throws Exception {
        if (command.getArgCount() < 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'psetex' command");
        }

        byte[] key = command.getArg(0);
        String keyStr = new String(key);
        long milliseconds = Long.parseLong(command.getArgAsString(1));
        byte[] value = command.getArg(2);

        adapter.set(key, value);
        long expireAtMs = System.currentTimeMillis() + milliseconds;
        expirationManager.setExpiration(keyStr, expireAtMs);

        return RedisResponse.ok();
    }

    private RedisMessage executeMsetnx(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2 || command.getArgCount() % 2 != 0) {
            return RedisResponse.error("ERR wrong number of arguments for 'msetnx' command");
        }

        List<String> keys = new ArrayList<>();
        List<byte[]> values = new ArrayList<>();

        for (int i = 0; i < command.getArgCount(); i += 2) {
            keys.add(command.getArgAsString(i));
            values.add(command.getArg(i + 1));
        }

        for (String key : keys) {
            if (expirationManager.checkAndDeleteIfExpired(key)) {
                continue;
            }
            if (adapter.exists(key.getBytes())) {
                return RedisResponse.integer(0);
            }
        }

        adapter.multiSet(keys, values);
        return RedisResponse.integer(1);
    }

    private RedisMessage executeGetset(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'getset' command");
        }

        byte[] key = command.getArg(0);
        String keyStr = new String(key);
        byte[] newValue = command.getArg(1);

        if (expirationManager.checkAndDeleteIfExpired(keyStr)) {
            adapter.set(key, newValue);
            return RedisResponse.nullBulkString();
        }

        byte[] oldValue = adapter.get(key);
        adapter.set(key, newValue);

        if (oldValue == null) {
            return RedisResponse.nullBulkString();
        }

        return RedisResponse.bulkString(oldValue);
    }

    private RedisMessage executeAppend(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'append' command");
        }

        byte[] key = command.getArg(0);
        String keyStr = new String(key);
        byte[] appendValue = command.getArg(1);

        if (expirationManager.checkAndDeleteIfExpired(keyStr)) {
            adapter.set(key, appendValue);
            return RedisResponse.integer(appendValue.length);
        }

        byte[] currentValue = adapter.get(key);
        byte[] newValue;

        if (currentValue == null) {
            newValue = appendValue;
        } else {
            newValue = new byte[currentValue.length + appendValue.length];
            System.arraycopy(currentValue, 0, newValue, 0, currentValue.length);
            System.arraycopy(appendValue, 0, newValue, currentValue.length, appendValue.length);
        }

        adapter.set(key, newValue);
        return RedisResponse.integer(newValue.length);
    }

    private RedisMessage executeStrlen(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'strlen' command");
        }

        byte[] key = command.getArg(0);
        String keyStr = new String(key);

        if (expirationManager.checkAndDeleteIfExpired(keyStr)) {
            return RedisResponse.integer(0);
        }

        byte[] value = adapter.get(key);

        if (value == null) {
            return RedisResponse.integer(0);
        }

        return RedisResponse.integer(value.length);
    }

    private RedisMessage executeDecr(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'decr' command");
        }

        byte[] key = command.getArg(0);
        String keyStr = new String(key);

        expirationManager.checkAndDeleteIfExpired(keyStr);

        byte[] current = adapter.get(key);
        long value = 0;
        if (current != null) {
            try {
                value = Long.parseLong(new String(current));
            } catch (NumberFormatException e) {
                return RedisResponse.error("ERR value is not an integer or out of range");
            }
        }
        value--;
        adapter.set(key, String.valueOf(value).getBytes());
        return RedisResponse.integer(value);
    }

    private RedisMessage executeDecrby(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'decrby' command");
        }

        byte[] key = command.getArg(0);
        String keyStr = new String(key);

        expirationManager.checkAndDeleteIfExpired(keyStr);

        long decrement;
        try {
            decrement = Long.parseLong(command.getArgAsString(1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not an integer or out of range");
        }

        byte[] current = adapter.get(key);
        long value = 0;
        if (current != null) {
            try {
                value = Long.parseLong(new String(current));
            } catch (NumberFormatException e) {
                return RedisResponse.error("ERR value is not an integer or out of range");
            }
        }
        value -= decrement;
        adapter.set(key, String.valueOf(value).getBytes());
        return RedisResponse.integer(value);
    }
}
