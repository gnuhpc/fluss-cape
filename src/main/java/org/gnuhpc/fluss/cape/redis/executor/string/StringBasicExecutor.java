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

package org.gnuhpc.fluss.cape.redis.executor.string;
import java.nio.charset.StandardCharsets;

import org.gnuhpc.fluss.cape.redis.executor.RedisCommandExecutor;
import org.gnuhpc.fluss.cape.redis.expiration.ExpirationManager;
import org.gnuhpc.fluss.cape.redis.protocol.RedisCommand;
import org.gnuhpc.fluss.cape.redis.protocol.RedisResponse;
import org.gnuhpc.fluss.cape.redis.storage.RedisStorageAdapter;
import org.gnuhpc.fluss.cape.redis.util.RedisErrorSanitizer;

import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Executor for basic String operations (GET, SET, etc.)
 */
public class StringBasicExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(StringBasicExecutor.class);

    private final RedisStorageAdapter adapter;
    private final ExpirationManager expirationManager;

    public StringBasicExecutor(RedisStorageAdapter adapter) {
        this.adapter = adapter;
        this.expirationManager = new ExpirationManager(adapter);
    }

    @Override
    public RedisMessage execute(RedisCommand command) {
        String cmd = command.getCommand();

        try {
            switch (cmd) {
                case "GET": return executeGet(command);
                case "SET": return executeSet(command);
                case "DEL": return executeDel(command);
                case "EXISTS": return executeExists(command);
                case "MGET": return executeMGet(command);
                case "MSET": return executeMSet(command);
                case "SETNX": return executeSetnx(command);
                case "SETEX": return executeSetex(command);
                case "PSETEX": return executePsetex(command);
                case "MSETNX": return executeMsetnx(command);
                case "GETSET": return executeGetset(command);
                case "APPEND": return executeAppend(command);
                case "STRLEN": return executeStrlen(command);
                case "GETRANGE": return executeGetrange(command);
                case "SETRANGE": return executeSetrange(command);
                case "GETEX": return executeGetex(command);
                case "GETDEL": return executeGetdel(command);
                case "PING": return RedisResponse.bulkString("PONG");
                // SUBSTR is deprecated alias for GETRANGE
                case "SUBSTR": return executeGetrange(command);
                default:
                    return RedisResponse.error("ERR unknown command '" + cmd + "' in StringBasicExecutor");
            }
        } catch (Exception e) {
            return RedisErrorSanitizer.sanitizeError(e, cmd);
        }
    }

    private RedisMessage executeGet(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) return RedisResponse.error("ERR wrong number of arguments for 'get' command");
        byte[] key = command.getArg(0);
        String keyStr = new String(key);
        if (expirationManager.checkAndDeleteIfExpired(keyStr)) return RedisResponse.nullBulkString();
        byte[] value = adapter.get(key);
        return value == null ? RedisResponse.nullBulkString() : RedisResponse.bulkString(value);
    }

    private RedisMessage executeSet(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) return RedisResponse.error("ERR wrong number of arguments for 'set' command");
        byte[] key = command.getArg(0);
        byte[] value = command.getArg(1);
        String keyStr = new String(key);

        boolean nx = false;
        boolean xx = false;
        boolean get = false;
        boolean keepTtl = false;
        Long expirationMs = null;

        int argIdx = 2;
        while (argIdx < command.getArgCount()) {
            String option = new String(command.getArg(argIdx++)).toUpperCase();
            switch (option) {
                case "NX": nx = true; break;
                case "XX": xx = true; break;
                case "GET": get = true; break;
                case "KEEPTTL": keepTtl = true; break;
                case "EX":
                    if (argIdx >= command.getArgCount()) return RedisResponse.error("ERR syntax error");
                    expirationMs = Long.parseLong(new String(command.getArg(argIdx++))) * 1000;
                    break;
                case "PX":
                    if (argIdx >= command.getArgCount()) return RedisResponse.error("ERR syntax error");
                    expirationMs = Long.parseLong(new String(command.getArg(argIdx++)));
                    break;
                case "EXAT":
                    if (argIdx >= command.getArgCount()) return RedisResponse.error("ERR syntax error");
                    expirationMs = (Long.parseLong(new String(command.getArg(argIdx++))) * 1000) - System.currentTimeMillis();
                    break;
                case "PXAT":
                    if (argIdx >= command.getArgCount()) return RedisResponse.error("ERR syntax error");
                    expirationMs = Long.parseLong(new String(command.getArg(argIdx++))) - System.currentTimeMillis();
                    break;
                default: return RedisResponse.error("ERR syntax error");
            }
        }

        if (nx && xx) return RedisResponse.error("ERR NX and XX options at the same time are not compatible");

        byte[] oldValue = null;
        if (get) {
            if (!expirationManager.checkAndDeleteIfExpired(keyStr)) oldValue = adapter.get(key);
        }

        boolean exists = adapter.exists(key);
        if (nx && exists) return get ? (oldValue == null ? RedisResponse.nullBulkString() : RedisResponse.bulkString(oldValue)) : RedisResponse.nullBulkString();
        if (xx && !exists) return RedisResponse.nullBulkString();

        adapter.set(key, value);
        if (!keepTtl && expirationMs != null && expirationMs > 0) expirationManager.setExpiration(keyStr, expirationMs);

        if (get) return oldValue == null ? RedisResponse.nullBulkString() : RedisResponse.bulkString(oldValue);
        return RedisResponse.ok();
    }

    private RedisMessage executeDel(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) return RedisResponse.error("ERR wrong number of arguments for 'del' command");
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
        if (command.getArgCount() < 1) return RedisResponse.error("ERR wrong number of arguments for 'exists' command");
        int count = 0;
        for (int i = 0; i < command.getArgCount(); i++) {
            byte[] key = command.getArg(i);
            String keyStr = new String(key);
            if (expirationManager.checkAndDeleteIfExpired(keyStr)) continue;
            if (adapter.exists(key)) count++;
        }
        return RedisResponse.integer(count);
    }

    private RedisMessage executeMGet(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) return RedisResponse.error("ERR wrong number of arguments for 'mget' command");
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < command.getArgCount(); i++) keys.add(command.getArgAsString(i));

        List<RedisMessage> values = new ArrayList<>();
        for (String key : keys) {
            if (expirationManager.checkAndDeleteIfExpired(key)) {
                values.add(RedisResponse.nullBulkString());
            } else {
                byte[] value = adapter.get(key.getBytes(StandardCharsets.UTF_8));
                values.add(value == null ? RedisResponse.nullBulkString() : RedisResponse.bulkString(value));
            }
        }
        return new ArrayRedisMessage(values);
    }

    private RedisMessage executeMSet(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2 || command.getArgCount() % 2 != 0) return RedisResponse.error("ERR wrong number of arguments for 'mset' command");
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
        if (command.getArgCount() < 2) return RedisResponse.error("ERR wrong number of arguments for 'setnx' command");
        byte[] key = command.getArg(0);
        String keyStr = new String(key);
        byte[] value = command.getArg(1);
        if (expirationManager.checkAndDeleteIfExpired(keyStr)) {
            adapter.set(key, value);
            return RedisResponse.integer(1);
        }
        if (adapter.exists(key)) return RedisResponse.integer(0);
        adapter.set(key, value);
        return RedisResponse.integer(1);
    }

    private RedisMessage executeSetex(RedisCommand command) throws Exception {
        if (command.getArgCount() < 3) return RedisResponse.error("ERR wrong number of arguments for 'setex' command");
        byte[] key = command.getArg(0);
        long seconds = Long.parseLong(command.getArgAsString(1));
        byte[] value = command.getArg(2);
        adapter.set(key, value);
        expirationManager.setExpiration(new String(key), System.currentTimeMillis() + (seconds * 1000));
        return RedisResponse.ok();
    }

    private RedisMessage executePsetex(RedisCommand command) throws Exception {
        if (command.getArgCount() < 3) return RedisResponse.error("ERR wrong number of arguments for 'psetex' command");
        byte[] key = command.getArg(0);
        long milliseconds = Long.parseLong(command.getArgAsString(1));
        byte[] value = command.getArg(2);
        adapter.set(key, value);
        expirationManager.setExpiration(new String(key), System.currentTimeMillis() + milliseconds);
        return RedisResponse.ok();
    }

    private RedisMessage executeMsetnx(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2 || command.getArgCount() % 2 != 0) return RedisResponse.error("ERR wrong number of arguments for 'msetnx' command");
        List<String> keys = new ArrayList<>();
        List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < command.getArgCount(); i += 2) {
            keys.add(command.getArgAsString(i));
            values.add(command.getArg(i + 1));
        }
        for (String key : keys) {
            if (!expirationManager.checkAndDeleteIfExpired(key) && adapter.exists(key.getBytes(StandardCharsets.UTF_8))) return RedisResponse.integer(0);
        }
        adapter.multiSet(keys, values);
        return RedisResponse.integer(1);
    }

    private RedisMessage executeGetset(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) return RedisResponse.error("ERR wrong number of arguments for 'getset' command");
        byte[] key = command.getArg(0);
        String keyStr = new String(key);
        byte[] newValue = command.getArg(1);
        if (expirationManager.checkAndDeleteIfExpired(keyStr)) {
            adapter.set(key, newValue);
            return RedisResponse.nullBulkString();
        }
        byte[] oldValue = adapter.get(key);
        adapter.set(key, newValue);
        return oldValue == null ? RedisResponse.nullBulkString() : RedisResponse.bulkString(oldValue);
    }

    private RedisMessage executeAppend(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) return RedisResponse.error("ERR wrong number of arguments for 'append' command");
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
        if (command.getArgCount() < 1) return RedisResponse.error("ERR wrong number of arguments for 'strlen' command");
        byte[] key = command.getArg(0);
        String keyStr = new String(key);
        if (expirationManager.checkAndDeleteIfExpired(keyStr)) return RedisResponse.integer(0);
        byte[] value = adapter.get(key);
        return value == null ? RedisResponse.integer(0) : RedisResponse.integer(value.length);
    }

    private RedisMessage executeGetrange(RedisCommand command) throws Exception {
        if (command.getArgCount() < 3) return RedisResponse.error("ERR wrong number of arguments for 'getrange' command");
        byte[] key = command.getArg(0);
        String keyStr = new String(key);
        if (expirationManager.checkAndDeleteIfExpired(keyStr)) return RedisResponse.bulkString(new byte[0]);
        int start = Integer.parseInt(command.getArgAsString(1));
        int end = Integer.parseInt(command.getArgAsString(2));
        byte[] value = adapter.get(key);
        if (value == null || value.length == 0) return RedisResponse.bulkString(new byte[0]);
        int len = value.length;
        if (start < 0) start = len + start;
        if (end < 0) end = len + end;
        start = Math.max(0, start);
        end = Math.min(end, len - 1);
        if (start > end || start >= len) return RedisResponse.bulkString(new byte[0]);
        int rangeLen = end - start + 1;
        byte[] range = new byte[rangeLen];
        System.arraycopy(value, start, range, 0, rangeLen);
        return RedisResponse.bulkString(range);
    }

    private RedisMessage executeSetrange(RedisCommand command) throws Exception {
        if (command.getArgCount() < 3) return RedisResponse.error("ERR wrong number of arguments for 'setrange' command");
        byte[] key = command.getArg(0);
        String keyStr = new String(key);
        expirationManager.checkAndDeleteIfExpired(keyStr);
        int offset = Integer.parseInt(command.getArgAsString(1));
        if (offset < 0) return RedisResponse.error("ERR offset is out of range");
        byte[] newValue = command.getArg(2);
        byte[] existing = adapter.get(key);
        if (existing == null) existing = new byte[0];
        int requiredLen = offset + newValue.length;
        byte[] result = requiredLen > existing.length ? new byte[requiredLen] : existing;
        if (requiredLen > existing.length) System.arraycopy(existing, 0, result, 0, existing.length);
        System.arraycopy(newValue, 0, result, offset, newValue.length);
        adapter.set(key, result);
        return RedisResponse.integer(result.length);
    }
    
    private RedisMessage executeGetex(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) return RedisResponse.error("ERR wrong number of arguments for 'getex' command");
        byte[] key = command.getArg(0);
        String keyStr = new String(key);
        if (expirationManager.checkAndDeleteIfExpired(keyStr)) return RedisResponse.nullBulkString();
        byte[] value = adapter.get(key);
        if (value == null) return RedisResponse.nullBulkString();

        for (int i = 1; i < command.getArgCount(); i++) {
            String option = command.getArgAsString(i).toUpperCase();
            if (option.equals("EX") && i + 1 < command.getArgCount()) {
                expirationManager.setExpiration(keyStr, System.currentTimeMillis() + (Long.parseLong(command.getArgAsString(++i)) * 1000));
            } else if (option.equals("PX") && i + 1 < command.getArgCount()) {
                expirationManager.setExpiration(keyStr, System.currentTimeMillis() + Long.parseLong(command.getArgAsString(++i)));
            } else if (option.equals("EXAT") && i + 1 < command.getArgCount()) {
                expirationManager.setExpiration(keyStr, Long.parseLong(command.getArgAsString(++i)) * 1000);
            } else if (option.equals("PXAT") && i + 1 < command.getArgCount()) {
                expirationManager.setExpiration(keyStr, Long.parseLong(command.getArgAsString(++i)));
            } else if (option.equals("PERSIST")) {
                expirationManager.removeExpiration(keyStr);
            }
        }
        return RedisResponse.bulkString(value);
    }
    
    private RedisMessage executeGetdel(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) return RedisResponse.error("ERR wrong number of arguments for 'getdel' command");
        byte[] key = command.getArg(0);
        String keyStr = new String(key);
        if (expirationManager.checkAndDeleteIfExpired(keyStr)) return RedisResponse.nullBulkString();
        byte[] value = adapter.get(key);
        if (value == null) return RedisResponse.nullBulkString();
        adapter.delete(key);
        expirationManager.removeExpiration(keyStr);
        return RedisResponse.bulkString(value);
    }
}
