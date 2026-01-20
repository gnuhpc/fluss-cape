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

import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class StringCommandExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(StringCommandExecutor.class);

    private final RedisStorageAdapter adapter;
    private final ExpirationManager expirationManager;

    public StringCommandExecutor(RedisStorageAdapter adapter) {
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
                case "GETRANGE":
                    return executeGetrange(command);
                case "SETRANGE":
                    return executeSetrange(command);
                case "INCRBYFLOAT":
                    return executeIncrByFloat(command);
                case "GETEX":
                    return executeGetex(command);
                case "GETDEL":
                    return executeGetdel(command);
                case "SETBIT":
                    return executeSetbit(command);
                case "GETBIT":
                    return executeGetbit(command);
                case "BITCOUNT":
                    return executeBitcount(command);
                case "BITPOS":
                    return executeBitpos(command);
                case "BITOP":
                    return executeBitop(command);
                case "LCS":
                    return executeLcs(command);
                case "BITFIELD":
                    return executeBitfield(command);
                case "BITFIELD_RO":
                    return executeBitfieldRo(command);
                case "SUBSTR":
                    return executeSubstr(command);
                case "STRALGO":
                    return executeStralgo(command);
                case "COPY":
                    return executeCopy(command);
                case "MOVE":
                    return executeMove(command);
                case "DUMP":
                    return executeDump(command);
                case "RESTORE":
                    return executeRestore(command);
                case "PING":
                    return RedisResponse.bulkString("PONG");
                default:
                    return RedisResponse.error("ERR unknown command '" + cmd + "'");
            }
        } catch (Exception e) {
            return RedisErrorSanitizer.sanitizeError(e, cmd);
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
        // SET key value [NX|XX] [GET] [EX seconds|PX milliseconds|EXAT unix-time-seconds|PXAT unix-time-milliseconds|KEEPTTL]
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'set' command");
        }

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
                case "NX":
                    nx = true;
                    break;
                case "XX":
                    xx = true;
                    break;
                case "GET":
                    get = true;
                    break;
                case "KEEPTTL":
                    keepTtl = true;
                    break;
                case "EX":
                    if (argIdx >= command.getArgCount()) {
                        return RedisResponse.error("ERR syntax error");
                    }
                    long seconds = Long.parseLong(new String(command.getArg(argIdx++)));
                    expirationMs = seconds * 1000;
                    break;
                case "PX":
                    if (argIdx >= command.getArgCount()) {
                        return RedisResponse.error("ERR syntax error");
                    }
                    expirationMs = Long.parseLong(new String(command.getArg(argIdx++)));
                    break;
                case "EXAT":
                    if (argIdx >= command.getArgCount()) {
                        return RedisResponse.error("ERR syntax error");
                    }
                    long unixSeconds = Long.parseLong(new String(command.getArg(argIdx++)));
                    expirationMs = (unixSeconds * 1000) - System.currentTimeMillis();
                    break;
                case "PXAT":
                    if (argIdx >= command.getArgCount()) {
                        return RedisResponse.error("ERR syntax error");
                    }
                    long unixMs = Long.parseLong(new String(command.getArg(argIdx++)));
                    expirationMs = unixMs - System.currentTimeMillis();
                    break;
                default:
                    return RedisResponse.error("ERR syntax error");
            }
        }

        if (nx && xx) {
            return RedisResponse.error("ERR NX and XX options at the same time are not compatible");
        }

        byte[] oldValue = null;
        if (get) {
            if (expirationManager.checkAndDeleteIfExpired(keyStr)) {
                oldValue = null;
            } else {
                oldValue = adapter.get(key);
            }
        }

        boolean exists = adapter.exists(key);

        if (nx && exists) {
            return get ? (oldValue == null ? RedisResponse.nullBulkString() 
                                           : RedisResponse.bulkString(oldValue)) 
                       : RedisResponse.nullBulkString();
        }

        if (xx && !exists) {
            return get ? RedisResponse.nullBulkString() : RedisResponse.nullBulkString();
        }

        adapter.set(key, value);

        if (!keepTtl && expirationMs != null && expirationMs > 0) {
            expirationManager.setExpiration(keyStr, expirationMs);
        }

        if (get) {
            return oldValue == null ? RedisResponse.nullBulkString() 
                                    : RedisResponse.bulkString(oldValue);
        }

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

    private RedisMessage executeGetrange(RedisCommand command) throws Exception {
        if (command.getArgCount() < 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'getrange' command");
        }

        byte[] key = command.getArg(0);
        String keyStr = new String(key);

        if (expirationManager.checkAndDeleteIfExpired(keyStr)) {
            return RedisResponse.bulkString(new byte[0]);
        }

        int start;
        int end;
        try {
            start = Integer.parseInt(command.getArgAsString(1));
            end = Integer.parseInt(command.getArgAsString(2));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not an integer or out of range");
        }

        byte[] value = adapter.get(key);
        if (value == null || value.length == 0) {
            return RedisResponse.bulkString(new byte[0]);
        }

        int len = value.length;

        // Handle negative indices (Python-style)
        if (start < 0) {
            start = len + start;
        }
        if (end < 0) {
            end = len + end;
        }

        // Clamp to valid range
        start = Math.max(0, start);
        end = Math.min(end, len - 1);

        // If start > end after clamping, return empty string
        if (start > end || start >= len) {
            return RedisResponse.bulkString(new byte[0]);
        }

        // Extract substring (end is inclusive in Redis GETRANGE)
        int rangeLen = end - start + 1;
        byte[] range = new byte[rangeLen];
        System.arraycopy(value, start, range, 0, rangeLen);

        return RedisResponse.bulkString(range);
    }

    private RedisMessage executeSetrange(RedisCommand command) throws Exception {
        if (command.getArgCount() < 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'setrange' command");
        }

        byte[] key = command.getArg(0);
        String keyStr = new String(key);

        expirationManager.checkAndDeleteIfExpired(keyStr);

        int offset;
        try {
            offset = Integer.parseInt(command.getArgAsString(1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not an integer or out of range");
        }

        if (offset < 0) {
            return RedisResponse.error("ERR offset is out of range");
        }

        byte[] newValue = command.getArg(2);

        byte[] existing = adapter.get(key);
        if (existing == null) {
            existing = new byte[0];
        }

        // Calculate required length
        int requiredLen = offset + newValue.length;

        byte[] result;
        if (requiredLen > existing.length) {
            // Expand array, pad with zeros
            result = new byte[requiredLen];
            System.arraycopy(existing, 0, result, 0, existing.length);
            // Zero-fill gap between existing length and offset (if any)
            // This is automatic with new byte[]
        } else {
            // Use existing array
            result = existing;
        }

        // Copy new value at offset
        System.arraycopy(newValue, 0, result, offset, newValue.length);

        adapter.set(key, result);
        return RedisResponse.integer(result.length);
    }

    private RedisMessage executeIncrByFloat(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'incrbyfloat' command");
        }

        byte[] key = command.getArg(0);
        String keyStr = new String(key);

        expirationManager.checkAndDeleteIfExpired(keyStr);

        double increment;
        try {
            increment = Double.parseDouble(command.getArgAsString(1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not a valid float");
        }

        byte[] current = adapter.get(key);
        double value = 0.0;
        if (current != null) {
            try {
                value = Double.parseDouble(new String(current));
            } catch (NumberFormatException e) {
                return RedisResponse.error("ERR value is not a valid float");
            }
        }
        value += increment;
        
        String result = String.valueOf(value);
        adapter.set(key, result.getBytes());
        return RedisResponse.bulkString(result.getBytes());
    }

    private RedisMessage executeGetex(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'getex' command");
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

        for (int i = 1; i < command.getArgCount(); i++) {
            String option = command.getArgAsString(i).toUpperCase();
            
            if (option.equals("EX") && i + 1 < command.getArgCount()) {
                long seconds = Long.parseLong(command.getArgAsString(i + 1));
                long expireAtMs = System.currentTimeMillis() + (seconds * 1000);
                expirationManager.setExpiration(keyStr, expireAtMs);
                i++;
            } else if (option.equals("PX") && i + 1 < command.getArgCount()) {
                long milliseconds = Long.parseLong(command.getArgAsString(i + 1));
                long expireAtMs = System.currentTimeMillis() + milliseconds;
                expirationManager.setExpiration(keyStr, expireAtMs);
                i++;
            } else if (option.equals("EXAT") && i + 1 < command.getArgCount()) {
                long unixTimeSeconds = Long.parseLong(command.getArgAsString(i + 1));
                long expireAtMs = unixTimeSeconds * 1000;
                expirationManager.setExpiration(keyStr, expireAtMs);
                i++;
            } else if (option.equals("PXAT") && i + 1 < command.getArgCount()) {
                long unixTimeMs = Long.parseLong(command.getArgAsString(i + 1));
                expirationManager.setExpiration(keyStr, unixTimeMs);
                i++;
            } else if (option.equals("PERSIST")) {
                expirationManager.removeExpiration(keyStr);
            }
        }

        return RedisResponse.bulkString(value);
    }

    private RedisMessage executeGetdel(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'getdel' command");
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

        adapter.delete(key);
        expirationManager.removeExpiration(keyStr);

        return RedisResponse.bulkString(value);
    }

    private RedisMessage executeSetbit(RedisCommand command) throws Exception {
        if (command.getArgCount() < 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'setbit' command");
        }

        byte[] key = command.getArg(0);
        String keyStr = new String(key);

        expirationManager.checkAndDeleteIfExpired(keyStr);

        long bitOffset;
        int bitValue;
        try {
            bitOffset = Long.parseLong(command.getArgAsString(1));
            bitValue = Integer.parseInt(command.getArgAsString(2));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR bit offset is not an integer or out of range");
        }

        if (bitOffset < 0) {
            return RedisResponse.error("ERR bit offset is not an integer or out of range");
        }

        if (bitValue != 0 && bitValue != 1) {
            return RedisResponse.error("ERR bit is not an integer or out of range");
        }

        int byteOffset = (int) (bitOffset / 8);
        int bitPosition = (int) (bitOffset % 8);

        byte[] value = adapter.get(key);
        if (value == null) {
            value = new byte[0];
        }

        if (byteOffset >= value.length) {
            byte[] newValue = new byte[byteOffset + 1];
            System.arraycopy(value, 0, newValue, 0, value.length);
            value = newValue;
        }

        int oldBitValue = (value[byteOffset] >> (7 - bitPosition)) & 1;

        if (bitValue == 1) {
            value[byteOffset] |= (byte) (1 << (7 - bitPosition));
        } else {
            value[byteOffset] &= (byte) ~(1 << (7 - bitPosition));
        }

        adapter.set(key, value);
        return RedisResponse.integer(oldBitValue);
    }

    private RedisMessage executeGetbit(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'getbit' command");
        }

        byte[] key = command.getArg(0);
        String keyStr = new String(key);

        if (expirationManager.checkAndDeleteIfExpired(keyStr)) {
            return RedisResponse.integer(0);
        }

        long bitOffset;
        try {
            bitOffset = Long.parseLong(command.getArgAsString(1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR bit offset is not an integer or out of range");
        }

        if (bitOffset < 0) {
            return RedisResponse.error("ERR bit offset is not an integer or out of range");
        }

        byte[] value = adapter.get(key);
        if (value == null) {
            return RedisResponse.integer(0);
        }

        int byteOffset = (int) (bitOffset / 8);
        int bitPosition = (int) (bitOffset % 8);

        if (byteOffset >= value.length) {
            return RedisResponse.integer(0);
        }

        int bitValue = (value[byteOffset] >> (7 - bitPosition)) & 1;
        return RedisResponse.integer(bitValue);
    }

    private RedisMessage executeBitcount(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'bitcount' command");
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

        int start = 0;
        int end = value.length - 1;

        if (command.getArgCount() >= 3) {
            try {
                start = Integer.parseInt(command.getArgAsString(1));
                end = Integer.parseInt(command.getArgAsString(2));
            } catch (NumberFormatException e) {
                return RedisResponse.error("ERR value is not an integer or out of range");
            }

            if (start < 0) {
                start = value.length + start;
            }
            if (end < 0) {
                end = value.length + end;
            }

            start = Math.max(0, start);
            end = Math.min(end, value.length - 1);

            if (start > end || start >= value.length) {
                return RedisResponse.integer(0);
            }
        }

        long count = 0;
        for (int i = start; i <= end; i++) {
            count += Integer.bitCount(value[i] & 0xFF);
        }

        return RedisResponse.integer(count);
    }

    private RedisMessage executeBitpos(RedisCommand command) throws Exception {
        // BITPOS key bit [start [end [BYTE|BIT]]]
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'bitpos' command");
        }

        byte[] key = command.getArg(0);
        String keyStr = new String(key);

        if (expirationManager.checkAndDeleteIfExpired(keyStr)) {
            return RedisResponse.integer(-1);
        }

        byte[] value = adapter.get(key);
        if (value == null) {
            return RedisResponse.integer(-1);
        }

        // Parse bit argument (0 or 1)
        int bit;
        try {
            bit = Integer.parseInt(new String(command.getArg(1)));
            if (bit != 0 && bit != 1) {
                return RedisResponse.error("ERR bit must be 0 or 1");
            }
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR bit is not an integer or out of range");
        }

        // Parse optional start and end
        int start = 0;
        int end = value.length - 1;
        
        if (command.getArgCount() >= 3) {
            try {
                start = Integer.parseInt(new String(command.getArg(2)));
            } catch (NumberFormatException e) {
                return RedisResponse.error("ERR value is not an integer or out of range");
            }
        }

        if (command.getArgCount() >= 4) {
            try {
                end = Integer.parseInt(new String(command.getArg(3)));
            } catch (NumberFormatException e) {
                return RedisResponse.error("ERR value is not an integer or out of range");
            }
        }

        // Handle negative indices
        if (start < 0) {
            start = value.length + start;
        }
        if (end < 0) {
            end = value.length + end;
        }

        start = Math.max(0, start);
        end = Math.min(end, value.length - 1);

        if (start > end || start >= value.length) {
            return RedisResponse.integer(-1);
        }

        // Find first bit position
        for (int byteIdx = start; byteIdx <= end; byteIdx++) {
            int byteVal = value[byteIdx] & 0xFF;
            for (int bitIdx = 7; bitIdx >= 0; bitIdx--) {
                int currentBit = (byteVal >> bitIdx) & 1;
                if (currentBit == bit) {
                    return RedisResponse.integer((long) byteIdx * 8 + (7 - bitIdx));
                }
            }
        }

        return RedisResponse.integer(-1);
    }

    private RedisMessage executeBitop(RedisCommand command) throws Exception {
        // BITOP operation destkey key [key ...]
        if (command.getArgCount() < 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'bitop' command");
        }

        String operation = new String(command.getArg(0)).toUpperCase();
        byte[] destKey = command.getArg(1);
        String destKeyStr = new String(destKey);

        // Get all source keys
        List<byte[]> values = new ArrayList<>();
        int maxLen = 0;

        for (int i = 2; i < command.getArgCount(); i++) {
            byte[] srcKey = command.getArg(i);
            String srcKeyStr = new String(srcKey);

            if (!expirationManager.checkAndDeleteIfExpired(srcKeyStr)) {
                byte[] value = adapter.get(srcKey);
                if (value != null) {
                    values.add(value);
                    maxLen = Math.max(maxLen, value.length);
                } else {
                    values.add(new byte[0]);
                }
            } else {
                values.add(new byte[0]);
            }
        }

        if (values.isEmpty()) {
            adapter.delete(destKey);
            return RedisResponse.integer(0);
        }

        byte[] result;

        switch (operation) {
            case "AND":
                result = new byte[maxLen];
                for (int i = 0; i < maxLen; i++) {
                    result[i] = (byte) 0xFF; // Start with all 1s for AND
                    for (byte[] value : values) {
                        if (i < value.length) {
                            result[i] &= value[i];
                        } else {
                            result[i] = 0; // AND with 0
                            break;
                        }
                    }
                }
                break;

            case "OR":
                result = new byte[maxLen];
                for (int i = 0; i < maxLen; i++) {
                    result[i] = 0;
                    for (byte[] value : values) {
                        if (i < value.length) {
                            result[i] |= value[i];
                        }
                    }
                }
                break;

            case "XOR":
                result = new byte[maxLen];
                for (int i = 0; i < maxLen; i++) {
                    result[i] = 0;
                    for (byte[] value : values) {
                        if (i < value.length) {
                            result[i] ^= value[i];
                        }
                    }
                }
                break;

            case "NOT":
                if (values.size() != 1) {
                    return RedisResponse.error(
                            "ERR BITOP NOT is called with multiple keys");
                }
                result = new byte[maxLen];
                byte[] value = values.get(0);
                for (int i = 0; i < maxLen; i++) {
                    if (i < value.length) {
                        result[i] = (byte) ~value[i];
                    } else {
                        result[i] = (byte) 0xFF;
                    }
                }
                break;

            default:
                return RedisResponse.error(
                        "ERR unknown operation '" + operation + "'");
        }

        adapter.set(destKey, result);
        return RedisResponse.integer(result.length);
    }

    private RedisMessage executeLcs(RedisCommand command) throws Exception {
        // LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'lcs' command");
        }

        byte[] key1 = command.getArg(0);
        byte[] key2 = command.getArg(1);
        String key1Str = new String(key1);
        String key2Str = new String(key2);

        // Check expiration
        if (expirationManager.checkAndDeleteIfExpired(key1Str)) {
            return RedisResponse.bulkString("");
        }
        if (expirationManager.checkAndDeleteIfExpired(key2Str)) {
            return RedisResponse.bulkString("");
        }

        byte[] value1 = adapter.get(key1);
        byte[] value2 = adapter.get(key2);

        if (value1 == null) {
            value1 = new byte[0];
        }
        if (value2 == null) {
            value2 = new byte[0];
        }

        // Parse options
        boolean lenOnly = false;
        boolean withIdx = false;
        int minMatchLen = 0;
        boolean withMatchLen = false;

        for (int i = 2; i < command.getArgCount(); i++) {
            String option = new String(command.getArg(i)).toUpperCase();
            switch (option) {
                case "LEN":
                    lenOnly = true;
                    break;
                case "IDX":
                    withIdx = true;
                    break;
                case "MINMATCHLEN":
                    if (i + 1 < command.getArgCount()) {
                        try {
                            minMatchLen = Integer.parseInt(new String(command.getArg(++i)));
                        } catch (NumberFormatException e) {
                            return RedisResponse.error("ERR minmatchlen is not an integer");
                        }
                    }
                    break;
                case "WITHMATCHLEN":
                    withMatchLen = true;
                    break;
            }
        }

        // Compute LCS using dynamic programming
        String lcs = computeLCS(new String(value1), new String(value2));

        if (lenOnly) {
            return RedisResponse.integer(lcs.length());
        } else if (withIdx) {
            // For simplicity, just return the LCS string for now
            // Full IDX implementation would be complex
            List<RedisMessage> result = new ArrayList<>();
            result.add(RedisResponse.bulkString("matches"));
            result.add(new ArrayRedisMessage(new ArrayList<>())); // Empty matches for now
            result.add(RedisResponse.bulkString("len"));
            result.add(RedisResponse.integer(lcs.length()));
            return new ArrayRedisMessage(result);
        } else {
            return RedisResponse.bulkString(lcs);
        }
    }

    private String computeLCS(String str1, String str2) {
        int m = str1.length();
        int n = str2.length();
        int[][] dp = new int[m + 1][n + 1];

        // Build LCS length table
        for (int i = 1; i <= m; i++) {
            for (int j = 1; j <= n; j++) {
                if (str1.charAt(i - 1) == str2.charAt(j - 1)) {
                    dp[i][j] = dp[i - 1][j - 1] + 1;
                } else {
                    dp[i][j] = Math.max(dp[i - 1][j], dp[i][j - 1]);
                }
            }
        }

        // Backtrack to find LCS string
        StringBuilder lcs = new StringBuilder();
        int i = m, j = n;
        while (i > 0 && j > 0) {
            if (str1.charAt(i - 1) == str2.charAt(j - 1)) {
                lcs.insert(0, str1.charAt(i - 1));
                i--;
                j--;
            } else if (dp[i - 1][j] > dp[i][j - 1]) {
                i--;
            } else {
                j--;
            }
        }

        return lcs.toString();
    }

    private RedisMessage executeBitfield(RedisCommand command) throws Exception {
        // BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset increment]
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'bitfield' command");
        }

        byte[] key = command.getArg(0);
        String keyStr = new String(key);

        if (expirationManager.checkAndDeleteIfExpired(keyStr)) {
            // Create empty value if expired
        }

        byte[] value = adapter.get(key);
        if (value == null) {
            value = new byte[0];
        }

        List<RedisMessage> results = new ArrayList<>();
        int argIdx = 1;

        while (argIdx < command.getArgCount()) {
            String operation = new String(command.getArg(argIdx++)).toUpperCase();

            switch (operation) {
                case "GET":
                    if (argIdx + 1 >= command.getArgCount()) {
                        return RedisResponse.error("ERR wrong number of arguments for BITFIELD");
                    }
                    String getType = new String(command.getArg(argIdx++));
                    int getOffset = Integer.parseInt(new String(command.getArg(argIdx++)));
                    // Simplified: just return 0 for now
                    results.add(RedisResponse.integer(0));
                    break;

                case "SET":
                    if (argIdx + 2 >= command.getArgCount()) {
                        return RedisResponse.error("ERR wrong number of arguments for BITFIELD");
                    }
                    String setType = new String(command.getArg(argIdx++));
                    int setOffset = Integer.parseInt(new String(command.getArg(argIdx++)));
                    long setValue = Long.parseLong(new String(command.getArg(argIdx++)));
                    // Simplified: just return 0 for now
                    results.add(RedisResponse.integer(0));
                    break;

                case "INCRBY":
                    if (argIdx + 2 >= command.getArgCount()) {
                        return RedisResponse.error("ERR wrong number of arguments for BITFIELD");
                    }
                    String incrType = new String(command.getArg(argIdx++));
                    int incrOffset = Integer.parseInt(new String(command.getArg(argIdx++)));
                    long increment = Long.parseLong(new String(command.getArg(argIdx++)));
                    // Simplified: just return 0 for now
                    results.add(RedisResponse.integer(0));
                    break;

                default:
                    return RedisResponse.error("ERR unknown BITFIELD subcommand '" + operation + "'");
            }
        }

        if (results.isEmpty()) {
            return new ArrayRedisMessage(new ArrayList<>());
        }

        return new ArrayRedisMessage(results);
    }

    private RedisMessage executeBitfieldRo(RedisCommand command) throws Exception {
        // BITFIELD_RO key GET type offset [GET type offset ...]
        // Read-only variant of BITFIELD (Redis 6.0+)
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'bitfield_ro' command");
        }

        byte[] key = command.getArg(0);
        String keyStr = new String(key);

        if (expirationManager.checkAndDeleteIfExpired(keyStr)) {
            return new ArrayRedisMessage(new ArrayList<>());
        }

        byte[] value = adapter.get(key);
        if (value == null) {
            value = new byte[0];
        }

        List<RedisMessage> results = new ArrayList<>();
        int argIdx = 1;

        while (argIdx < command.getArgCount()) {
            String operation = new String(command.getArg(argIdx++)).toUpperCase();

            if (!"GET".equals(operation)) {
                return RedisResponse.error(
                        "ERR BITFIELD_RO only supports the GET subcommand");
            }

            if (argIdx + 1 >= command.getArgCount()) {
                return RedisResponse.error("ERR wrong number of arguments for BITFIELD_RO");
            }

            String getType = new String(command.getArg(argIdx++));
            int getOffset = Integer.parseInt(new String(command.getArg(argIdx++)));
            // Simplified: just return 0 for now (same as BITFIELD GET)
            results.add(RedisResponse.integer(0));
        }

        if (results.isEmpty()) {
            return new ArrayRedisMessage(new ArrayList<>());
        }

        return new ArrayRedisMessage(results);
    }

    private RedisMessage executeSubstr(RedisCommand command) throws Exception {
        // SUBSTR key start end
        // Deprecated - alias for GETRANGE
        if (command.getArgCount() < 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'substr' command");
        }

        // Simply delegate to GETRANGE
        return executeGetrange(command);
    }

    private RedisMessage executeStralgo(RedisCommand command) throws Exception {
        // STRALGO LCS algo-specific-argument [algo-specific-argument ...]
        // STRALGO LCS KEYS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]
        // STRALGO LCS STRINGS string1 string2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]

        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'stralgo' command");
        }

        String algo = new String(command.getArg(0)).toUpperCase();
        if (!"LCS".equals(algo)) {
            return RedisResponse.error(
                    "ERR Unknown algorithm '" + algo + "'. Valid algorithms are: LCS");
        }

        if (command.getArgCount() < 4) {
            return RedisResponse.error(
                    "ERR wrong number of arguments for 'stralgo lcs' command");
        }

        String inputType = new String(command.getArg(1)).toUpperCase();
        if (!"KEYS".equals(inputType) && !"STRINGS".equals(inputType)) {
            return RedisResponse.error(
                    "ERR Expected 'KEYS' or 'STRINGS' but got '" + inputType + "'");
        }

        // Get the two inputs (keys or strings)
        byte[] input1 = command.getArg(2);
        byte[] input2 = command.getArg(3);
        byte[] value1;
        byte[] value2;

        if ("KEYS".equals(inputType)) {
            // Read from keys
            String key1Str = new String(input1);
            String key2Str = new String(input2);

            // Check expiration
            if (expirationManager.checkAndDeleteIfExpired(key1Str)) {
                value1 = new byte[0];
            } else {
                value1 = adapter.get(input1);
                if (value1 == null) {
                    value1 = new byte[0];
                }
            }

            if (expirationManager.checkAndDeleteIfExpired(key2Str)) {
                value2 = new byte[0];
            } else {
                value2 = adapter.get(input2);
                if (value2 == null) {
                    value2 = new byte[0];
                }
            }
        } else {
            // Use strings directly
            value1 = input1;
            value2 = input2;
        }

        // Parse options (same as LCS)
        boolean lenOnly = false;
        boolean withIdx = false;
        int minMatchLen = 0;
        boolean withMatchLen = false;

        for (int i = 4; i < command.getArgCount(); i++) {
            String option = new String(command.getArg(i)).toUpperCase();
            switch (option) {
                case "LEN":
                    lenOnly = true;
                    break;
                case "IDX":
                    withIdx = true;
                    break;
                case "MINMATCHLEN":
                    if (i + 1 < command.getArgCount()) {
                        try {
                            minMatchLen = Integer.parseInt(new String(command.getArg(++i)));
                        } catch (NumberFormatException e) {
                            return RedisResponse.error("ERR minmatchlen is not an integer");
                        }
                    }
                    break;
                case "WITHMATCHLEN":
                    withMatchLen = true;
                    break;
            }
        }

        // Compute LCS using dynamic programming (reuse existing implementation)
        String lcs = computeLCS(new String(value1), new String(value2));

        if (lenOnly) {
            return RedisResponse.integer(lcs.length());
        } else if (withIdx) {
            // For simplicity, return the basic structure
            // Full IDX implementation would require tracking match positions
            List<RedisMessage> result = new ArrayList<>();
            result.add(RedisResponse.bulkString("matches"));
            result.add(new ArrayRedisMessage(new ArrayList<>())); // Empty matches array
            result.add(RedisResponse.bulkString("len"));
            result.add(RedisResponse.integer(lcs.length()));
            return new ArrayRedisMessage(result);
        } else {
            return RedisResponse.bulkString(lcs);
        }
    }

    private RedisMessage executeCopy(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'copy' command");
        }

        byte[] sourceKey = command.getArg(0);
        byte[] destKey = command.getArg(1);
        String sourceKeyStr = new String(sourceKey);
        String destKeyStr = new String(destKey);

        boolean replace = false;
        
        for (int i = 2; i < command.getArgCount(); i++) {
            String option = new String(command.getArg(i)).toUpperCase();
            
            if (option.equals("REPLACE")) {
                replace = true;
            } else if (option.equals("DB")) {
                if (i + 1 < command.getArgCount()) {
                    i++;
                }
                return RedisResponse.error("ERR DB option not supported in single-database mode");
            }
        }

        if (expirationManager.checkAndDeleteIfExpired(sourceKeyStr)) {
            return RedisResponse.integer(0);
        }

        byte[] sourceValue = adapter.get(sourceKey);
        if (sourceValue == null) {
            return RedisResponse.integer(0);
        }

        if (!replace && adapter.exists(destKey)) {
            return RedisResponse.integer(0);
        }

        adapter.set(destKey, sourceValue);

        Long expiration = expirationManager.getExpiration(sourceKeyStr);
        if (expiration != null && expiration > System.currentTimeMillis()) {
            expirationManager.setExpiration(destKeyStr, expiration);
        }

        return RedisResponse.integer(1);
    }

    private RedisMessage executeMove(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'move' command");
        }

        return RedisResponse.error("ERR MOVE command not supported in single-database mode");
    }

    private RedisMessage executeDump(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'dump' command");
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

        byte[] serialized = serializeForDump(value, expirationManager.getExpiration(keyStr));
        
        return RedisResponse.bulkString(serialized);
    }

    private RedisMessage executeRestore(RedisCommand command) throws Exception {
        if (command.getArgCount() < 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'restore' command");
        }

        byte[] key = command.getArg(0);
        String keyStr = new String(key);
        
        long ttl;
        try {
            ttl = Long.parseLong(new String(command.getArg(1)));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR invalid TTL value");
        }

        byte[] serializedValue = command.getArg(2);
        
        boolean replace = false;
        boolean absTtl = false;
        
        for (int i = 3; i < command.getArgCount(); i++) {
            String option = new String(command.getArg(i)).toUpperCase();
            
            switch (option) {
                case "REPLACE":
                    replace = true;
                    break;
                case "ABSTTL":
                    absTtl = true;
                    break;
                case "IDLETIME":
                case "FREQ":
                    if (i + 1 < command.getArgCount()) {
                        i++;
                    }
                    break;
            }
        }

        if (!replace && adapter.exists(key)) {
            return RedisResponse.error("ERR Target key name already exists");
        }

        byte[] value = deserializeFromDump(serializedValue);
        if (value == null) {
            return RedisResponse.error("ERR DUMP payload version or checksum are wrong");
        }

        adapter.set(key, value);

        if (ttl > 0) {
            long expireAtMs;
            if (absTtl) {
                expireAtMs = ttl;
            } else {
                expireAtMs = System.currentTimeMillis() + ttl;
            }
            expirationManager.setExpiration(keyStr, expireAtMs);
        }

        return RedisResponse.ok();
    }

    private byte[] serializeForDump(byte[] value, Long expiration) {
        // Format: TYPE(1) + LEN(4) + VALUE + TTL(8) + CHECKSUM(8)
        int totalSize = 1 + 4 + value.length + 8 + 8;
        byte[] result = new byte[totalSize];
        int offset = 0;
        
        result[offset++] = 0;
        
        result[offset++] = (byte) (value.length >> 24);
        result[offset++] = (byte) (value.length >> 16);
        result[offset++] = (byte) (value.length >> 8);
        result[offset++] = (byte) value.length;
        
        System.arraycopy(value, 0, result, offset, value.length);
        offset += value.length;
        
        long ttlValue = 0;
        if (expiration != null && expiration > System.currentTimeMillis()) {
            ttlValue = expiration - System.currentTimeMillis();
        }
        for (int i = 7; i >= 0; i--) {
            result[offset++] = (byte) (ttlValue >> (i * 8));
        }
        
        long checksum = 0;
        for (int i = 0; i < offset; i++) {
            checksum ^= result[i];
        }
        for (int i = 7; i >= 0; i--) {
            result[offset++] = (byte) (checksum >> (i * 8));
        }
        
        return result;
    }

    private byte[] deserializeFromDump(byte[] serialized) {
        if (serialized == null || serialized.length < 21) {
            return null;
        }
        
        int offset = 0;
        
        byte type = serialized[offset++];
        if (type != 0) {
            return null;
        }
        
        int length = ((serialized[offset++] & 0xFF) << 24)
                   | ((serialized[offset++] & 0xFF) << 16)
                   | ((serialized[offset++] & 0xFF) << 8)
                   | (serialized[offset++] & 0xFF);
        
        if (offset + length + 16 != serialized.length) {
            return null;
        }
        
        byte[] value = new byte[length];
        System.arraycopy(serialized, offset, value, 0, length);
        
        return value;
    }
}
