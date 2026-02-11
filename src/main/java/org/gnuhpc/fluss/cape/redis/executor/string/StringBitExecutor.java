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
 * Executor for bitwise String operations (SETBIT, GETBIT, etc.)
 */
public class StringBitExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(StringBitExecutor.class);

    private final RedisStorageAdapter adapter;
    private final ExpirationManager expirationManager;

    public StringBitExecutor(RedisStorageAdapter adapter) {
        this.adapter = adapter;
        this.expirationManager = new ExpirationManager(adapter);
    }

    @Override
    public RedisMessage execute(RedisCommand command) {
        String cmd = command.getCommand();

        try {
            switch (cmd) {
                case "SETBIT": return executeSetbit(command);
                case "GETBIT": return executeGetbit(command);
                case "BITCOUNT": return executeBitcount(command);
                case "BITPOS": return executeBitpos(command);
                case "BITOP": return executeBitop(command);
                case "BITFIELD": return executeBitfield(command);
                case "BITFIELD_RO": return executeBitfieldRo(command);
                default:
                    return RedisResponse.error("ERR unknown command '" + cmd + "' in StringBitExecutor");
            }
        } catch (Exception e) {
            return RedisErrorSanitizer.sanitizeError(e, cmd);
        }
    }

    private RedisMessage executeSetbit(RedisCommand command) throws Exception {
        if (command.getArgCount() < 3) return RedisResponse.error("ERR wrong number of arguments for 'setbit' command");
        byte[] key = command.getArg(0);
        expirationManager.checkAndDeleteIfExpired(new String(key));

        long bitOffset;
        int bitValue;
        try {
            bitOffset = Long.parseLong(command.getArgAsString(1));
            bitValue = Integer.parseInt(command.getArgAsString(2));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR bit offset is not an integer or out of range");
        }

        if (bitOffset < 0) return RedisResponse.error("ERR bit offset is not an integer or out of range");
        if (bitValue != 0 && bitValue != 1) return RedisResponse.error("ERR bit is not an integer or out of range");

        int byteOffset = (int) (bitOffset / 8);
        int bitPosition = (int) (bitOffset % 8);

        byte[] value = adapter.get(key);
        if (value == null) value = new byte[0];

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
        if (command.getArgCount() < 2) return RedisResponse.error("ERR wrong number of arguments for 'getbit' command");
        byte[] key = command.getArg(0);
        if (expirationManager.checkAndDeleteIfExpired(new String(key))) return RedisResponse.integer(0);

        long bitOffset;
        try {
            bitOffset = Long.parseLong(command.getArgAsString(1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR bit offset is not an integer or out of range");
        }

        if (bitOffset < 0) return RedisResponse.error("ERR bit offset is not an integer or out of range");

        byte[] value = adapter.get(key);
        if (value == null) return RedisResponse.integer(0);

        int byteOffset = (int) (bitOffset / 8);
        int bitPosition = (int) (bitOffset % 8);

        if (byteOffset >= value.length) return RedisResponse.integer(0);

        int bitValue = (value[byteOffset] >> (7 - bitPosition)) & 1;
        return RedisResponse.integer(bitValue);
    }

    private RedisMessage executeBitcount(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) return RedisResponse.error("ERR wrong number of arguments for 'bitcount' command");
        byte[] key = command.getArg(0);
        if (expirationManager.checkAndDeleteIfExpired(new String(key))) return RedisResponse.integer(0);

        byte[] value = adapter.get(key);
        if (value == null) return RedisResponse.integer(0);

        int start = 0;
        int end = value.length - 1;

        if (command.getArgCount() >= 3) {
            try {
                start = Integer.parseInt(command.getArgAsString(1));
                end = Integer.parseInt(command.getArgAsString(2));
            } catch (NumberFormatException e) {
                return RedisResponse.error("ERR value is not an integer or out of range");
            }
            int len = value.length;
            if (start < 0) start = len + start;
            if (end < 0) end = len + end;
            start = Math.max(0, start);
            end = Math.min(end, len - 1);
            if (start > end || start >= len) return RedisResponse.integer(0);
        }

        long count = 0;
        for (int i = start; i <= end; i++) {
            count += Integer.bitCount(value[i] & 0xFF);
        }
        return RedisResponse.integer(count);
    }

    private RedisMessage executeBitpos(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) return RedisResponse.error("ERR wrong number of arguments for 'bitpos' command");
        byte[] key = command.getArg(0);
        if (expirationManager.checkAndDeleteIfExpired(new String(key))) return RedisResponse.integer(-1);
        
        byte[] value = adapter.get(key);
        if (value == null) return RedisResponse.integer(-1);

        int bit;
        try {
            bit = Integer.parseInt(new String(command.getArg(1)));
            if (bit != 0 && bit != 1) return RedisResponse.error("ERR bit must be 0 or 1");
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR bit is not an integer or out of range");
        }

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

        if (start < 0) start = value.length + start;
        if (end < 0) end = value.length + end;
        start = Math.max(0, start);
        end = Math.min(end, value.length - 1);
        if (start > end || start >= value.length) return RedisResponse.integer(-1);

        for (int byteIdx = start; byteIdx <= end; byteIdx++) {
            int byteVal = value[byteIdx] & 0xFF;
            for (int bitIdx = 7; bitIdx >= 0; bitIdx--) {
                int currentBit = (byteVal >> bitIdx) & 1;
                if (currentBit == bit) return RedisResponse.integer((long) byteIdx * 8 + (7 - bitIdx));
            }
        }
        return RedisResponse.integer(-1);
    }

    private RedisMessage executeBitop(RedisCommand command) throws Exception {
        if (command.getArgCount() < 3) return RedisResponse.error("ERR wrong number of arguments for 'bitop' command");
        String operation = new String(command.getArg(0)).toUpperCase();
        byte[] destKey = command.getArg(1);

        List<byte[]> values = new ArrayList<>();
        int maxLen = 0;

        for (int i = 2; i < command.getArgCount(); i++) {
            byte[] srcKey = command.getArg(i);
            if (!expirationManager.checkAndDeleteIfExpired(new String(srcKey))) {
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

        byte[] result = new byte[maxLen];
        switch (operation) {
            case "AND":
                for (int i = 0; i < maxLen; i++) {
                    result[i] = (byte) 0xFF;
                    for (byte[] value : values) {
                        if (i < value.length) result[i] &= value[i];
                        else { result[i] = 0; break; }
                    }
                }
                break;
            case "OR":
                for (int i = 0; i < maxLen; i++) {
                    result[i] = 0;
                    for (byte[] value : values) if (i < value.length) result[i] |= value[i];
                }
                break;
            case "XOR":
                for (int i = 0; i < maxLen; i++) {
                    result[i] = 0;
                    for (byte[] value : values) if (i < value.length) result[i] ^= value[i];
                }
                break;
            case "NOT":
                if (values.size() != 1) return RedisResponse.error("ERR BITOP NOT is called with multiple keys");
                byte[] value = values.get(0);
                for (int i = 0; i < maxLen; i++) {
                    if (i < value.length) result[i] = (byte) ~value[i];
                    else result[i] = (byte) 0xFF;
                }
                break;
            default: return RedisResponse.error("ERR unknown operation '" + operation + "'");
        }

        adapter.set(destKey, result);
        return RedisResponse.integer(result.length);
    }

    private RedisMessage executeBitfield(RedisCommand command) throws Exception {
        // Simplified implementation as placeholder
        return new ArrayRedisMessage(new ArrayList<>());
    }

    private RedisMessage executeBitfieldRo(RedisCommand command) throws Exception {
        // Simplified implementation as placeholder
        return new ArrayRedisMessage(new ArrayList<>());
    }
}
