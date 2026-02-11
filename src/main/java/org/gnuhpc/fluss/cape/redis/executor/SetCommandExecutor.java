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

import org.gnuhpc.fluss.cape.redis.expiration.ExpirationManager;
import org.gnuhpc.fluss.cape.redis.protocol.RedisCommand;
import org.gnuhpc.fluss.cape.redis.protocol.RedisResponse;
import org.gnuhpc.fluss.cape.redis.storage.RedisSingleTableAdapter;
import org.gnuhpc.fluss.cape.redis.storage.RedisStorageAdapter;
import org.gnuhpc.fluss.cape.redis.util.ScanCursorManager;
import org.gnuhpc.fluss.cape.redis.util.RedisErrorSanitizer;

import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SetCommandExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(SetCommandExecutor.class);
    private static final String REDIS_TYPE = "set";
    private static final byte[] EMPTY_VALUE = new byte[0];

    private final RedisStorageAdapter adapter;
    private final ExpirationManager expirationManager;
    private final ScanCursorManager cursorManager;

    public SetCommandExecutor(RedisStorageAdapter adapter) {
        this.adapter = adapter;
        this.expirationManager = new ExpirationManager(adapter);
        this.cursorManager = new ScanCursorManager();
    }

    @Override
    public RedisMessage execute(RedisCommand command) {
        String cmd = command.getCommand();

        try {
            switch (cmd) {
                case "SADD":
                    return executeSAdd(command);
                case "SREM":
                    return executeSRem(command);
                case "SISMEMBER":
                    return executeSIsMember(command);
                case "SMEMBERS":
                    return executeSMembers(command);
                case "SCARD":
                    return executeSCard(command);
                case "SMOVE":
                    return executeSMove(command);
                case "SDIFF":
                    return executeSDiff(command);
                case "SDIFFSTORE":
                    return executeSDiffStore(command);
                case "SINTER":
                    return executeSInter(command);
                case "SINTERSTORE":
                    return executeSInterStore(command);
                case "SINTERCARD":
                    return executeSInterCard(command);
                case "SUNION":
                    return executeSUnion(command);
                case "SUNIONSTORE":
                    return executeSUnionStore(command);
                case "SPOP":
                    return executeSPop(command);
                case "SRANDMEMBER":
                    return executeSRandMember(command);
                case "SSCAN":
                    return executeSScan(command);
                default:
                    return RedisResponse.error("ERR unknown command '" + cmd + "'");
            }
        } catch (Exception e) {
            LOG.error("Error executing command: {}", cmd, e);
            return RedisErrorSanitizer.sanitizeError(e, cmd);
        }
    }

    private RedisMessage executeSAdd(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'sadd' command");
        }

        String key = command.getArgAsString(0);
        int added = 0;

        for (int i = 1; i < command.getArgCount(); i++) {
            String member = command.getArgAsString(i);

            boolean exists = adapter.getByCompositeKey(key, member) != null;
            adapter.setByCompositeKey(key, REDIS_TYPE, member, EMPTY_VALUE, null);

            if (!exists) {
                added++;
            }
        }

        return RedisResponse.integer(added);
    }

    private RedisMessage executeSRem(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'srem' command");
        }

        String key = command.getArgAsString(0);
        int removed = 0;

        for (int i = 1; i < command.getArgCount(); i++) {
            String member = command.getArgAsString(i);

            if (adapter.getByCompositeKey(key, member) != null) {
                adapter.deleteByCompositeKey(key, member);
                removed++;
            }
        }

        return RedisResponse.integer(removed);
    }

    private RedisMessage executeSIsMember(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'sismember' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.integer(0);
        }
        
        String member = command.getArgAsString(1);
        byte[] value = adapter.getByCompositeKey(key, member);

        return RedisResponse.integer(value != null ? 1 : 0);
    }

    private RedisMessage executeSMembers(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'smembers' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }
        
        List<RedisSingleTableAdapter.KeyValue> kvList = adapter.scanByKey(key);

        List<byte[]> results = new ArrayList<>();
        for (RedisSingleTableAdapter.KeyValue kv : kvList) {
            results.add(kv.subKey.getBytes(StandardCharsets.UTF_8));
        }

        return RedisResponse.bytesArray(results);
    }

    private RedisMessage executeSCard(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'scard' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.integer(0);
        }
        
        long count = adapter.countByKey(key);

        return RedisResponse.integer(count);
    }

    private RedisMessage executeSMove(RedisCommand command) throws Exception {
        if (command.getArgCount() < 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'smove' command");
        }

        String sourceKey = command.getArgAsString(0);
        String destKey = command.getArgAsString(1);
        String member = command.getArgAsString(2);

        if (adapter.getByCompositeKey(sourceKey, member) == null) {
            return RedisResponse.integer(0);
        }

        adapter.deleteByCompositeKey(sourceKey, member);
        adapter.setByCompositeKey(destKey, REDIS_TYPE, member, EMPTY_VALUE, null);

        return RedisResponse.integer(1);
    }

    private RedisMessage executeSDiff(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'sdiff' command");
        }

        String firstKey = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(firstKey)) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }
        
        List<RedisSingleTableAdapter.KeyValue> firstSet = adapter.scanByKey(firstKey);
        java.util.Set<String> result = new java.util.HashSet<>();
        
        for (RedisSingleTableAdapter.KeyValue kv : firstSet) {
            result.add(kv.subKey);
        }

        for (int i = 1; i < command.getArgCount(); i++) {
            String key = command.getArgAsString(i);
            
            if (expirationManager.checkAndDeleteIfExpired(key)) {
                continue;
            }
            
            List<RedisSingleTableAdapter.KeyValue> otherSet = adapter.scanByKey(key);
            for (RedisSingleTableAdapter.KeyValue kv : otherSet) {
                result.remove(kv.subKey);
            }
        }

        List<byte[]> results = new ArrayList<>();
        for (String member : result) {
            results.add(member.getBytes(StandardCharsets.UTF_8));
        }

        return RedisResponse.bytesArray(results);
    }

    private RedisMessage executeSDiffStore(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'sdiffstore' command");
        }

        String destKey = command.getArgAsString(0);
        String firstKey = command.getArgAsString(1);
        
        if (expirationManager.checkAndDeleteIfExpired(firstKey)) {
            adapter.deleteByKey(destKey);
            return RedisResponse.integer(0);
        }
        
        List<RedisSingleTableAdapter.KeyValue> firstSet = adapter.scanByKey(firstKey);
        java.util.Set<String> result = new java.util.HashSet<>();
        
        for (RedisSingleTableAdapter.KeyValue kv : firstSet) {
            result.add(kv.subKey);
        }

        for (int i = 2; i < command.getArgCount(); i++) {
            String key = command.getArgAsString(i);
            
            if (expirationManager.checkAndDeleteIfExpired(key)) {
                continue;
            }
            
            List<RedisSingleTableAdapter.KeyValue> otherSet = adapter.scanByKey(key);
            for (RedisSingleTableAdapter.KeyValue kv : otherSet) {
                result.remove(kv.subKey);
            }
        }

        adapter.deleteByKey(destKey);
        
        for (String member : result) {
            adapter.setByCompositeKey(destKey, REDIS_TYPE, member, EMPTY_VALUE, null);
        }

        return RedisResponse.integer(result.size());
    }

    private RedisMessage executeSInter(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'sinter' command");
        }

        java.util.Set<String> result = null;

        for (int i = 0; i < command.getArgCount(); i++) {
            String key = command.getArgAsString(i);
            
            if (expirationManager.checkAndDeleteIfExpired(key)) {
                return RedisResponse.bytesArray(Collections.emptyList());
            }
            
            List<RedisSingleTableAdapter.KeyValue> kvList = adapter.scanByKey(key);
            java.util.Set<String> currentSet = new java.util.HashSet<>();
            
            for (RedisSingleTableAdapter.KeyValue kv : kvList) {
                currentSet.add(kv.subKey);
            }

            if (result == null) {
                result = currentSet;
            } else {
                result.retainAll(currentSet);
            }

            if (result.isEmpty()) {
                break;
            }
        }

        List<byte[]> results = new ArrayList<>();
        if (result != null) {
            for (String member : result) {
                results.add(member.getBytes(StandardCharsets.UTF_8));
            }
        }

        return RedisResponse.bytesArray(results);
    }

    private RedisMessage executeSInterStore(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'sinterstore' command");
        }

        String destKey = command.getArgAsString(0);
        java.util.Set<String> result = null;

        for (int i = 1; i < command.getArgCount(); i++) {
            String key = command.getArgAsString(i);
            
            if (expirationManager.checkAndDeleteIfExpired(key)) {
                adapter.deleteByKey(destKey);
                return RedisResponse.integer(0);
            }
            
            List<RedisSingleTableAdapter.KeyValue> kvList = adapter.scanByKey(key);
            java.util.Set<String> currentSet = new java.util.HashSet<>();
            
            for (RedisSingleTableAdapter.KeyValue kv : kvList) {
                currentSet.add(kv.subKey);
            }

            if (result == null) {
                result = currentSet;
            } else {
                result.retainAll(currentSet);
            }

            if (result.isEmpty()) {
                break;
            }
        }

        adapter.deleteByKey(destKey);
        
        if (result != null) {
            for (String member : result) {
                adapter.setByCompositeKey(destKey, REDIS_TYPE, member, EMPTY_VALUE, null);
            }
        }

        return RedisResponse.integer(result != null ? result.size() : 0);
    }

    private RedisMessage executeSInterCard(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'sintercard' command");
        }

        int numKeys;
        try {
            numKeys = Integer.parseInt(command.getArgAsString(0));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR numkeys should be greater than 0");
        }

        if (numKeys <= 0 || numKeys > command.getArgCount() - 1) {
            return RedisResponse.error("ERR numkeys should be greater than 0");
        }

        java.util.Set<String> result = null;

        for (int i = 0; i < numKeys; i++) {
            String key = command.getArgAsString(i + 1);
            
            if (expirationManager.checkAndDeleteIfExpired(key)) {
                return RedisResponse.integer(0);
            }
            
            List<RedisSingleTableAdapter.KeyValue> kvList = adapter.scanByKey(key);
            java.util.Set<String> currentSet = new java.util.HashSet<>();
            
            for (RedisSingleTableAdapter.KeyValue kv : kvList) {
                currentSet.add(kv.subKey);
            }

            if (result == null) {
                result = currentSet;
            } else {
                result.retainAll(currentSet);
            }

            if (result.isEmpty()) {
                break;
            }
        }

        return RedisResponse.integer(result != null ? result.size() : 0);
    }

    private RedisMessage executeSUnion(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'sunion' command");
        }

        java.util.Set<String> result = new java.util.HashSet<>();

        for (int i = 0; i < command.getArgCount(); i++) {
            String key = command.getArgAsString(i);
            
            if (expirationManager.checkAndDeleteIfExpired(key)) {
                continue;
            }
            
            List<RedisSingleTableAdapter.KeyValue> kvList = adapter.scanByKey(key);
            for (RedisSingleTableAdapter.KeyValue kv : kvList) {
                result.add(kv.subKey);
            }
        }

        List<byte[]> results = new ArrayList<>();
        for (String member : result) {
            results.add(member.getBytes(StandardCharsets.UTF_8));
        }

        return RedisResponse.bytesArray(results);
    }

    private RedisMessage executeSUnionStore(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'sunionstore' command");
        }

        String destKey = command.getArgAsString(0);
        java.util.Set<String> result = new java.util.HashSet<>();

        for (int i = 1; i < command.getArgCount(); i++) {
            String key = command.getArgAsString(i);
            
            if (expirationManager.checkAndDeleteIfExpired(key)) {
                continue;
            }
            
            List<RedisSingleTableAdapter.KeyValue> kvList = adapter.scanByKey(key);
            for (RedisSingleTableAdapter.KeyValue kv : kvList) {
                result.add(kv.subKey);
            }
        }

        adapter.deleteByKey(destKey);
        
        for (String member : result) {
            adapter.setByCompositeKey(destKey, REDIS_TYPE, member, EMPTY_VALUE, null);
        }

        return RedisResponse.integer(result.size());
    }

    private RedisMessage executeSPop(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'spop' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.nullBulkString();
        }
        
        int count = 1;
        if (command.getArgCount() >= 2) {
            try {
                count = Integer.parseInt(command.getArgAsString(1));
            } catch (NumberFormatException e) {
                return RedisResponse.error("ERR value is not an integer or out of range");
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

        count = Math.min(count, kvList.size());

        for (int i = 0; i < count; i++) {
            int index = random.nextInt(kvList.size());
            RedisSingleTableAdapter.KeyValue kv = kvList.remove(index);
            results.add(kv.subKey.getBytes(StandardCharsets.UTF_8));
            adapter.deleteByCompositeKey(key, kv.subKey);
        }

        if (count == 1) {
            return RedisResponse.bulkString(results.get(0));
        } else {
            return RedisResponse.bytesArray(results);
        }
    }

    private RedisMessage executeSRandMember(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'srandmember' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.nullBulkString();
        }
        
        int count = 1;
        if (command.getArgCount() >= 2) {
            try {
                count = Integer.parseInt(command.getArgAsString(1));
            } catch (NumberFormatException e) {
                return RedisResponse.error("ERR value is not an integer or out of range");
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
            return RedisResponse.bulkString(randomKv.subKey.getBytes(StandardCharsets.UTF_8));
        } else {
            int absCount = Math.abs(count);
            for (int i = 0; i < absCount; i++) {
                RedisSingleTableAdapter.KeyValue randomKv = kvList.get(random.nextInt(kvList.size()));
                results.add(randomKv.subKey.getBytes(StandardCharsets.UTF_8));
            }
            return RedisResponse.bytesArray(results);
        }
    }

    private RedisMessage executeSScan(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'sscan' command");
        }

        String key = command.getArgAsString(0);
        long cursor;
        try {
            cursor = Long.parseLong(command.getArgAsString(1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR invalid cursor");
        }

        // Check expiration
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            List<RedisMessage> result = new ArrayList<>();
            result.add(RedisResponse.bulkString("0".getBytes(StandardCharsets.UTF_8)));
            result.add(RedisResponse.bytesArray(Collections.emptyList()));
            return RedisResponse.array(result);
        }

        // Type check
        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            List<RedisMessage> result = new ArrayList<>();
            result.add(RedisResponse.bulkString("0".getBytes(StandardCharsets.UTF_8)));
            result.add(RedisResponse.bytesArray(Collections.emptyList()));
            return RedisResponse.array(result);
        }

        // Parse COUNT option
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

        // Get all members
        List<RedisSingleTableAdapter.KeyValue> kvList = adapter.scanByKey(key);
        List<String> members = new ArrayList<>();
        for (RedisSingleTableAdapter.KeyValue kv : kvList) {
            members.add(kv.subKey);
        }

        // Use cursor manager for pagination
        ScanCursorManager.ScanResult scanResult = cursorManager.scan(cursor, members, count);

        // Build response: [nextCursor, [member1, member2, ...]]
        List<byte[]> memberBytes = new ArrayList<>();
        for (String member : scanResult.items) {
            memberBytes.add(member.getBytes(StandardCharsets.UTF_8));
        }

        List<RedisMessage> result = new ArrayList<>();
        result.add(RedisResponse.bulkString(String.valueOf(scanResult.nextCursor).getBytes(StandardCharsets.UTF_8)));
        result.add(RedisResponse.bytesArray(memberBytes));

        return RedisResponse.array(result);
    }
}
