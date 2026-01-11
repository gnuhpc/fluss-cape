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

public class SetCommandExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(SetCommandExecutor.class);
    private static final String REDIS_TYPE = "set";
    private static final byte[] EMPTY_VALUE = new byte[0];

    private final RedisFlussAdapter adapter;
    private final ExpirationManager expirationManager;

    public SetCommandExecutor(RedisFlussAdapter adapter) {
        this.adapter = adapter;
        this.expirationManager = new ExpirationManager(adapter);
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
                default:
                    return RedisResponse.error("ERR unknown command '" + cmd + "'");
            }
        } catch (Exception e) {
            LOG.error("Error executing command: {}", cmd, e);
            return RedisResponse.error("ERR " + e.getMessage());
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
        
        List<RedisFlussAdapter.KeyValue> kvList = adapter.scanByKey(key);

        List<byte[]> results = new ArrayList<>();
        for (RedisFlussAdapter.KeyValue kv : kvList) {
            results.add(kv.subKey.getBytes());
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
}
