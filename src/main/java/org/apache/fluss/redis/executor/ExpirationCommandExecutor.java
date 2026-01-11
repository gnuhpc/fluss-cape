/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

public class ExpirationCommandExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ExpirationCommandExecutor.class);

    private final RedisFlussAdapter adapter;
    private final ExpirationManager expirationManager;

    public ExpirationCommandExecutor(RedisFlussAdapter adapter) {
        this.adapter = adapter;
        this.expirationManager = new ExpirationManager(adapter);
    }

    @Override
    public RedisMessage execute(RedisCommand command) {
        String cmd = command.getCommand();

        try {
            switch (cmd) {
                case "EXPIRE":
                    return executeExpire(command);
                case "EXPIREAT":
                    return executeExpireAt(command);
                case "PEXPIRE":
                    return executePExpire(command);
                case "TTL":
                    return executeTTL(command);
                case "PTTL":
                    return executePTTL(command);
                case "PERSIST":
                    return executePersist(command);
                default:
                    return RedisResponse.error("ERR unknown expiration command '" + cmd + "'");
            }
        } catch (Exception e) {
            LOG.error("Error executing expiration command: {}", cmd, e);
            return RedisResponse.error("ERR " + e.getMessage());
        }
    }

    private RedisMessage executeExpire(RedisCommand command) throws Exception {
        if (command.getArgCount() != 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'EXPIRE' command");
        }

        String key = command.getArgAsString(0);
        long seconds;

        try {
            seconds = Long.parseLong(command.getArgAsString(1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not an integer or out of range");
        }

        if (seconds <= 0) {
            expirationManager.deleteExpiredKey(key);
            return RedisResponse.integer(1);
        }

        if (!adapter.keyExists(key)) {
            return RedisResponse.integer(0);
        }

        long expireAtMs = System.currentTimeMillis() + (seconds * 1000);
        expirationManager.setExpiration(key, expireAtMs);

        return RedisResponse.integer(1);
    }

    private RedisMessage executeExpireAt(RedisCommand command) throws Exception {
        if (command.getArgCount() != 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'EXPIREAT' command");
        }

        String key = command.getArgAsString(0);
        long timestampSeconds;

        try {
            timestampSeconds = Long.parseLong(command.getArgAsString(1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not an integer or out of range");
        }

        if (!adapter.keyExists(key)) {
            return RedisResponse.integer(0);
        }

        long expireAtMs = timestampSeconds * 1000;

        if (expireAtMs <= System.currentTimeMillis()) {
            expirationManager.deleteExpiredKey(key);
            return RedisResponse.integer(1);
        }

        expirationManager.setExpiration(key, expireAtMs);

        return RedisResponse.integer(1);
    }

    private RedisMessage executePExpire(RedisCommand command) throws Exception {
        if (command.getArgCount() != 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'PEXPIRE' command");
        }

        String key = command.getArgAsString(0);
        long milliseconds;

        try {
            milliseconds = Long.parseLong(command.getArgAsString(1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not an integer or out of range");
        }

        if (milliseconds <= 0) {
            expirationManager.deleteExpiredKey(key);
            return RedisResponse.integer(1);
        }

        if (!adapter.keyExists(key)) {
            return RedisResponse.integer(0);
        }

        long expireAtMs = System.currentTimeMillis() + milliseconds;
        expirationManager.setExpiration(key, expireAtMs);

        return RedisResponse.integer(1);
    }

    private RedisMessage executeTTL(RedisCommand command) throws Exception {
        if (command.getArgCount() != 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'TTL' command");
        }

        String key = command.getArgAsString(0);

        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.integer(-2);
        }

        if (!adapter.keyExists(key)) {
            return RedisResponse.integer(-2);
        }

        Long expireAt = expirationManager.getExpiration(key);
        if (expireAt == null) {
            return RedisResponse.integer(-1);
        }

        long remainingMs = expireAt - System.currentTimeMillis();
        if (remainingMs < 0) {
            expirationManager.deleteExpiredKey(key);
            return RedisResponse.integer(-2);
        }

        long remainingSeconds = remainingMs / 1000;
        return RedisResponse.integer(remainingSeconds);
    }

    private RedisMessage executePTTL(RedisCommand command) throws Exception {
        if (command.getArgCount() != 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'PTTL' command");
        }

        String key = command.getArgAsString(0);

        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.integer(-2);
        }

        if (!adapter.keyExists(key)) {
            return RedisResponse.integer(-2);
        }

        Long expireAt = expirationManager.getExpiration(key);
        if (expireAt == null) {
            return RedisResponse.integer(-1);
        }

        long remainingMs = expireAt - System.currentTimeMillis();
        if (remainingMs < 0) {
            expirationManager.deleteExpiredKey(key);
            return RedisResponse.integer(-2);
        }

        return RedisResponse.integer(remainingMs);
    }

    private RedisMessage executePersist(RedisCommand command) throws Exception {
        if (command.getArgCount() != 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'PERSIST' command");
        }

        String key = command.getArgAsString(0);

        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.integer(0);
        }

        if (!adapter.keyExists(key)) {
            return RedisResponse.integer(0);
        }

        Long expireAt = expirationManager.getExpiration(key);
        if (expireAt == null) {
            return RedisResponse.integer(0);
        }

        expirationManager.removeExpiration(key);

        return RedisResponse.integer(1);
    }
}
