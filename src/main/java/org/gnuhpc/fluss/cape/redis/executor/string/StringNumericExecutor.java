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

import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class StringNumericExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(StringNumericExecutor.class);

    private final RedisStorageAdapter adapter;
    private final ExpirationManager expirationManager;

    public StringNumericExecutor(RedisStorageAdapter adapter) {
        this.adapter = adapter;
        this.expirationManager = new ExpirationManager(adapter);
    }

    @Override
    public RedisMessage execute(RedisCommand command) {
        String cmd = command.getCommand();

        try {
            switch (cmd) {
                case "INCR": return executeIncr(command);
                case "INCRBY": return executeIncrBy(command);
                case "DECR": return executeDecr(command);
                case "DECRBY": return executeDecrBy(command);
                case "INCRBYFLOAT": return executeIncrByFloat(command);
                default:
                    return RedisResponse.error("ERR unknown command '" + cmd + "' in StringNumericExecutor");
            }
        } catch (Exception e) {
            return RedisErrorSanitizer.sanitizeError(e, cmd);
        }
    }

    private RedisMessage executeIncr(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'incr' command");
        }
        byte[] key = command.getArg(0);
        expirationManager.checkAndDeleteIfExpired(new String(key, StandardCharsets.UTF_8));
        return RedisResponse.integer(adapter.incr(key));
    }

    private RedisMessage executeIncrBy(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'incrby' command");
        }
        byte[] key = command.getArg(0);
        expirationManager.checkAndDeleteIfExpired(new String(key, StandardCharsets.UTF_8));
        long increment = Long.parseLong(command.getArgAsString(1));
        return RedisResponse.integer(adapter.incrBy(key, increment));
    }

    private RedisMessage executeDecr(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'decr' command");
        }
        byte[] key = command.getArg(0);
        expirationManager.checkAndDeleteIfExpired(new String(key, StandardCharsets.UTF_8));
        return RedisResponse.integer(adapter.decr(key));
    }

    private RedisMessage executeDecrBy(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'decrby' command");
        }
        byte[] key = command.getArg(0);
        expirationManager.checkAndDeleteIfExpired(new String(key, StandardCharsets.UTF_8));
        long decrement;
        try {
            decrement = Long.parseLong(command.getArgAsString(1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not an integer or out of range");
        }
        return RedisResponse.integer(adapter.decrBy(key, decrement));
    }

    private RedisMessage executeIncrByFloat(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'incrbyfloat' command");
        }
        byte[] key = command.getArg(0);
        expirationManager.checkAndDeleteIfExpired(new String(key, StandardCharsets.UTF_8));
        double increment;
        try {
            increment = Double.parseDouble(command.getArgAsString(1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not a valid float");
        }
        double result = adapter.incrByFloat(key, increment);
        return RedisResponse.bulkString(String.valueOf(result).getBytes(StandardCharsets.UTF_8));
    }
}
