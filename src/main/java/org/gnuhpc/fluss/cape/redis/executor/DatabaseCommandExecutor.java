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

import org.gnuhpc.fluss.cape.redis.protocol.RedisCommand;
import org.gnuhpc.fluss.cape.redis.protocol.RedisResponse;
import org.gnuhpc.fluss.cape.redis.storage.RedisSingleTableAdapter;
import org.gnuhpc.fluss.cape.redis.storage.RedisStorageAdapter;
import org.gnuhpc.fluss.cape.redis.util.RedisErrorSanitizer;

import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Executor for database-level Redis commands.
 * 
 * <p>Supported commands:
 * <ul>
 *   <li>FLUSHALL - Delete all keys from all databases</li>
 *   <li>FLUSHDB - Delete all keys from current database</li>
 * </ul>
 */
public class DatabaseCommandExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseCommandExecutor.class);

    private final RedisStorageAdapter adapter;

    public DatabaseCommandExecutor(RedisStorageAdapter adapter) {
        this.adapter = adapter;
    }

    @Override
    public RedisMessage execute(RedisCommand command) {
        String cmd = command.getCommand();

        try {
            switch (cmd) {
                case "FLUSHALL":
                case "FLUSHDB":
                    return executeFlush(command);
                default:
                    return RedisResponse.error("ERR unknown database command '" + cmd + "'");
            }
        } catch (Exception e) {
            LOG.error("Error executing database command: {}", cmd, e);
            return RedisErrorSanitizer.sanitizeError(e, cmd);
        }
    }

    /**
     * FLUSHALL / FLUSHDB - Delete all keys from the database.
     * 
     * <p>Since Fluss doesn't have multiple database support, FLUSHALL and FLUSHDB
     * have the same behavior - they delete all keys from the Redis data table.
     * 
     * <p>Syntax: FLUSHALL [ASYNC|SYNC]
     * <p>Syntax: FLUSHDB [ASYNC|SYNC]
     * 
     * <p>Options:
     * <ul>
     *   <li>ASYNC - Delete keys in background (not yet supported, treated as SYNC)</li>
     *   <li>SYNC - Delete keys synchronously (default)</li>
     * </ul>
     * 
     * @return OK if successful
     */
    private RedisMessage executeFlush(RedisCommand command) {
        // Parse optional ASYNC/SYNC argument
        boolean async = false;
        if (command.getArgCount() > 0) {
            String option = command.getArgAsString(0).toUpperCase();
            if (option.equals("ASYNC")) {
                async = true;
                LOG.info("ASYNC mode requested but not implemented, falling back to SYNC");
            } else if (!option.equals("SYNC")) {
                return RedisResponse.error("ERR syntax error");
            }
        }

        try {
            Set<String> allKeys = adapter.getAllKeys();

            LOG.info("Flushing {} keys from database", allKeys.size());

            int deletedCount = 0;
            for (String key : allKeys) {
                try {
                    adapter.delete(key.getBytes(StandardCharsets.UTF_8));
                    deletedCount++;
                } catch (Exception e) {
                    LOG.warn("Failed to delete key '{}'", key, e);
                }
            }

            LOG.info("Successfully deleted {}/{} keys", deletedCount, allKeys.size());

            return RedisResponse.ok();
        } catch (Exception e) {
            LOG.error("Error executing FLUSH command", e);
            return RedisErrorSanitizer.sanitizeError(e, "FLUSHDB");
        }
    }
}
