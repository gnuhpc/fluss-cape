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

import org.apache.fluss.redis.protocol.RedisCommand;
import org.apache.fluss.redis.protocol.RedisResponse;

import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class RedisCommandRouter {

    private static final Logger LOG = LoggerFactory.getLogger(RedisCommandRouter.class);

    private final Map<String, RedisCommandExecutor> executors = new HashMap<>();

    public void registerExecutor(String command, RedisCommandExecutor executor) {
        executors.put(command.toUpperCase(), executor);
        LOG.info("Registered Redis executor for command: {}", command);
    }

    public RedisMessage route(RedisCommand command) {
        String commandName = command.getCommand();
        RedisCommandExecutor executor = executors.get(commandName);

        if (executor == null) {
            LOG.warn("Unsupported Redis command: {}", commandName);
            return RedisResponse.error("ERR unknown command '" + commandName + "'");
        }

        try {
            return executor.execute(command);
        } catch (Exception e) {
            LOG.error("Error executing command: {}", commandName, e);
            return RedisResponse.error("ERR " + e.getMessage());
        }
    }
}
