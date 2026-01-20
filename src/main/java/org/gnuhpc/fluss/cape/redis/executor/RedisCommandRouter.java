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

import org.gnuhpc.fluss.cape.redis.protocol.RedisCommand;
import org.gnuhpc.fluss.cape.redis.protocol.RedisResponse;
import org.gnuhpc.fluss.cape.redis.util.BlockingOperationQueue;
import org.gnuhpc.fluss.cape.redis.util.RedisErrorSanitizer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisCommandRouter {

    private static final Logger LOG = LoggerFactory.getLogger(RedisCommandRouter.class);

    private final Map<String, RedisCommandExecutor> executors = new ConcurrentHashMap<>();
    private TransactionCommandExecutor transactionExecutor;

    public void registerExecutor(String command, RedisCommandExecutor executor) {
        executors.put(command.toUpperCase(), executor);
        LOG.info("Registered Redis executor for command: {}", command);
        
        // Cache transaction executor reference for fast lookup
        if (executor instanceof TransactionCommandExecutor) {
            this.transactionExecutor = (TransactionCommandExecutor) executor;
        }
    }

    public RedisMessage route(RedisCommand command, ChannelHandlerContext ctx, BlockingOperationQueue blockingQueue) {
        String commandName = command.getCommand();
        
        // Check if in transaction mode - queue commands if not MULTI/EXEC/DISCARD
        if (transactionExecutor != null && transactionExecutor.isInTransaction(ctx)) {
            if (!commandName.equals("EXEC") && !commandName.equals("DISCARD") && !commandName.equals("MULTI")) {
                transactionExecutor.queueCommand(ctx, command);
                return RedisResponse.bulkString("QUEUED");
            }
        }
        
        RedisCommandExecutor executor = executors.get(commandName);

        if (executor == null) {
            LOG.warn("Unsupported Redis command: {}", commandName);
            return RedisResponse.error("ERR unknown command '" + commandName + "'");
        }

        try {
            if (executor instanceof BlockingAwareExecutor) {
                return ((BlockingAwareExecutor) executor).execute(command, ctx, blockingQueue);
            } else {
                return executor.execute(command);
            }
        } catch (Exception e) {
            LOG.error("Error executing command: {}", commandName, e);
            return RedisErrorSanitizer.sanitizeError(e, commandName);
        }
    }
}
