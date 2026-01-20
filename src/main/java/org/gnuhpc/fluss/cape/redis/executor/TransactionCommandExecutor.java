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
import org.gnuhpc.fluss.cape.redis.util.RedisErrorSanitizer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionCommandExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionCommandExecutor.class);

    private final Map<ChannelHandlerContext, TransactionContext> transactions;
    private final RedisCommandRouter router;

    public TransactionCommandExecutor(RedisCommandRouter router) {
        this.transactions = new ConcurrentHashMap<>();
        this.router = router;
    }

    @Override
    public RedisMessage execute(RedisCommand command) {
        String cmd = command.getCommand();

        try {
            switch (cmd) {
                case "MULTI":
                    return executeMulti(command);
                case "EXEC":
                    return executeExec(command);
                case "DISCARD":
                    return executeDiscard(command);
                case "WATCH":
                    return executeWatch(command);
                case "UNWATCH":
                    return executeUnwatch(command);
                default:
                    return RedisResponse.error("ERR unknown transaction command '" + cmd + "'");
            }
        } catch (Exception e) {
            LOG.error("Error executing transaction command: {}", cmd, e);
            return RedisErrorSanitizer.sanitizeError(e, cmd);
        }
    }

    private RedisMessage executeMulti(RedisCommand command) {
        ChannelHandlerContext ctx = command.getContext();
        if (ctx == null) {
            return RedisResponse.error("ERR no channel context available");
        }

        if (transactions.containsKey(ctx)) {
            return RedisResponse.error("ERR MULTI calls can not be nested");
        }

        transactions.put(ctx, new TransactionContext());
        return RedisResponse.ok();
    }

    private RedisMessage executeExec(RedisCommand command) {
        ChannelHandlerContext ctx = command.getContext();
        if (ctx == null) {
            return RedisResponse.error("ERR no channel context available");
        }

        TransactionContext txn = transactions.remove(ctx);
        if (txn == null) {
            return RedisResponse.error("ERR EXEC without MULTI");
        }

        List<RedisMessage> results = new ArrayList<>();
        for (RedisCommand queuedCommand : txn.commands) {
            RedisMessage result = router.route(queuedCommand, ctx, null);
            results.add(result);
        }

        return new ArrayRedisMessage(results);
    }

    private RedisMessage executeDiscard(RedisCommand command) {
        ChannelHandlerContext ctx = command.getContext();
        if (ctx == null) {
            return RedisResponse.error("ERR no channel context available");
        }

        TransactionContext txn = transactions.remove(ctx);
        if (txn == null) {
            return RedisResponse.error("ERR DISCARD without MULTI");
        }

        return RedisResponse.ok();
    }

    private RedisMessage executeWatch(RedisCommand command) {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'watch' command");
        }

        return RedisResponse.ok();
    }

    private RedisMessage executeUnwatch(RedisCommand command) {
        return RedisResponse.ok();
    }

    public boolean isInTransaction(ChannelHandlerContext ctx) {
        return transactions.containsKey(ctx);
    }

    public void queueCommand(ChannelHandlerContext ctx, RedisCommand command) {
        TransactionContext txn = transactions.get(ctx);
        if (txn != null) {
            txn.commands.add(command);
        }
    }

    private static class TransactionContext {
        final List<RedisCommand> commands = new ArrayList<>();
    }
}
