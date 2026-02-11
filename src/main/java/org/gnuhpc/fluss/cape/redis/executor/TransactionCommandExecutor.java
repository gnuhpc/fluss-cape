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

/**
 * Executor for Redis transaction commands (MULTI/EXEC/DISCARD/WATCH/UNWATCH).
 *
 * <p><b>CRITICAL - Multi-Instance Deployment Limitation:</b>
 * <p>Transaction state is stored **per-connection** in local memory using
 * {@code Map<ChannelHandlerContext, TransactionContext>}. This creates a critical
 * limitation in multi-instance deployments.
 *
 * <p><b>The Problem:</b>
 * <pre>
 * Client → Load Balancer → CAPE Instance A: MULTI
 *                        → CAPE Instance B: SET key value  (routed to different instance!)
 *                        → CAPE Instance C: EXEC           (transaction context lost!)
 * Result: "ERR EXEC without MULTI" ❌
 * </pre>
 *
 * <p><b>Why This Happens:</b>
 * <ul>
 *   <li>Transaction queue stored in Instance A's memory (line 39)</li>
 *   <li>Load balancer routes next command to Instance B or C</li>
 *   <li>Instance B/C has no knowledge of transaction started on Instance A</li>
 *   <li>EXEC fails because transaction context doesn't exist</li>
 * </ul>
 *
 * <p><b>Impact on Multi-Instance Deployments:</b>
 * <table>
 *   <tr><th>Deployment</th><th>Works?</th><th>Requirement</th></tr>
 *   <tr><td>Single instance</td><td>✅ Yes</td><td>None</td></tr>
 *   <tr><td>Multiple instances + sticky sessions</td><td>✅ Yes</td><td>Load balancer session affinity</td></tr>
 *   <tr><td>Multiple instances without sticky sessions</td><td>❌ No</td><td>Will fail randomly</td></tr>
 * </table>
 *
 * <p><b>Required Configuration (HAProxy Example):</b>
 * <pre>
 * backend redis_cape
 *     balance source                    # Source IP hashing for session affinity
 *     hash-type consistent              # Consistent hashing
 *     server cape1 10.0.0.1:6379 check
 *     server cape2 10.0.0.2:6379 check
 *     server cape3 10.0.0.3:6379 check
 * </pre>
 *
 * <p><b>Required Configuration (Nginx Example):</b>
 * <pre>
 * upstream redis_cape {
 *     ip_hash;                          # Source IP hashing
 *     server 10.0.0.1:6379;
 *     server 10.0.0.2:6379;
 *     server 10.0.0.3:6379;
 * }
 * </pre>
 *
 * <p><b>Why Not Implement Distributed Transactions?</b>
 * <p>Storing transaction state in Fluss would require:
 * <ul>
 *   <li>Unique transaction ID generation across instances</li>
 *   <li>Serializing queued commands to Fluss</li>
 *   <li>Distributed lock management</li>
 *   <li>Timeout and cleanup of abandoned transactions</li>
 *   <li>Significant performance overhead (network roundtrip per command)</li>
 * </ul>
 * <p>Given that Fluss itself doesn't support true transactions (see CheckAndMutate
 * limitations), this complexity is not justified. Sticky sessions are simpler and
 * more performant.
 *
 * <p><b>Production Recommendations:</b>
 * <ul>
 *   <li>✅ <b>REQUIRED</b>: Configure load balancer with sticky sessions (source IP hash)</li>
 *   <li>✅ Monitor for "EXEC without MULTI" errors (indicates session affinity issues)</li>
 *   <li>✅ Test multi-instance deployment thoroughly before production</li>
 *   <li>⚠️ Document sticky session requirement for ops team</li>
 *   <li>⚠️ Consider single instance if sticky sessions not feasible</li>
 * </ul>
 *
 * <p><b>Alternative Approaches:</b>
 * <ol>
 *   <li><b>Avoid MULTI/EXEC</b> - Use atomic single commands instead</li>
 *   <li><b>Application-level batching</b> - Batch commands at app layer, not Redis</li>
 *   <li><b>Lua scripts</b> - Not supported in CAPE, but worth noting for reference</li>
 * </ol>
 *
 * @see <a href="https://github.com/gnuhpc/fluss-cape/blob/main/docs/REDIS-GUIDE.md#multi-instance-deployment">
 *     Redis Guide - Multi-Instance Deployment</a>
 * @see <a href="https://github.com/gnuhpc/fluss-cape/blob/main/docs/GETTING-STARTED.md#sticky-sessions">
 *     Getting Started - Sticky Sessions Configuration</a>
 */
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
