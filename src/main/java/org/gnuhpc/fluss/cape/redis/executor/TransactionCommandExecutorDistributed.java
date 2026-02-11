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
import org.gnuhpc.fluss.cape.redis.transaction.DistributedTransactionManager;
import org.gnuhpc.fluss.cape.redis.transaction.DistributedTransactionManager.SerializableCommand;
import org.gnuhpc.fluss.cape.redis.util.RedisErrorSanitizer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionCommandExecutorDistributed implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionCommandExecutorDistributed.class);

    private final Map<ChannelHandlerContext, String> contextToTxnId;
    private final Map<ChannelHandlerContext, Boolean> contextHasQueueError;
    private final DistributedTransactionManager txnManager;
    private final RedisCommandRouter router;

    public TransactionCommandExecutorDistributed(
            DistributedTransactionManager txnManager, 
            RedisCommandRouter router) {
        this.txnManager = txnManager;
        this.router = router;
        this.contextToTxnId = new ConcurrentHashMap<>();
        this.contextHasQueueError = new ConcurrentHashMap<>();
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

        if (contextToTxnId.containsKey(ctx)) {
            return RedisResponse.error("ERR MULTI calls can not be nested");
        }

        try {
            String clientId = ctx.channel().id().asLongText();
            String txnId = txnManager.beginTransaction(clientId);
            contextToTxnId.put(ctx, txnId);
            contextHasQueueError.put(ctx, false);
            
            LOG.info("Transaction started: txnId={}, clientId={} [DISTRIBUTED]", 
                txnId, clientId);
            return RedisResponse.ok();
        } catch (Exception e) {
            LOG.error("Failed to start transaction", e);
            return RedisErrorSanitizer.sanitizeError(e, "MULTI");
        }
    }

    private RedisMessage executeExec(RedisCommand command) {
        ChannelHandlerContext ctx = command.getContext();
        if (ctx == null) {
            return RedisResponse.error("ERR no channel context available");
        }

        String txnId = contextToTxnId.remove(ctx);
        Boolean hasQueueError = contextHasQueueError.remove(ctx);
        
        if (txnId == null) {
            return RedisResponse.error("ERR EXEC without MULTI");
        }

        if (Boolean.TRUE.equals(hasQueueError)) {
            try {
                txnManager.discardTransaction(txnId);
                LOG.warn("Transaction aborted due to previous queue error: txnId={}", txnId);
            } catch (Exception e) {
                LOG.error("Failed to discard failed transaction: txnId={}", txnId, e);
            }
            return RedisResponse.error("EXECABORT Transaction discarded because of previous errors");
        }

        try {
            List<SerializableCommand> commands = txnManager.getCommandsForExecution(txnId);
            
            txnManager.markExecuting(txnId);
            
            List<RedisMessage> results = new ArrayList<>();
            boolean executionFailed = false;
            Exception executionException = null;
            
            for (SerializableCommand serialCmd : commands) {
                try {
                    RedisCommand redisCmd = reconstructCommand(serialCmd, ctx);
                    RedisMessage result = router.route(redisCmd, ctx, null);
                    results.add(result);
                } catch (Exception e) {
                    LOG.error("Command execution failed in transaction: txnId={}, command={}", 
                        txnId, serialCmd.command, e);
                    results.add(RedisErrorSanitizer.sanitizeError(e, serialCmd.command));
                    executionFailed = true;
                    executionException = e;
                }
            }
            
            if (!executionFailed) {
                txnManager.markCommitted(txnId);
                LOG.info("Transaction committed: txnId={}, commandCount={}", 
                    txnId, results.size());
            } else {
                txnManager.markFailed(txnId);
                LOG.error("Transaction failed during execution: txnId={}, commandCount={}", 
                    txnId, results.size());
            }
            
            return new ArrayRedisMessage(results);
            
        } catch (Exception e) {
            LOG.error("Failed to execute transaction: txnId={}", txnId, e);
            try {
                txnManager.markFailed(txnId);
            } catch (Exception failEx) {
                LOG.error("Failed to mark transaction as FAILED: txnId={}", txnId, failEx);
            }
            return RedisErrorSanitizer.sanitizeError(e, "EXEC");
        }
    }

    private RedisMessage executeDiscard(RedisCommand command) {
        ChannelHandlerContext ctx = command.getContext();
        if (ctx == null) {
            return RedisResponse.error("ERR no channel context available");
        }

        String txnId = contextToTxnId.remove(ctx);
        contextHasQueueError.remove(ctx);
        
        if (txnId == null) {
            return RedisResponse.error("ERR DISCARD without MULTI");
        }

        try {
            txnManager.discardTransaction(txnId);
            LOG.info("Transaction discarded: txnId={}", txnId);
            return RedisResponse.ok();
        } catch (Exception e) {
            LOG.error("Failed to discard transaction: txnId={}", txnId, e);
            return RedisErrorSanitizer.sanitizeError(e, "DISCARD");
        }
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
        return contextToTxnId.containsKey(ctx);
    }

    public RedisMessage queueCommand(ChannelHandlerContext ctx, RedisCommand command) {
        String txnId = contextToTxnId.get(ctx);
        if (txnId == null) {
            return null;
        }
        
        try {
            SerializableCommand serialCmd = serializeCommand(command);
            txnManager.queueCommand(txnId, serialCmd);
            LOG.trace("Command queued: txnId={}, command={}", 
                txnId, command.getCommand());
            return new SimpleStringRedisMessage("QUEUED");
        } catch (Exception e) {
            LOG.error("Failed to queue command: txnId={}, command={}", 
                txnId, command.getCommand(), e);
            contextHasQueueError.put(ctx, true);
            return RedisErrorSanitizer.sanitizeError(e, command.getCommand());
        }
    }

    private SerializableCommand serializeCommand(RedisCommand command) {
        List<byte[]> args = new ArrayList<>();
        for (int i = 0; i < command.getArgCount(); i++) {
            args.add(command.getArg(i));
        }
        return new SerializableCommand(command.getCommand(), args);
    }

    private RedisCommand reconstructCommand(SerializableCommand serialCmd, ChannelHandlerContext ctx) {
        RedisCommand cmd = new RedisCommand(serialCmd.command, serialCmd.args);
        cmd.setContext(ctx);
        return cmd;
    }
}
