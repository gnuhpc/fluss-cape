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

package org.gnuhpc.fluss.cape.redis.server;

import org.gnuhpc.fluss.cape.redis.auth.RedisConnectionContext;
import org.gnuhpc.fluss.cape.redis.executor.RedisCommandRouter;
import org.gnuhpc.fluss.cape.redis.protocol.RedisCommand;
import org.gnuhpc.fluss.cape.redis.protocol.RedisResponse;
import org.gnuhpc.fluss.cape.redis.util.BlockingOperationQueue;
import org.gnuhpc.fluss.cape.redis.util.RedisErrorSanitizer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisConnectionHandler extends SimpleChannelInboundHandler<RedisMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisConnectionHandler.class);

    private final RedisCommandRouter commandRouter;
    private final BlockingOperationQueue blockingQueue;
    private final boolean authenticationEnabled;

    public RedisConnectionHandler(
            RedisCommandRouter commandRouter,
            BlockingOperationQueue blockingQueue,
            boolean authenticationEnabled) {
        this.commandRouter = commandRouter;
        this.blockingQueue = blockingQueue;
        this.authenticationEnabled = authenticationEnabled;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RedisMessage msg) {
        LOG.debug("Received Redis message: {}", msg.getClass().getSimpleName());

        if (!(msg instanceof ArrayRedisMessage)) {
            LOG.warn("Unexpected message type: {}", msg.getClass().getName());
            ctx.writeAndFlush(RedisResponse.error("ERR Protocol error: expected Array"));
            return;
        }

        try {
            RedisCommand command = RedisCommand.parse((ArrayRedisMessage) msg);
            command.setContext(ctx);
            LOG.debug("Executing Redis command: {}", command);

            RedisConnectionContext connCtx = RedisConnectionContext.get(ctx.channel());

            if (authenticationEnabled && !command.getCommand().equals("AUTH")) {
                if (!connCtx.isAuthenticated()) {
                    ctx.writeAndFlush(
                            RedisResponse.error("NOAUTH Authentication required."));
                    return;
                }
            }

            connCtx.incrementRequestCount();

            RedisMessage response = commandRouter.route(command, ctx, blockingQueue);

            if (response != null) {
                ctx.writeAndFlush(response);
            }

        } catch (Exception e) {
            ctx.writeAndFlush(RedisErrorSanitizer.sanitizeError(e, "COMMAND_PARSE"));
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOG.debug("Channel inactive, cancelling blocking operations for {}", ctx.channel().id());
        blockingQueue.cancelAllForChannel(ctx);
        commandRouter.cleanupSubscriptions(ctx);
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("Exception in Redis connection handler", cause);
        blockingQueue.cancelAllForChannel(ctx);
        commandRouter.cleanupSubscriptions(ctx);
        ctx.close();
    }
}
