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

package org.apache.fluss.redis.server;

import org.apache.fluss.redis.executor.RedisCommandRouter;
import org.apache.fluss.redis.protocol.RedisCommand;
import org.apache.fluss.redis.protocol.RedisResponse;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisConnectionHandler extends SimpleChannelInboundHandler<RedisMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisConnectionHandler.class);

    private final RedisCommandRouter commandRouter;

    public RedisConnectionHandler(RedisCommandRouter commandRouter) {
        this.commandRouter = commandRouter;
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
            LOG.info("Executing Redis command: {}", command);

            RedisMessage response = commandRouter.route(command);
            ctx.writeAndFlush(response);

        } catch (Exception e) {
            LOG.error("Error processing Redis command", e);
            ctx.writeAndFlush(RedisResponse.error("ERR " + e.getMessage()));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("Exception in Redis connection handler", cause);
        ctx.close();
    }
}
