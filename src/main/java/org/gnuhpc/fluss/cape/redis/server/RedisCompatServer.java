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

import org.gnuhpc.fluss.cape.common.server.AbstractNettyServer;
import org.gnuhpc.fluss.cape.redis.executor.RedisCommandRouter;
import org.gnuhpc.fluss.cape.redis.util.BlockingOperationQueue;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.redis.RedisArrayAggregator;
import io.netty.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;
import io.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLEngine;

public class RedisCompatServer extends AbstractNettyServer {

    private final RedisCommandRouter commandRouter;
    private final BlockingOperationQueue blockingQueue;
    private final boolean authenticationEnabled;
    private final RedisSslContextProvider sslProvider;

    public RedisCompatServer(
            String host,
            int port,
            RedisCommandRouter commandRouter,
            boolean authenticationEnabled,
            RedisSslContextProvider sslProvider) {
        super(host, port);
        this.commandRouter = commandRouter;
        this.blockingQueue = new BlockingOperationQueue();
        this.authenticationEnabled = authenticationEnabled;
        this.sslProvider = sslProvider;
    }

    public RedisCompatServer(
            String host, int port, RedisCommandRouter commandRouter, boolean authenticationEnabled) {
        this(host, port, commandRouter, authenticationEnabled, null);
    }

    public RedisCompatServer(String host, int port, RedisCommandRouter commandRouter) {
        this(host, port, commandRouter, false, null);
    }

    @Override
    protected String getProtocolName() {
        return "Redis";
    }

    @Override
    protected ChannelInitializer<SocketChannel> createChannelInitializer() {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ChannelPipeline p = ch.pipeline();
                
                if (sslProvider != null && sslProvider.isSslEnabled()) {
                    SSLEngine engine = sslProvider.getSslContext().newEngine(ch.alloc());
                    engine.setUseClientMode(false);
                    p.addLast(new SslHandler(engine));
                }
                
                p.addLast(new RedisDecoder());
                p.addLast(new RedisBulkStringAggregator());
                p.addLast(new RedisArrayAggregator());
                p.addLast(new RedisEncoder());
                p.addLast(
                        new RedisConnectionHandler(
                                commandRouter, blockingQueue, authenticationEnabled));
            }
        };
    }
}
