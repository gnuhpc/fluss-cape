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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.redis.RedisArrayAggregator;
import io.netty.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class RedisCompatServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RedisCompatServer.class);

    private final String host;
    private final int port;
    private final RedisCommandRouter commandRouter;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public RedisCompatServer(String host, int port, RedisCommandRouter commandRouter) {
        this.host = host;
        this.port = port;
        this.commandRouter = commandRouter;
    }

    public void start() throws IOException {
        LOG.info("Starting Redis compatibility server on {}:{}", host, port);

        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childHandler(
                            new ChannelInitializer<SocketChannel>() {
                                @Override
                                protected void initChannel(SocketChannel ch) {
                                    ChannelPipeline p = ch.pipeline();
                                    p.addLast(new RedisDecoder());
                                    p.addLast(new RedisBulkStringAggregator());
                                    p.addLast(new RedisArrayAggregator());
                                    p.addLast(new RedisEncoder());
                                    p.addLast(new RedisConnectionHandler(commandRouter));
                                }
                            });

            this.serverChannel = bootstrap.bind(host, port).sync().channel();

            InetSocketAddress bindAddress = (InetSocketAddress) serverChannel.localAddress();
            LOG.info(
                    "Redis compatibility server started on {}:{}",
                    bindAddress.getHostString(),
                    bindAddress.getPort());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Failed to start Redis compatibility server", e);
        } catch (Exception e) {
            close();
            throw new IOException("Failed to start Redis compatibility server", e);
        }
    }

    @Override
    public void close() {
        LOG.info("Shutting down Redis compatibility server");

        if (serverChannel != null) {
            serverChannel.close();
        }

        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }

        LOG.info("Redis compatibility server shut down complete");
    }

    public int getBoundPort() {
        if (serverChannel != null && serverChannel.localAddress() instanceof InetSocketAddress) {
            return ((InetSocketAddress) serverChannel.localAddress()).getPort();
        }
        return -1;
    }
}
