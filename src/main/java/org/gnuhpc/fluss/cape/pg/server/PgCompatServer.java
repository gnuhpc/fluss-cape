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

package org.gnuhpc.fluss.cape.pg.server;

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
import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.gnuhpc.fluss.cape.pg.protocol.PgPacketDecoder;
import org.gnuhpc.fluss.cape.pg.protocol.PgPacketEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PgCompatServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PgCompatServer.class);

    private final String host;
    private final int port;
    private final PgAuthConfig authConfig;
    private final Connection flussConnection;
    private final Admin flussAdmin;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public PgCompatServer(
            String host,
            int port,
            PgAuthConfig authConfig,
            Connection flussConnection,
            Admin flussAdmin) {
        this.host = host;
        this.port = port;
        this.authConfig = authConfig;
        this.flussConnection = flussConnection;
        this.flussAdmin = flussAdmin;
    }

    public void start() throws IOException {
        LOG.info("Starting PostgreSQL compatibility server on {}:{}", host, port);

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
                                    ChannelPipeline pipeline = ch.pipeline();
                                    pipeline.addLast(new PgPacketDecoder());
                                    pipeline.addLast(new PgPacketEncoder());
                                    pipeline.addLast(new PgConnectionHandler(
                                            authConfig,
                                            flussConnection,
                                            flussAdmin));
                                }
                            });

            this.serverChannel = bootstrap.bind(host, port).sync().channel();

            InetSocketAddress bindAddress = (InetSocketAddress) serverChannel.localAddress();
            LOG.info(
                    "PostgreSQL compatibility server started on {}:{}",
                    bindAddress.getHostString(),
                    bindAddress.getPort());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Failed to start PostgreSQL compatibility server", e);
        } catch (Exception e) {
            close();
            throw new IOException("Failed to start PostgreSQL compatibility server", e);
        }
    }

    @Override
    public void close() {
        LOG.info("Shutting down PostgreSQL compatibility server");

        if (serverChannel != null) {
            serverChannel.close();
        }

        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
    }
}
