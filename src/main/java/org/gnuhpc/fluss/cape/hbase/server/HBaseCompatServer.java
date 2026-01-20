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

package org.gnuhpc.fluss.cape.hbase.server;

import org.apache.fluss.config.Configuration;
import org.gnuhpc.fluss.cape.hbase.executor.HBaseRequestRouter;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcDecoder;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcEncoder;
import org.apache.fluss.rpc.netty.NettyUtils;
import org.apache.fluss.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.fluss.shaded.netty4.io.netty.buffer.PooledByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.logging.LogLevel;
import org.apache.fluss.shaded.netty4.io.netty.handler.logging.LoggingHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/** Netty-based HBase RPC protocol compatibility server for Apache Fluss. */
public class HBaseCompatServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseCompatServer.class);

    private final Configuration config;
    private final String host;
    private final int port;
    private final HBaseRequestRouter requestRouter;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public HBaseCompatServer(
            Configuration config, String host, int port, HBaseRequestRouter requestRouter) {
        this.config = config;
        this.host = host;
        this.port = port;
        this.requestRouter = requestRouter;
    }

    public void start() throws IOException {
        LOG.info("Starting HBase compatibility server on {}:{}", host, port);

        this.bossGroup = NettyUtils.newEventLoopGroup(1, "hbase-compat-boss");
        this.workerGroup = NettyUtils.newEventLoopGroup(0, "hbase-compat-worker");

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap
                    .group(bossGroup, workerGroup)
                    .channel(NettyUtils.getServerSocketChannelClass(workerGroup))
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(
                        new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                LOG.debug("Initializing channel for {}", ch.remoteAddress());
                                ch.pipeline()
                                        .addLast("decoder", new HBaseRpcDecoder())
                                        .addLast("encoder", new HBaseRpcEncoder())
                                        .addLast(
                                                "handler",
                                                new HBaseConnectionHandler(requestRouter));
                            }
                        });

        this.serverChannel = bootstrap.bind(host, port).sync().channel();

        InetSocketAddress bindAddress = (InetSocketAddress) serverChannel.localAddress();
        LOG.info(
                    "HBase compatibility server started on {}:{}",
                    bindAddress.getHostString(),
                    bindAddress.getPort());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Failed to start HBase compatibility server", e);
        } catch (Exception e) {
            close();
            throw new IOException("Failed to start HBase compatibility server", e);
        }
    }

    @Override
    public void close() {
        LOG.info("Shutting down HBase compatibility server");

        if (serverChannel != null) {
            serverChannel.close();
        }

        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }

        LOG.info("HBase compatibility server shut down complete");
    }

    public int getBoundPort() {
        if (serverChannel != null && serverChannel.localAddress() instanceof InetSocketAddress) {
            return ((InetSocketAddress) serverChannel.localAddress()).getPort();
        }
        return -1;
    }
}
