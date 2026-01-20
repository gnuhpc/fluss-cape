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

package org.gnuhpc.fluss.cape.kafka.server;

import org.apache.fluss.client.Connection;
import org.apache.fluss.rpc.netty.NettyUtils;
import org.apache.fluss.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.fluss.shaded.netty4.io.netty.buffer.PooledByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldPrepender;
import org.apache.fluss.shaded.netty4.io.netty.handler.logging.LogLevel;
import org.apache.fluss.shaded.netty4.io.netty.handler.logging.LoggingHandler;
import org.gnuhpc.fluss.cape.kafka.config.KafkaCompatConfig;
import org.gnuhpc.fluss.cape.kafka.handler.KafkaRequestHandler;
import org.gnuhpc.fluss.cape.kafka.protocol.KafkaProtocolDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class KafkaCompatServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaCompatServer.class);

    private final KafkaCompatConfig config;
    private final Connection flussConnection;
    private final KafkaRequestHandler requestHandler;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public KafkaCompatServer(Connection flussConnection, KafkaCompatConfig config) {
        this.flussConnection = flussConnection;
        this.config = config;
        this.requestHandler = new KafkaRequestHandler(flussConnection, config);
    }

    public void start() throws IOException {
        LOG.info("Starting Kafka compatibility server on {}:{}", config.getHost(), config.getPort());

        this.bossGroup = NettyUtils.newEventLoopGroup(1, "kafka-compat-boss");
        this.workerGroup = NettyUtils.newEventLoopGroup(0, "kafka-compat-worker");

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
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
                                            Integer.MAX_VALUE, 0, 4, 0, 4))
                                    .addLast("frameEncoder", new LengthFieldPrepender(4))
                                    .addLast("connectionHandler", new KafkaConnectionHandler())
                                    .addLast("protocolDecoder", new KafkaProtocolDecoder(requestHandler));
                        }
                    });

            this.serverChannel = bootstrap.bind(config.getHost(), config.getPort()).sync().channel();

            InetSocketAddress bindAddress = (InetSocketAddress) serverChannel.localAddress();
            LOG.info("Kafka compatibility server started on {}:{}", 
                    bindAddress.getHostString(), bindAddress.getPort());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Failed to start Kafka compatibility server", e);
        } catch (Exception e) {
            close();
            throw new IOException("Failed to start Kafka compatibility server", e);
        }
    }

    @Override
    public void close() {
        LOG.info("Shutting down Kafka compatibility server");

        if (requestHandler != null) {
            requestHandler.close();
        }

        if (serverChannel != null) {
            serverChannel.close();
        }

        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }

        LOG.info("Kafka compatibility server shut down complete");
    }

    public int getBoundPort() {
        if (serverChannel != null && serverChannel.localAddress() instanceof InetSocketAddress) {
            return ((InetSocketAddress) serverChannel.localAddress()).getPort();
        }
        return -1;
    }
}
