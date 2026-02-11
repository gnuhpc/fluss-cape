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

package org.gnuhpc.fluss.cape.common.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public abstract class AbstractNettyServer implements ProtocolServer {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractNettyServer.class);
    
    private static final int DEFAULT_SO_BACKLOG = 1024;
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 15;
    
    protected final String host;
    protected final int port;
    
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private volatile boolean running = false;
    
    protected AbstractNettyServer(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    @Override
    public void start() throws IOException {
        LOG.info("Starting {} server on {}:{}", getProtocolName(), host, port);
        
        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup();
        
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            configureBootstrap(bootstrap);
            
            bootstrap
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, getSoBacklog())
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
            
            if (isLoggingEnabled()) {
                bootstrap.handler(new LoggingHandler(getLogLevel()));
            }
            
            bootstrap.childHandler(createChannelInitializer());
            
            this.serverChannel = bootstrap.bind(host, port).sync().channel();
            
            InetSocketAddress bindAddress = (InetSocketAddress) serverChannel.localAddress();
            running = true;
            
            LOG.info("{} server started on {}:{}", 
                    getProtocolName(),
                    bindAddress.getHostString(),
                    bindAddress.getPort());
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            try {
                close();
            } catch (Exception closeException) {
                LOG.warn("Failed to close server during startup failure", closeException);
            }
            throw new IOException("Failed to start " + getProtocolName() + " server", e);
        } catch (Exception e) {
            try {
                close();
            } catch (Exception closeException) {
                LOG.warn("Failed to close server during startup failure", closeException);
            }
            throw new IOException("Failed to start " + getProtocolName() + " server", e);
        }
    }
    
    @Override
    public void close() throws Exception {
        if (!running && serverChannel == null) {
            return;
        }
        
        LOG.info("Shutting down {} server...", getProtocolName());
        running = false;
        
        try {
            if (serverChannel != null) {
                serverChannel.close().await(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while closing server channel", e);
        }
        
        shutdownEventLoopGroup(workerGroup, "worker");
        shutdownEventLoopGroup(bossGroup, "boss");
        
        LOG.info("{} server stopped", getProtocolName());
    }
    
    @Override
    public int getBoundPort() {
        if (serverChannel != null && serverChannel.localAddress() != null) {
            return ((InetSocketAddress) serverChannel.localAddress()).getPort();
        }
        return port;
    }
    
    protected abstract String getProtocolName();
    
    protected abstract ChannelInitializer<SocketChannel> createChannelInitializer();
    
    protected void configureBootstrap(ServerBootstrap bootstrap) {
    }
    
    protected int getSoBacklog() {
        return DEFAULT_SO_BACKLOG;
    }
    
    protected boolean isLoggingEnabled() {
        return true;
    }
    
    protected LogLevel getLogLevel() {
        return LogLevel.INFO;
    }
    
    protected boolean isRunning() {
        return running;
    }
    
    protected EventLoopGroup getBossGroup() {
        return bossGroup;
    }
    
    protected EventLoopGroup getWorkerGroup() {
        return workerGroup;
    }
    
    private void shutdownEventLoopGroup(EventLoopGroup group, String name) {
        if (group == null) {
            return;
        }
        
        try {
            group.shutdownGracefully(1, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .await(SHUTDOWN_TIMEOUT_SECONDS + 5, TimeUnit.SECONDS);
            
            if (!group.isShutdown()) {
                LOG.warn("{} EventLoopGroup ({}) did not shut down in time", 
                        getProtocolName(), name);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while shutting down {} EventLoopGroup", name, e);
        }
    }
}
