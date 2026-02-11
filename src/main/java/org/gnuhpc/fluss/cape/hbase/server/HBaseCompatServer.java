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
import org.gnuhpc.fluss.cape.common.server.AbstractNettyServer;
import org.gnuhpc.fluss.cape.hbase.executor.HBaseRequestRouter;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcDecoder;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcEncoder;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseCompatServer extends AbstractNettyServer {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseCompatServer.class);

    private final Configuration config;
    private final HBaseRequestRouter requestRouter;

    public HBaseCompatServer(
            Configuration config, String host, int port, HBaseRequestRouter requestRouter) {
        super(host, port);
        this.config = config;
        this.requestRouter = requestRouter;
    }

    @Override
    protected String getProtocolName() {
        return "HBase";
    }

    @Override
    protected ChannelInitializer<SocketChannel> createChannelInitializer() {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                LOG.debug("Initializing channel for {}", ch.remoteAddress());
                ch.pipeline()
                        .addLast("decoder", new HBaseRpcDecoder())
                        .addLast("encoder", new HBaseRpcEncoder())
                        .addLast("handler", new HBaseConnectionHandler(requestRouter));
            }
        };
    }

    public Configuration getConfig() {
        return config;
    }
}
