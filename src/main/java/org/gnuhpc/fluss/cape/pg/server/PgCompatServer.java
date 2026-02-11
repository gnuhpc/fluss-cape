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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.gnuhpc.fluss.cape.common.server.AbstractNettyServer;
import org.gnuhpc.fluss.cape.pg.protocol.PgPacketDecoder;
import org.gnuhpc.fluss.cape.pg.protocol.PgPacketEncoder;

public class PgCompatServer extends AbstractNettyServer {

    private final PgAuthConfig authConfig;
    private final String defaultDatabase;
    private final Connection flussConnection;
    private final Admin flussAdmin;

    public PgCompatServer(
            String host,
            int port,
            PgAuthConfig authConfig,
            String defaultDatabase,
            Connection flussConnection,
            Admin flussAdmin) {
        super(host, port);
        this.authConfig = authConfig;
        this.defaultDatabase = defaultDatabase;
        this.flussConnection = flussConnection;
        this.flussAdmin = flussAdmin;
    }

    @Override
    protected String getProtocolName() {
        return "PostgreSQL";
    }

    @Override
    protected ChannelInitializer<SocketChannel> createChannelInitializer() {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new PgPacketDecoder());
                pipeline.addLast(new PgPacketEncoder());
                pipeline.addLast(
                        new PgConnectionHandler(
                                authConfig,
                                flussConnection,
                                flussAdmin,
                                defaultDatabase));
            }
        };
    }
}
