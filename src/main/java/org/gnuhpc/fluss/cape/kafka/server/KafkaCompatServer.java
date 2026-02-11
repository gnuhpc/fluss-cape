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
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.gnuhpc.fluss.cape.common.server.AbstractNettyServer;
import org.gnuhpc.fluss.cape.kafka.config.KafkaCompatConfig;
import org.gnuhpc.fluss.cape.kafka.handler.KafkaRequestHandler;
import org.gnuhpc.fluss.cape.kafka.protocol.KafkaProtocolDecoder;

public class KafkaCompatServer extends AbstractNettyServer {

    private final KafkaCompatConfig config;
    private final Connection flussConnection;
    private final KafkaRequestHandler requestHandler;

    public KafkaCompatServer(Connection flussConnection, KafkaCompatConfig config) {
        super(config.getHost(), config.getPort());
        this.flussConnection = flussConnection;
        this.config = config;
        this.requestHandler = new KafkaRequestHandler(flussConnection, config);
    }

    @Override
    protected String getProtocolName() {
        return "Kafka";
    }

    @Override
    protected ChannelInitializer<SocketChannel> createChannelInitializer() {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ch.pipeline()
                        .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
                                Integer.MAX_VALUE, 0, 4, 0, 4))
                        .addLast("frameEncoder", new LengthFieldPrepender(4))
                        .addLast("connectionHandler", new KafkaConnectionHandler())
                        .addLast("protocolDecoder", new KafkaProtocolDecoder(requestHandler));
            }
        };
    }

    @Override
    public void close() throws Exception {
        if (requestHandler != null) {
            requestHandler.close();
        }
        super.close();
    }
}
