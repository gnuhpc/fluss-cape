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

import org.gnuhpc.fluss.cape.hbase.executor.HBaseRequestRouter;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcRequest;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcResponse;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/** Handles incoming HBase RPC requests and routes them to appropriate executors. */
public class HBaseConnectionHandler extends SimpleChannelInboundHandler<HBaseRpcRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseConnectionHandler.class);

    private final HBaseRequestRouter requestRouter;

    public HBaseConnectionHandler(HBaseRequestRouter requestRouter) {
        this.requestRouter = requestRouter;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        LOG.info("HBase client connected from {}", ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        LOG.info("HBase client disconnected from {}", ctx.channel().remoteAddress());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HBaseRpcRequest request)
            throws Exception {
        LOG.debug(
                "Processing request: callId={}, method={}",
                request.getCallId(),
                request.getMethodName());

        CompletableFuture<HBaseRpcResponse> responseFuture = requestRouter.route(request);

        responseFuture.whenComplete(
                (response, throwable) -> {
                    if (throwable != null) {
                        LOG.error(
                                "Request failed: callId={}, method={}",
                                request.getCallId(),
                                request.getMethodName(),
                                throwable);
                        HBaseRpcResponse errorResponse =
                                HBaseRpcResponse.failure(
                                        request.getCallId(),
                                        throwable instanceof Exception
                                                ? (Exception) throwable
                                                : new Exception(throwable));
                        ctx.writeAndFlush(errorResponse);
                    } else {
                        LOG.debug(
                                "Sending response: callId={}",
                                request.getCallId());
                        ctx.writeAndFlush(response);
                    }
                });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.error("Exception caught in HBase connection handler", cause);
        ctx.close();
    }
}
