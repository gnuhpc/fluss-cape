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

package org.apache.fluss.hbase.server;

import org.apache.fluss.hbase.executor.HBaseRequestRouter;
import org.apache.fluss.hbase.protocol.HBaseRpcRequest;
import org.apache.fluss.hbase.protocol.HBaseRpcResponse;
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
        writeDebugLog("[HANDLER] *** CLIENT CONNECTED: " + ctx.channel().remoteAddress());
        System.err.println("[HANDLER] *** CLIENT CONNECTED: " + ctx.channel().remoteAddress());
        LOG.info("HBase client connected from {}", ctx.channel().remoteAddress());
    }

    private void writeDebugLog(String message) {
        try (java.io.FileWriter fw = new java.io.FileWriter("/tmp/hbase-debug.log", true)) {
            fw.write(new java.util.Date() + ": " + message + "\n");
        } catch (Exception e) {
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        LOG.info("HBase client disconnected from {}", ctx.channel().remoteAddress());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HBaseRpcRequest request)
            throws Exception {
        System.err.println("[HANDLER] *** CHANNEL READ START: callId=" + request.getCallId() + 
                " method=" + request.getMethodName());
        LOG.info(
                "[HANDLER] START channelRead0: callId={}, method={}",
                request.getCallId(),
                request.getMethodName());

        CompletableFuture<HBaseRpcResponse> responseFuture = requestRouter.route(request);
        System.err.println("[HANDLER] *** GOT FUTURE FROM ROUTER: callId=" + request.getCallId());
        LOG.info(
                "[HANDLER] Got future from router for callId={}, method={}",
                request.getCallId(),
                request.getMethodName());

        responseFuture.whenComplete(
                (response, throwable) -> {
                    LOG.info(
                            "[HANDLER] Future completed: callId={}, hasError={}, hasResponse={}",
                            request.getCallId(),
                            throwable != null,
                            response != null);
                    if (throwable != null) {
                        LOG.error(
                                "[HANDLER] Request failed: callId={}, method={}",
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
                        LOG.info(
                                "[HANDLER] Writing response: callId={}, hasMessage={}",
                                request.getCallId(),
                                response.getResponseMessage() != null);
                        ctx.writeAndFlush(response);
                    }
                });
        LOG.info("[HANDLER] END channelRead0: callId={}", request.getCallId());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.err.println("[HANDLER] *** EXCEPTION CAUGHT: " + cause.getMessage());
        cause.printStackTrace(System.err);
        LOG.error("Exception caught in HBase connection handler", cause);
        ctx.close();
    }
}
