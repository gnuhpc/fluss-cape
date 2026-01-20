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

package org.gnuhpc.fluss.cape.kafka.protocol;

import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.fluss.shaded.netty4.io.netty.handler.timeout.IdleState;
import org.apache.fluss.shaded.netty4.io.netty.handler.timeout.IdleStateEvent;
import org.apache.fluss.shaded.netty4.io.netty.util.ReferenceCountUtil;

import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.RequestAndSize;
import org.apache.kafka.common.requests.RequestHeader;
import org.gnuhpc.fluss.cape.kafka.handler.KafkaRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;

public class KafkaProtocolDecoder extends SimpleChannelInboundHandler<ByteBuf> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProtocolDecoder.class);

    private final KafkaRequestHandler requestHandler;
    private final ConcurrentLinkedDeque<KafkaRequest> inflightResponses = new ConcurrentLinkedDeque<>();
    protected final AtomicBoolean isActive = new AtomicBoolean(true);
    protected volatile ChannelHandlerContext ctx;
    protected SocketAddress remoteAddress;

    public KafkaProtocolDecoder(KafkaRequestHandler requestHandler) {
        super(false);
        this.requestHandler = requestHandler;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
        CompletableFuture<AbstractResponse> future = new CompletableFuture<>();
        boolean needRelease = false;
        try {
            KafkaRequest request = parseRequest(ctx, future, buffer);
            inflightResponses.addLast(request);
            future.whenCompleteAsync((r, t) -> sendResponse(ctx), ctx.executor());
            
            requestHandler.processRequest(request);

            if (!isActive.get()) {
                LOG.warn("Received a request on an inactive channel: {}", remoteAddress);
                request.fail(new RuntimeException("Channel is inactive"));
                needRelease = true;
            }
        } catch (Throwable t) {
            needRelease = true;
            LOG.error("Error handling request", t);
            future.completeExceptionally(t);
        } finally {
            if (needRelease) {
                ReferenceCountUtil.release(buffer);
            }
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.ctx = ctx;
        this.remoteAddress = ctx.channel().remoteAddress();
        isActive.set(true);
        LOG.info("New connection from {}", ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        LOG.info("Connection closed from {}", ctx.channel().remoteAddress());
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state().equals(IdleState.ALL_IDLE)) {
                LOG.warn("Connection {} is idle, closing...", ctx.channel().remoteAddress());
                ctx.close();
            }
        }
    }

    private void sendResponse(ChannelHandlerContext ctx) {
        KafkaRequest request;
        while ((request = inflightResponses.peekFirst()) != null) {
            CompletableFuture<AbstractResponse> f = request.future();
            boolean isDone = f.isDone();
            boolean cancelled = request.cancelled();

            LOG.debug("Checking response for request ID {}: isDone={}, cancelled={}", 
                     request.requestId(), isDone, cancelled);

            if (!isDone) {
                LOG.debug("Request ID {} not done yet, breaking", request.requestId());
                break;
            }

            if (cancelled) {
                inflightResponses.pollFirst();
                request.releaseBuffer();
                continue;
            }

            inflightResponses.pollFirst();
            if (isActive.get()) {
                ByteBuf buffer = request.responseBuffer();
                LOG.info("Sending response for request ID {} ({} bytes)", 
                        request.requestId(), buffer.readableBytes());
                ctx.writeAndFlush(buffer);
            } else {
                request.releaseBuffer();
            }
        }
    }

    protected void close() {
        isActive.set(false);
        ctx.close();
        LOG.warn("Close channel {} with {} pending requests.", remoteAddress, inflightResponses.size());
        for (KafkaRequest request : inflightResponses) {
            request.cancel();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.error("Exception caught on channel {}", remoteAddress, cause);
        close();
    }

    private static KafkaRequest parseRequest(
            ChannelHandlerContext ctx, CompletableFuture<AbstractResponse> future, ByteBuf buffer) {
        ByteBuffer nioBuffer = buffer.nioBuffer();
        RequestHeader header = RequestHeader.parse(nioBuffer);
        if (isUnsupportedApiVersionRequest(header)) {
            ApiVersionsRequest request =
                    new ApiVersionsRequest.Builder(header.apiVersion()).build();
            return new KafkaRequest(
                    API_VERSIONS, header.apiVersion(), header, request, buffer, ctx, future);
        }
        RequestAndSize request =
                org.apache.kafka.common.requests.AbstractRequest.parseRequest(
                        header.apiKey(), header.apiVersion(), nioBuffer);
        return new KafkaRequest(
                header.apiKey(), header.apiVersion(), header, request.request, buffer, ctx, future);
    }

    private static boolean isUnsupportedApiVersionRequest(RequestHeader header) {
        return header.apiKey() == API_VERSIONS && !API_VERSIONS.isVersionSupported(header.apiVersion());
    }
}
