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

package org.gnuhpc.fluss.cape.hbase.security;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.ByteToMessageDecoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** Netty channel handler for HBase SASL authentication challenge-response protocol. */
public class HBaseSaslHandler extends ByteToMessageDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseSaslHandler.class);

    private enum SaslState {
        WAITING_INITIAL_RESPONSE,
        CHALLENGE_RESPONSE,
        COMPLETED
    }

    private final Configuration configuration;
    private final String clientAddress;
    private HBaseSaslAuthenticator authenticator;
    private SaslState saslState = SaslState.WAITING_INITIAL_RESPONSE;
    private String mechanism;

    public HBaseSaslHandler(Configuration configuration, String clientAddress, String mechanism) {
        this.configuration = configuration;
        this.clientAddress = clientAddress;
        this.mechanism = mechanism;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
            throws Exception {
        switch (saslState) {
            case WAITING_INITIAL_RESPONSE:
                if (decodeInitialResponse(ctx, in)) {
                    saslState = SaslState.CHALLENGE_RESPONSE;
                }
                break;
            case CHALLENGE_RESPONSE:
                decodeChallengeResponse(ctx, in);
                break;
            case COMPLETED:
                ctx.pipeline().remove(this);
                ctx.fireChannelRead(in.retain());
                break;
        }
    }

    private boolean decodeInitialResponse(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        if (in.readableBytes() < 4) {
            return false;
        }

        in.markReaderIndex();
        int tokenLength = in.readInt();

        if (in.readableBytes() < tokenLength) {
            in.resetReaderIndex();
            return false;
        }

        byte[] token = new byte[tokenLength];
        in.readBytes(token);

        if (authenticator == null) {
            authenticator = new HBaseSaslAuthenticator(configuration, mechanism);
            authenticator.initialize(clientAddress);
        }

        byte[] challenge = authenticator.evaluateResponse(token);

        if (authenticator.isComplete()) {
            LOG.info(
                    "SASL authentication completed for user: {}",
                    authenticator.getAuthenticatedUser());
            saslState = SaslState.COMPLETED;
            sendSuccessResponse(ctx, challenge);
        } else {
            sendChallengeResponse(ctx, challenge);
        }

        return true;
    }

    private void decodeChallengeResponse(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        if (in.readableBytes() < 4) {
            return;
        }

        in.markReaderIndex();
        int tokenLength = in.readInt();

        if (in.readableBytes() < tokenLength) {
            in.resetReaderIndex();
            return;
        }

        byte[] token = new byte[tokenLength];
        in.readBytes(token);

        byte[] challenge = authenticator.evaluateResponse(token);

        if (authenticator.isComplete()) {
            LOG.info(
                    "SASL authentication completed for user: {}",
                    authenticator.getAuthenticatedUser());
            saslState = SaslState.COMPLETED;
            sendSuccessResponse(ctx, challenge);
        } else {
            sendChallengeResponse(ctx, challenge);
        }
    }

    private void sendChallengeResponse(ChannelHandlerContext ctx, byte[] challenge) {
        if (challenge != null && challenge.length > 0) {
            ByteBuf response = ctx.alloc().buffer(4 + challenge.length);
            response.writeInt(challenge.length);
            response.writeBytes(challenge);
            ctx.writeAndFlush(response);
        }
    }

    private void sendSuccessResponse(ChannelHandlerContext ctx, byte[] finalToken) {
        ByteBuf response = ctx.alloc().buffer(4 + (finalToken != null ? finalToken.length : 0));
        response.writeInt(finalToken != null ? finalToken.length : 0);
        if (finalToken != null) {
            response.writeBytes(finalToken);
        }
        ctx.writeAndFlush(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof AuthenticationException) {
            LOG.error("SASL authentication failed", cause);
        } else {
            LOG.error("Unexpected error during SASL handshake", cause);
        }

        sendAuthenticationFailure(ctx, cause.getMessage());
        ctx.close();
    }

    private void sendAuthenticationFailure(ChannelHandlerContext ctx, String errorMessage) {
        byte[] errorBytes = errorMessage.getBytes();
        ByteBuf response = ctx.alloc().buffer(4 + errorBytes.length);
        response.writeInt(-errorBytes.length);
        response.writeBytes(errorBytes);
        ctx.writeAndFlush(response);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (authenticator != null) {
            authenticator.dispose();
        }
        super.channelInactive(ctx);
    }

    public String getAuthenticatedUser() {
        return authenticator != null ? authenticator.getAuthenticatedUser() : null;
    }
}
