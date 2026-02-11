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

package org.gnuhpc.fluss.cape.hbase.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * Netty decoder for HBase RPC protocol.
 *
 * <p>Handles the following message formats: 1. Connection preamble: 6 bytes ["HBas" + version +
 * authType] 2. ConnectionHeader: Protobuf message with user info, service name, cell block codec 3.
 * RPC Request: [length:4][RequestHeader:PB][Param:PB][CellBlock:bytes]
 */
public class HBaseRpcDecoder extends ByteToMessageDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseRpcDecoder.class);

    /** HBase RPC preamble magic: "HBas" (0x48426173). */
    private static final byte[] HBASE_MAGIC = {'H', 'B', 'a', 's'};

    private static final int PREAMBLE_LENGTH = 6;
    private static final int LENGTH_FIELD_LENGTH = 4;

    private static final java.util.Set<String> ALLOWED_CELL_BLOCK_CODECS =
            new java.util.HashSet<>(java.util.Arrays.asList("KeyValueCodec", "KeyValueCodecWithTags"));

    /** Connection state tracking. */
    private enum State {
        /** Waiting for 6-byte preamble. */
        PREAMBLE,
        /** Waiting for ConnectionHeader protobuf. */
        CONNECTION_HEADER,
        /** Processing normal RPC requests. */
        RPC_REQUEST
    }

    private State state = State.PREAMBLE;

    @Nullable private String cellBlockCodec;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
            throws Exception {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Decode called: state={}, readableBytes={}", state, in.readableBytes());
        }
        switch (state) {
            case PREAMBLE:
                if (decodePreamble(ctx, in)) {
                    state = State.CONNECTION_HEADER;
                }
                break;
            case CONNECTION_HEADER:
                if (decodeConnectionHeader(ctx, in)) {
                    state = State.RPC_REQUEST;
                }
                break;
            case RPC_REQUEST:
                decodeRpcRequest(in, out);
                break;
        }
    }

    /**
     * Decode the 6-byte HBase preamble: ["HBas" + version + authType].
     *
     * @return true if preamble was successfully decoded
     */
    private boolean decodePreamble(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        if (in.readableBytes() < PREAMBLE_LENGTH) {
            return false;
        }

        // Mark reader index for potential rollback
        in.markReaderIndex();

        // Verify magic bytes
        for (int i = 0; i < HBASE_MAGIC.length; i++) {
            if (in.readByte() != HBASE_MAGIC[i]) {
                throw new CorruptedFrameException(
                        "Invalid HBase RPC magic. Expected 'HBas' but got different bytes.");
            }
        }

        byte version = in.readByte();
        byte authType = in.readByte();

        LOG.info(
                "Received HBase connection preamble from {}. Version: {}, AuthType: {}",
                ctx.channel().remoteAddress(),
                version,
                authType);

        // Validate version (HBase 2.x uses version 0)
        if (version != 0) {
            LOG.warn("Unsupported HBase RPC version: {}. Expected version 0.", version);
        }

        // AuthType: 0=SIMPLE, 80=KERBEROS, 81=DIGEST (token)
        if (authType != 0) {
            LOG.info(
                    "Client requested authentication type {}. Only SIMPLE auth (0) is fully supported.",
                    authType);
        }

        return true;
    }

    /**
     * Decode the ConnectionHeader protobuf message.
     *
     * @return true if ConnectionHeader was successfully decoded
     */
    private boolean decodeConnectionHeader(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        if (in.readableBytes() < LENGTH_FIELD_LENGTH) {
            return false;
        }

        // Mark for potential rollback
        in.markReaderIndex();

        int headerLength = in.readInt();

        if (headerLength < 0 || headerLength > 1024 * 1024) {
            throw new CorruptedFrameException("Invalid ConnectionHeader length: " + headerLength);
        }

        if (in.readableBytes() < headerLength) {
            // Not enough bytes yet, rollback
            in.resetReaderIndex();
            return false;
        }

        // Parse ConnectionHeader protobuf
        byte[] headerBytes = new byte[headerLength];
        in.readBytes(headerBytes);

        RPCProtos.ConnectionHeader connectionHeader =
                RPCProtos.ConnectionHeader.parseFrom(headerBytes);

        String serviceName = connectionHeader.getServiceName();
        String cellBlockCodecClass = connectionHeader.getCellBlockCodecClass();

        // Extract cell block codec name (e.g.,
        // "org.apache.hadoop.hbase.codec.KeyValueCodec" -> "KeyValueCodec")
        if (cellBlockCodecClass != null && !cellBlockCodecClass.isEmpty()) {
            int lastDot = cellBlockCodecClass.lastIndexOf('.');
            this.cellBlockCodec =
                    lastDot >= 0 ? cellBlockCodecClass.substring(lastDot + 1) : cellBlockCodecClass;
        }

        if (cellBlockCodec != null && !cellBlockCodec.isEmpty()
                && !ALLOWED_CELL_BLOCK_CODECS.contains(cellBlockCodec)) {
            throw new CorruptedFrameException(
                    "Unsupported cell block codec: " + cellBlockCodec);
        }

        LOG.info(
                "Received ConnectionHeader from {}. Service: {}, CellBlockCodec: {}, User: {}",
                ctx.channel().remoteAddress(),
                serviceName,
                cellBlockCodec,
                connectionHeader.hasUserInfo()
                        ? connectionHeader.getUserInfo().getEffectiveUser()
                        : "anonymous");

        // Send ConnectionHeaderResponse back to client (required by HBase protocol)
        sendConnectionHeaderResponse(ctx);

        LOG.info("State transitioning to RPC_REQUEST for {}", ctx.channel().remoteAddress());
        return true;
    }

    private void sendConnectionHeaderResponse(ChannelHandlerContext ctx) {
        try {
            RPCProtos.ConnectionHeaderResponse response =
                    RPCProtos.ConnectionHeaderResponse.newBuilder().build();

            byte[] responseBytes = response.toByteArray();

            io.netty.buffer.ByteBuf buffer =
                    ctx.alloc().buffer(responseBytes.length + LENGTH_FIELD_LENGTH);
            buffer.writeInt(responseBytes.length);
            buffer.writeBytes(responseBytes);

            ctx.writeAndFlush(buffer);

            LOG.info("Sent ConnectionHeaderResponse to {}", ctx.channel().remoteAddress());

        } catch (Exception e) {
            LOG.error("Failed to send ConnectionHeaderResponse", e);
            ctx.close();
        }
    }

    /**
     * Decode RPC request frames: [length:4][RequestHeader:PB][Param:PB][CellBlock:bytes].
     *
     * <p>Structure: - length (4 bytes): Total length of the rest of the frame - RequestHeader
     * (varint-prefixed PB): Contains callId, methodName, requestParam flag - Param (varint-prefixed
     * PB): Method-specific request message - CellBlock (optional): Raw cell data
     *
     * <p>Refactored to use HBase's ProtobufUtil and CodedInputStream for parsing,
     * following the pattern in HBase's ServerRpcConnection.processRequest().
     */
    private void decodeRpcRequest(ByteBuf in, List<Object> out) throws IOException {
        // Need at least 4 bytes for length field
        if (in.readableBytes() < LENGTH_FIELD_LENGTH) {
            return;
        }

        in.markReaderIndex();
        int frameLength = in.readInt();

        // Validate frame length
        if (frameLength < 0 || frameLength > 100 * 1024 * 1024) {
            throw new CorruptedFrameException("Invalid RPC frame length: " + frameLength);
        }

        // Check if full frame is available
        if (in.readableBytes() < frameLength) {
            in.resetReaderIndex();
            return;
        }

        // Extract frame bytes for parsing with HBase's CodedInputStream
        byte[] frameBytes = new byte[frameLength];
        in.readBytes(frameBytes);

        // Use HBase's CodedInputStream for varint parsing (like ServerRpcConnection)
        CodedInputStream cis = CodedInputStream.newInstance(frameBytes);
        cis.enableAliasing(true);

        // Parse RequestHeader using HBase's ProtobufUtil.mergeFrom()
        int headerSize = cis.readRawVarint32();
        Message.Builder headerBuilder = RPCProtos.RequestHeader.newBuilder();
        ProtobufUtil.mergeFrom(headerBuilder, cis, headerSize);
        RPCProtos.RequestHeader requestHeader = (RPCProtos.RequestHeader) headerBuilder.build();
        int offset = cis.getTotalBytesRead();

        int callId = requestHeader.getCallId();
        String methodName = requestHeader.getMethodName();

        // Parse request parameter (varint-prefixed protobuf)
        byte[] paramBytes = null;
        if (requestHeader.hasRequestParam() && requestHeader.getRequestParam()) {
            int paramSize = cis.readRawVarint32();
            offset = cis.getTotalBytesRead();
            if (paramSize > 0) {
                if (offset + paramSize > frameBytes.length) {
                    throw new CorruptedFrameException(
                            "Invalid request param size: " + paramSize + " at offset " + offset);
                }
                paramBytes = new byte[paramSize];
                System.arraycopy(frameBytes, offset, paramBytes, 0, paramSize);
                offset += paramSize;
            }
        }

        // Remaining bytes are the CellBlock (if present)
        byte[] cellBlock = null;
        if (requestHeader.hasCellBlockMeta()) {
            int cellBlockLength = requestHeader.getCellBlockMeta().getLength();
            if (offset + cellBlockLength > frameBytes.length) {
                throw new CorruptedFrameException(
                        "Invalid cell block length: " + cellBlockLength + " at offset " + offset);
            }
            cellBlock = new byte[cellBlockLength];
            System.arraycopy(frameBytes, offset, cellBlock, 0, cellBlockLength);

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Decoded RPC request: callId={}, method={}, cellBlockSize={}",
                        callId, methodName, cellBlock.length);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Decoded RPC request: callId={}, method={}, noCellBlock",
                        callId, methodName);
            }
        }

        HBaseRpcRequest request = new HBaseRpcRequest(callId, methodName, paramBytes, cellBlock);
        if (LOG.isTraceEnabled()) {
            LOG.trace("[DECODER] Created HBaseRpcRequest: callId={} method={}", callId, methodName);
        }
        out.add(request);
    }

    public String getCellBlockCodec() {
        return cellBlockCodec;
    }
}
