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

import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.MessageToByteEncoder;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Netty encoder for HBase RPC responses.
 * Wire format: [length:4][ResponseHeader:PB][Response:PB][CellBlock:bytes]
 */
public class HBaseRpcEncoder extends MessageToByteEncoder<HBaseRpcResponse> {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseRpcEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, HBaseRpcResponse response, ByteBuf out)
            throws Exception {
        try {
            if (response.isSuccess()) {
                encodeSuccessResponse(response, out);
            } else {
                encodeErrorResponse(response, out);
            }
        } catch (Exception e) {
            LOG.error("Failed to encode response for callId={}", response.getCallId(), e);
            throw e;
        }
    }

    private void encodeSuccessResponse(HBaseRpcResponse response, ByteBuf out) throws IOException {
        byte[] cellBlock = response.getCellBlock();
        
        RPCProtos.ResponseHeader.Builder headerBuilder = 
                RPCProtos.ResponseHeader.newBuilder().setCallId(response.getCallId());

        if (cellBlock != null && cellBlock.length > 0) {
            RPCProtos.CellBlockMeta cellBlockMeta =
                    RPCProtos.CellBlockMeta.newBuilder().setLength(cellBlock.length).build();
            headerBuilder.setCellBlockMeta(cellBlockMeta);
        }

        Message header = headerBuilder.build();
        Message result = null;

        if (response.getResponseMessage() != null) {
            if (response.getResponseMessage() instanceof Message) {
                result = (Message) response.getResponseMessage();
            } else {
                throw new IllegalArgumentException(
                        "Response message must be protobuf Message, got: "
                                + response.getResponseMessage().getClass());
            }
        }

        int headerSize = header.getSerializedSize();
        int headerVintSize = CodedOutputStream.computeUInt32SizeNoTag(headerSize);
        int resultSize = result != null ? result.getSerializedSize() : 0;
        int resultVintSize = result != null ? CodedOutputStream.computeUInt32SizeNoTag(resultSize) : 0;
        int cellBlockSize = cellBlock != null ? cellBlock.length : 0;

        int totalSize = headerSize + headerVintSize + resultSize + resultVintSize + cellBlockSize;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Encoding response: callId={}, totalSize={}, cellBlockSize={}",
                    response.getCallId(), totalSize, cellBlockSize);
        }

        out.writeInt(totalSize);

        int protobufSize = headerSize + headerVintSize + resultSize + resultVintSize;
        ByteBuffer nioBuf = ByteBuffer.allocate(protobufSize);
        CodedOutputStream cos = CodedOutputStream.newInstance(nioBuf);

        cos.writeMessageNoTag(header);
        if (result != null) {
            cos.writeMessageNoTag(result);
        }
        cos.flush();

        nioBuf.flip();
        out.writeBytes(nioBuf);

        if (cellBlock != null) {
            out.writeBytes(cellBlock);
        }
    }

    private void encodeErrorResponse(HBaseRpcResponse response, ByteBuf out) throws IOException {
        Exception exception = response.getException();

        RPCProtos.ExceptionResponse.Builder exceptionBuilder =
                RPCProtos.ExceptionResponse.newBuilder()
                        .setExceptionClassName(exception.getClass().getName());

        String stackTrace = exception.getMessage() != null
                ? exception.getMessage() + "\n" + getStackTrace(exception)
                : getStackTrace(exception);
        exceptionBuilder.setStackTrace(stackTrace);

        RPCProtos.ResponseHeader header = RPCProtos.ResponseHeader.newBuilder()
                .setCallId(response.getCallId())
                .setException(exceptionBuilder.build())
                .build();

        byte[] headerBytes = header.toByteArray();
        int headerVintSize = CodedOutputStream.computeUInt32SizeNoTag(headerBytes.length);
        int totalLength = headerVintSize + headerBytes.length;

        out.writeInt(totalLength);
        writeVarint32(out, headerBytes.length);
        out.writeBytes(headerBytes);

        LOG.warn("Encoded error response: callId={}, exception={}",
                response.getCallId(), exception.getClass().getSimpleName());
    }

    private void writeVarint32(ByteBuf buf, int value) {
        while ((value & ~0x7F) != 0) {
            buf.writeByte((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buf.writeByte(value & 0x7F);
    }

    private String getStackTrace(Throwable t) {
        StringBuilder sb = new StringBuilder();
        sb.append(t.getClass().getName());
        if (t.getMessage() != null) {
            sb.append(": ").append(t.getMessage());
        }
        sb.append("\n");

        StackTraceElement[] elements = t.getStackTrace();
        int limit = Math.min(elements.length, 20);
        for (int i = 0; i < limit; i++) {
            sb.append("\tat ").append(elements[i].toString()).append("\n");
        }
        if (elements.length > limit) {
            sb.append("\t... ").append(elements.length - limit).append(" more\n");
        }

        return sb.toString();
    }
}
