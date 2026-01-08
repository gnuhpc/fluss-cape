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

package org.apache.fluss.hbase.protocol;

import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.MessageToByteEncoder;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Netty encoder for HBase RPC responses.
 *
 * <p>Encodes responses in HBase RPC format:
 * [length:4][ResponseHeader:PB][Response:PB][CellBlock:bytes]
 */
public class HBaseRpcEncoder extends MessageToByteEncoder<HBaseRpcResponse> {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseRpcEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, HBaseRpcResponse response, ByteBuf out)
            throws Exception {
        LOG.info(
                "encode() called: callId={}, isSuccess={}",
                response.getCallId(),
                response.isSuccess());
        try {
            if (response.isSuccess()) {
                encodeSuccessResponse(response, out);
            } else {
                encodeErrorResponse(response, out);
            }
        } catch (Exception e) {
            LOG.error("Exception during encoding: callId=" + response.getCallId(), e);
            throw e;
        }
    }

    private void encodeSuccessResponse(HBaseRpcResponse response, ByteBuf out) throws IOException {
        LOG.info("encodeSuccessResponse START: callId={}", response.getCallId());

        byte[] cellBlock = response.getCellBlock();
        RPCProtos.ResponseHeader.Builder headerBuilder =
                RPCProtos.ResponseHeader.newBuilder().setCallId(response.getCallId());

        Message header = headerBuilder.build();
        Message result = null;

        if (cellBlock != null && cellBlock.length > 0) {
            RPCProtos.CellBlockMeta cellBlockMeta =
                    RPCProtos.CellBlockMeta.newBuilder().setLength(cellBlock.length).build();
            header =
                    RPCProtos.ResponseHeader.newBuilder()
                            .setCallId(response.getCallId())
                            .setCellBlockMeta(cellBlockMeta)
                            .build();
            LOG.info("Added CellBlockMeta to response header: length={}", cellBlock.length);
        }

        if (response.getResponseMessage() != null) {
            if (response.getResponseMessage() instanceof Message) {
                result = (Message) response.getResponseMessage();
            } else {
                throw new IllegalArgumentException(
                        "Response message must be protobuf Message, got: "
                                + response.getResponseMessage().getClass());
            }
        }

        LOG.info(
                "About to encode response: callId={}, header={}, result={}, cellBlock={}",
                response.getCallId(),
                header != null ? "present" : "NULL",
                result != null ? result.getClass().getSimpleName() : "null",
                cellBlock != null ? cellBlock.length + " bytes" : "null");

        if (header == null) {
            LOG.error("CRITICAL: ResponseHeader is NULL for callId={}", response.getCallId());
            throw new IllegalStateException("ResponseHeader cannot be null");
        }

        int headerSerializedSize = header.getSerializedSize();
        int headerVintSize =
                org.apache.hbase.thirdparty.com.google.protobuf.CodedOutputStream
                        .computeUInt32SizeNoTag(headerSerializedSize);
        int resultSerializedSize = result != null ? result.getSerializedSize() : 0;
        int resultVintSize =
                result != null
                        ? org.apache.hbase.thirdparty.com.google.protobuf.CodedOutputStream
                                .computeUInt32SizeNoTag(resultSerializedSize)
                        : 0;
        int cellBlockSize = cellBlock != null ? cellBlock.length : 0;

        int totalSize =
                headerSerializedSize
                        + headerVintSize
                        + resultSerializedSize
                        + resultVintSize
                        + cellBlockSize;

        LOG.info(
                "Encoding RPC response: callId={}, totalSize={}, headerSize={}, resultSize={}, cellBlockSize={}",
                response.getCallId(),
                totalSize,
                headerSerializedSize,
                resultSerializedSize,
                cellBlockSize);

        int writerIndexBefore = out.writerIndex();
        LOG.info("ByteBuf writerIndex before writing totalSize: {}", writerIndexBefore);

        out.writeInt(totalSize);

        int writerIndexAfterInt = out.writerIndex();
        LOG.info(
                "ByteBuf writerIndex after writing totalSize: {} (wrote {} bytes)",
                writerIndexAfterInt,
                writerIndexAfterInt - writerIndexBefore);

        // Log what we actually wrote
        byte[] totalSizeBytes = new byte[4];
        out.getBytes(writerIndexBefore, totalSizeBytes);
        LOG.info("Total size bytes written (hex): {}", bytesToHex(totalSizeBytes));

        int protobufSize =
                headerSerializedSize + headerVintSize + resultSerializedSize + resultVintSize;
        byte[] protobufBytes = new byte[protobufSize];
        java.nio.ByteBuffer nioBuf = java.nio.ByteBuffer.wrap(protobufBytes);

        org.apache.hbase.thirdparty.com.google.protobuf.CodedOutputStream cos =
                org.apache.hbase.thirdparty.com.google.protobuf.CodedOutputStream.newInstance(
                        nioBuf);

        // Log header details before writing
        if (header instanceof RPCProtos.ResponseHeader) {
            RPCProtos.ResponseHeader respHeader = (RPCProtos.ResponseHeader) header;
            LOG.info(
                    "Writing ResponseHeader: callId={}, hasException={}, hasCellBlockMeta={}",
                    respHeader.getCallId(),
                    respHeader.hasException(),
                    respHeader.hasCellBlockMeta());
        }

        cos.writeMessageNoTag(header);
        if (result != null) {
            cos.writeMessageNoTag(result);
        }
        cos.flush();

        // CRITICAL: ByteBuffer position shows how much was written
        int bytesWritten = nioBuf.position();
        LOG.info("CodedOutputStream wrote {} bytes (expected {})", bytesWritten, protobufSize);

        if (bytesWritten == 0) {
            LOG.error(
                    "CRITICAL: CodedOutputStream wrote 0 bytes! header={}, result={}",
                    header != null ? header.getClass().getSimpleName() : "null",
                    result != null ? result.getClass().getSimpleName() : "null");
            throw new IllegalStateException("CodedOutputStream wrote 0 bytes");
        }

        // Write only the actual bytes written, not the whole array
        byte[] actualBytes = new byte[bytesWritten];
        nioBuf.flip();
        nioBuf.get(actualBytes);
        out.writeBytes(actualBytes);

        LOG.info("Protobuf bytes (hex): {}", bytesToHex(actualBytes));

        int finalWriterIndex = out.writerIndex();
        LOG.info(
                "Final ByteBuf state: writerIndex={}, totalBytesWritten={}",
                finalWriterIndex,
                finalWriterIndex - writerIndexBefore);

        byte[] fullResponse = new byte[finalWriterIndex - writerIndexBefore];
        out.getBytes(writerIndexBefore, fullResponse);
        LOG.info("FULL response bytes sent (hex): {}", bytesToHex(fullResponse));

        if (cellBlock != null) {
            out.writeBytes(cellBlock);
        }

        LOG.info(
                "Final ByteBuf before return: readerIndex={}, writerIndex={}, readableBytes={}",
                out.readerIndex(),
                out.writerIndex(),
                out.readableBytes());

        LOG.info("Successfully encoded response for callId={}", response.getCallId());
    }

    private void encodeErrorResponse(HBaseRpcResponse response, ByteBuf out) throws IOException {
        Exception exception = response.getException();

        RPCProtos.ExceptionResponse.Builder exceptionBuilder =
                RPCProtos.ExceptionResponse.newBuilder()
                        .setExceptionClassName(exception.getClass().getName());

        if (exception.getMessage() != null) {
            exceptionBuilder.setStackTrace(
                    exception.getMessage() + "\n" + getStackTrace(exception));
        } else {
            exceptionBuilder.setStackTrace(getStackTrace(exception));
        }

        RPCProtos.ResponseHeader.Builder headerBuilder =
                RPCProtos.ResponseHeader.newBuilder()
                        .setCallId(response.getCallId())
                        .setException(exceptionBuilder.build());

        byte[] headerBytes = headerBuilder.build().toByteArray();

        int totalLength = computeVarint32Size(headerBytes.length) + headerBytes.length;

        out.writeInt(totalLength);
        writeVarint32(out, headerBytes.length);
        out.writeBytes(headerBytes);

        LOG.warn(
                "Encoded error response: callId={}, exception={}",
                response.getCallId(),
                exception.getClass().getSimpleName());
    }

    private void writeVarint32(ByteBuf buf, int value) {
        while ((value & ~0x7F) != 0) {
            buf.writeByte((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buf.writeByte(value & 0x7F);
    }

    private int computeVarint32Size(int value) {
        if ((value & (~0 << 7)) == 0) {
            return 1;
        }
        if ((value & (~0 << 14)) == 0) {
            return 2;
        }
        if ((value & (~0 << 21)) == 0) {
            return 3;
        }
        if ((value & (~0 << 28)) == 0) {
            return 4;
        }
        return 5;
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

    private String bytesToHex(byte[] bytes) {
        return bytesToHex(bytes, bytes.length);
    }

    private String bytesToHex(byte[] bytes, int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length && i < bytes.length; i++) {
            sb.append(String.format("%02x", bytes[i] & 0xFF));
            if ((i + 1) % 16 == 0 && i < length - 1) {
                sb.append("\n");
            } else if (i < length - 1) {
                sb.append(" ");
            }
        }
        return sb.toString();
    }
}
