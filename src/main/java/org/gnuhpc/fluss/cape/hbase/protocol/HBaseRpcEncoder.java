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
import io.netty.handler.codec.MessageToByteEncoder;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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

        int headerSize = CodedOutputStream.computeMessageSizeNoTag(header);
        int resultSize = result != null ? CodedOutputStream.computeMessageSizeNoTag(result) : 0;
        int cellBlockSize = cellBlock != null ? cellBlock.length : 0;

        int protobufSize = headerSize + resultSize;
        int totalSize = protobufSize + cellBlockSize;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Encoding response: callId={}, totalSize={}, cellBlockSize={}",
                    response.getCallId(), totalSize, cellBlockSize);
        }

        out.writeInt(totalSize);

        byte[] protobufBytes = new byte[protobufSize];
        CodedOutputStream cos = CodedOutputStream.newInstance(protobufBytes);
        cos.writeMessageNoTag(header);
        if (result != null) {
            cos.writeMessageNoTag(result);
        }
        cos.flush();
        out.writeBytes(protobufBytes);

        if (cellBlock != null) {
            out.writeBytes(cellBlock);
        }
    }

    private void encodeErrorResponse(HBaseRpcResponse response, ByteBuf out) throws IOException {
        Exception exception = response.getException();

        RPCProtos.ExceptionResponse.Builder exceptionBuilder =
                RPCProtos.ExceptionResponse.newBuilder()
                        .setExceptionClassName(exception.getClass().getName());

        exceptionBuilder.setStackTrace(sanitizeException(exception));

        RPCProtos.ResponseHeader header = RPCProtos.ResponseHeader.newBuilder()
                .setCallId(response.getCallId())
                .setException(exceptionBuilder.build())
                .build();

        int headerSize = CodedOutputStream.computeMessageSizeNoTag(header);
        out.writeInt(headerSize);
        byte[] headerBytes = new byte[headerSize];
        CodedOutputStream cos = CodedOutputStream.newInstance(headerBytes);
        cos.writeMessageNoTag(header);
        cos.flush();
        out.writeBytes(headerBytes);

        LOG.warn("Encoded error response: callId={}, exception={}",
                response.getCallId(), exception.getClass().getSimpleName());
    }

    private String sanitizeException(Throwable t) {
        String message = t.getMessage();
        if (message != null) {
            message = message.replace("\r", " ").replace("\n", " ").trim();
        }
        String base = t.getClass().getSimpleName();
        String combined = message == null || message.isEmpty()
                ? base
                : base + ": " + message;
        int maxLength = 512;
        return combined.length() > maxLength
                ? combined.substring(0, maxLength)
                : combined;
    }
}
