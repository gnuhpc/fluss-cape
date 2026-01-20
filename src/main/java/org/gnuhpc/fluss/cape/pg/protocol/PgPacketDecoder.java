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

package org.gnuhpc.fluss.cape.pg.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessageParser;

public class PgPacketDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < 5) {
            return;
        }
        int readerIndex = in.readerIndex();
        byte firstByte = in.getByte(readerIndex);
        if (PgFrontendMessageParser.isTypedMessage(firstByte)) {
            if (in.readableBytes() < 5) {
                return;
            }
            int length = in.getInt(readerIndex + 1);
            if (in.readableBytes() < length + 1) {
                return;
            }
            byte type = in.readByte();
            in.readInt();
            ByteBuf payload = in.readRetainedSlice(length - 4);
            try {
                out.add(PgFrontendMessageParser.parseTyped(type, payload));
            } finally {
                payload.release();
            }
            return;
        }

        int length = in.getInt(readerIndex);
        if (length <= 0 || in.readableBytes() < length) {
            return;
        }
        ByteBuf payload = in.readRetainedSlice(length);
        try {
            out.add(PgFrontendMessageParser.parseStartup(payload));
        } finally {
            payload.release();
        }
    }
}
