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

package org.gnuhpc.fluss.cape.pg.protocol.frontend;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.gnuhpc.fluss.cape.pg.protocol.PgMessageReader;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.BindMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.CancelRequest;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.CloseMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.CopyDataMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.CopyDoneMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.CopyFailMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.DescribeMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.ExecuteMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.FlushMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.ParseMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.PasswordMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.QueryMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.SslRequest;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.StartupMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.SyncMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.TerminateMessage;

public final class PgFrontendMessageParser {

    private static final Set<Byte> TYPED_MESSAGES = new HashSet<>();

    static {
        TYPED_MESSAGES.add((byte) 'Q');
        TYPED_MESSAGES.add((byte) 'P');
        TYPED_MESSAGES.add((byte) 'B');
        TYPED_MESSAGES.add((byte) 'D');
        TYPED_MESSAGES.add((byte) 'E');
        TYPED_MESSAGES.add((byte) 'S');
        TYPED_MESSAGES.add((byte) 'H');
        TYPED_MESSAGES.add((byte) 'C');
        TYPED_MESSAGES.add((byte) 'X');
        TYPED_MESSAGES.add((byte) 'd');
        TYPED_MESSAGES.add((byte) 'c');
        TYPED_MESSAGES.add((byte) 'f');
        TYPED_MESSAGES.add((byte) 'p');
    }

    private PgFrontendMessageParser() {
    }

    public static boolean isTypedMessage(byte firstByte) {
        return TYPED_MESSAGES.contains(firstByte);
    }

    public static PgFrontendMessage parseStartup(ByteBuf payload) {
        int length = payload.readInt();
        if (length < 8) {
            throw new IllegalArgumentException("Invalid startup message length");
        }
        int protocol = payload.readInt();
        if (protocol == SslRequest.REQUEST_CODE) {
            return new SslRequest();
        }
        if (protocol == CancelRequest.REQUEST_CODE) {
            int processId = payload.readInt();
            int secretKey = payload.readInt();
            return new CancelRequest(processId, secretKey);
        }
        Map<String, String> parameters = new HashMap<>();
        while (payload.isReadable()) {
            String key = PgMessageReader.readCString(payload);
            if (key.isEmpty()) {
                break;
            }
            String value = PgMessageReader.readCString(payload);
            parameters.put(key, value);
        }
        return new StartupMessage(protocol, parameters);
    }

    public static PgFrontendMessage parseTyped(byte type, ByteBuf payload) {
        switch (type) {
            case 'Q':
                return new QueryMessage(PgMessageReader.readCString(payload));
            case 'P':
                return parseParse(payload);
            case 'B':
                return parseBind(payload);
            case 'D':
                return new DescribeMessage((char) payload.readByte(), PgMessageReader.readCString(payload));
            case 'E':
                return new ExecuteMessage(PgMessageReader.readCString(payload), payload.readInt());
            case 'C':
                return new CloseMessage((char) payload.readByte(), PgMessageReader.readCString(payload));
            case 'S':
                return new SyncMessage();
            case 'H':
                return new FlushMessage();
            case 'X':
                return new TerminateMessage();
            case 'd':
                return new CopyDataMessage(PgMessageReader.readBytes(payload, payload.readableBytes()));
            case 'c':
                return new CopyDoneMessage();
            case 'f':
                return new CopyFailMessage(PgMessageReader.readCString(payload));
            case 'p':
                return new PasswordMessage(PgMessageReader.readCString(payload));
            default:
                throw new IllegalArgumentException("Unsupported message type: " + (char) type);
        }
    }

    private static PgFrontendMessage parseParse(ByteBuf payload) {
        String name = PgMessageReader.readCString(payload);
        String sql = PgMessageReader.readCString(payload);
        int paramCount = payload.readShort();
        int[] paramTypes = new int[paramCount];
        for (int i = 0; i < paramCount; i++) {
            paramTypes[i] = payload.readInt();
        }
        return new ParseMessage(name, sql, paramTypes);
    }

    private static PgFrontendMessage parseBind(ByteBuf payload) {
        String portal = PgMessageReader.readCString(payload);
        String statement = PgMessageReader.readCString(payload);
        short[] formatCodes = readFormatCodes(payload);
        int paramCount = payload.readShort();
        List<byte[]> params = new ArrayList<>(paramCount);
        for (int i = 0; i < paramCount; i++) {
            int length = payload.readInt();
            if (length == -1) {
                params.add(null);
            } else {
                params.add(PgMessageReader.readBytes(payload, length));
            }
        }
        short[] resultFormats = readFormatCodes(payload);
        return new BindMessage(portal, statement, formatCodes, params, resultFormats);
    }

    private static short[] readFormatCodes(ByteBuf payload) {
        int count = payload.readShort();
        if (count == 0) {
            return new short[] {0};
        }
        short[] codes = new short[count];
        for (int i = 0; i < count; i++) {
            codes[i] = payload.readShort();
        }
        return codes;
    }
}
