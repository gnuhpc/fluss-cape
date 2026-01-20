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

package org.gnuhpc.fluss.cape.pg.protocol.backend;

import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.gnuhpc.fluss.cape.pg.protocol.PgMessageWriter;

public final class PgBackendMessages {

    private PgBackendMessages() {
    }

    public static final class SslResponse implements PgBackendMessage {
        @Override
        public void encode(ByteBuf out) {
            out.writeByte('N');
        }
    }

    public static final class AuthenticationOk implements PgBackendMessage {
        @Override
        public void encode(ByteBuf out) {
            out.writeByte('R');
            out.writeInt(8);
            out.writeInt(0);
        }
    }

    public static final class AuthenticationCleartextPassword implements PgBackendMessage {
        @Override
        public void encode(ByteBuf out) {
            out.writeByte('R');
            out.writeInt(8);
            out.writeInt(3);
        }
    }

    public static final class ParameterStatus implements PgBackendMessage {
        private final String name;
        private final String value;

        public ParameterStatus(String name, String value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public void encode(ByteBuf out) {
            byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
            out.writeByte('S');
            out.writeInt(4 + nameBytes.length + 1 + valueBytes.length + 1);
            out.writeBytes(nameBytes);
            out.writeByte(0);
            out.writeBytes(valueBytes);
            out.writeByte(0);
        }
    }

    public static final class BackendKeyData implements PgBackendMessage {
        private final int processId;
        private final int secretKey;

        public BackendKeyData(int processId, int secretKey) {
            this.processId = processId;
            this.secretKey = secretKey;
        }

        @Override
        public void encode(ByteBuf out) {
            out.writeByte('K');
            out.writeInt(12);
            out.writeInt(processId);
            out.writeInt(secretKey);
        }
    }

    public enum ReadyStatus {
        IDLE('I'),
        TRANSACTION('T'),
        FAILED('E');

        private final char code;

        ReadyStatus(char code) {
            this.code = code;
        }

        public char getCode() {
            return code;
        }
    }

    public static final class ReadyForQuery implements PgBackendMessage {
        private final ReadyStatus status;

        public ReadyForQuery(ReadyStatus status) {
            this.status = status;
        }

        @Override
        public void encode(ByteBuf out) {
            out.writeByte('Z');
            out.writeInt(5);
            out.writeByte(status.getCode());
        }
    }

    public static final class ErrorResponse implements PgBackendMessage {
        private final String severity;
        private final String code;
        private final String message;

        public ErrorResponse(String severity, String code, String message) {
            this.severity = severity;
            this.code = code;
            this.message = message;
        }

        public String getSeverity() {
            return severity;
        }

        public String getCode() {
            return code;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public void encode(ByteBuf out) {
            byte[] severityBytes = severity.getBytes(StandardCharsets.UTF_8);
            byte[] codeBytes = code.getBytes(StandardCharsets.UTF_8);
            byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8);
            int length = 4
                    + 1 + severityBytes.length + 1
                    + 1 + codeBytes.length + 1
                    + 1 + messageBytes.length + 1
                    + 1;
            out.writeByte('E');
            out.writeInt(length);
            out.writeByte('S');
            out.writeBytes(severityBytes);
            out.writeByte(0);
            out.writeByte('C');
            out.writeBytes(codeBytes);
            out.writeByte(0);
            out.writeByte('M');
            out.writeBytes(messageBytes);
            out.writeByte(0);
            out.writeByte(0);
        }
    }

    public static final class CommandComplete implements PgBackendMessage {
        private final String commandTag;

        public CommandComplete(String commandTag) {
            this.commandTag = commandTag;
        }

        @Override
        public void encode(ByteBuf out) {
            out.writeByte('C');
            byte[] commandBytes = commandTag.getBytes(StandardCharsets.UTF_8);
            out.writeInt(4 + commandBytes.length + 1);
            out.writeBytes(commandBytes);
            out.writeByte(0);
        }
    }

    public static final class RowDescription implements PgBackendMessage {
        private final List<Field> fields;

        public RowDescription(List<Field> fields) {
            this.fields = fields;
        }

        public List<Field> getFields() {
            return fields;
        }

        @Override
        public void encode(ByteBuf out) {
            int length = 4 + 2;
            for (Field field : fields) {
                length += field.encodedLength();
            }
            out.writeByte('T');
            out.writeInt(length);
            out.writeShort(fields.size());
            for (Field field : fields) {
                field.encode(out);
            }
        }

        public static final class Field {
            private final String name;
            private final int typeOid;
            private final short typeSize;
            private final int typeModifier;
            private final short formatCode;

            public Field(String name, int typeOid, short typeSize, int typeModifier, short formatCode) {
                this.name = name;
                this.typeOid = typeOid;
                this.typeSize = typeSize;
                this.typeModifier = typeModifier;
                this.formatCode = formatCode;
            }

            public int getTypeOid() {
                return typeOid;
            }

            private int encodedLength() {
                return name.getBytes(StandardCharsets.UTF_8).length + 1 + 4 + 2 + 4 + 2 + 4 + 2;
            }

            private void encode(ByteBuf out) {
                PgMessageWriter.writeCString(out, name);
                out.writeInt(0);
                out.writeShort(0);
                out.writeInt(typeOid);
                out.writeShort(typeSize);
                out.writeInt(typeModifier);
                out.writeShort(formatCode);
            }
        }
    }

    public static final class DataRow implements PgBackendMessage {
        private final List<byte[]> values;

        public DataRow(List<byte[]> values) {
            this.values = values;
        }

        @Override
        public void encode(ByteBuf out) {
            int length = 4 + 2;
            for (byte[] value : values) {
                length += 4 + (value == null ? 0 : value.length);
            }
            out.writeByte('D');
            out.writeInt(length);
            out.writeShort(values.size());
            for (byte[] value : values) {
                if (value == null) {
                    out.writeInt(-1);
                } else {
                    out.writeInt(value.length);
                    out.writeBytes(value);
                }
            }
        }
    }

    public static final class ParseComplete implements PgBackendMessage {
        @Override
        public void encode(ByteBuf out) {
            out.writeByte('1');
            out.writeInt(4);
        }
    }

    public static final class BindComplete implements PgBackendMessage {
        @Override
        public void encode(ByteBuf out) {
            out.writeByte('2');
            out.writeInt(4);
        }
    }

    public static final class CloseComplete implements PgBackendMessage {
        @Override
        public void encode(ByteBuf out) {
            out.writeByte('3');
            out.writeInt(4);
        }
    }

    public static final class NoData implements PgBackendMessage {
        @Override
        public void encode(ByteBuf out) {
            out.writeByte('n');
            out.writeInt(4);
        }
    }

    public static final class ParameterDescription implements PgBackendMessage {
        private final int[] parameterTypeOids;

        public ParameterDescription(int[] parameterTypeOids) {
            this.parameterTypeOids = parameterTypeOids;
        }

        @Override
        public void encode(ByteBuf out) {
            out.writeByte('t');
            out.writeInt(4 + 2 + parameterTypeOids.length * 4);
            out.writeShort(parameterTypeOids.length);
            for (int oid : parameterTypeOids) {
                out.writeInt(oid);
            }
        }
    }

    public static final class CopyInResponse implements PgBackendMessage {
        private final short format;
        private final short[] columnFormats;

        public CopyInResponse(short format, short[] columnFormats) {
            this.format = format;
            this.columnFormats = columnFormats;
        }

        @Override
        public void encode(ByteBuf out) {
            out.writeByte('G');
            out.writeInt(4 + 2 + 2 + columnFormats.length * 2);
            out.writeShort(format);
            out.writeShort(columnFormats.length);
            for (short code : columnFormats) {
                out.writeShort(code);
            }
        }
    }
}
