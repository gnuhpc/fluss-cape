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

import java.util.List;
import java.util.Map;

public final class PgFrontendMessages {

    private PgFrontendMessages() {
    }

    public static final class StartupMessage implements PgFrontendMessage {
        private final int protocolVersion;
        private final Map<String, String> parameters;

        public StartupMessage(int protocolVersion, Map<String, String> parameters) {
            this.protocolVersion = protocolVersion;
            this.parameters = parameters;
        }

        public int getProtocolVersion() {
            return protocolVersion;
        }

        public Map<String, String> getParameters() {
            return parameters;
        }

        public String getParameter(String key) {
            return parameters.get(key);
        }
    }

    public static final class SslRequest implements PgFrontendMessage {
        public static final int REQUEST_CODE = 80877103;
    }

    public static final class CancelRequest implements PgFrontendMessage {
        public static final int REQUEST_CODE = 80877102;
        private final int processId;
        private final int secretKey;

        public CancelRequest(int processId, int secretKey) {
            this.processId = processId;
            this.secretKey = secretKey;
        }

        public int getProcessId() {
            return processId;
        }

        public int getSecretKey() {
            return secretKey;
        }
    }

    public static final class PasswordMessage implements PgFrontendMessage {
        private final String password;

        public PasswordMessage(String password) {
            this.password = password;
        }

        public String getPassword() {
            return password;
        }
    }

    public static final class QueryMessage implements PgFrontendMessage {
        private final String sql;

        public QueryMessage(String sql) {
            this.sql = sql;
        }

        public String getSql() {
            return sql;
        }
    }

    public static final class ParseMessage implements PgFrontendMessage {
        private final String name;
        private final String sql;
        private final int[] parameterTypeOids;

        public ParseMessage(String name, String sql, int[] parameterTypeOids) {
            this.name = name;
            this.sql = sql;
            this.parameterTypeOids = parameterTypeOids;
        }

        public String getName() {
            return name;
        }

        public String getSql() {
            return sql;
        }

        public int[] getParameterTypeOids() {
            return parameterTypeOids;
        }
    }

    public static final class BindMessage implements PgFrontendMessage {
        private final String portal;
        private final String statement;
        private final short[] parameterFormatCodes;
        private final List<byte[]> parameters;
        private final short[] resultFormatCodes;

        public BindMessage(
                String portal,
                String statement,
                short[] parameterFormatCodes,
                List<byte[]> parameters,
                short[] resultFormatCodes) {
            this.portal = portal;
            this.statement = statement;
            this.parameterFormatCodes = parameterFormatCodes;
            this.parameters = parameters;
            this.resultFormatCodes = resultFormatCodes;
        }

        public String getPortal() {
            return portal;
        }

        public String getStatement() {
            return statement;
        }

        public short[] getParameterFormatCodes() {
            return parameterFormatCodes;
        }

        public List<byte[]> getParameters() {
            return parameters;
        }

        public short[] getResultFormatCodes() {
            return resultFormatCodes;
        }
    }

    public static final class DescribeMessage implements PgFrontendMessage {
        private final char type;
        private final String name;

        public DescribeMessage(char type, String name) {
            this.type = type;
            this.name = name;
        }

        public char getType() {
            return type;
        }

        public String getName() {
            return name;
        }
    }

    public static final class ExecuteMessage implements PgFrontendMessage {
        private final String portal;
        private final int maxRows;

        public ExecuteMessage(String portal, int maxRows) {
            this.portal = portal;
            this.maxRows = maxRows;
        }

        public String getPortal() {
            return portal;
        }

        public int getMaxRows() {
            return maxRows;
        }
    }

    public static final class CloseMessage implements PgFrontendMessage {
        private final char type;
        private final String name;

        public CloseMessage(char type, String name) {
            this.type = type;
            this.name = name;
        }

        public char getType() {
            return type;
        }

        public String getName() {
            return name;
        }
    }

    public static final class SyncMessage implements PgFrontendMessage {
    }

    public static final class FlushMessage implements PgFrontendMessage {
    }

    public static final class TerminateMessage implements PgFrontendMessage {
    }

    public static final class CopyDataMessage implements PgFrontendMessage {
        private final byte[] payload;

        public CopyDataMessage(byte[] payload) {
            this.payload = payload;
        }

        public byte[] getPayload() {
            return payload;
        }
    }

    public static final class CopyDoneMessage implements PgFrontendMessage {
    }

    public static final class CopyFailMessage implements PgFrontendMessage {
        private final String message;

        public CopyFailMessage(String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }
    }
}
