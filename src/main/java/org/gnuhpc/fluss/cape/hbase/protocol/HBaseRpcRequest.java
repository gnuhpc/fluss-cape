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

import javax.annotation.Nullable;

/**
 * Encapsulates a decoded HBase RPC request.
 *
 * <p>This class wraps the HBase RPC protocol components: - Call ID: Unique identifier for matching
 * request/response - Method Name: The RPC method being invoked (e.g., "Get", "Put", "Scan") -
 * Request Param: The protobuf message specific to the method - Cell Block: Optional binary block
 * containing cell data for performance
 */
public class HBaseRpcRequest {

    private final int callId;
    private final String methodName;
    private final Object requestParam;
    @Nullable private final byte[] cellBlock;

    public HBaseRpcRequest(
            int callId, String methodName, Object requestParam, @Nullable byte[] cellBlock) {
        this.callId = callId;
        this.methodName = methodName;
        this.requestParam = requestParam;
        this.cellBlock = cellBlock;
    }

    public int getCallId() {
        return callId;
    }

    public String getMethodName() {
        return methodName;
    }

    public Object getRequestParam() {
        return requestParam;
    }

    @Nullable
    public byte[] getCellBlock() {
        return cellBlock;
    }

    @Override
    public String toString() {
        return "HBaseRpcRequest{"
                + "callId="
                + callId
                + ", methodName='"
                + methodName
                + '\''
                + ", hasCellBlock="
                + (cellBlock != null)
                + '}';
    }
}
