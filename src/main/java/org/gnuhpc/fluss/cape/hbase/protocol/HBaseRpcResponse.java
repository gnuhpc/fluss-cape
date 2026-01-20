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

/** HBase RPC response wrapper containing call ID, result message, and optional cell block. */
public class HBaseRpcResponse {

    private final int callId;
    @Nullable private final Object responseMessage;
    @Nullable private final Exception exception;
    @Nullable private final byte[] cellBlock;

    public HBaseRpcResponse(
            int callId,
            @Nullable Object responseMessage,
            @Nullable Exception exception,
            @Nullable byte[] cellBlock) {
        this.callId = callId;
        this.responseMessage = responseMessage;
        this.exception = exception;
        this.cellBlock = cellBlock;
    }

    public static HBaseRpcResponse success(int callId, Object response) {
        return new HBaseRpcResponse(callId, response, null, null);
    }

    public static HBaseRpcResponse success(int callId, Object response, byte[] cellBlock) {
        return new HBaseRpcResponse(callId, response, null, cellBlock);
    }

    public static HBaseRpcResponse failure(int callId, Exception exception) {
        return new HBaseRpcResponse(callId, null, exception, null);
    }

    public int getCallId() {
        return callId;
    }

    @Nullable
    public Object getResponseMessage() {
        return responseMessage;
    }

    @Nullable
    public Exception getException() {
        return exception;
    }

    @Nullable
    public byte[] getCellBlock() {
        return cellBlock;
    }

    public boolean isSuccess() {
        return exception == null;
    }
}
