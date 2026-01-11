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

package org.apache.fluss.hbase.executor;

import org.apache.fluss.hbase.protocol.HBaseRpcRequest;
import org.apache.fluss.hbase.protocol.HBaseRpcResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Routes HBase RPC requests to appropriate operation executors. */
public class HBaseRequestRouter {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseRequestRouter.class);

    private final Map<String, HBaseOperationExecutor> executors = new HashMap<>();

    public void registerExecutor(String methodName, HBaseOperationExecutor executor) {
        executors.put(methodName, executor);
        LOG.info("Registered executor for method: {}", methodName);
    }

    public CompletableFuture<HBaseRpcResponse> route(HBaseRpcRequest request) {
        System.err.println("[ROUTER] START route: callId=" + request.getCallId() + ", method=" + request.getMethodName());
        LOG.info(
                "[ROUTER] START route: callId={}, method={}",
                request.getCallId(),
                request.getMethodName());

        String methodName = request.getMethodName();
        byte[] requestBytes = (byte[]) request.getRequestParam();

        System.err.println("[ROUTER] Extracting table name for method=" + methodName + ", requestBytesSize=" + (requestBytes != null ? requestBytes.length : 0));
        LOG.info(
                "[ROUTER] Extracting table name for method={}, requestBytesSize={}",
                methodName,
                requestBytes != null ? requestBytes.length : 0);

        String lookupKey = methodName;
        String tableName = TableNameExtractor.extractTableName(methodName, requestBytes);
        if (tableName != null) {
            lookupKey = methodName + "-" + tableName;
        }

        System.err.println("[ROUTER] Routing decision: method=" + methodName + ", table=" + tableName + ", lookupKey=" + lookupKey + ", availableExecutors=" + executors.keySet());
        LOG.info(
                "[ROUTER] Routing decision: method={}, table={}, lookupKey={}, availableExecutors={}",
                methodName,
                tableName,
                lookupKey,
                executors.keySet());

        HBaseOperationExecutor executor = executors.get(lookupKey);

        if (executor == null) {
            System.err.println("[ROUTER] NO EXECUTOR FOUND for method: " + methodName + " (table: " + tableName + ", lookup key: " + lookupKey + ")");
            LOG.warn(
                    "[ROUTER] No executor found for method: {} (table: {}, lookup key: {})",
                    methodName,
                    tableName,
                    lookupKey);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(
                            request.getCallId(),
                            new UnsupportedOperationException(
                                    "Method not supported: " + methodName)));
        }

        System.err.println("[ROUTER] Dispatching to executor: callId=" + request.getCallId() + ", executor=" + executor.getClass().getSimpleName());
        LOG.info(
                "[ROUTER] Dispatching to executor: callId={}, executor={}",
                request.getCallId(),
                executor.getClass().getSimpleName());
        CompletableFuture<HBaseRpcResponse> future = executor.execute(request);
        System.err.println("[ROUTER] Executor returned future: callId=" + request.getCallId() + ", futureIsDone=" + future.isDone());
        LOG.info(
                "[ROUTER] Executor returned future: callId={}, futureIsDone={}",
                request.getCallId(),
                future.isDone());
        return future;
    }
}
