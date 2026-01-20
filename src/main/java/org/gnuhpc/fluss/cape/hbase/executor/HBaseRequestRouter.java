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

package org.gnuhpc.fluss.cape.hbase.executor;

import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcRequest;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/** Routes HBase RPC requests to appropriate operation executors. */
public class HBaseRequestRouter {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseRequestRouter.class);

    private final Map<String, HBaseOperationExecutor> executors = new ConcurrentHashMap<>();

    public void registerExecutor(String methodName, HBaseOperationExecutor executor) {
        executors.put(methodName, executor);
        LOG.info("Registered executor for method: {}", methodName);
    }

    public void unregisterExecutor(String methodName) {
        HBaseOperationExecutor executor = executors.remove(methodName);
        if (executor != null) {
            closeExecutor(executor);
            LOG.info("Unregistered executor for method: {}", methodName);
        }
    }

    public void unregisterExecutors(String prefix) {
        executors.entrySet().removeIf(entry -> {
            if (entry.getKey().startsWith(prefix)) {
                closeExecutor(entry.getValue());
                LOG.info("Unregistered executor for method: {}", entry.getKey());
                return true;
            }
            return false;
        });
    }

    private void closeExecutor(HBaseOperationExecutor executor) {
        if (executor instanceof GetExecutor) {
            ((GetExecutor) executor).close();
        } else if (executor instanceof PutExecutor) {
            ((PutExecutor) executor).close();
        } else if (executor instanceof ScanExecutor) {
            ((ScanExecutor) executor).close();
        } else if (executor instanceof MultiExecutor) {
            ((MultiExecutor) executor).close();
        } else if (executor instanceof CheckAndMutateExecutor) {
            ((CheckAndMutateExecutor) executor).close();
        }
    }

    private boolean isAdminOperation(String methodName) {
        switch (methodName) {
            case "CreateTable":
            case "DeleteTable":
            case "DisableTable":
            case "EnableTable":
            case "ModifyTable":
            case "GetTableNames":
            case "GetTableDescriptors":
            case "IsMasterRunning":
                return true;
            default:
                return false;
        }
    }

    public CompletableFuture<HBaseRpcResponse> route(HBaseRpcRequest request) {
        LOG.debug(
                "Routing request: callId={}, method={}",
                request.getCallId(),
                request.getMethodName());

        String methodName = request.getMethodName();
        byte[] requestBytes = (byte[]) request.getRequestParam();

        String lookupKey = methodName;
        String tableName = TableNameExtractor.extractTableName(methodName, requestBytes);
        boolean isAdminOperation = isAdminOperation(methodName);
        if (tableName != null && !isAdminOperation) {
            lookupKey = methodName + "-" + tableName;
        }

        LOG.trace(
                "Route lookup: method={}, table={}, lookupKey={}",
                methodName,
                tableName,
                lookupKey);

        HBaseOperationExecutor executor = executors.get(lookupKey);

        if (executor == null) {
            LOG.warn(
                    "No executor found for method: {} (table: {}, lookup key: {})",
                    methodName,
                    tableName,
                    lookupKey);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(
                            request.getCallId(),
                            new UnsupportedOperationException(
                                    "Method not supported: " + methodName)));
        }

        LOG.debug(
                "Dispatching to executor: callId={}, executor={}",
                request.getCallId(),
                executor.getClass().getSimpleName());
        return executor.execute(request);
    }
}
