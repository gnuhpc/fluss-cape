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

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.TablePath;
import org.apache.hadoop.hbase.TableName;
import org.gnuhpc.fluss.cape.hbase.metadata.TableStateManager;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcRequest;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcResponse;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

public class DeleteTableExecutor implements HBaseOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteTableExecutor.class);

    private final Admin flussAdmin;
    private final TableUnregistrationCallback unregistrationCallback;
    private final TableStateManager stateManager;

    public interface TableUnregistrationCallback {
        void unregisterTableExecutors(TablePath tablePath);
    }

    public DeleteTableExecutor(
            Admin flussAdmin,
            TableUnregistrationCallback unregistrationCallback,
            TableStateManager stateManager) {
        this.flussAdmin = flussAdmin;
        this.unregistrationCallback = unregistrationCallback;
        this.stateManager = stateManager;
    }

    @Override
    public CompletableFuture<HBaseRpcResponse> execute(HBaseRpcRequest request) {
        try {
            byte[] requestBytes = (byte[]) request.getRequestParam();
            MasterProtos.DeleteTableRequest deleteTableRequest =
                    MasterProtos.DeleteTableRequest.parseFrom(requestBytes);

            HBaseProtos.TableName tableName = deleteTableRequest.getTableName();
            String namespace =
                    new String(tableName.getNamespace().toByteArray(), StandardCharsets.UTF_8);
            String qualifier =
                    new String(tableName.getQualifier().toByteArray(), StandardCharsets.UTF_8);

            TablePath tablePath = TablePath.of(namespace, qualifier);

            LOG.info("[DeleteTable] Deleting table: {}.{}", namespace, qualifier);

            return flussAdmin
                    .dropTable(tablePath, false)
                    .thenApply(
                            v -> {
                                unregistrationCallback.unregisterTableExecutors(tablePath);
                                
                                TableName hbaseTableName =
                                        TableName.valueOf(namespace, qualifier);
                                stateManager.removeTable(hbaseTableName);
                                
                                MasterProtos.DeleteTableResponse response =
                                        MasterProtos.DeleteTableResponse.newBuilder()
                                                .setProcId(1L)
                                                .build();
                                LOG.info(
                                        "[DeleteTable] Successfully deleted table: {}.{}",
                                        namespace,
                                        qualifier);
                                return HBaseRpcResponse.success(request.getCallId(), response);
                            })
                    .exceptionally(
                            throwable -> {
                                LOG.error(
                                        "[DeleteTable] Failed to delete table: {}.{}",
                                        namespace,
                                        qualifier,
                                        throwable);
                                return HBaseRpcResponse.failure(
                                        request.getCallId(),
                                        throwable instanceof Exception
                                                ? (Exception) throwable
                                                : new Exception(throwable));
                            });

        } catch (org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException e) {
            LOG.error("[DeleteTable] Invalid protobuf in request: {}", e.getMessage(), e);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(request.getCallId(), e));
        } catch (RuntimeException e) {
            LOG.error("[DeleteTable] Failed to parse request: {}", e.getMessage(), e);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(request.getCallId(), e));
        }
    }
}
