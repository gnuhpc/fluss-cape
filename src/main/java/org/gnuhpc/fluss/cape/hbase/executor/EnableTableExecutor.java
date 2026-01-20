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

import org.gnuhpc.fluss.cape.hbase.metadata.TableStateManager;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcRequest;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcResponse;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

public class EnableTableExecutor implements HBaseOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(EnableTableExecutor.class);

    private final TableStateManager stateManager;

    public EnableTableExecutor(TableStateManager stateManager) {
        this.stateManager = stateManager;
    }

    @Override
    public CompletableFuture<HBaseRpcResponse> execute(HBaseRpcRequest request) {
        try {
            byte[] requestBytes = (byte[]) request.getRequestParam();
            MasterProtos.EnableTableRequest enableRequest =
                    MasterProtos.EnableTableRequest.parseFrom(requestBytes);

            HBaseProtos.TableName tableName = enableRequest.getTableName();
            String namespace =
                    new String(tableName.getNamespace().toByteArray(), StandardCharsets.UTF_8);
            String qualifier =
                    new String(tableName.getQualifier().toByteArray(), StandardCharsets.UTF_8);

            TableName hbaseTableName = TableName.valueOf(namespace, qualifier);
            stateManager.enableTable(hbaseTableName);

            LOG.info("[EnableTable] Enabled table: {}.{}", namespace, qualifier);

            MasterProtos.EnableTableResponse response =
                    MasterProtos.EnableTableResponse.newBuilder().setProcId(1L).build();
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.success(request.getCallId(), response));

        } catch (Exception e) {
            LOG.error("[EnableTable] Failed to parse request", e);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(request.getCallId(), e));
        }
    }
}
