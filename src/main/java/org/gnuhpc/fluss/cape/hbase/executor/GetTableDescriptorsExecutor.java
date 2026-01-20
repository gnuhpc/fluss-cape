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
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.DataType;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcRequest;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcResponse;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Executor for HBase GetTableDescriptors RPC method (used by 'describe' command).
 * Retrieves table schema information from Fluss and converts to HBase TableDescriptor format.
 */
public class GetTableDescriptorsExecutor implements HBaseOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(GetTableDescriptorsExecutor.class);
    private final Admin flussAdmin;

    public GetTableDescriptorsExecutor(Admin flussAdmin) {
        this.flussAdmin = flussAdmin;
    }

    @Override
    public CompletableFuture<HBaseRpcResponse> execute(HBaseRpcRequest request) {
        try {
            byte[] requestBytes = (byte[]) request.getRequestParam();
            MasterProtos.GetTableDescriptorsRequest getDescriptorsRequest =
                    MasterProtos.GetTableDescriptorsRequest.parseFrom(requestBytes);

            LOG.info(
                    "[GetTableDescriptors] Parsed request: tableNamesCount={}, regex={}, includeSysTables={}",
                    getDescriptorsRequest.getTableNamesCount(),
                    getDescriptorsRequest.hasRegex() ? getDescriptorsRequest.getRegex() : "none",
                    getDescriptorsRequest.hasIncludeSysTables()
                            ? getDescriptorsRequest.getIncludeSysTables()
                            : false);

            List<HBaseProtos.TableName> requestedTables = getDescriptorsRequest.getTableNamesList();
            
            if (requestedTables.isEmpty()) {
                // No specific tables requested, return empty response
                LOG.info("[GetTableDescriptors] No tables requested, returning empty response");
                MasterProtos.GetTableDescriptorsResponse response =
                        MasterProtos.GetTableDescriptorsResponse.newBuilder().build();
                return CompletableFuture.completedFuture(
                        HBaseRpcResponse.success(request.getCallId(), response));
            }

            // Build list of futures to fetch table descriptors
            List<CompletableFuture<HBaseProtos.TableSchema>> futures = new ArrayList<>();
            
            for (HBaseProtos.TableName tableName : requestedTables) {
                String namespace = new String(tableName.getNamespace().toByteArray(), StandardCharsets.UTF_8);
                String qualifier = new String(tableName.getQualifier().toByteArray(), StandardCharsets.UTF_8);
                
                TablePath tablePath = TablePath.of(namespace, qualifier);
                
                LOG.info("[GetTableDescriptors] Fetching descriptor for table: {}.{}", namespace, qualifier);
                
                CompletableFuture<HBaseProtos.TableSchema> future = flussAdmin
                        .getTableInfo(tablePath)
                        .thenApply(tableInfo -> 
                                convertToHBaseTableSchema(tableName, tableInfo));
                
                futures.add(future);
            }

            // Wait for all futures and build response
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .thenApply(v -> {
                        List<HBaseProtos.TableSchema> schemas = new ArrayList<>();
                        for (CompletableFuture<HBaseProtos.TableSchema> future : futures) {
                            try {
                                schemas.add(future.join());
                            } catch (Exception e) {
                                LOG.error("[GetTableDescriptors] Failed to get table schema", e);
                            }
                        }

                        MasterProtos.GetTableDescriptorsResponse response =
                                MasterProtos.GetTableDescriptorsResponse.newBuilder()
                                        .addAllTableSchema(schemas)
                                        .build();

                        LOG.info("[GetTableDescriptors] Returning {} table schemas", schemas.size());
                        return HBaseRpcResponse.success(request.getCallId(), response);
                    })
                    .exceptionally(throwable -> {
                        LOG.error("[GetTableDescriptors] Failed to get table descriptors", throwable);
                        return HBaseRpcResponse.failure(
                                request.getCallId(),
                                throwable instanceof Exception
                                        ? (Exception) throwable
                                        : new Exception(throwable));
                    });

        } catch (Exception e) {
            LOG.error("[GetTableDescriptors] Failed to parse request", e);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(request.getCallId(), e));
        }
    }

    /**
     * Converts Fluss TableInfo to HBase TableSchema protobuf.
     */
    private HBaseProtos.TableSchema convertToHBaseTableSchema(
            HBaseProtos.TableName tableName, TableInfo tableInfo) {
        
        RowType rowType = tableInfo.getRowType();
        
        Set<String> columnFamilies = new HashSet<>();
        columnFamilies.add("cf");
        
        for (String fieldName : rowType.getFieldNames()) {
            if (fieldName.contains("_")) {
                String[] parts = fieldName.split("_", 2);
                columnFamilies.add(parts[0]);
            }
        }

        // Build HBase ColumnFamilySchema for each family
        List<HBaseProtos.ColumnFamilySchema> familySchemas = new ArrayList<>();
        
        for (String familyName : columnFamilies) {
            HBaseProtos.ColumnFamilySchema familySchema =
                    HBaseProtos.ColumnFamilySchema.newBuilder()
                            .setName(ByteString.copyFrom(familyName.getBytes(StandardCharsets.UTF_8)))
                            .build();
            familySchemas.add(familySchema);
        }

        // Build TableSchema
        HBaseProtos.TableSchema.Builder schemaBuilder =
                HBaseProtos.TableSchema.newBuilder()
                        .setTableName(tableName)
                        .addAllColumnFamilies(familySchemas);

        // Add table attributes (optional metadata)
        schemaBuilder.addAttributes(
                HBaseProtos.BytesBytesPair.newBuilder()
                        .setFirst(ByteString.copyFromUtf8("FLUSS_TABLE"))
                        .setSecond(ByteString.copyFromUtf8("true"))
                        .build());

        LOG.info(
                "[GetTableDescriptors] Converted table {}.{} with {} column families",
                new String(tableName.getNamespace().toByteArray(), StandardCharsets.UTF_8),
                new String(tableName.getQualifier().toByteArray(), StandardCharsets.UTF_8),
                familySchemas.size());

        return schemaBuilder.build();
    }
}
