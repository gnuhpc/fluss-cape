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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcRequest;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcResponse;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Executor for HBase CreateTable RPC method. Converts HBase TableSchema to Fluss TableDescriptor
 * and creates the table in Fluss.
 *
 * <p>Mapping strategy: - HBase namespace:qualifier → Fluss database.table - HBase column families
 * → Fluss columns (type STRING by default) - HBase rowkey → Fluss primary key (single column named
 * "rowkey", type BYTES) - All column family columns stored as separate Fluss columns with naming:
 * cf_qualifier
 */
public class CreateTableExecutor implements HBaseOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(CreateTableExecutor.class);

    private final Connection flussConnection;
    private final Admin flussAdmin;
    private final TableRegistrationCallback registrationCallback;

    /**
     * Callback interface for registering CRUD executors after table creation.
     */
    public interface TableRegistrationCallback {
        CompletableFuture<Void> registerTableExecutors(TablePath tablePath);
    }

    public CreateTableExecutor(
            Connection flussConnection,
            Admin flussAdmin,
            TableRegistrationCallback registrationCallback) {
        this.flussConnection = flussConnection;
        this.flussAdmin = flussAdmin;
        this.registrationCallback = registrationCallback;
    }

    @Override
    public CompletableFuture<HBaseRpcResponse> execute(HBaseRpcRequest request) {
        try {
            byte[] requestBytes = (byte[]) request.getRequestParam();
            MasterProtos.CreateTableRequest createTableRequest =
                    MasterProtos.CreateTableRequest.parseFrom(requestBytes);

            HBaseProtos.TableSchema tableSchema = createTableRequest.getTableSchema();
            HBaseProtos.TableName tableName = tableSchema.getTableName();

            String namespace =
                    new String(tableName.getNamespace().toByteArray(), StandardCharsets.UTF_8);
            String qualifier =
                    new String(tableName.getQualifier().toByteArray(), StandardCharsets.UTF_8);

            LOG.info(
                    "[CreateTable] Creating table: {}.{} with {} column families",
                    namespace,
                    qualifier,
                    tableSchema.getColumnFamiliesCount());

            // Convert HBase TableSchema to Fluss TableDescriptor
            TablePath tablePath = TablePath.of(namespace, qualifier);
            TableDescriptor flussTableDescriptor = convertToFlussTableDescriptor(tableSchema);

            // Create database if it doesn't exist
            return flussAdmin
                    .databaseExists(namespace)
                    .thenCompose(
                            exists -> {
                                if (!exists) {
                                    LOG.info(
                                            "[CreateTable] Creating database: {}",
                                            namespace);
                                    DatabaseDescriptor dbDesc = DatabaseDescriptor.builder().build();
                                    return flussAdmin.createDatabase(namespace, dbDesc, false);
                                } else {
                                    return CompletableFuture.completedFuture(null);
                                }
                            })
                    .thenCompose(
                            v -> {
                                LOG.info(
                                        "[CreateTable] Creating Fluss table: {}",
                                        tablePath);
                                // Track whether table already exists using CompletableFuture wrapper
                                CompletableFuture<Boolean> tableExistsTracker = new CompletableFuture<>();
                                
                                return flussAdmin.createTable(tablePath, flussTableDescriptor, false)
                                        .handle((result, ex) -> {
                                            if (ex != null) {
                                                Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                                                String exClassName = cause.getClass().getName();
                                                if (exClassName.contains("TableAlreadyExistException") || 
                                                    (cause.getMessage() != null && cause.getMessage().contains("already exists"))) {
                                                    LOG.info(
                                                            "[CreateTable] Table {} already exists in Fluss, will register executors anyway",
                                                            tablePath);
                                                    tableExistsTracker.complete(true);  // Mark as already existed
                                                    return null;  // Continue to executor registration
                                                }
                                                // Re-throw other exceptions
                                                tableExistsTracker.completeExceptionally(ex);
                                                throw (RuntimeException) ex;
                                            }
                                            tableExistsTracker.complete(false);  // Newly created
                                            return result;
                                        })
                                        .thenCompose(createResult -> {
                                            // Register CRUD executors for this table
                                            LOG.info(
                                                    "[CreateTable] Registering CRUD executors for table: {}",
                                                    tablePath);
                                            return registrationCallback.registerTableExecutors(tablePath)
                                                    .whenComplete((regResult, throwable) -> {
                                                        if (throwable != null) {
                                                            LOG.error(
                                                                    "[CreateTable] Failed to register executors for table: {}",
                                                                    tablePath,
                                                                    throwable);
                                                        } else {
                                                            LOG.info(
                                                                    "[CreateTable] Successfully registered executors for table: {}",
                                                                    tablePath);
                                                        }
                                                    })
                                                    .thenCombine(tableExistsTracker, (regResult, alreadyExists) -> alreadyExists);
                                        });
                            })
                    .thenApply(
                            alreadyExists -> {
                                if (alreadyExists) {
                                    // Table already existed - return error to user
                                    LOG.warn(
                                            "[CreateTable] Table {}.{} already exists, returning error to client",
                                            namespace,
                                            qualifier);
                                    throw new RuntimeException(
                                            String.format("Table %s.%s already exists", namespace, qualifier));
                                }
                                // Return success response with dummy proc_id
                                MasterProtos.CreateTableResponse response =
                                        MasterProtos.CreateTableResponse.newBuilder()
                                                .setProcId(1L)
                                                .build();
                                LOG.info(
                                        "[CreateTable] Successfully created table: {}.{}",
                                        namespace,
                                        qualifier);
                                return HBaseRpcResponse.success(request.getCallId(), response);
                            })
                    .exceptionally(
                            throwable -> {
                                LOG.error(
                                        "[CreateTable] Failed to create table: {}.{}",
                                        namespace,
                                        qualifier,
                                        throwable);
                                return HBaseRpcResponse.failure(
                                        request.getCallId(),
                                        throwable instanceof Exception
                                                ? (Exception) throwable
                                                : new Exception(throwable));
                            });

        } catch (Exception e) {
            LOG.error("[CreateTable] Failed to parse request", e);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(request.getCallId(), e));
        }
    }

    /**
     * Converts HBase TableSchema to Fluss TableDescriptor.
     *
     * <p>Schema mapping: - Creates a "rowkey" column (BYTES) as primary key - For each column
     * family, creates columns based on typical HBase usage patterns - Default: each CF gets a
     * single STRING column with the CF name
     *
     * <p>Note: HBase allows dynamic qualifiers within column families, but Fluss requires fixed
     * schema. We create a reasonable default schema that users can work with.
     */
    private TableDescriptor convertToFlussTableDescriptor(HBaseProtos.TableSchema tableSchema) {
        Schema.Builder schemaBuilder = Schema.newBuilder();

        schemaBuilder.column("rowkey", DataTypes.BYTES());
        schemaBuilder.primaryKey("rowkey");

        for (HBaseProtos.ColumnFamilySchema cfSchema : tableSchema.getColumnFamiliesList()) {
            String cfName = new String(cfSchema.getName().toByteArray(), StandardCharsets.UTF_8);
            
            schemaBuilder.column(cfName, DataTypes.BYTES());

            LOG.info("[CreateTable] Added column for column family: {} (type: BYTES)", cfName);
        }

        Schema schema = schemaBuilder.build();
        
        return TableDescriptor.builder()
                .schema(schema)
                .build();
    }
}
