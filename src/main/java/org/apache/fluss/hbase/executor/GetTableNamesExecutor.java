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

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.hbase.protocol.HBaseRpcRequest;
import org.apache.fluss.hbase.protocol.HBaseRpcResponse;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

/**
 * Executor for HBase GetTableNames RPC method. Lists tables from Fluss databases and converts them
 * to HBase TableName format.
 */
public class GetTableNamesExecutor implements HBaseOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(GetTableNamesExecutor.class);
    private final Admin flussAdmin;

    public GetTableNamesExecutor(Admin flussAdmin) {
        this.flussAdmin = flussAdmin;
    }

    @Override
    public CompletableFuture<HBaseRpcResponse> execute(HBaseRpcRequest request) {
        try {
            byte[] requestBytes = (byte[]) request.getRequestParam();
            MasterProtos.GetTableNamesRequest getTableNamesRequest =
                    MasterProtos.GetTableNamesRequest.parseFrom(requestBytes);

            LOG.info(
                    "[GetTableNames] Parsed request: regex={}, namespace={}, includeSysTables={}",
                    getTableNamesRequest.hasRegex() ? getTableNamesRequest.getRegex() : "none",
                    getTableNamesRequest.hasNamespace()
                            ? getTableNamesRequest.getNamespace()
                            : "none",
                    getTableNamesRequest.hasIncludeSysTables()
                            ? getTableNamesRequest.getIncludeSysTables()
                            : false);

            String namespace =
                    getTableNamesRequest.hasNamespace()
                            ? getTableNamesRequest.getNamespace()
                            : "default";
            String regex = getTableNamesRequest.hasRegex() ? getTableNamesRequest.getRegex() : null;
            boolean includeSysTables =
                    getTableNamesRequest.hasIncludeSysTables()
                            ? getTableNamesRequest.getIncludeSysTables()
                            : false;

            Pattern regexPattern = (regex != null) ? Pattern.compile(regex) : null;

            CompletableFuture<List<HBaseProtos.TableName>> tableNamesFuture;

            if ("*".equals(namespace) || namespace.isEmpty()) {
                tableNamesFuture =
                        flussAdmin
                                .listDatabases()
                                .thenCompose(
                                        databases -> {
                                            List<CompletableFuture<List<HBaseProtos.TableName>>>
                                                    futures = new ArrayList<>();
                                            for (String db : databases) {
                                                futures.add(
                                                        listTablesInDatabase(
                                                                db,
                                                                regexPattern,
                                                                includeSysTables));
                                            }
                                            return CompletableFuture.allOf(
                                                            futures.toArray(
                                                                    new CompletableFuture[0]))
                                                    .thenApply(
                                                            v -> {
                                                                List<HBaseProtos.TableName>
                                                                        allTables =
                                                                                new ArrayList<>();
                                                                for (CompletableFuture<
                                                                                List<
                                                                                        HBaseProtos
                                                                                                .TableName>>
                                                                        future : futures) {
                                                                    allTables.addAll(future.join());
                                                                }
                                                                return allTables;
                                                            });
                                        });
            } else {
                tableNamesFuture = listTablesInDatabase(namespace, regexPattern, includeSysTables);
            }

            return tableNamesFuture
                    .thenApply(
                            tableNames -> {
                                MasterProtos.GetTableNamesResponse response =
                                        MasterProtos.GetTableNamesResponse.newBuilder()
                                                .addAllTableNames(tableNames)
                                                .build();

                                LOG.info("[GetTableNames] Returning {} tables", tableNames.size());
                                return HBaseRpcResponse.success(request.getCallId(), response);
                            })
                    .exceptionally(
                            throwable -> {
                                LOG.error("[GetTableNames] Failed to list tables", throwable);
                                return HBaseRpcResponse.failure(
                                        request.getCallId(),
                                        throwable instanceof Exception
                                                ? (Exception) throwable
                                                : new Exception(throwable));
                            });

        } catch (Exception e) {
            LOG.error("[GetTableNames] Failed to parse request", e);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(request.getCallId(), e));
        }
    }

    private CompletableFuture<List<HBaseProtos.TableName>> listTablesInDatabase(
            String databaseName, Pattern regexPattern, boolean includeSysTables) {

        return flussAdmin
                .listTables(databaseName)
                .thenApply(
                        tables -> {
                            List<HBaseProtos.TableName> result = new ArrayList<>();

                            for (String tableName : tables) {
                                if (!includeSysTables && tableName.startsWith("__")) {
                                    continue;
                                }

                                if (regexPattern != null
                                        && !regexPattern.matcher(tableName).matches()) {
                                    continue;
                                }

                                HBaseProtos.TableName hbaseTableName =
                                        HBaseProtos.TableName.newBuilder()
                                                .setNamespace(
                                                        ByteString.copyFrom(
                                                                databaseName.getBytes(
                                                                        StandardCharsets.UTF_8)))
                                                .setQualifier(
                                                        ByteString.copyFrom(
                                                                tableName.getBytes(
                                                                        StandardCharsets.UTF_8)))
                                                .build();

                                result.add(hbaseTableName);
                            }

                            LOG.info(
                                    "[GetTableNames] Found {} tables in database '{}'",
                                    result.size(),
                                    databaseName);
                            return result;
                        });
    }
}
