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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.hbase.mapping.CellConverter;
import org.apache.fluss.hbase.mapping.RowKeyEncoder;
import org.apache.fluss.hbase.protocol.HBaseRpcRequest;
import org.apache.fluss.hbase.protocol.HBaseRpcResponse;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Executor that handles HBase Put and Delete operations by translating them to Fluss upserts. */
public class PutExecutor implements HBaseOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(PutExecutor.class);

    private final Connection flussConnection;
    private final TablePath tablePath;
    private final RowType rowType;
    private final RowKeyEncoder rowKeyEncoder;
    private final CellConverter cellConverter;

    private final UpsertWriter upsertWriter;
    private final Lookuper lookuper;
    private final CheckAndMutateExecutor checkAndMutateExecutor;

    public PutExecutor(
            Connection flussConnection,
            TablePath tablePath,
            RowType rowType,
            RowKeyEncoder rowKeyEncoder,
            CellConverter cellConverter) {
        this.flussConnection = flussConnection;
        this.tablePath = tablePath;
        this.rowType = rowType;
        this.rowKeyEncoder = rowKeyEncoder;
        this.cellConverter = cellConverter;

        try {
            Table table = flussConnection.getTable(tablePath);
            this.upsertWriter = table.newUpsert().createWriter();
            this.lookuper = table.newLookup().createLookuper();
            this.checkAndMutateExecutor =
                    new CheckAndMutateExecutor(
                            flussConnection, tablePath, rowType, rowKeyEncoder, cellConverter);
            LOG.info(
                    "PutExecutor initialized with UpsertWriter, Lookuper, and CheckAndMutateExecutor for table {}",
                    tablePath);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to initialize PutExecutor for table " + tablePath, e);
        }
    }

    @Override
    public CompletableFuture<HBaseRpcResponse> execute(HBaseRpcRequest request) {
        LOG.info(
                "[PUTEXEC] START execute: callId={}, methodName={}",
                request.getCallId(),
                request.getMethodName());
        try {
            byte[] requestBytes = (byte[]) request.getRequestParam();

            if ("Multi".equals(request.getMethodName())) {
                return executeRowMutations(request.getCallId(), requestBytes);
            }

            LOG.info(
                    "[PUTEXEC] Parsing MutateRequest protobuf, size={} bytes", requestBytes.length);
            ClientProtos.MutateRequest mutateRequest =
                    ClientProtos.MutateRequest.parseFrom(requestBytes);

            // Check if this is a CheckAndMutate request
            if (mutateRequest.hasCondition()) {
                LOG.info(
                        "[PUTEXEC] Detected CheckAndMutate request, delegating to CheckAndMutateExecutor");
                return checkAndMutateExecutor.execute(request);
            }

            ClientProtos.MutationProto mutation = mutateRequest.getMutation();
            LOG.info(
                    "[PUTEXEC] Parsed mutation: type={}, rowKeySize={}, columnValueCount={}",
                    mutation.getMutateType(),
                    mutation.getRow().size(),
                    mutation.getColumnValueCount());

            byte[] rowKey = mutation.getRow().toByteArray();
            GenericRow keyRow = rowKeyEncoder.decodeRowKey(rowKey);
            LOG.info(
                    "[PUTEXEC] Decoded rowKey: {} ({})",
                    keyRow,
                    org.apache.hadoop.hbase.util.Bytes.toStringBinary(rowKey));

            switch (mutation.getMutateType()) {
                case PUT:
                    LOG.info("[PUTEXEC] Routing to executePut for callId={}", request.getCallId());
                    return executePut(request.getCallId(), keyRow, mutation);
                case DELETE:
                    LOG.info(
                            "[PUTEXEC] Routing to executeDelete for callId={}",
                            request.getCallId());
                    return executeDelete(request.getCallId(), keyRow);
                case INCREMENT:
                    LOG.info(
                            "[PUTEXEC] Routing to executeIncrement for callId={}",
                            request.getCallId());
                    return executeIncrement(request.getCallId(), keyRow, mutation);
                case APPEND:
                    LOG.info(
                            "[PUTEXEC] Routing to executeAppend for callId={}",
                            request.getCallId());
                    return executeAppend(request.getCallId(), keyRow, mutation);
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported mutation type: " + mutation.getMutateType());
            }

        } catch (Exception e) {
            System.err.println("[PUTEXEC] FATAL: Exception in execute() for callId=" + request.getCallId());
            e.printStackTrace(System.err);
            LOG.error(
                    "[PUTEXEC] FATAL: Exception in execute() for callId=" + request.getCallId(), e);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(request.getCallId(), e));
        }
    }

    private CompletableFuture<HBaseRpcResponse> executePut(
            int callId, GenericRow keyRow, ClientProtos.MutationProto mutation) {
        try {
            LOG.info(
                    "[PUTEXEC] executePut START: callId={}, keyRow={}, columnValueCount={}",
                    callId,
                    keyRow,
                    mutation.getColumnValueCount());

            LOG.info("[PUTEXEC] Calling lookuper.lookup() for callId={}", callId);
            CompletableFuture<org.apache.fluss.client.lookup.LookupResult> lookupFuture =
                    lookuper.lookup(keyRow);
            LOG.info(
                    "[PUTEXEC] lookup() returned future (isDone={}), attaching .handle() chain",
                    lookupFuture.isDone());

            return lookupFuture
                    .handle(
                            (lookupResult, lookupError) -> {
                                LOG.info(
                                        "[PUTEXEC] .handle() invoked: callId={}, hasError={}, hasResult={}",
                                        callId,
                                        lookupError != null,
                                        lookupResult != null);
                                if (lookupError != null) {
                                    LOG.warn(
                                            "[PUTEXEC] Lookup error for callId={}: {}",
                                            callId,
                                            lookupError.getMessage(),
                                            lookupError);
                                    return null;
                                }
                                if (lookupResult != null
                                        && lookupResult.getRowList() != null
                                        && !lookupResult.getRowList().isEmpty()) {
                                    LOG.info(
                                            "[PUTEXEC] Existing row found for callId={}, rowCount={}",
                                            callId,
                                            lookupResult.getRowList().size());
                                    return lookupResult.getRowList().get(0);
                                } else {
                                    LOG.info(
                                            "[PUTEXEC] No existing row found for callId={} (will insert new)",
                                            callId);
                                }
                                return null;
                            })
                    .thenCompose(
                            existingRow -> {
                                LOG.info(
                                        "[PUTEXEC] .thenCompose() invoked: callId={}, buildingRow",
                                        callId);
                                GenericRow fullRow =
                                        buildRowFromMutation(keyRow, mutation, existingRow);
                                LOG.info(
                                        "[PUTEXEC] Built fullRow: callId={}, row={}",
                                        callId,
                                        fullRow);
                                LOG.info(
                                        "[PUTEXEC] Calling upsertWriter.upsert() for callId={}",
                                        callId);
                                return upsertWriter
                                        .upsert(fullRow)
                                        .thenApply(
                                                upsertResult -> {
                                                    LOG.info(
                                                            "[PUTEXEC] upsert completed for callId={}",
                                                            callId);
                                                    return null;
                                                });
                            })
                    .thenApply(
                            result -> {
                                LOG.info(
                                        "[PUTEXEC] .thenApply() SUCCESS: callId={}, building response",
                                        callId);
                                ClientProtos.MutateResponse response =
                                        ClientProtos.MutateResponse.newBuilder().build();
                                HBaseRpcResponse rpcResponse =
                                        HBaseRpcResponse.success(callId, response);
                                LOG.info(
                                        "[PUTEXEC] executePut COMPLETED SUCCESSFULLY: callId={}",
                                        callId);
                                return rpcResponse;
                            })
                    .exceptionally(
                            throwable -> {
                                System.err.println("[PUTEXEC] EXCEPTION in future chain for callId=" + callId);
                                throwable.printStackTrace(System.err);
                                LOG.error(
                                        "[PUTEXEC] EXCEPTION in future chain for callId=" + callId,
                                        throwable);
                                return HBaseRpcResponse.failure(
                                        callId,
                                        throwable instanceof Exception
                                                ? (Exception) throwable
                                                : new Exception(throwable));
                            });

        } catch (Exception e) {
            System.err.println("[PUTEXEC] FATAL: Exception initiating executePut for callId=" + callId);
            e.printStackTrace(System.err);
            LOG.error("[PUTEXEC] FATAL: Exception initiating executePut for callId=" + callId, e);
            return CompletableFuture.completedFuture(HBaseRpcResponse.failure(callId, e));
        }
    }

    private CompletableFuture<HBaseRpcResponse> executeDelete(int callId, GenericRow keyRow) {
        return upsertWriter
                .delete(keyRow)
                .thenApply(
                        result -> {
                            ClientProtos.MutateResponse response =
                                    ClientProtos.MutateResponse.newBuilder().build();
                            return HBaseRpcResponse.success(callId, response);
                        })
                .exceptionally(
                        throwable -> {
                            LOG.error("Fluss delete failed", throwable);
                            return HBaseRpcResponse.failure(
                                    callId,
                                    throwable instanceof Exception
                                            ? (Exception) throwable
                                            : new Exception(throwable));
                        });
    }

    private CompletableFuture<HBaseRpcResponse> executeIncrement(
            int callId, GenericRow keyRow, ClientProtos.MutationProto mutation) {
        try {
            LOG.info(
                    "[PUTEXEC] executeIncrement START: callId={}, keyRow={}, columnValueCount={}",
                    callId,
                    keyRow,
                    mutation.getColumnValueCount());

            return lookuper.lookup(keyRow)
                    .handle(
                            (lookupResult, lookupError) -> {
                                if (lookupError != null) {
                                    LOG.warn(
                                            "[PUTEXEC] Lookup error for increment callId={}: {}",
                                            callId,
                                            lookupError.getMessage(),
                                            lookupError);
                                    return null;
                                }
                                if (lookupResult != null
                                        && lookupResult.getRowList() != null
                                        && !lookupResult.getRowList().isEmpty()) {
                                    return lookupResult.getRowList().get(0);
                                }
                                return null;
                            })
                    .thenCompose(
                            existingRow -> {
                                GenericRow fullRow =
                                        buildIncrementRow(keyRow, mutation, existingRow);
                                LOG.info(
                                        "[PUTEXEC] Built increment row: callId={}, row={}",
                                        callId,
                                        fullRow);
                                return upsertWriter
                                        .upsert(fullRow)
                                        .thenApply(
                                                upsertResult -> {
                                                    LOG.info(
                                                            "[PUTEXEC] Increment completed for callId={}",
                                                            callId);
                                                    return fullRow;
                                                });
                            })
                    .thenApply(
                            fullRow -> {
                                ClientProtos.MutateResponse.Builder responseBuilder =
                                        ClientProtos.MutateResponse.newBuilder();
                                org.apache.fluss.row.InternalRow resultRow = fullRow;
                                byte[] rowKeyBytes = rowKeyEncoder.encodeRowKey(keyRow);
                                ClientProtos.Result resultProto =
                                        buildResultProto(rowKeyBytes, resultRow);
                                responseBuilder.setResult(resultProto);
                                return HBaseRpcResponse.success(callId, responseBuilder.build());
                            })
                    .exceptionally(
                            throwable -> {
                                LOG.error(
                                        "[PUTEXEC] EXCEPTION in increment for callId=" + callId,
                                        throwable);
                                return HBaseRpcResponse.failure(
                                        callId,
                                        throwable instanceof Exception
                                                ? (Exception) throwable
                                                : new Exception(throwable));
                            });

        } catch (Exception e) {
            LOG.error(
                    "[PUTEXEC] FATAL: Exception initiating executeIncrement for callId=" + callId,
                    e);
            return CompletableFuture.completedFuture(HBaseRpcResponse.failure(callId, e));
        }
    }

    private CompletableFuture<HBaseRpcResponse> executeAppend(
            int callId, GenericRow keyRow, ClientProtos.MutationProto mutation) {
        try {
            LOG.info(
                    "[PUTEXEC] executeAppend START: callId={}, keyRow={}, columnValueCount={}",
                    callId,
                    keyRow,
                    mutation.getColumnValueCount());

            return lookuper.lookup(keyRow)
                    .handle(
                            (lookupResult, lookupError) -> {
                                if (lookupError != null) {
                                    LOG.warn(
                                            "[PUTEXEC] Lookup error for append callId={}: {}",
                                            callId,
                                            lookupError.getMessage(),
                                            lookupError);
                                    return null;
                                }
                                if (lookupResult != null
                                        && lookupResult.getRowList() != null
                                        && !lookupResult.getRowList().isEmpty()) {
                                    return lookupResult.getRowList().get(0);
                                }
                                return null;
                            })
                    .thenCompose(
                            existingRow -> {
                                GenericRow fullRow = buildAppendRow(keyRow, mutation, existingRow);
                                LOG.info(
                                        "[PUTEXEC] Built append row: callId={}, row={}",
                                        callId,
                                        fullRow);
                                return upsertWriter
                                        .upsert(fullRow)
                                        .thenApply(
                                                upsertResult -> {
                                                    LOG.info(
                                                            "[PUTEXEC] Append completed for callId={}",
                                                            callId);
                                                    return fullRow;
                                                });
                            })
                    .thenApply(
                            fullRow -> {
                                ClientProtos.MutateResponse.Builder responseBuilder =
                                        ClientProtos.MutateResponse.newBuilder();
                                org.apache.fluss.row.InternalRow resultRow = fullRow;
                                byte[] rowKeyBytes = rowKeyEncoder.encodeRowKey(keyRow);
                                ClientProtos.Result resultProto =
                                        buildResultProto(rowKeyBytes, resultRow);
                                responseBuilder.setResult(resultProto);
                                return HBaseRpcResponse.success(callId, responseBuilder.build());
                            })
                    .exceptionally(
                            throwable -> {
                                LOG.error(
                                        "[PUTEXEC] EXCEPTION in append for callId=" + callId,
                                        throwable);
                                return HBaseRpcResponse.failure(
                                        callId,
                                        throwable instanceof Exception
                                                ? (Exception) throwable
                                                : new Exception(throwable));
                            });

        } catch (Exception e) {
            LOG.error(
                    "[PUTEXEC] FATAL: Exception initiating executeAppend for callId=" + callId, e);
            return CompletableFuture.completedFuture(HBaseRpcResponse.failure(callId, e));
        }
    }

    private CompletableFuture<HBaseRpcResponse> executeRowMutations(
            int callId, byte[] requestBytes) {
        try {
            ClientProtos.MultiRequest multiRequest =
                    ClientProtos.MultiRequest.parseFrom(requestBytes);

            if (multiRequest.getRegionActionCount() == 0) {
                ClientProtos.MultiResponse emptyResponse =
                        ClientProtos.MultiResponse.newBuilder().build();
                return CompletableFuture.completedFuture(
                        HBaseRpcResponse.success(callId, emptyResponse));
            }

            ClientProtos.RegionAction regionAction = multiRequest.getRegionAction(0);
            if (regionAction.getActionCount() == 0) {
                ClientProtos.MultiResponse emptyResponse =
                        ClientProtos.MultiResponse.newBuilder().build();
                return CompletableFuture.completedFuture(
                        HBaseRpcResponse.success(callId, emptyResponse));
            }

            byte[] rowKey = null;
            List<ClientProtos.MutationProto> mutations = new ArrayList<>();

            for (ClientProtos.Action action : regionAction.getActionList()) {
                if (action.hasMutation()) {
                    ClientProtos.MutationProto mutation = action.getMutation();
                    byte[] currentRowKey = mutation.getRow().toByteArray();

                    if (rowKey == null) {
                        rowKey = currentRowKey;
                    } else if (!java.util.Arrays.equals(rowKey, currentRowKey)) {
                        throw new IllegalArgumentException(
                                "RowMutations must all target the same row");
                    }

                    mutations.add(mutation);
                }
            }

            if (mutations.isEmpty()) {
                ClientProtos.MultiResponse emptyResponse =
                        ClientProtos.MultiResponse.newBuilder().build();
                return CompletableFuture.completedFuture(
                        HBaseRpcResponse.success(callId, emptyResponse));
            }

            GenericRow keyRow = rowKeyEncoder.decodeRowKey(rowKey);
            return executeRowMutationsInternal(callId, keyRow, mutations);

        } catch (Exception e) {
            LOG.error("[PUTEXEC] FATAL: Exception in executeRowMutations for callId=" + callId, e);
            return CompletableFuture.completedFuture(HBaseRpcResponse.failure(callId, e));
        }
    }

    private CompletableFuture<HBaseRpcResponse> executeRowMutationsInternal(
            int callId, GenericRow keyRow, List<ClientProtos.MutationProto> mutations) {
        return lookuper.lookup(keyRow)
                .handle(
                        (lookupResult, lookupError) -> {
                            if (lookupError != null) {
                                LOG.warn(
                                        "[PUTEXEC] Lookup error for RowMutations callId={}: {}",
                                        callId,
                                        lookupError.getMessage());
                                return null;
                            }
                            if (lookupResult != null
                                    && lookupResult.getRowList() != null
                                    && !lookupResult.getRowList().isEmpty()) {
                                return lookupResult.getRowList().get(0);
                            }
                            return null;
                        })
                .thenCompose(
                        existingRow -> {
                            GenericRow fullRow =
                                    buildRowFromMultipleMutations(keyRow, mutations, existingRow);
                            return upsertWriter.upsert(fullRow).thenApply(result -> fullRow);
                        })
                .thenApply(
                        fullRow -> {
                            ClientProtos.MultiResponse.Builder multiResponse =
                                    ClientProtos.MultiResponse.newBuilder();
                            ClientProtos.RegionActionResult.Builder resultBuilder =
                                    ClientProtos.RegionActionResult.newBuilder();

                            for (int i = 0; i < mutations.size(); i++) {
                                ClientProtos.ResultOrException.Builder roeBuilder =
                                        ClientProtos.ResultOrException.newBuilder();
                                roeBuilder.setResult(ClientProtos.Result.getDefaultInstance());
                                roeBuilder.setIndex(i);
                                resultBuilder.addResultOrException(roeBuilder.build());
                            }

                            multiResponse.addRegionActionResult(resultBuilder.build());
                            return HBaseRpcResponse.success(callId, multiResponse.build());
                        })
                .exceptionally(
                        throwable -> {
                            LOG.error(
                                    "[PUTEXEC] EXCEPTION in RowMutations for callId=" + callId,
                                    throwable);
                            return HBaseRpcResponse.failure(
                                    callId,
                                    throwable instanceof Exception
                                            ? (Exception) throwable
                                            : new Exception(throwable));
                        });
    }

    private GenericRow buildRowFromMultipleMutations(
            GenericRow keyRow,
            List<ClientProtos.MutationProto> mutations,
            org.apache.fluss.row.InternalRow existingRow) {
        GenericRow fullRow = new GenericRow(rowType.getFieldCount());

        for (int i = 0; i < keyRow.getFieldCount(); i++) {
            if (!keyRow.isNullAt(i)) {
                fullRow.setField(i, keyRow.getField(i));
            }
        }

        if (existingRow != null) {
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                if (keyRow.isNullAt(i) && !existingRow.isNullAt(i)) {
                    DataType fieldType = rowType.getFields().get(i).getType();
                    Object value = extractFieldValue(existingRow, i, fieldType);
                    fullRow.setField(i, value);
                }
            }
        }

        for (ClientProtos.MutationProto mutation : mutations) {
            switch (mutation.getMutateType()) {
                case PUT:
                    applyPutMutation(fullRow, mutation);
                    break;
                case DELETE:
                    break;
                case INCREMENT:
                    applyIncrementMutation(fullRow, mutation, existingRow);
                    break;
                case APPEND:
                    applyAppendMutation(fullRow, mutation, existingRow);
                    break;
                default:
                    LOG.warn(
                            "Unsupported mutation type in RowMutations: {}",
                            mutation.getMutateType());
            }
        }

        return fullRow;
    }

    private void applyPutMutation(GenericRow fullRow, ClientProtos.MutationProto mutation) {
        for (ClientProtos.MutationProto.ColumnValue columnValue : mutation.getColumnValueList()) {
            for (ClientProtos.MutationProto.ColumnValue.QualifierValue qualifierValue :
                    columnValue.getQualifierValueList()) {
                if (!qualifierValue.hasValue()) {
                    continue;
                }

                byte[] qualifier = qualifierValue.getQualifier().toByteArray();
                byte[] value = qualifierValue.getValue().toByteArray();
                String qualifierStr = org.apache.hadoop.hbase.util.Bytes.toString(qualifier);
                int fieldIndex = findFieldIndex(qualifierStr);

                if (fieldIndex >= 0) {
                    Object convertedValue =
                            cellConverter.bytesToValue(
                                    value, rowType.getFields().get(fieldIndex).getType());
                    fullRow.setField(fieldIndex, convertedValue);
                }
            }
        }
    }

    private void applyIncrementMutation(
            GenericRow fullRow,
            ClientProtos.MutationProto mutation,
            org.apache.fluss.row.InternalRow existingRow) {
        for (ClientProtos.MutationProto.ColumnValue columnValue : mutation.getColumnValueList()) {
            for (ClientProtos.MutationProto.ColumnValue.QualifierValue qualifierValue :
                    columnValue.getQualifierValueList()) {
                if (!qualifierValue.hasValue()) {
                    continue;
                }

                byte[] qualifier = qualifierValue.getQualifier().toByteArray();
                byte[] deltaBytes = qualifierValue.getValue().toByteArray();
                String qualifierStr = org.apache.hadoop.hbase.util.Bytes.toString(qualifier);
                int fieldIndex = findFieldIndex(qualifierStr);

                if (fieldIndex >= 0) {
                    DataType fieldType = rowType.getFields().get(fieldIndex).getType();
                    long delta = org.apache.hadoop.hbase.util.Bytes.toLong(deltaBytes);
                    long currentValue = 0L;

                    if (existingRow != null && !existingRow.isNullAt(fieldIndex)) {
                        currentValue = extractLongValue(existingRow, fieldIndex, fieldType);
                    }

                    long newValue = currentValue + delta;
                    fullRow.setField(fieldIndex, convertLongToFieldType(newValue, fieldType));
                }
            }
        }
    }

    private void applyAppendMutation(
            GenericRow fullRow,
            ClientProtos.MutationProto mutation,
            org.apache.fluss.row.InternalRow existingRow) {
        for (ClientProtos.MutationProto.ColumnValue columnValue : mutation.getColumnValueList()) {
            for (ClientProtos.MutationProto.ColumnValue.QualifierValue qualifierValue :
                    columnValue.getQualifierValueList()) {
                if (!qualifierValue.hasValue()) {
                    continue;
                }

                byte[] qualifier = qualifierValue.getQualifier().toByteArray();
                byte[] appendBytes = qualifierValue.getValue().toByteArray();
                String qualifierStr = org.apache.hadoop.hbase.util.Bytes.toString(qualifier);
                int fieldIndex = findFieldIndex(qualifierStr);

                if (fieldIndex >= 0) {
                    DataType fieldType = rowType.getFields().get(fieldIndex).getType();
                    byte[] currentBytes = new byte[0];

                    if (existingRow != null && !existingRow.isNullAt(fieldIndex)) {
                        currentBytes = extractBytesValue(existingRow, fieldIndex, fieldType);
                    }

                    byte[] newBytes = new byte[currentBytes.length + appendBytes.length];
                    System.arraycopy(currentBytes, 0, newBytes, 0, currentBytes.length);
                    System.arraycopy(
                            appendBytes, 0, newBytes, currentBytes.length, appendBytes.length);

                    fullRow.setField(fieldIndex, cellConverter.bytesToValue(newBytes, fieldType));
                }
            }
        }
    }

    private GenericRow buildRowFromMutation(
            GenericRow keyRow,
            ClientProtos.MutationProto mutation,
            org.apache.fluss.row.InternalRow existingRow) {
        LOG.info(
                "[PUTEXEC] buildRowFromMutation: keyRow={}, hasExisting={}, fieldCount={}",
                keyRow,
                existingRow != null,
                rowType.getFieldCount());
        GenericRow fullRow = new GenericRow(rowType.getFieldCount());

        // Copy primary key fields from keyRow
        for (int i = 0; i < keyRow.getFieldCount(); i++) {
            if (!keyRow.isNullAt(i)) {
                fullRow.setField(i, keyRow.getField(i));
            }
        }
        LOG.info("[PUTEXEC] Copied {} key fields from keyRow", keyRow.getFieldCount());

        // Copy existing values for non-primary-key fields
        if (existingRow != null) {
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                if (keyRow.isNullAt(i) && !existingRow.isNullAt(i)) {
                    DataType fieldType = rowType.getFields().get(i).getType();
                    Object value = extractFieldValue(existingRow, i, fieldType);
                    fullRow.setField(i, value);
                }
            }
        }

        // Apply new values from mutation (overrides existing)
        for (ClientProtos.MutationProto.ColumnValue columnValue : mutation.getColumnValueList()) {
            for (ClientProtos.MutationProto.ColumnValue.QualifierValue qualifierValue :
                    columnValue.getQualifierValueList()) {
                if (!qualifierValue.hasValue()) {
                    continue;
                }

                byte[] qualifier = qualifierValue.getQualifier().toByteArray();
                byte[] value = qualifierValue.getValue().toByteArray();

                String qualifierStr = org.apache.hadoop.hbase.util.Bytes.toString(qualifier);
                int fieldIndex = findFieldIndex(qualifierStr);

                if (fieldIndex >= 0) {
                    Object convertedValue =
                            cellConverter.bytesToValue(
                                    value, rowType.getFields().get(fieldIndex).getType());
                    fullRow.setField(fieldIndex, convertedValue);
                } else {
                    LOG.warn("Unknown qualifier in mutation: {}", qualifierStr);
                }
            }
        }

        return fullRow;
    }

    private GenericRow buildIncrementRow(
            GenericRow keyRow,
            ClientProtos.MutationProto mutation,
            org.apache.fluss.row.InternalRow existingRow) {
        GenericRow fullRow = new GenericRow(rowType.getFieldCount());

        // Copy primary key fields
        for (int i = 0; i < keyRow.getFieldCount(); i++) {
            if (!keyRow.isNullAt(i)) {
                fullRow.setField(i, keyRow.getField(i));
            }
        }

        // Copy existing values
        if (existingRow != null) {
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                if (keyRow.isNullAt(i) && !existingRow.isNullAt(i)) {
                    DataType fieldType = rowType.getFields().get(i).getType();
                    Object value = extractFieldValue(existingRow, i, fieldType);
                    fullRow.setField(i, value);
                }
            }
        }

        // Apply increment deltas
        for (ClientProtos.MutationProto.ColumnValue columnValue : mutation.getColumnValueList()) {
            for (ClientProtos.MutationProto.ColumnValue.QualifierValue qualifierValue :
                    columnValue.getQualifierValueList()) {
                if (!qualifierValue.hasValue()) {
                    continue;
                }

                byte[] qualifier = qualifierValue.getQualifier().toByteArray();
                byte[] deltaBytes = qualifierValue.getValue().toByteArray();

                String qualifierStr = org.apache.hadoop.hbase.util.Bytes.toString(qualifier);
                int fieldIndex = findFieldIndex(qualifierStr);

                if (fieldIndex >= 0) {
                    DataType fieldType = rowType.getFields().get(fieldIndex).getType();
                    long delta = org.apache.hadoop.hbase.util.Bytes.toLong(deltaBytes);
                    long currentValue = 0L;

                    if (existingRow != null && !existingRow.isNullAt(fieldIndex)) {
                        currentValue = extractLongValue(existingRow, fieldIndex, fieldType);
                    }

                    long newValue = currentValue + delta;
                    fullRow.setField(fieldIndex, convertLongToFieldType(newValue, fieldType));
                } else {
                    LOG.warn("Unknown qualifier in increment: {}", qualifierStr);
                }
            }
        }

        return fullRow;
    }

    private GenericRow buildAppendRow(
            GenericRow keyRow,
            ClientProtos.MutationProto mutation,
            org.apache.fluss.row.InternalRow existingRow) {
        GenericRow fullRow = new GenericRow(rowType.getFieldCount());

        // Copy primary key fields
        for (int i = 0; i < keyRow.getFieldCount(); i++) {
            if (!keyRow.isNullAt(i)) {
                fullRow.setField(i, keyRow.getField(i));
            }
        }

        // Copy existing values
        if (existingRow != null) {
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                if (keyRow.isNullAt(i) && !existingRow.isNullAt(i)) {
                    DataType fieldType = rowType.getFields().get(i).getType();
                    Object value = extractFieldValue(existingRow, i, fieldType);
                    fullRow.setField(i, value);
                }
            }
        }

        // Apply append values
        for (ClientProtos.MutationProto.ColumnValue columnValue : mutation.getColumnValueList()) {
            for (ClientProtos.MutationProto.ColumnValue.QualifierValue qualifierValue :
                    columnValue.getQualifierValueList()) {
                if (!qualifierValue.hasValue()) {
                    continue;
                }

                byte[] qualifier = qualifierValue.getQualifier().toByteArray();
                byte[] appendBytes = qualifierValue.getValue().toByteArray();

                String qualifierStr = org.apache.hadoop.hbase.util.Bytes.toString(qualifier);
                int fieldIndex = findFieldIndex(qualifierStr);

                if (fieldIndex >= 0) {
                    DataType fieldType = rowType.getFields().get(fieldIndex).getType();
                    byte[] currentBytes = new byte[0];

                    if (existingRow != null && !existingRow.isNullAt(fieldIndex)) {
                        currentBytes = extractBytesValue(existingRow, fieldIndex, fieldType);
                    }

                    byte[] newBytes = new byte[currentBytes.length + appendBytes.length];
                    System.arraycopy(currentBytes, 0, newBytes, 0, currentBytes.length);
                    System.arraycopy(
                            appendBytes, 0, newBytes, currentBytes.length, appendBytes.length);

                    fullRow.setField(fieldIndex, cellConverter.bytesToValue(newBytes, fieldType));
                } else {
                    LOG.warn("Unknown qualifier in append: {}", qualifierStr);
                }
            }
        }

        return fullRow;
    }

    private long extractLongValue(org.apache.fluss.row.InternalRow row, int pos, DataType type) {
        switch (type.getTypeRoot()) {
            case TINYINT:
                return row.getByte(pos);
            case SMALLINT:
                return row.getShort(pos);
            case INTEGER:
                return row.getInt(pos);
            case BIGINT:
                return row.getLong(pos);
            default:
                throw new UnsupportedOperationException("Unsupported type for increment: " + type);
        }
    }

    private Object convertLongToFieldType(long value, DataType type) {
        switch (type.getTypeRoot()) {
            case TINYINT:
                return (byte) value;
            case SMALLINT:
                return (short) value;
            case INTEGER:
                return (int) value;
            case BIGINT:
                return value;
            default:
                throw new UnsupportedOperationException("Unsupported type for increment: " + type);
        }
    }

    private byte[] extractBytesValue(org.apache.fluss.row.InternalRow row, int pos, DataType type) {
        switch (type.getTypeRoot()) {
            case STRING:
            case CHAR:
                return row.getString(pos).toBytes();
            case BINARY:
                org.apache.fluss.types.BinaryType binaryType =
                        (org.apache.fluss.types.BinaryType) type;
                return row.getBinary(pos, binaryType.getLength());
            case BYTES:
                return row.getBytes(pos);
            default:
                throw new UnsupportedOperationException("Unsupported type for append: " + type);
        }
    }

    private ClientProtos.Result buildResultProto(
            byte[] rowKey, org.apache.fluss.row.InternalRow resultRow) {
        ClientProtos.Result.Builder resultBuilder = ClientProtos.Result.newBuilder();

        List<org.apache.hadoop.hbase.Cell> cells =
                cellConverter.rowToCells(rowKey, resultRow, System.currentTimeMillis());

        for (org.apache.hadoop.hbase.Cell cell : cells) {
            org.apache.hadoop.hbase.shaded.protobuf.generated.CellProtos.Cell.Builder cellBuilder =
                    org.apache.hadoop.hbase.shaded.protobuf.generated.CellProtos.Cell.newBuilder();
            cellBuilder.setRow(
                    org.apache.hbase.thirdparty.com.google.protobuf.ByteString.copyFrom(
                            cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
            cellBuilder.setFamily(
                    org.apache.hbase.thirdparty.com.google.protobuf.ByteString.copyFrom(
                            cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
            cellBuilder.setQualifier(
                    org.apache.hbase.thirdparty.com.google.protobuf.ByteString.copyFrom(
                            cell.getQualifierArray(),
                            cell.getQualifierOffset(),
                            cell.getQualifierLength()));
            cellBuilder.setTimestamp(cell.getTimestamp());
            cellBuilder.setCellType(
                    org.apache.hadoop.hbase.shaded.protobuf.generated.CellProtos.CellType.PUT);
            cellBuilder.setValue(
                    org.apache.hbase.thirdparty.com.google.protobuf.ByteString.copyFrom(
                            cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));

            resultBuilder.addCell(cellBuilder.build());
        }

        return resultBuilder.build();
    }

    private Object extractFieldValue(org.apache.fluss.row.InternalRow row, int pos, DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return row.getBoolean(pos);
            case TINYINT:
                return row.getByte(pos);
            case SMALLINT:
                return row.getShort(pos);
            case INTEGER:
                return row.getInt(pos);
            case BIGINT:
                return row.getLong(pos);
            case FLOAT:
                return row.getFloat(pos);
            case DOUBLE:
                return row.getDouble(pos);
            case STRING:
            case CHAR:
                return row.getString(pos);
            case BINARY:
                org.apache.fluss.types.BinaryType binaryType =
                        (org.apache.fluss.types.BinaryType) type;
                return row.getBinary(pos, binaryType.getLength());
            case BYTES:
                return row.getBytes(pos);
            case DECIMAL:
                org.apache.fluss.types.DecimalType decimalType =
                        (org.apache.fluss.types.DecimalType) type;
                return row.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale())
                        .toBigDecimal();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return row.getTimestampNtz(pos, 3);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return row.getTimestampLtz(pos, 3);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type for field extraction: " + type);
        }
    }

    private int findFieldIndex(String fieldName) {
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (rowType.getFields().get(i).getName().equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    public void close() {
        if (upsertWriter != null) {
            try {
                upsertWriter.flush();
            } catch (Exception e) {
                LOG.warn("Failed to flush upsert writer", e);
            }
        }
    }
}
