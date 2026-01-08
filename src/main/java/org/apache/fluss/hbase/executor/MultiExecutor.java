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
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CellProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Executor for HBase Multi operations (batch Get/Put/Delete). Groups operations by bucket for
 * efficient batch processing.
 */
public class MultiExecutor implements HBaseOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(MultiExecutor.class);

    private final Connection flussConnection;
    private final TablePath tablePath;
    private final RowType rowType;
    private final RowKeyEncoder rowKeyEncoder;
    private final CellConverter cellConverter;

    private final Lookuper lookuper;
    private final UpsertWriter upsertWriter;

    public MultiExecutor(
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

        // Eager initialization to prevent netty event loop deadlock
        try {
            Table table = flussConnection.getTable(tablePath);
            this.lookuper = table.newLookup().createLookuper();
            this.upsertWriter = table.newUpsert().createWriter();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize MultiExecutor", e);
        }
    }

    @Override
    public CompletableFuture<HBaseRpcResponse> execute(HBaseRpcRequest request) {
        try {
            byte[] requestBytes = (byte[]) request.getRequestParam();
            ClientProtos.MultiRequest multiRequest =
                    ClientProtos.MultiRequest.parseFrom(requestBytes);

            List<CompletableFuture<ClientProtos.RegionActionResult>> actionFutures =
                    new ArrayList<>();

            for (ClientProtos.RegionAction regionAction : multiRequest.getRegionActionList()) {
                CompletableFuture<ClientProtos.RegionActionResult> actionFuture =
                        executeRegionAction(regionAction);
                actionFutures.add(actionFuture);
            }

            return CompletableFuture.allOf(actionFutures.toArray(new CompletableFuture[0]))
                    .thenApply(
                            v -> {
                                List<ClientProtos.RegionActionResult> results =
                                        actionFutures.stream()
                                                .map(CompletableFuture::join)
                                                .collect(Collectors.toList());

                                ClientProtos.MultiResponse.Builder multiResponse =
                                        ClientProtos.MultiResponse.newBuilder();
                                multiResponse.addAllRegionActionResult(results);

                                return HBaseRpcResponse.success(
                                        request.getCallId(), multiResponse.build());
                            })
                    .exceptionally(
                            throwable -> {
                                LOG.error("Multi operation failed", throwable);
                                return HBaseRpcResponse.failure(
                                        request.getCallId(),
                                        throwable instanceof Exception
                                                ? (Exception) throwable
                                                : new Exception(throwable));
                            });

        } catch (Exception e) {
            LOG.error("Failed to parse Multi request", e);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(request.getCallId(), e));
        }
    }

    private CompletableFuture<ClientProtos.RegionActionResult> executeRegionAction(
            ClientProtos.RegionAction regionAction) {

        // Group actions by type for optimized batch processing
        List<ActionContext> getActions = new ArrayList<>();
        List<ActionContext> putActions = new ArrayList<>();
        List<ActionContext> deleteActions = new ArrayList<>();

        for (ClientProtos.Action action : regionAction.getActionList()) {
            ActionContext ctx = new ActionContext(action);

            if (action.hasGet()) {
                getActions.add(ctx);
            } else if (action.hasMutation()) {
                ClientProtos.MutationProto.MutationType mutationType =
                        action.getMutation().getMutateType();
                if (mutationType == ClientProtos.MutationProto.MutationType.PUT) {
                    putActions.add(ctx);
                } else if (mutationType == ClientProtos.MutationProto.MutationType.DELETE) {
                    deleteActions.add(ctx);
                }
            }
        }

        // Execute batches in parallel
        CompletableFuture<Void> getFuture = executeBatchGet(getActions);
        CompletableFuture<Void> putFuture = executeBatchPut(putActions);
        CompletableFuture<Void> deleteFuture = executeBatchDelete(deleteActions);

        return CompletableFuture.allOf(getFuture, putFuture, deleteFuture)
                .thenApply(
                        v -> {
                            ClientProtos.RegionActionResult.Builder resultBuilder =
                                    ClientProtos.RegionActionResult.newBuilder();

                            // Combine results maintaining original order
                            Map<Integer, ClientProtos.ResultOrException> resultMap =
                                    new HashMap<>();
                            collectResults(getActions, resultMap);
                            collectResults(putActions, resultMap);
                            collectResults(deleteActions, resultMap);

                            for (int i = 0; i < regionAction.getActionCount(); i++) {
                                ClientProtos.ResultOrException result = resultMap.get(i);
                                if (result != null) {
                                    resultBuilder.addResultOrException(result);
                                } else {
                                    resultBuilder.addResultOrException(
                                            ClientProtos.ResultOrException.newBuilder()
                                                    .setException(
                                                            buildException(
                                                                    new IllegalStateException(
                                                                            "No result for action "
                                                                                    + i)))
                                                    .setIndex(i)
                                                    .build());
                                }
                            }

                            return resultBuilder.build();
                        });
    }

    private CompletableFuture<Void> executeBatchGet(List<ActionContext> getActions) {
        if (getActions.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (ActionContext ctx : getActions) {
            try {
                byte[] rowKey = ctx.action.getGet().getRow().toByteArray();
                GenericRow keyRow = rowKeyEncoder.decodeRowKey(rowKey);

                CompletableFuture<Void> future =
                        lookuper.lookup(keyRow)
                                .thenAccept(
                                        lookupResult -> {
                                            try {
                                                InternalRow resultRow = null;
                                                if (lookupResult.getRowList() != null
                                                        && !lookupResult.getRowList().isEmpty()) {
                                                    resultRow = lookupResult.getRowList().get(0);
                                                }

                                                Result hbaseResult =
                                                        convertToHBaseResult(keyRow, resultRow);
                                                ctx.result =
                                                        ClientProtos.ResultOrException.newBuilder()
                                                                .setResult(
                                                                        convertResultToProto(
                                                                                hbaseResult))
                                                                .setIndex(ctx.action.getIndex())
                                                                .build();
                                            } catch (Exception e) {
                                                LOG.error("Failed to process Get result", e);
                                                ctx.result =
                                                        ClientProtos.ResultOrException.newBuilder()
                                                                .setException(buildException(e))
                                                                .setIndex(ctx.action.getIndex())
                                                                .build();
                                            }
                                        })
                                .exceptionally(
                                        throwable -> {
                                            ctx.result =
                                                    ClientProtos.ResultOrException.newBuilder()
                                                            .setException(
                                                                    buildException(
                                                                            (Exception) throwable))
                                                            .setIndex(ctx.action.getIndex())
                                                            .build();
                                            return null;
                                        });
                futures.add(future);

            } catch (Exception e) {
                LOG.error("Failed to decode row key for Get", e);
                ctx.result =
                        ClientProtos.ResultOrException.newBuilder()
                                .setException(buildException(e))
                                .setIndex(ctx.action.getIndex())
                                .build();
            }
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    private CompletableFuture<Void> executeBatchPut(List<ActionContext> putActions) {
        if (putActions.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (ActionContext ctx : putActions) {
            try {
                ClientProtos.MutationProto mutation = ctx.action.getMutation();
                byte[] rowKey = mutation.getRow().toByteArray();
                GenericRow keyRow = rowKeyEncoder.decodeRowKey(rowKey);

                CompletableFuture<Void> future =
                        lookuper.lookup(keyRow)
                                .handle(
                                        (lookupResult, lookupError) -> {
                                            if (lookupError != null) {
                                                LOG.warn(
                                                        "Lookup error for batch PUT: {}",
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
                                                    buildRowFromMutation(
                                                            keyRow, mutation, existingRow);
                                            return upsertWriter.upsert(fullRow);
                                        })
                                .thenAccept(
                                        result -> {
                                            ctx.result =
                                                    ClientProtos.ResultOrException.newBuilder()
                                                            .setResult(
                                                                    ClientProtos.Result
                                                                            .getDefaultInstance())
                                                            .setIndex(ctx.action.getIndex())
                                                            .build();
                                        })
                                .exceptionally(
                                        throwable -> {
                                            ctx.result =
                                                    ClientProtos.ResultOrException.newBuilder()
                                                            .setException(
                                                                    buildException(
                                                                            (Exception) throwable))
                                                            .setIndex(ctx.action.getIndex())
                                                            .build();
                                            return null;
                                        });
                futures.add(future);

            } catch (Exception e) {
                LOG.error("Failed to process Put action", e);
                ctx.result =
                        ClientProtos.ResultOrException.newBuilder()
                                .setException(buildException(e))
                                .setIndex(ctx.action.getIndex())
                                .build();
            }
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    private CompletableFuture<Void> executeBatchDelete(List<ActionContext> deleteActions) {
        if (deleteActions.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (ActionContext ctx : deleteActions) {
            try {
                byte[] rowKey = ctx.action.getMutation().getRow().toByteArray();
                GenericRow keyRow = rowKeyEncoder.decodeRowKey(rowKey);

                CompletableFuture<Void> future =
                        upsertWriter
                                .delete(keyRow)
                                .thenAccept(
                                        result -> {
                                            ctx.result =
                                                    ClientProtos.ResultOrException.newBuilder()
                                                            .setResult(
                                                                    ClientProtos.Result
                                                                            .getDefaultInstance())
                                                            .setIndex(ctx.action.getIndex())
                                                            .build();
                                        })
                                .exceptionally(
                                        throwable -> {
                                            ctx.result =
                                                    ClientProtos.ResultOrException.newBuilder()
                                                            .setException(
                                                                    buildException(
                                                                            (Exception) throwable))
                                                            .setIndex(ctx.action.getIndex())
                                                            .build();
                                            return null;
                                        });
                futures.add(future);

            } catch (Exception e) {
                LOG.error("Failed to process Delete action", e);
                ctx.result =
                        ClientProtos.ResultOrException.newBuilder()
                                .setException(buildException(e))
                                .setIndex(ctx.action.getIndex())
                                .build();
            }
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    private void collectResults(
            List<ActionContext> actions, Map<Integer, ClientProtos.ResultOrException> resultMap) {
        for (ActionContext ctx : actions) {
            if (ctx.result != null) {
                resultMap.put(ctx.action.getIndex(), ctx.result);
            }
        }
    }

    private Result convertToHBaseResult(GenericRow keyRow, InternalRow resultRow) {
        if (resultRow == null) {
            return Result.EMPTY_RESULT;
        }

        byte[] rowKey = rowKeyEncoder.encodeRowKey(keyRow);
        return Result.create(
                cellConverter
                        .rowToCells(rowKey, resultRow, System.currentTimeMillis())
                        .toArray(new org.apache.hadoop.hbase.Cell[0]));
    }

    private ClientProtos.Result convertResultToProto(Result result) {
        ClientProtos.Result.Builder resultBuilder = ClientProtos.Result.newBuilder();

        for (org.apache.hadoop.hbase.Cell cell : result.rawCells()) {
            CellProtos.Cell.Builder cellBuilder = CellProtos.Cell.newBuilder();
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
            cellBuilder.setCellType(CellProtos.CellType.PUT);
            cellBuilder.setValue(
                    org.apache.hbase.thirdparty.com.google.protobuf.ByteString.copyFrom(
                            cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));

            resultBuilder.addCell(cellBuilder.build());
        }

        return resultBuilder.build();
    }

    private GenericRow buildRowFromMutation(
            GenericRow keyRow, ClientProtos.MutationProto mutation, InternalRow existingRow) {
        int columnValueCount = mutation.getColumnValueCount();
        LOG.info(
                "buildRowFromMutation: keyRow fields={}, columnValues={}, existingRow={}",
                keyRow.getFieldCount(),
                columnValueCount,
                existingRow == null ? "null" : "present");

        GenericRow fullRow = new GenericRow(rowType.getFieldCount());

        // Copy primary key fields from keyRow
        for (int i = 0; i < keyRow.getFieldCount(); i++) {
            if (!keyRow.isNullAt(i)) {
                fullRow.setField(i, keyRow.getField(i));
            }
        }

        // Copy existing values for non-primary-key fields
        if (existingRow != null) {
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                if (keyRow.isNullAt(i) && !existingRow.isNullAt(i)) {
                    org.apache.fluss.types.DataType fieldType =
                            rowType.getFields().get(i).getType();
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
                }
            }
        }

        return fullRow;
    }

    private Object extractFieldValue(
            InternalRow row, int pos, org.apache.fluss.types.DataType type) {
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

    private HBaseProtos.NameBytesPair buildException(Exception e) {
        String exceptionClass = e.getClass().getName();
        String exceptionMessage = e.getMessage() != null ? e.getMessage() : "";
        String fullMessage = exceptionClass + ": " + exceptionMessage;

        return HBaseProtos.NameBytesPair.newBuilder()
                .setName(exceptionClass)
                .setValue(
                        org.apache.hbase.thirdparty.com.google.protobuf.ByteString.copyFromUtf8(
                                fullMessage))
                .build();
    }

    public void close() {
        // Lookuper doesn't need explicit close in new API
        if (upsertWriter != null) {
            try {
                upsertWriter.flush();
            } catch (Exception e) {
                LOG.warn("Failed to flush upsert writer", e);
            }
        }
    }

    private static class ActionContext {
        final ClientProtos.Action action;
        ClientProtos.ResultOrException result;

        ActionContext(ClientProtos.Action action) {
            this.action = action;
        }
    }
}
