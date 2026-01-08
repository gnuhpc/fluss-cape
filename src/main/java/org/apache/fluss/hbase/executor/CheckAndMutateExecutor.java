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
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Executor for HBase CheckAndMutate operations (conditional mutations). Evaluates a condition on a
 * column value and performs the mutation only if the condition is met.
 */
public class CheckAndMutateExecutor implements HBaseOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(CheckAndMutateExecutor.class);

    private final Connection flussConnection;
    private final TablePath tablePath;
    private final RowType rowType;
    private final RowKeyEncoder rowKeyEncoder;
    private final CellConverter cellConverter;

    private final Lookuper lookuper;
    private final UpsertWriter upsertWriter;

    public CheckAndMutateExecutor(
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
            this.lookuper = table.newLookup().createLookuper();
            this.upsertWriter = table.newUpsert().createWriter();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize CheckAndMutateExecutor", e);
        }
    }

    @Override
    public CompletableFuture<HBaseRpcResponse> execute(HBaseRpcRequest request) {
        try {
            byte[] requestBytes = (byte[]) request.getRequestParam();
            ClientProtos.MutateRequest mutateRequest =
                    ClientProtos.MutateRequest.parseFrom(requestBytes);

            if (!mutateRequest.hasCondition()) {
                return CompletableFuture.completedFuture(
                        HBaseRpcResponse.failure(
                                request.getCallId(),
                                new IllegalArgumentException(
                                        "CheckAndMutate requires a condition")));
            }

            ClientProtos.Condition condition = mutateRequest.getCondition();
            ClientProtos.MutationProto mutation = mutateRequest.getMutation();

            byte[] rowKey = mutation.getRow().toByteArray();
            GenericRow keyRow = rowKeyEncoder.decodeRowKey(rowKey);

            return executeCheckAndMutate(request.getCallId(), keyRow, condition, mutation);

        } catch (Exception e) {
            LOG.error("Failed to execute CheckAndMutate", e);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(request.getCallId(), e));
        }
    }

    private CompletableFuture<HBaseRpcResponse> executeCheckAndMutate(
            int callId,
            GenericRow keyRow,
            ClientProtos.Condition condition,
            ClientProtos.MutationProto mutation) {

        return lookuper.lookup(keyRow)
                .thenCompose(
                        lookupResult -> {
                            InternalRow existingRow = null;
                            if (lookupResult != null
                                    && lookupResult.getRowList() != null
                                    && !lookupResult.getRowList().isEmpty()) {
                                existingRow = lookupResult.getRowList().get(0);
                            }

                            boolean conditionMet = evaluateCondition(condition, existingRow);

                            if (!conditionMet) {
                                ClientProtos.MutateResponse response =
                                        ClientProtos.MutateResponse.newBuilder()
                                                .setProcessed(false)
                                                .build();
                                return CompletableFuture.completedFuture(
                                        HBaseRpcResponse.success(callId, response));
                            }

                            return executeMutation(callId, keyRow, mutation, existingRow);
                        })
                .exceptionally(
                        throwable -> {
                            LOG.error("CheckAndMutate failed", throwable);
                            return HBaseRpcResponse.failure(
                                    callId,
                                    throwable instanceof Exception
                                            ? (Exception) throwable
                                            : new Exception(throwable));
                        });
    }

    private boolean evaluateCondition(ClientProtos.Condition condition, InternalRow existingRow) {
        if (condition == null) {
            return true;
        }

        if (!condition.hasFamily() || !condition.hasQualifier()) {
            return existingRow != null;
        }

        String qualifierStr = Bytes.toString(condition.getQualifier().toByteArray());
        int fieldIndex = findFieldIndex(qualifierStr);

        if (fieldIndex < 0) {
            LOG.warn("Unknown qualifier in condition: {}", qualifierStr);
            return false;
        }

        byte[] expectedValue = null;
        if (condition.hasComparator()) {
            try {
                ByteArrayComparable comparator =
                        ProtobufUtil.toComparator(condition.getComparator());
                expectedValue = comparator.getValue();
            } catch (IOException e) {
                LOG.error("Failed to deserialize comparator", e);
                throw new RuntimeException("Failed to deserialize comparator", e);
            }
        }

        byte[] actualValue = null;
        if (existingRow != null && !existingRow.isNullAt(fieldIndex)) {
            DataType fieldType = rowType.getFields().get(fieldIndex).getType();
            actualValue = valueToBytes(existingRow, fieldIndex, fieldType);
        }

        HBaseProtos.CompareType compareType =
                condition.hasCompareType()
                        ? condition.getCompareType()
                        : HBaseProtos.CompareType.EQUAL;
        return compare(actualValue, expectedValue, compareType);
    }

    private boolean compare(
            byte[] actualValue, byte[] expectedValue, HBaseProtos.CompareType compareType) {

        if (compareType == HBaseProtos.CompareType.NO_OP) {
            return true;
        }

        if (actualValue == null && expectedValue == null) {
            return compareType == HBaseProtos.CompareType.EQUAL;
        }

        if (actualValue == null) {
            return compareType == HBaseProtos.CompareType.NOT_EQUAL;
        }

        if (expectedValue == null) {
            return compareType == HBaseProtos.CompareType.NOT_EQUAL;
        }

        int cmp = Bytes.compareTo(actualValue, expectedValue);

        switch (compareType) {
            case EQUAL:
                return cmp == 0;
            case NOT_EQUAL:
                return cmp != 0;
            case LESS:
                return cmp < 0;
            case LESS_OR_EQUAL:
                return cmp <= 0;
            case GREATER:
                return cmp > 0;
            case GREATER_OR_EQUAL:
                return cmp >= 0;
            default:
                return false;
        }
    }

    private CompletableFuture<HBaseRpcResponse> executeMutation(
            int callId,
            GenericRow keyRow,
            ClientProtos.MutationProto mutation,
            InternalRow existingRow) {

        switch (mutation.getMutateType()) {
            case PUT:
                return executePut(callId, keyRow, mutation, existingRow);
            case DELETE:
                return executeDelete(callId, keyRow);
            default:
                Exception ex =
                        new UnsupportedOperationException(
                                "Unsupported mutation type in CheckAndMutate: "
                                        + mutation.getMutateType());
                return CompletableFuture.completedFuture(HBaseRpcResponse.failure(callId, ex));
        }
    }

    private CompletableFuture<HBaseRpcResponse> executePut(
            int callId,
            GenericRow keyRow,
            ClientProtos.MutationProto mutation,
            InternalRow existingRow) {

        GenericRow fullRow = buildRowFromMutation(keyRow, mutation, existingRow);

        return upsertWriter
                .upsert(fullRow)
                .thenApply(
                        result -> {
                            ClientProtos.MutateResponse response =
                                    ClientProtos.MutateResponse.newBuilder()
                                            .setProcessed(true)
                                            .build();
                            return HBaseRpcResponse.success(callId, response);
                        });
    }

    private CompletableFuture<HBaseRpcResponse> executeDelete(int callId, GenericRow keyRow) {
        return upsertWriter
                .delete(keyRow)
                .thenApply(
                        result -> {
                            ClientProtos.MutateResponse response =
                                    ClientProtos.MutateResponse.newBuilder()
                                            .setProcessed(true)
                                            .build();
                            return HBaseRpcResponse.success(callId, response);
                        });
    }

    private GenericRow buildRowFromMutation(
            GenericRow keyRow, ClientProtos.MutationProto mutation, InternalRow existingRow) {

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

        for (ClientProtos.MutationProto.ColumnValue columnValue : mutation.getColumnValueList()) {
            for (ClientProtos.MutationProto.ColumnValue.QualifierValue qualifierValue :
                    columnValue.getQualifierValueList()) {
                if (!qualifierValue.hasValue()) {
                    continue;
                }

                byte[] qualifier = qualifierValue.getQualifier().toByteArray();
                byte[] value = qualifierValue.getValue().toByteArray();
                String qualifierStr = Bytes.toString(qualifier);
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

    private Object extractFieldValue(InternalRow row, int pos, DataType type) {
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

    private byte[] valueToBytes(InternalRow row, int pos, DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return Bytes.toBytes(row.getBoolean(pos));
            case TINYINT:
                return new byte[] {row.getByte(pos)};
            case SMALLINT:
                return Bytes.toBytes(row.getShort(pos));
            case INTEGER:
                return Bytes.toBytes(row.getInt(pos));
            case BIGINT:
                return Bytes.toBytes(row.getLong(pos));
            case FLOAT:
                return Bytes.toBytes(row.getFloat(pos));
            case DOUBLE:
                return Bytes.toBytes(row.getDouble(pos));
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
                throw new UnsupportedOperationException(
                        "Unsupported type for HBase conversion: " + type);
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
