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
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.gnuhpc.fluss.cape.hbase.mapping.CellConverter;
import org.gnuhpc.fluss.cape.hbase.mapping.DynamicTableCodec;
import org.gnuhpc.fluss.cape.hbase.mapping.RowKeyEncoder;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcRequest;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcResponse;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CellProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Executor that handles HBase Get operations by translating them to Fluss primary key lookups. */
public class GetExecutor implements HBaseOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(GetExecutor.class);

    private final Connection flussConnection;
    private final TablePath tablePath;
    private final RowKeyEncoder rowKeyEncoder;
    private final CellConverter cellConverter;
    private final RowType rowType;

    private final ThreadLocal<Lookuper> lookuper;

    public GetExecutor(
            Connection flussConnection,
            TablePath tablePath,
            RowKeyEncoder rowKeyEncoder,
            CellConverter cellConverter) {
        this.flussConnection = flussConnection;
        this.tablePath = tablePath;
        this.rowKeyEncoder = rowKeyEncoder;
        this.cellConverter = cellConverter;
        this.rowType = cellConverter.getRowType();
        this.lookuper = ThreadLocal.withInitial(this::createLookuper);
    }

    @Override
    public CompletableFuture<HBaseRpcResponse> execute(HBaseRpcRequest request) {
        try {
            byte[] requestBytes = (byte[]) request.getRequestParam();
            ClientProtos.GetRequest getRequest = ClientProtos.GetRequest.parseFrom(requestBytes);

            ClientProtos.Get get = getRequest.getGet();
            byte[] rowKey = get.getRow().toByteArray();

            GenericRow keyRow = rowKeyEncoder.decodeRowKey(rowKey);

            return executeGet(request.getCallId(), keyRow);

        } catch (org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException e) {
            LOG.error("Invalid protobuf in Get request: {}", e.getMessage(), e);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(request.getCallId(), e));
        } catch (RuntimeException e) {
            LOG.error("Failed to execute HBase Get request: {}", e.getMessage(), e);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(request.getCallId(), e));
        }
    }

    private CompletableFuture<HBaseRpcResponse> executeGet(int callId, GenericRow keyRow) {
        return getLookuper()
                .lookup(keyRow)
                .thenApply(
                        lookupResult -> {
                            try {
                                InternalRow resultRow = null;
                                if (lookupResult.getRowList() != null
                                        && !lookupResult.getRowList().isEmpty()) {
                                    resultRow = lookupResult.getRowList().get(0);
                                }

                                Result hbaseResult = convertToHBaseResult(keyRow, resultRow);

                                ClientProtos.GetResponse.Builder responseBuilder =
                                        ClientProtos.GetResponse.newBuilder();
                                if (hbaseResult != null) {
                                    responseBuilder.setResult(convertResultToProto(hbaseResult));
                                }

                                return HBaseRpcResponse.success(callId, responseBuilder.build());

                            } catch (RuntimeException e) {
                                LOG.error("Failed to convert Fluss result to HBase result: {}", 
                                        e.getMessage(), e);
                                return HBaseRpcResponse.failure(callId, e);
                            }
                        })
                .exceptionally(
                        throwable -> {
                            LOG.error("Fluss lookup failed", throwable);
                            return HBaseRpcResponse.failure(
                                    callId,
                                    throwable instanceof Exception
                                            ? (Exception) throwable
                                            : new Exception(throwable));
                        });
    }

    private Result convertToHBaseResult(GenericRow keyRow, InternalRow resultRow) {
        if (resultRow == null) {
            return Result.EMPTY_RESULT;
        }

        byte[] rowKey = rowKeyEncoder.encodeRowKey(keyRow);

        if (isDynamicTable()) {
            List<Cell> allCells = new ArrayList<>();
            
            for (int i = 1; i < rowType.getFieldCount(); i++) {
                if (!resultRow.isNullAt(i)) {
                    String cfName = rowType.getFields().get(i).getName();
                    byte[] encodedData = resultRow.getBytes(i);
                    List<Cell> cells = DynamicTableCodec.decodeColumnFamily(
                            cfName, rowKey, encodedData, System.currentTimeMillis());
                    allCells.addAll(cells);
                }
            }
            
            return Result.create(allCells.toArray(new Cell[0]));
        } else {
            return Result.create(
                    cellConverter
                            .rowToCells(rowKey, resultRow, System.currentTimeMillis())
                            .toArray(new Cell[0]));
        }
    }

    private boolean isDynamicTable() {
        if (rowType.getFieldCount() < 2) {
            return false;
        }
        if (!"rowkey".equals(rowType.getFields().get(0).getName())) {
            return false;
        }
        for (int i = 1; i < rowType.getFieldCount(); i++) {
            DataType fieldType = rowType.getFields().get(i).getType();
            if (fieldType.getTypeRoot() != org.apache.fluss.types.DataTypeRoot.BYTES) {
                return false;
            }
        }
        return true;
    }

    private ClientProtos.Result convertResultToProto(Result result) {
        ClientProtos.Result.Builder resultBuilder = ClientProtos.Result.newBuilder();

        org.apache.hadoop.hbase.Cell[] cells = result.rawCells();
        if (cells == null) {
            return resultBuilder.build();
        }

        for (org.apache.hadoop.hbase.Cell cell : cells) {
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

    private Lookuper getLookuper() {
        return lookuper.get();
    }

    private Lookuper createLookuper() {
        try {
            Table table = flussConnection.getTable(tablePath);
            return table.newLookup().createLookuper();
        } catch (RuntimeException e) {
            throw new RuntimeException(
                    "Failed to create Fluss lookuper for table " + tablePath, e);
        }
    }

    public void close() {
        lookuper.remove();
    }
}
