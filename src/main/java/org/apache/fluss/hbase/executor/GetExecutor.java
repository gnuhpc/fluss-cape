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
import org.apache.fluss.hbase.mapping.CellConverter;
import org.apache.fluss.hbase.mapping.RowKeyEncoder;
import org.apache.fluss.hbase.protocol.HBaseRpcRequest;
import org.apache.fluss.hbase.protocol.HBaseRpcResponse;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CellProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/** Executor that handles HBase Get operations by translating them to Fluss primary key lookups. */
public class GetExecutor implements HBaseOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(GetExecutor.class);

    private final Connection flussConnection;
    private final TablePath tablePath;
    private final RowKeyEncoder rowKeyEncoder;
    private final CellConverter cellConverter;

    private volatile Lookuper lookuper;

    public GetExecutor(
            Connection flussConnection,
            TablePath tablePath,
            RowKeyEncoder rowKeyEncoder,
            CellConverter cellConverter) {
        this.flussConnection = flussConnection;
        this.tablePath = tablePath;
        this.rowKeyEncoder = rowKeyEncoder;
        this.cellConverter = cellConverter;
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

        } catch (Exception e) {
            LOG.error("Failed to execute HBase Get request", e);
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

                            } catch (Exception e) {
                                LOG.error("Failed to convert Fluss result to HBase result", e);
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
        return Result.create(
                cellConverter
                        .rowToCells(rowKey, resultRow, System.currentTimeMillis())
                        .toArray(new org.apache.hadoop.hbase.Cell[0]));
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
        if (lookuper == null) {
            synchronized (this) {
                if (lookuper == null) {
                    try {
                        Table table = flussConnection.getTable(tablePath);
                        lookuper = table.newLookup().createLookuper();
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to create Fluss lookuper", e);
                    }
                }
            }
        }
        return lookuper;
    }

    public void close() {
        // Lookuper doesn't need explicit close in new API
    }
}
