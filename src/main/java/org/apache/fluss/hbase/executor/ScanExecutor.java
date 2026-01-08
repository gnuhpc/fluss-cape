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
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.hbase.mapping.CellConverter;
import org.apache.fluss.hbase.mapping.RowKeyEncoder;
import org.apache.fluss.hbase.protocol.HBaseRpcRequest;
import org.apache.fluss.hbase.protocol.HBaseRpcResponse;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/** Executor for HBase Scan operations. */
public class ScanExecutor implements HBaseOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ScanExecutor.class);

    private final Connection flussConnection;
    private final TablePath tablePath;
    private final RowKeyEncoder rowKeyEncoder;
    private final CellConverter cellConverter;
    private final RowType rowType;
    private final InternalRow.FieldGetter[] fieldGetters;

    private final AtomicLong scannerIdGenerator = new AtomicLong(0);
    private final Map<Long, ScanSession> activeSessions = new HashMap<>();

    public ScanExecutor(
            Connection flussConnection,
            TablePath tablePath,
            RowKeyEncoder rowKeyEncoder,
            CellConverter cellConverter) {
        this.flussConnection = flussConnection;
        this.tablePath = tablePath;
        this.rowKeyEncoder = rowKeyEncoder;
        this.cellConverter = cellConverter;
        this.rowType = cellConverter.getRowType();
        this.fieldGetters = InternalRow.createFieldGetters(rowType);
    }

    @Override
    public CompletableFuture<HBaseRpcResponse> execute(HBaseRpcRequest request) {
        try {
            byte[] requestBytes = (byte[]) request.getRequestParam();
            ClientProtos.ScanRequest scanRequest = ClientProtos.ScanRequest.parseFrom(requestBytes);

            if (scanRequest.hasScannerId()) {
                if (scanRequest.hasCloseScanner() && scanRequest.getCloseScanner()) {
                    return handleScanClose(request.getCallId(), scanRequest.getScannerId());
                } else {
                    return handleScanNext(
                            request.getCallId(),
                            scanRequest.getScannerId(),
                            scanRequest.getNumberOfRows());
                }
            } else if (scanRequest.hasScan()) {
                return handleScanOpen(request.getCallId(), scanRequest.getScan());
            } else {
                throw new IllegalArgumentException("Invalid scan request");
            }

        } catch (Exception e) {
            LOG.error("Failed to parse scan request", e);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(request.getCallId(), e));
        }
    }

    private CompletableFuture<HBaseRpcResponse> handleScanOpen(int callId, ClientProtos.Scan scan) {
        try {
            long scannerId = scannerIdGenerator.incrementAndGet();

            byte[] startRow = scan.getStartRow().toByteArray();
            byte[] stopRow = scan.getStopRow().toByteArray();

            Filter filter = null;
            if (scan.hasFilter()) {
                try {
                    filter = ProtobufUtil.toFilter(scan.getFilter());
                } catch (Exception e) {
                    LOG.warn("Failed to deserialize filter", e);
                }
            }

            Table table = flussConnection.getTable(tablePath);
            TableInfo tableInfo = table.getTableInfo();
            long tableId = tableInfo.getTableId();

            List<Result> allResults = new ArrayList<>();

            Admin admin = flussConnection.getAdmin();
            KvSnapshots kvSnapshots;
            try {
                kvSnapshots = admin.getLatestKvSnapshots(tablePath).get();
                LOG.debug(
                        "Retrieved KV snapshots for table {}: {} buckets with snapshots",
                        tablePath,
                        kvSnapshots.getBucketIds().size());
            } catch (Exception e) {
                LOG.error("Failed to get latest KV snapshots for table {}", tablePath, e);
                return CompletableFuture.completedFuture(HBaseRpcResponse.failure(callId, e));
            }

            for (int bucketId : kvSnapshots.getBucketIds()) {
                if (!kvSnapshots.getSnapshotId(bucketId).isPresent()) {
                    LOG.warn("Bucket {} has no snapshot yet, skipping", bucketId);
                    continue;
                }

                long snapshotId = kvSnapshots.getSnapshotId(bucketId).getAsLong();
                TableBucket tableBucket = new TableBucket(tableId, bucketId);

                LOG.debug("Scanning bucket {} with snapshotId {}", bucketId, snapshotId);

                BatchScanner scanner = table.newScan().createBatchScanner(tableBucket, snapshotId);

                try {
                    while (true) {
                        CloseableIterator<InternalRow> iterator =
                                scanner.pollBatch(Duration.ofMillis(100));
                        if (iterator == null) {
                            break;
                        }

                        try {
                            while (iterator.hasNext()) {
                                InternalRow row = iterator.next();
                                GenericRow keyRow = extractKeyFromRow(row);
                                byte[] rowKey = rowKeyEncoder.encodeRowKey(keyRow);

                                if (startRow != null
                                        && startRow.length > 0
                                        && org.apache.hadoop.hbase.util.Bytes.compareTo(
                                                        rowKey, startRow)
                                                < 0) {
                                    continue;
                                }

                                if (stopRow != null
                                        && stopRow.length > 0
                                        && org.apache.hadoop.hbase.util.Bytes.compareTo(
                                                        rowKey, stopRow)
                                                >= 0) {
                                    continue;
                                }

                                List<Cell> cells =
                                        cellConverter.rowToCells(
                                                rowKey, row, System.currentTimeMillis());
                                Result result = Result.create(cells);

                                if (filter != null) {
                                    try {
                                        if (filter.filterRowKey(
                                                org.apache.hadoop.hbase.KeyValueUtil
                                                        .createFirstOnRow(rowKey))) {
                                            continue;
                                        }
                                    } catch (Exception e) {
                                        LOG.warn("Filter failed", e);
                                    }
                                }

                                allResults.add(result);
                            }
                        } finally {
                            iterator.close();
                        }
                    }
                } finally {
                    scanner.close();
                }
            }

            ScanSession session = new ScanSession(scannerId, allResults, scan.getCaching());
            activeSessions.put(scannerId, session);

            ClientProtos.ScanResponse response =
                    ClientProtos.ScanResponse.newBuilder()
                            .setScannerId(scannerId)
                            .setMoreResults(true)
                            .build();

            LOG.info("Scan opened: id={}, rows={}", scannerId, allResults.size());
            return CompletableFuture.completedFuture(HBaseRpcResponse.success(callId, response));

        } catch (Exception e) {
            LOG.error("Scan open failed", e);
            return CompletableFuture.completedFuture(HBaseRpcResponse.failure(callId, e));
        }
    }

    private CompletableFuture<HBaseRpcResponse> handleScanNext(
            int callId, long scannerId, int numberOfRows) {
        ScanSession session = activeSessions.get(scannerId);
        if (session == null) {
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(
                            callId, new IllegalStateException("Scanner not found: " + scannerId)));
        }

        try {
            int batchSize = numberOfRows > 0 ? numberOfRows : session.caching;
            List<Result> results = new ArrayList<>();

            int remaining = session.allResults.size() - session.currentIndex;
            int toReturn = Math.min(batchSize, remaining);

            for (int i = 0; i < toReturn; i++) {
                results.add(session.allResults.get(session.currentIndex++));
            }

            boolean moreResults = session.currentIndex < session.allResults.size();

            ClientProtos.ScanResponse.Builder responseBuilder =
                    ClientProtos.ScanResponse.newBuilder()
                            .setScannerId(scannerId)
                            .setMoreResults(moreResults);

            for (Result result : results) {
                ClientProtos.Result protoResult =
                        org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil.toResult(result);
                responseBuilder.addResults(protoResult);
            }

            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.success(callId, responseBuilder.build()));

        } catch (Exception e) {
            LOG.error("Scan next failed", e);
            return CompletableFuture.completedFuture(HBaseRpcResponse.failure(callId, e));
        }
    }

    private CompletableFuture<HBaseRpcResponse> handleScanClose(int callId, long scannerId) {
        ScanSession session = activeSessions.remove(scannerId);
        if (session != null) {
            session.close();
            LOG.info("Scan closed: id={}", scannerId);
        }

        ClientProtos.ScanResponse response =
                ClientProtos.ScanResponse.newBuilder()
                        .setScannerId(scannerId)
                        .setMoreResults(false)
                        .build();

        return CompletableFuture.completedFuture(HBaseRpcResponse.success(callId, response));
    }

    private GenericRow extractKeyFromRow(InternalRow row) {
        GenericRow keyRow = new GenericRow(row.getFieldCount());
        for (int i = 0; i < row.getFieldCount(); i++) {
            Object fieldValue = row.isNullAt(i) ? null : fieldGetters[i].getFieldOrNull(row);
            keyRow.setField(i, fieldValue);
        }
        return keyRow;
    }

    private static class ScanSession {
        final long scannerId;
        final List<Result> allResults;
        final int caching;
        int currentIndex = 0;

        ScanSession(long scannerId, List<Result> allResults, int caching) {
            this.scannerId = scannerId;
            this.allResults = allResults;
            this.caching = caching > 0 ? caching : 100;
        }

        void close() {
            allResults.clear();
        }
    }
}
