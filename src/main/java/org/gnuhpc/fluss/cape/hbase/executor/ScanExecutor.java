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
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.record.ChangeType;
import org.gnuhpc.fluss.cape.hbase.mapping.CellConverter;
import org.gnuhpc.fluss.cape.hbase.mapping.DynamicTableCodec;
import org.gnuhpc.fluss.cape.hbase.mapping.RowKeyEncoder;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcRequest;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcResponse;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Executor for HBase Scan operations. */
public class ScanExecutor implements HBaseOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ScanExecutor.class);
    private static final int MAX_SCAN_SESSIONS = 1000;
    private static final long SESSION_TIMEOUT_MS = 300_000; // 5 minutes
    
    // Memory protection limits
    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final int MAX_SCAN_RESULTS = 10_000;  // Reduced from 100k to prevent OOM
    private static final int WARN_THRESHOLD = 5_000;      // Warn when approaching limit

    private final Connection flussConnection;
    private final TablePath tablePath;
    private final RowKeyEncoder rowKeyEncoder;
    private final CellConverter cellConverter;
    private final RowType rowType;
    private final InternalRow.FieldGetter[] fieldGetters;

    private final AtomicLong scannerIdGenerator = new AtomicLong(0);
    private final Map<Long, ScanSession> activeSessions = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleanupExecutor;

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
        
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "scan-session-cleanup-" + tablePath);
            t.setDaemon(true);
            return t;
        });
        
        this.cleanupExecutor.scheduleAtFixedRate(
            this::cleanupStaleSessions,
            60, 60, TimeUnit.SECONDS
        );
        
        LOG.info("ScanExecutor initialized for table {} with config: " +
                "maxSessions={}, sessionTimeout={}ms, maxScanResults={}, defaultBatchSize={}, warnThreshold={}",
                tablePath, MAX_SCAN_SESSIONS, SESSION_TIMEOUT_MS, 
                MAX_SCAN_RESULTS, DEFAULT_BATCH_SIZE, WARN_THRESHOLD);
    }

    @Override
    public CompletableFuture<HBaseRpcResponse> execute(HBaseRpcRequest request) {
        try {
            byte[] requestBytes = (byte[]) request.getRequestParam();
            ClientProtos.ScanRequest scanRequest = ClientProtos.ScanRequest.parseFrom(requestBytes);

            LOG.info(
                    "Scan request for table {}: hasScannerId={}, scannerId={}, hasCloseScanner={}, numberOfRows={}",
                    tablePath,
                    scanRequest.hasScannerId(),
                    scanRequest.hasScannerId() ? scanRequest.getScannerId() : "N/A",
                    scanRequest.hasCloseScanner(),
                    scanRequest.hasNumberOfRows() ? scanRequest.getNumberOfRows() : "N/A");

            // Determine which operation to perform
            boolean hasCloseScanner = scanRequest.hasCloseScanner() && scanRequest.getCloseScanner();
            boolean noScannerId = !scanRequest.hasScannerId() || scanRequest.getScannerId() == 0;

            if (noScannerId) {
                // Open new scanner
                LOG.info("Opening new scanner for table {}", tablePath);
                if (!scanRequest.hasScan()) {
                    return CompletableFuture.completedFuture(
                            HBaseRpcResponse.failure(
                                    request.getCallId(),
                                    new IllegalArgumentException("Scan request missing scan details")));
                }
                return handleScanOpen(request.getCallId(), scanRequest.getScan())
                        .thenCompose(openResponse -> {
                            if (!hasCloseScanner || !openResponse.isSuccess()) {
                                return CompletableFuture.completedFuture(openResponse);
                            }
                            ClientProtos.ScanResponse openScanResponse =
                                    (ClientProtos.ScanResponse) openResponse.getResponseMessage();
                            long scannerId = openScanResponse.getScannerId();
                            int numberOfRows = scanRequest.hasNumberOfRows()
                                    ? scanRequest.getNumberOfRows()
                                    : DEFAULT_BATCH_SIZE;
                                return handleScanNext(
                                                request.getCallId(),
                                                scannerId,
                                                numberOfRows,
                                                scanRequest.hasNextCallSeq()
                                                        ? scanRequest.getNextCallSeq()
                                                        : -1)
                                        .thenCompose(nextResponse -> {
                                            handleScanClose(request.getCallId(), scannerId);
                                            return CompletableFuture.completedFuture(nextResponse);
                                        });
                        });
            }

            if (hasCloseScanner) {
                // Close existing scanner
                LOG.info("Closing scanner: {}", scanRequest.getScannerId());
                return handleScanClose(request.getCallId(), scanRequest.getScannerId());
            }

            // Fetch next batch from existing scanner
            LOG.info("Fetching next batch for scanner: {}", scanRequest.getScannerId());
            int numberOfRows = scanRequest.hasNumberOfRows()
                    ? scanRequest.getNumberOfRows()
                    : DEFAULT_BATCH_SIZE;
                return handleScanNext(
                        request.getCallId(),
                        scanRequest.getScannerId(),
                        numberOfRows,
                        scanRequest.hasNextCallSeq() ? scanRequest.getNextCallSeq() : -1);


        } catch (Exception e) {
            LOG.error("Failed to execute HBase Scan request for table {}", tablePath, e);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(request.getCallId(), e));
        }
    }

    private CompletableFuture<HBaseRpcResponse> handleScanOpen(int callId, ClientProtos.Scan scan) {
        try {
            if (activeSessions.size() >= MAX_SCAN_SESSIONS) {
                LOG.warn("Maximum scan sessions limit reached: {}", MAX_SCAN_SESSIONS);
                return CompletableFuture.completedFuture(
                        HBaseRpcResponse.failure(
                                callId,
                                new IllegalStateException(
                                        "Too many active scanners: " + activeSessions.size() +
                                        " (max: " + MAX_SCAN_SESSIONS + ")")));
            }
            
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

            Admin admin = flussConnection.getAdmin();
            KvSnapshots kvSnapshots;
            try {
                kvSnapshots = admin.getLatestKvSnapshots(tablePath).get();
                LOG.debug(
                        "Retrieved KV snapshots for table {}: {} buckets with snapshots",
                        tablePath,
                        kvSnapshots.getBucketIds().size());
            } catch (java.util.concurrent.ExecutionException e) {
                LOG.error("Failed to get latest KV snapshots for table {} (execution error)", tablePath, e);
                return CompletableFuture.completedFuture(HBaseRpcResponse.failure(callId, e));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("Interrupted while getting KV snapshots for table {}", tablePath, e);
                return CompletableFuture.completedFuture(HBaseRpcResponse.failure(callId, e));
            }

            List<Integer> bucketIds = new ArrayList<>(kvSnapshots.getBucketIds());
            Collections.sort(bucketIds);

            boolean hasSnapshot = false;
            for (int bucketId : bucketIds) {
                if (kvSnapshots.getSnapshotId(bucketId).isPresent()) {
                    hasSnapshot = true;
                    break;
                }
            }

            LOG.info("Scan opened for table {}: {} buckets, hasSnapshot={}", 
                tablePath, bucketIds.size(), hasSnapshot);

            List<Result> allResults = new ArrayList<>();
            int totalResults = 0;

            if (hasSnapshot) {
                Map<ByteArrayWrapper, Result> mergedResults = new HashMap<>();
                Map<Integer, Long> endOffsets = new HashMap<>();
                Map<Integer, OptionalLong> snapshotOffsets = new HashMap<>();

                ListOffsetsResult latestOffsetsResult =
                        admin.listOffsets(tablePath, bucketIds, new OffsetSpec.LatestSpec());

                int totalSnapshotRows = 0;
                for (int bucketId : bucketIds) {
                    OptionalLong snapshotId = kvSnapshots.getSnapshotId(bucketId);
                    OptionalLong snapshotOffset = kvSnapshots.getLogOffset(bucketId);
                    snapshotOffsets.put(bucketId, snapshotOffset);
                    
                    LOG.info("Bucket {}: snapshotId={}, snapshotOffset={}", 
                        bucketId, 
                        snapshotId.isPresent() ? snapshotId.getAsLong() : "NONE",
                        snapshotOffset.isPresent() ? snapshotOffset.getAsLong() : "NONE");
                    
                    if (!snapshotId.isPresent()) {
                        continue;
                    }

                    long endOffset = latestOffsetsResult.bucketResult(bucketId).get();
                    endOffsets.put(bucketId, endOffset);
                    LOG.info("Bucket {}: endOffset={}", bucketId, endOffset);

                    TableBucket tableBucket = new TableBucket(tableId, bucketId);
                    LOG.info("Scanning bucket {} with snapshotId {}", bucketId, snapshotId.getAsLong());

                    BatchScanner scanner =
                            table.newScan().createBatchScanner(tableBucket, snapshotId.getAsLong());

                    int bucketRowCount = 0;
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
                                    Result result = buildResultFromRow(row);
                                    if (result == null || result.isEmpty()) {
                                        continue;
                                    }
                                    byte[] rowKey = result.getRow();

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

                                    mergedResults.put(new ByteArrayWrapper(rowKey), result);
                                    bucketRowCount++;
                                    totalResults++;
                                    
                                    if (totalResults == WARN_THRESHOLD) {
                                        LOG.warn("Scan approaching limit for table {}: {} results (limit: {})",
                                                tablePath, totalResults, MAX_SCAN_RESULTS);
                                    }
                                    
                                    if (totalResults >= MAX_SCAN_RESULTS) {
                                        LOG.error("Scan result limit reached for table {}: {} results. " +
                                                "Results will be truncated. Consider using filters or smaller scan ranges.",
                                                tablePath, MAX_SCAN_RESULTS);
                                        throw new IllegalStateException(
                                                "Scan result limit exceeded: " + MAX_SCAN_RESULTS +
                                                ". Use filters or smaller ranges to reduce result set.");
                                    }
                                }
                            } finally {
                                iterator.close();
                            }
                        }
                    } finally {
                        scanner.close();
                    }
                    
                    LOG.info("Bucket {}: scanned {} rows from snapshot", bucketId, bucketRowCount);
                    totalSnapshotRows += bucketRowCount;
                }
                
                LOG.info("Total snapshot rows before log replay: {}", totalSnapshotRows);

                LogScanner logScanner = table.newScan().createLogScanner();
                try {
                    int subscribedBuckets = 0;
                    for (int bucketId : bucketIds) {
                        OptionalLong snapshotOffset = snapshotOffsets.get(bucketId);
                        if (snapshotOffset == null || !snapshotOffset.isPresent()) {
                            LOG.warn("Bucket {}: No snapshot offset, skipping log replay", bucketId);
                            continue;
                        }
                        logScanner.subscribe(bucketId, snapshotOffset.getAsLong());
                        subscribedBuckets++;
                        LOG.info("Bucket {}: Subscribed to LogScanner from offset {}", 
                            bucketId, snapshotOffset.getAsLong());
                    }
                    
                    LOG.info("LogScanner: Subscribed {} buckets, endOffsets.size={}", 
                        subscribedBuckets, endOffsets.size());

                    boolean hasAnyOffsets = !endOffsets.isEmpty();
                    if (hasAnyOffsets) {
                        long lastActivity = System.currentTimeMillis();
                        int pollAttempts = 0;
                        int totalLogRecords = 0;
                        while (true) {
                            ScanRecords records = logScanner.poll(Duration.ofMillis(200));
                            pollAttempts++;
                            
                            if (records == null || records.isEmpty()) {
                                if (System.currentTimeMillis() - lastActivity > 1000) {
                                    LOG.info("LogScanner: Exiting after {} poll attempts, {} log records processed", 
                                        pollAttempts, totalLogRecords);
                                    break;
                                }
                                continue;
                            }
                            lastActivity = System.currentTimeMillis();
                            
                            int recordsInBatch = 0;
                            for (TableBucket bucket : records.buckets()) {
                                Long endOffset = endOffsets.get(bucket.getBucket());
                                if (endOffset == null) {
                                    continue;
                                }
                                for (ScanRecord record : records.records(bucket)) {
                                    if (record.logOffset() >= endOffset) {
                                        continue;
                                    }
                                    applyLogRecord(mergedResults, record);
                                    recordsInBatch++;
                                    totalLogRecords++;
                                }
                            }
                            
                            if (recordsInBatch > 0) {
                                LOG.info("LogScanner: Processed {} records in this batch", recordsInBatch);
                            }

                            boolean done = true;
                            for (int bucketId : endOffsets.keySet()) {
                                if (!hasReachedEnd(records, bucketId, endOffsets.get(bucketId))) {
                                    done = false;
                                    break;
                                }
                            }
                            if (done) {
                                LOG.info("LogScanner: Reached end offsets for all buckets");
                                break;
                            }
                        }
                    } else {
                        LOG.warn("LogScanner: No endOffsets available, skipping log replay");
                    }
                } finally {
                    logScanner.close();
                }
                
                List<Result> mergedList = new ArrayList<>(mergedResults.values());
                if (mergedList.size() > 10000) {
                    LOG.warn("Large result set detected ({} rows), using incremental sort to reduce memory pressure", 
                        mergedList.size());
                    mergedList.sort(Comparator.comparing(Result::getRow, Arrays::compare));
                    allResults.addAll(mergedList);
                } else {
                    allResults.addAll(mergedList);
                    allResults.sort(Comparator.comparing(Result::getRow, Arrays::compare));
                }
                
                LOG.info("Scan complete: {} results after merge and sort", allResults.size());
            } else {
                LOG.warn("No snapshots available for table {}. Falling back to limit scan.", tablePath);
                for (int bucketId : bucketIds) {
                    TableBucket tableBucket = new TableBucket(tableId, bucketId);
                    BatchScanner scanner =
                            table.newScan().limit(Integer.MAX_VALUE).createBatchScanner(tableBucket);
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
                                    Result result = buildResultFromRow(row);
                                    if (result == null || result.isEmpty()) {
                                        continue;
                                    }
                                    byte[] rowKey = result.getRow();

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
                                    totalResults++;
                                    
                                    if (totalResults == WARN_THRESHOLD) {
                                        LOG.warn("Scan approaching limit for table {}: {} results (limit: {})",
                                                tablePath, totalResults, MAX_SCAN_RESULTS);
                                    }
                                    
                                    if (totalResults >= MAX_SCAN_RESULTS) {
                                        LOG.error("Scan result limit reached for table {}: {} results. " +
                                                "Results will be truncated. Consider using filters or smaller scan ranges.",
                                                tablePath, MAX_SCAN_RESULTS);
                                        throw new IllegalStateException(
                                                "Scan result limit exceeded: " + MAX_SCAN_RESULTS +
                                                ". Use filters or smaller ranges to reduce result set.");
                                    }
                                }
                            } finally {
                                iterator.close();
                            }
                        }
                    } finally {
                        scanner.close();
                    }
                }
            }

            ScanSession session = new ScanSession(scannerId, allResults, scan.getCaching());
            activeSessions.put(scannerId, session);

            // Return first batch of results immediately in the open response
            int caching = scan.getCaching() > 0 ? scan.getCaching() : DEFAULT_BATCH_SIZE;
            List<Result> firstBatch = new ArrayList<>();
            int toReturn = Math.min(caching, allResults.size());
            
            for (int i = 0; i < toReturn; i++) {
                firstBatch.add(allResults.get(i));
            }
            session.currentIndex = toReturn;
            
            boolean moreResults = toReturn < allResults.size();

            ClientProtos.ScanResponse.Builder responseBuilder =
                    ClientProtos.ScanResponse.newBuilder()
                            .setScannerId(scannerId)
                            .setMoreResults(moreResults);

            for (Result result : firstBatch) {
                ClientProtos.Result protoResult =
                        org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil.toResult(result);
                responseBuilder.addResults(protoResult);
            }

            LOG.info("Scan opened: id={}, totalRows={}, returningInResponse={}, moreResults={}", 
                scannerId, allResults.size(), firstBatch.size(), moreResults);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.success(callId, responseBuilder.build()));

        } catch (Exception e) {
            LOG.error("Scan open failed", e);
            return CompletableFuture.completedFuture(HBaseRpcResponse.failure(callId, e));
        }
    }

    private CompletableFuture<HBaseRpcResponse> handleScanNext(
            int callId, long scannerId, int numberOfRows, long requestNextCallSeq) {
        ScanSession session = activeSessions.get(scannerId);
        if (session == null) {
            LOG.warn("Scanner not found: id={}", scannerId);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(
                            callId, new IllegalStateException("Scanner not found: " + scannerId)));
        }

        if (session.nextCallSeq >= 0 && requestNextCallSeq >= 0
                && requestNextCallSeq != session.nextCallSeq) {
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(
                            callId,
                            new IllegalStateException(
                                    "Out of order scan request: expected nextCallSeq="
                                            + session.nextCallSeq
                                            + ", got="
                                            + requestNextCallSeq)));
        }

        session.updateAccessTime();
        if (requestNextCallSeq >= 0) {
            session.nextCallSeq = requestNextCallSeq + 1;
        }

        try {
            int batchSize = numberOfRows > 0 ? numberOfRows : session.caching;
            List<Result> results = new ArrayList<>();

            int remaining = session.allResults.size() - session.currentIndex;
            int toReturn = Math.min(batchSize, remaining);
            
            LOG.info("Scan next: id={}, totalResults={}, currentIndex={}, remaining={}, batchSize={}, toReturn={}", 
                scannerId, session.allResults.size(), session.currentIndex, remaining, batchSize, toReturn);

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
            
            LOG.info("Scan next response: id={}, returning {} results, moreResults={}", 
                scannerId, results.size(), moreResults);

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

    private Result buildResultFromRow(InternalRow row) {
        GenericRow keyRow = extractKeyFromRow(row);
        byte[] rowKey = rowKeyEncoder.encodeRowKey(keyRow);
        List<Cell> cells;
        if (isDynamicTable()) {
            cells = new ArrayList<>();
            for (int i = 1; i < rowType.getFieldCount(); i++) {
                if (!row.isNullAt(i)) {
                    String cfName = rowType.getFields().get(i).getName();
                    byte[] encodedData = row.getBytes(i);
                    List<Cell> cfCells = DynamicTableCodec.decodeColumnFamily(
                            cfName, rowKey, encodedData, System.currentTimeMillis());
                    cells.addAll(cfCells);
                }
            }
        } else {
            cells = cellConverter.rowToCells(rowKey, row, System.currentTimeMillis());
        }
        return cells.isEmpty() ? Result.EMPTY_RESULT : Result.create(cells);
    }

    private void applyLogRecord(Map<ByteArrayWrapper, Result> mergedResults, ScanRecord record) {
        InternalRow row = record.getRow();
        if (row == null) {
            return;
        }
        Result result = buildResultFromRow(row);
        byte[] rowKey = result.getRow();
        if (record.getChangeType() == ChangeType.DELETE
                || record.getChangeType() == ChangeType.UPDATE_BEFORE) {
            mergedResults.remove(new ByteArrayWrapper(rowKey));
        } else {
            mergedResults.put(new ByteArrayWrapper(rowKey), result);
        }
    }

    private boolean hasReachedEnd(ScanRecords records, int bucketId, long endOffset) {
        for (TableBucket bucket : records.buckets()) {
            if (bucket.getBucket() != bucketId) {
                continue;
            }
            List<ScanRecord> bucketRecords = records.records(bucket);
            if (!bucketRecords.isEmpty()) {
                long lastOffset = bucketRecords.get(bucketRecords.size() - 1).logOffset();
                return lastOffset >= endOffset;
            }
        }
        return false;
    }

    private void cleanupStaleSessions() {
        long now = System.currentTimeMillis();
        int initialSize = activeSessions.size();
        
        activeSessions.entrySet().removeIf(entry -> {
            ScanSession session = entry.getValue();
            if ((now - session.lastAccessTime) > SESSION_TIMEOUT_MS) {
                LOG.info("Cleaning up stale scanner session: scannerId={}, age={}ms",
                        entry.getKey(), now - session.lastAccessTime);
                session.close();
                return true;
            }
            return false;
        });
        
        int removedCount = initialSize - activeSessions.size();
        if (removedCount > 0) {
            LOG.info("Cleaned up {} stale scanner sessions, active sessions: {}",
                    removedCount, activeSessions.size());
        }
    }

    public void close() {
        LOG.info("Shutting down ScanExecutor for table {}", tablePath);
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // Close all active sessions
        activeSessions.values().forEach(ScanSession::close);
        activeSessions.clear();
    }

    private static class ByteArrayWrapper {
        private final byte[] data;

        private ByteArrayWrapper(byte[] data) {
            this.data = data == null ? new byte[0] : Arrays.copyOf(data, data.length);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof ByteArrayWrapper)) {
                return false;
            }
            ByteArrayWrapper other = (ByteArrayWrapper) obj;
            return Arrays.equals(data, other.data);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
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

    private static class ScanSession {
        final long scannerId;
        final List<Result> allResults;
        final int caching;
        int currentIndex = 0;
        long lastAccessTime;

        long nextCallSeq = -1;

        ScanSession(long scannerId, List<Result> allResults, int caching) {
            this.scannerId = scannerId;
            this.allResults = allResults;
            this.caching = caching > 0 ? caching : DEFAULT_BATCH_SIZE;
            this.lastAccessTime = System.currentTimeMillis();
        }
        
        void updateAccessTime() {
            this.lastAccessTime = System.currentTimeMillis();
        }

        void close() {
            allResults.clear();
        }
    }
}
