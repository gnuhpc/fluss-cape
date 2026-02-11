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

/**
 * STREAMING VERSION of ScanExecutor that fixes P0 memory leak issue.
 * 
 * Key changes:
 * 1. StreamingScanSession holds scanner iterators instead of buffering all results
 * 2. Results are materialized on-demand during handleScanNext calls
 * 3. Memory footprint is O(batch_size) instead of O(total_results)
 * 
 * Architecture:
 * - For snapshot scans: Iterate through buckets on-demand, close previous scanner before opening next
 * - For hybrid scans: Buffer only the merged snapshot Map (still memory-intensive but better than before),
 *   then stream through sorted results
 */
public class ScanExecutorStreaming implements HBaseOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ScanExecutorStreaming.class);
    private static final int MAX_SCAN_SESSIONS = 1000;
    private static final long SESSION_TIMEOUT_MS = 300_000; // 5 minutes
    
    // Memory protection limits
    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final int MAX_SNAPSHOT_BUFFER = 10_000;  // Max rows to buffer in hybrid scan
    private static final int WARN_THRESHOLD = 5_000;

    private final Connection flussConnection;
    private final TablePath tablePath;
    private final RowKeyEncoder rowKeyEncoder;
    private final CellConverter cellConverter;
    private final RowType rowType;
    private final InternalRow.FieldGetter[] fieldGetters;

    private final AtomicLong scannerIdGenerator = new AtomicLong(0);
    private final Map<Long, StreamingScanSession> activeSessions = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleanupExecutor;

    public ScanExecutorStreaming(
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
            Thread t = new Thread(r, "scan-session-cleanup-streaming-" + tablePath);
            t.setDaemon(true);
            return t;
        });
        
        this.cleanupExecutor.scheduleAtFixedRate(
            this::cleanupStaleSessions,
            60, 60, TimeUnit.SECONDS
        );
        
        LOG.info("StreamingScanExecutor initialized for table {} (MEMORY-OPTIMIZED VERSION)", tablePath);
    }

    @Override
    public CompletableFuture<HBaseRpcResponse> execute(HBaseRpcRequest request) {
        try {
            byte[] requestBytes = (byte[]) request.getRequestParam();
            ClientProtos.ScanRequest scanRequest = ClientProtos.ScanRequest.parseFrom(requestBytes);

            LOG.debug("Scan request for table {}: hasScannerId={}, scannerId={}, hasCloseScanner={}",
                    tablePath,
                    scanRequest.hasScannerId(),
                    scanRequest.hasScannerId() ? scanRequest.getScannerId() : "N/A",
                    scanRequest.hasCloseScanner());

            boolean hasCloseScanner = scanRequest.hasCloseScanner() && scanRequest.getCloseScanner();
            boolean noScannerId = !scanRequest.hasScannerId() || scanRequest.getScannerId() == 0;

            if (noScannerId) {
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
                return handleScanClose(request.getCallId(), scanRequest.getScannerId());
            }

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
                                        "Too many active scanners: " + activeSessions.size())));
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
            } catch (Exception e) {
                LOG.error("Failed to get KV snapshots for table {}", tablePath, e);
                return CompletableFuture.completedFuture(HBaseRpcResponse.failure(callId, e));
            }

            List<Integer> bucketIds = new ArrayList<>(kvSnapshots.getBucketIds());
            Collections.sort(bucketIds);

            boolean hasSnapshot = bucketIds.stream()
                    .anyMatch(bucketId -> kvSnapshots.getSnapshotId(bucketId).isPresent());

            Map<Integer, Long> endOffsets = new HashMap<>();
            if (hasSnapshot) {
                ListOffsetsResult latestOffsetsResult =
                        admin.listOffsets(tablePath, bucketIds, new OffsetSpec.LatestSpec());
                for (int bucketId : bucketIds) {
                    try {
                        long endOffset = latestOffsetsResult.bucketResult(bucketId).get();
                        endOffsets.put(bucketId, endOffset);
                    } catch (Exception e) {
                        LOG.warn("Failed to get end offset for bucket {}", bucketId, e);
                    }
                }
            }

            // Create streaming session - NO data buffering here!
            StreamingScanSession session = new StreamingScanSession(
                    scannerId,
                    scan.getCaching() > 0 ? scan.getCaching() : DEFAULT_BATCH_SIZE,
                    table,
                    startRow,
                    stopRow,
                    filter,
                    bucketIds,
                    tableId,
                    hasSnapshot,
                    kvSnapshots,
                    endOffsets,
                    rowKeyEncoder,
                    cellConverter,
                    rowType,
                    fieldGetters);

            activeSessions.put(scannerId, session);

            LOG.info("Scan opened (STREAMING): id={}, buckets={}, hasSnapshot={}", 
                scannerId, bucketIds.size(), hasSnapshot);

            // Return response with scannerId - client will call handleScanNext to get data
            ClientProtos.ScanResponse.Builder responseBuilder =
                    ClientProtos.ScanResponse.newBuilder()
                            .setScannerId(scannerId)
                            .setMoreResults(true); // Assume more results until we scan

            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.success(callId, responseBuilder.build()));

        } catch (Exception e) {
            LOG.error("Scan open failed", e);
            return CompletableFuture.completedFuture(HBaseRpcResponse.failure(callId, e));
        }
    }

    private CompletableFuture<HBaseRpcResponse> handleScanNext(
            int callId, long scannerId, int numberOfRows, long requestNextCallSeq) {
        StreamingScanSession session = activeSessions.get(scannerId);
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
            List<Result> results = session.fetchNextBatch(numberOfRows);
            boolean moreResults = session.hasMoreData();

            ClientProtos.ScanResponse.Builder responseBuilder =
                    ClientProtos.ScanResponse.newBuilder()
                            .setScannerId(scannerId)
                            .setMoreResults(moreResults);

            for (Result result : results) {
                ClientProtos.Result protoResult =
                        ProtobufUtil.toResult(result);
                responseBuilder.addResults(protoResult);
            }
            
            LOG.debug("Scan next (STREAMING): id={}, returned {} results, moreResults={}", 
                scannerId, results.size(), moreResults);

            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.success(callId, responseBuilder.build()));

        } catch (Exception e) {
            LOG.error("Scan next failed for scanner {}", scannerId, e);
            return CompletableFuture.completedFuture(HBaseRpcResponse.failure(callId, e));
        }
    }

    private CompletableFuture<HBaseRpcResponse> handleScanClose(int callId, long scannerId) {
        StreamingScanSession session = activeSessions.remove(scannerId);
        if (session != null) {
            session.close();
            LOG.info("Scan closed (STREAMING): id={}", scannerId);
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

    private void cleanupStaleSessions() {
        long now = System.currentTimeMillis();
        int initialSize = activeSessions.size();
        
        activeSessions.entrySet().removeIf(entry -> {
            StreamingScanSession session = entry.getValue();
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
        LOG.info("Shutting down StreamingScanExecutor for table {}", tablePath);
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        activeSessions.values().forEach(StreamingScanSession::close);
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

    /**
     * Streaming scan session that holds scanner iterators instead of buffering all results.
     * Memory footprint: O(batch_size) instead of O(total_results)
     */
    private class StreamingScanSession {
        final long scannerId;
        final int caching;
        final Table table;
        final byte[] startRow;
        final byte[] stopRow;
        final Filter filter;
        final List<Integer> bucketIds;
        final long tableId;
        final boolean hasSnapshot;
        final KvSnapshots kvSnapshots;
        final Map<Integer, Long> endOffsets;
        final RowKeyEncoder rowKeyEncoder;
        final CellConverter cellConverter;
        final RowType rowType;
        final InternalRow.FieldGetter[] fieldGetters;
        
        // Streaming state for simple scan (no snapshot)
        int currentBucketIndex = 0;
        BatchScanner currentBatchScanner = null;
        CloseableIterator<InternalRow> currentIterator = null;
        
        // For hybrid scan - we still buffer snapshot but apply streaming to results
        Map<ByteArrayWrapper, Result> snapshotMergeMap = null;
        List<Result> sortedSnapshotResults = null;
        int sortedResultsIndex = 0;
        boolean hybridScanInitialized = false;
        
        long lastAccessTime;
        long nextCallSeq = -1;
        int totalResultsFetched = 0;

        StreamingScanSession(
                long scannerId,
                int caching,
                Table table,
                byte[] startRow,
                byte[] stopRow,
                Filter filter,
                List<Integer> bucketIds,
                long tableId,
                boolean hasSnapshot,
                KvSnapshots kvSnapshots,
                Map<Integer, Long> endOffsets,
                RowKeyEncoder rowKeyEncoder,
                CellConverter cellConverter,
                RowType rowType,
                InternalRow.FieldGetter[] fieldGetters) {
            this.scannerId = scannerId;
            this.caching = caching;
            this.table = table;
            this.startRow = startRow;
            this.stopRow = stopRow;
            this.filter = filter;
            this.bucketIds = bucketIds;
            this.tableId = tableId;
            this.hasSnapshot = hasSnapshot;
            this.kvSnapshots = kvSnapshots;
            this.endOffsets = endOffsets;
            this.rowKeyEncoder = rowKeyEncoder;
            this.cellConverter = cellConverter;
            this.rowType = rowType;
            this.fieldGetters = fieldGetters;
            this.lastAccessTime = System.currentTimeMillis();
        }
        
        void updateAccessTime() {
            this.lastAccessTime = System.currentTimeMillis();
        }

        /**
         * Fetch next batch of results on-demand.
         * This is where the streaming magic happens - no pre-buffering!
         */
        List<Result> fetchNextBatch(int requestedRows) throws Exception {
            if (hasSnapshot) {
                return fetchNextBatchHybrid(requestedRows);
            } else {
                return fetchNextBatchSimple(requestedRows);
            }
        }

        /**
         * Simple scan without snapshots - purely streaming through buckets
         */
        private List<Result> fetchNextBatchSimple(int requestedRows) throws Exception {
            List<Result> batch = new ArrayList<>();
            
            while (batch.size() < requestedRows && currentBucketIndex < bucketIds.size()) {
                // Lazy open scanner for current bucket
                if (currentBatchScanner == null) {
                    int bucketId = bucketIds.get(currentBucketIndex);
                    TableBucket tableBucket = new TableBucket(tableId, bucketId);
                    currentBatchScanner = table.newScan().limit(Integer.MAX_VALUE)
                            .createBatchScanner(tableBucket);
                    LOG.debug("Scanner {}: Opened BatchScanner for bucket {}", scannerId, bucketId);
                }
                
                // Fetch rows from current bucket
                while (batch.size() < requestedRows) {
                    if (currentIterator == null || !currentIterator.hasNext()) {
                        if (currentIterator != null) {
                            currentIterator.close();
                            currentIterator = null;
                        }
                        
                        CloseableIterator<InternalRow> nextBatch = 
                                currentBatchScanner.pollBatch(Duration.ofMillis(100));
                        if (nextBatch == null) {
                            // No more data in this bucket, move to next
                            currentBatchScanner.close();
                            currentBatchScanner = null;
                            currentBucketIndex++;
                            break;
                        }
                        currentIterator = nextBatch;
                    }
                    
                    if (currentIterator.hasNext()) {
                        InternalRow row = currentIterator.next();
                        Result result = buildResultFromRow(row);
                        if (result != null && !result.isEmpty()) {
                            byte[] rowKey = result.getRow();
                            
                            if (matchesRowRange(rowKey) && matchesFilter(rowKey)) {
                                batch.add(result);
                                totalResultsFetched++;
                            }
                        }
                    }
                }
            }
            
            LOG.debug("Scanner {}: Fetched {} results (simple scan)", scannerId, batch.size());
            return batch;
        }

        /**
         * Hybrid scan with snapshots - we buffer snapshot merge map once,
         * then stream through sorted results
         */
        private List<Result> fetchNextBatchHybrid(int requestedRows) throws Exception {
            // Lazy initialization of hybrid scan
            if (!hybridScanInitialized) {
                initializeHybridScan();
                hybridScanInitialized = true;
            }
            
            // Stream through sorted results
            List<Result> batch = new ArrayList<>();
            while (batch.size() < requestedRows && sortedResultsIndex < sortedSnapshotResults.size()) {
                batch.add(sortedSnapshotResults.get(sortedResultsIndex++));
            }
            
            LOG.debug("Scanner {}: Fetched {} results from hybrid scan (index {}/{})", 
                scannerId, batch.size(), sortedResultsIndex, sortedSnapshotResults.size());
            return batch;
        }

        /**
         * Initialize hybrid scan: load snapshot + replay log, then sort
         * This still buffers results but it's unavoidable for consistency
         */
        private void initializeHybridScan() throws Exception {
            LOG.info("Scanner {}: Initializing hybrid scan...", scannerId);
            snapshotMergeMap = new HashMap<>();
            int snapshotRowCount = 0;
            
            // Phase 1: Load snapshot data
            for (int bucketId : bucketIds) {
                OptionalLong snapshotId = kvSnapshots.getSnapshotId(bucketId);
                if (!snapshotId.isPresent()) {
                    continue;
                }
                
                TableBucket tableBucket = new TableBucket(tableId, bucketId);
                try (BatchScanner scanner = table.newScan()
                        .createBatchScanner(tableBucket, snapshotId.getAsLong())) {
                    
                    while (true) {
                        CloseableIterator<InternalRow> iterator = scanner.pollBatch(Duration.ofMillis(100));
                        if (iterator == null) {
                            break;
                        }
                        
                        try {
                            while (iterator.hasNext()) {
                                InternalRow row = iterator.next();
                                Result result = buildResultFromRow(row);
                                if (result != null && !result.isEmpty()) {
                                    byte[] rowKey = result.getRow();
                                    if (matchesRowRange(rowKey) && matchesFilter(rowKey)) {
                                        snapshotMergeMap.put(new ByteArrayWrapper(rowKey), result);
                                        snapshotRowCount++;
                                        
                                        if (snapshotRowCount >= MAX_SNAPSHOT_BUFFER) {
                                            LOG.error("Scanner {}: Snapshot buffer limit reached: {}", 
                                                scannerId, MAX_SNAPSHOT_BUFFER);
                                            throw new IllegalStateException(
                                                "Scan snapshot too large: " + MAX_SNAPSHOT_BUFFER);
                                        }
                                    }
                                }
                            }
                        } finally {
                            iterator.close();
                        }
                    }
                }
            }
            
            LOG.info("Scanner {}: Loaded {} snapshot rows", scannerId, snapshotRowCount);
            
            // Phase 2: Replay log
            replayLog();
            
            // Phase 3: Sort results
            sortedSnapshotResults = new ArrayList<>(snapshotMergeMap.values());
            sortedSnapshotResults.sort(Comparator.comparing(Result::getRow, Arrays::compare));
            
            // Free the merge map to save memory
            snapshotMergeMap.clear();
            snapshotMergeMap = null;
            
            LOG.info("Scanner {}: Hybrid scan initialized with {} final results", 
                scannerId, sortedSnapshotResults.size());
        }

        private void replayLog() throws Exception {
            Map<Integer, OptionalLong> snapshotOffsets = new HashMap<>();
            for (int bucketId : bucketIds) {
                snapshotOffsets.put(bucketId, kvSnapshots.getLogOffset(bucketId));
            }
            
            try (LogScanner logScanner = table.newScan().createLogScanner()) {
                int subscribedBuckets = 0;
                for (int bucketId : bucketIds) {
                    OptionalLong snapshotOffset = snapshotOffsets.get(bucketId);
                    if (snapshotOffset != null && snapshotOffset.isPresent()) {
                        logScanner.subscribe(bucketId, snapshotOffset.getAsLong());
                        subscribedBuckets++;
                    }
                }
                
                LOG.info("Scanner {}: Subscribed {} buckets to LogScanner", scannerId, subscribedBuckets);
                
                if (!endOffsets.isEmpty()) {
                    long lastActivity = System.currentTimeMillis();
                    int totalLogRecords = 0;
                    
                    while (true) {
                        ScanRecords records = logScanner.poll(Duration.ofMillis(200));
                        
                        if (records == null || records.isEmpty()) {
                            if (System.currentTimeMillis() - lastActivity > 1000) {
                                break;
                            }
                            continue;
                        }
                        lastActivity = System.currentTimeMillis();
                        
                        for (TableBucket bucket : records.buckets()) {
                            Long endOffset = endOffsets.get(bucket.getBucket());
                            if (endOffset == null) {
                                continue;
                            }
                            for (ScanRecord record : records.records(bucket)) {
                                if (record.logOffset() >= endOffset) {
                                    continue;
                                }
                                applyLogRecord(record);
                                totalLogRecords++;
                            }
                        }
                        
                        boolean done = true;
                        for (int bucketId : endOffsets.keySet()) {
                            if (!hasReachedEnd(records, bucketId, endOffsets.get(bucketId))) {
                                done = false;
                                break;
                            }
                        }
                        if (done) {
                            break;
                        }
                    }
                    
                    LOG.info("Scanner {}: Replayed {} log records", scannerId, totalLogRecords);
                }
            }
        }

        private void applyLogRecord(ScanRecord record) {
            InternalRow row = record.getRow();
            if (row == null) {
                return;
            }
            Result result = buildResultFromRow(row);
            byte[] rowKey = result.getRow();
            if (record.getChangeType() == ChangeType.DELETE
                    || record.getChangeType() == ChangeType.UPDATE_BEFORE) {
                snapshotMergeMap.remove(new ByteArrayWrapper(rowKey));
            } else {
                snapshotMergeMap.put(new ByteArrayWrapper(rowKey), result);
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

        private boolean matchesRowRange(byte[] rowKey) {
            if (startRow != null && startRow.length > 0
                    && org.apache.hadoop.hbase.util.Bytes.compareTo(rowKey, startRow) < 0) {
                return false;
            }
            if (stopRow != null && stopRow.length > 0
                    && org.apache.hadoop.hbase.util.Bytes.compareTo(rowKey, stopRow) >= 0) {
                return false;
            }
            return true;
        }

        private boolean matchesFilter(byte[] rowKey) {
            if (filter != null) {
                try {
                    return !filter.filterRowKey(
                            org.apache.hadoop.hbase.KeyValueUtil.createFirstOnRow(rowKey));
                } catch (Exception e) {
                    LOG.warn("Filter failed for scanner {}", scannerId, e);
                    return false;
                }
            }
            return true;
        }

        boolean hasMoreData() {
            if (hasSnapshot) {
                return hybridScanInitialized && sortedResultsIndex < sortedSnapshotResults.size();
            } else {
                return currentBucketIndex < bucketIds.size() || 
                       (currentIterator != null && currentIterator.hasNext());
            }
        }

        void close() {
            try {
                if (currentIterator != null) {
                    currentIterator.close();
                    currentIterator = null;
                }
            } catch (Exception e) {
                LOG.warn("Error closing iterator for scanner {}", scannerId, e);
            }
            
            try {
                if (currentBatchScanner != null) {
                    currentBatchScanner.close();
                    currentBatchScanner = null;
                }
            } catch (Exception e) {
                LOG.warn("Error closing batch scanner for scanner {}", scannerId, e);
            }
            
            if (snapshotMergeMap != null) {
                snapshotMergeMap.clear();
                snapshotMergeMap = null;
            }
            
            if (sortedSnapshotResults != null) {
                sortedSnapshotResults.clear();
                sortedSnapshotResults = null;
            }
            
            LOG.debug("Scanner {} closed, total results fetched: {}", scannerId, totalResultsFetched);
        }
    }
}
