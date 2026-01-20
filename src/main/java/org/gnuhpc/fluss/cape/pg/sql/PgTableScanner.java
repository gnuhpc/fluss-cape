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

package org.gnuhpc.fluss.cape.pg.sql;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PgTableScanner {

    private static final Logger LOG = LoggerFactory.getLogger(PgTableScanner.class);

    private PgTableScanner() {
    }

    public static List<InternalRow> scanTable(Connection connection, Table table, 
            List<String> projectedColumns, int limit) throws Exception {
        TableInfo tableInfo = table.getTableInfo();
        TablePath tablePath = tableInfo.getTablePath();
        long tableId = tableInfo.getTableId();
        int numBuckets = tableInfo.getNumBuckets();
        
        Admin admin = connection.getAdmin();
        KvSnapshots kvSnapshots = admin.getLatestKvSnapshots(tablePath).get();
        
        List<Integer> bucketIds = new ArrayList<>();
        for (int i = 0; i < numBuckets; i++) {
            bucketIds.add(i);
        }
        Collections.sort(bucketIds);
        
        boolean hasSnapshot = false;
        for (int bucketId : bucketIds) {
            if (kvSnapshots.getSnapshotId(bucketId).isPresent()) {
                hasSnapshot = true;
                break;
            }
        }
        
        LOG.info("PgTableScanner for {}: {} buckets, hasSnapshot={}", 
            tablePath, bucketIds.size(), hasSnapshot);
        
        List<InternalRow> allRows;
        
        if (hasSnapshot) {
            allRows = scanWithHybridStrategy(connection, table, kvSnapshots, bucketIds, tableId, 
                projectedColumns, limit, admin, tablePath);
        } else {
            allRows = scanWithLimitStrategy(table, bucketIds, tableId, 
                projectedColumns, limit);
        }
        
        LOG.info("PgTableScanner complete: {} rows returned", allRows.size());
        return allRows;
    }
    
    private static List<InternalRow> scanWithHybridStrategy(
            Connection connection, Table table, KvSnapshots kvSnapshots, List<Integer> bucketIds,
            long tableId, List<String> projectedColumns, int limit,
            Admin admin, TablePath tablePath) throws Exception {
        
        TableInfo tableInfo = table.getTableInfo();
        RowType rowType = tableInfo.getRowType();
        List<String> primaryKeys = tableInfo.getPrimaryKeys();
        Map<ByteArrayWrapper, InternalRow> mergedResults = new HashMap<>();
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
            
            BatchScanner scanner;
            if (projectedColumns == null || projectedColumns.isEmpty()) {
                scanner = table.newScan().createBatchScanner(tableBucket, snapshotId.getAsLong());
            } else {
                scanner = table.newScan().project(projectedColumns)
                    .createBatchScanner(tableBucket, snapshotId.getAsLong());
            }
            
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
                            byte[] rowKey = extractRowKey(row, rowType, primaryKeys);
                            mergedResults.put(new ByteArrayWrapper(rowKey), row);
                            bucketRowCount++;
                            
                            if (limit > 0 && mergedResults.size() >= limit) {
                                return new ArrayList<>(mergedResults.values());
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
        
        LogScanner logScanner;
        if (projectedColumns == null || projectedColumns.isEmpty()) {
            logScanner = table.newScan().createLogScanner();
        } else {
            logScanner = table.newScan().project(projectedColumns).createLogScanner();
        }
        
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
            
            if (!endOffsets.isEmpty()) {
                Map<Integer, Long> currentOffsets = new HashMap<>();
                for (int bucketId : endOffsets.keySet()) {
                    OptionalLong snapshotOffset = snapshotOffsets.get(bucketId);
                    currentOffsets.put(bucketId, snapshotOffset.isPresent() ? snapshotOffset.getAsLong() : 0L);
                }
                
                int pollAttempts = 0;
                int totalLogRecords = 0;
                int consecutiveEmptyPolls = 0;
                final int MAX_EMPTY_POLLS = 10;
                
                while (true) {
                    ScanRecords records = logScanner.poll(Duration.ofMillis(200));
                    pollAttempts++;
                    
                    if (records == null || records.isEmpty()) {
                        consecutiveEmptyPolls++;
                        
                        boolean allBucketsReachedEnd = true;
                        for (Map.Entry<Integer, Long> entry : endOffsets.entrySet()) {
                            int bucketId = entry.getKey();
                            long targetEnd = entry.getValue();
                            long currentEnd = currentOffsets.getOrDefault(bucketId, 0L);
                            
                            if (currentEnd < targetEnd - 1) {
                                allBucketsReachedEnd = false;
                                break;
                            }
                        }
                        
                        if (allBucketsReachedEnd || consecutiveEmptyPolls >= MAX_EMPTY_POLLS) {
                            LOG.info("LogScanner: Exiting - allReachedEnd={}, emptyPolls={}/{}, totalRecords={}", 
                                allBucketsReachedEnd, consecutiveEmptyPolls, MAX_EMPTY_POLLS, totalLogRecords);
                            break;
                        }
                        continue;
                    }
                    
                    consecutiveEmptyPolls = 0;
                    
                    int recordsInBatch = 0;
                    for (TableBucket bucket : records.buckets()) {
                        int bucketId = bucket.getBucket();
                        Long endOffset = endOffsets.get(bucketId);
                        if (endOffset == null) {
                            continue;
                        }
                        
                        for (ScanRecord record : records.records(bucket)) {
                            long recordOffset = record.logOffset();
                            
                            currentOffsets.put(bucketId, Math.max(
                                currentOffsets.getOrDefault(bucketId, 0L), 
                                recordOffset
                            ));
                            
                            if (recordOffset >= endOffset) {
                                LOG.debug("Bucket {}: Skipping offset {} >= endOffset {}", 
                                    bucketId, recordOffset, endOffset);
                                continue;
                            }
                            
                            InternalRow row = record.getRow();
                            byte[] rowKey = extractRowKey(row, rowType, primaryKeys);
                            
                            if (record.getChangeType() == ChangeType.DELETE
                                    || record.getChangeType() == ChangeType.UPDATE_BEFORE) {
                                mergedResults.remove(new ByteArrayWrapper(rowKey));
                            } else {
                                mergedResults.put(new ByteArrayWrapper(rowKey), row);
                            }
                            
                            recordsInBatch++;
                            totalLogRecords++;
                            
                            if (limit > 0 && mergedResults.size() >= limit) {
                                return new ArrayList<>(mergedResults.values());
                            }
                        }
                    }
                    
                    if (recordsInBatch > 0) {
                        LOG.info("LogScanner: Processed {} records, currentOffsets={}", 
                            recordsInBatch, currentOffsets);
                    }
                }
            } else {
                LOG.warn("LogScanner: No endOffsets available, skipping log replay");
            }
        } finally {
            logScanner.close();
        }
        
        return new ArrayList<>(mergedResults.values());
    }
    
    private static List<InternalRow> scanWithLimitStrategy(
            Table table, List<Integer> bucketIds, long tableId,
            List<String> projectedColumns, int limit) throws Exception {
        
        LOG.warn("No snapshots available. Falling back to limit scan.");
        
        List<InternalRow> allRows = new ArrayList<>();
        
        for (int bucketId : bucketIds) {
            TableBucket tableBucket = new TableBucket(tableId, bucketId);
            
            BatchScanner scanner;
            if (projectedColumns == null || projectedColumns.isEmpty()) {
                scanner = table.newScan().limit(Integer.MAX_VALUE).createBatchScanner(tableBucket);
            } else {
                scanner = table.newScan().project(projectedColumns)
                    .limit(Integer.MAX_VALUE).createBatchScanner(tableBucket);
            }
            
            try {
                while (true) {
                    CloseableIterator<InternalRow> iterator =
                            scanner.pollBatch(Duration.ofMillis(100));
                    if (iterator == null) {
                        break;
                    }
                    
                    try {
                        while (iterator.hasNext()) {
                            allRows.add(iterator.next());
                            if (limit > 0 && allRows.size() >= limit) {
                                return allRows;
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
        
        return allRows;
    }
    
    private static byte[] extractRowKey(InternalRow row, RowType rowType, List<String> primaryKeys) {
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            // 无 PK 表：使用全部字段（保持向后兼容）
            return extractRowKeyLegacy(row, rowType);
        }
        
        StringBuilder sb = new StringBuilder();
        InternalRow.FieldGetter[] getters = InternalRow.createFieldGetters(rowType);
        
        for (String pkColumn : primaryKeys) {
            int index = fieldIndex(rowType, pkColumn);
            if (row.isNullAt(index)) {
                sb.append("NULL");
            } else {
                Object field = getters[index].getFieldOrNull(row);
                // 使用类型感知的序列化
                sb.append(serializeField(field, rowType.getFields().get(index).getType()));
            }
            sb.append("|");
        }
        return sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
    
    // 保留旧逻辑作为无 PK 表的 fallback
    private static byte[] extractRowKeyLegacy(InternalRow row, RowType rowType) {
        StringBuilder sb = new StringBuilder();
        InternalRow.FieldGetter[] getters = InternalRow.createFieldGetters(rowType);
        for (int i = 0; i < row.getFieldCount(); i++) {
            if (row.isNullAt(i)) {
                sb.append("NULL");
            } else {
                Object field = getters[i].getFieldOrNull(row);
                sb.append(field != null ? field.toString() : "NULL");
            }
            sb.append("|");
        }
        return sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
    
    // 添加类型感知序列化
    private static String serializeField(Object value, org.apache.fluss.types.DataType dataType) {
        if (value == null) {
            return "NULL";
        }
        org.apache.fluss.types.DataTypeRoot root = dataType.getTypeRoot();
        switch (root) {
            case BYTES:
            case BINARY:
                return java.util.Base64.getEncoder().encodeToString((byte[]) value);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return String.valueOf(((java.time.LocalDateTime) value).toEpochSecond(java.time.ZoneOffset.UTC));
            case DATE:
                return String.valueOf(((java.time.LocalDate) value).toEpochDay());
            default:
                return value.toString();
        }
    }
    
    private static int fieldIndex(RowType rowType, String column) {
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (rowType.getFields().get(i).getName().equalsIgnoreCase(column)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Unknown column: " + column);
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
            if (obj == null || getClass() != obj.getClass()) {
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

    public static boolean isScanFeasible(TableInfo tableInfo) {
        return true;
    }
}
