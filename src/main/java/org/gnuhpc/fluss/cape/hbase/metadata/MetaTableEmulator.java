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

package org.gnuhpc.fluss.cape.hbase.metadata;

import org.gnuhpc.fluss.cape.hbase.executor.HBaseOperationExecutor;
import org.gnuhpc.fluss.cape.hbase.protocol.CellBlockBuilder;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcRequest;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcResponse;

import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CellProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MetaTableEmulator implements HBaseOperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(MetaTableEmulator.class);

    private static final byte[] META_FAMILY = Bytes.toBytes("info");
    private static final byte[] REGIONINFO_QUALIFIER = Bytes.toBytes("regioninfo");
    private static final byte[] SERVER_QUALIFIER = Bytes.toBytes("server");
    private static final byte[] SERVERSTARTCODE_QUALIFIER = Bytes.toBytes("serverstartcode");
    private static final byte[] SERVERNAME_QUALIFIER = Bytes.toBytes("sn");
    private static final byte[] SEQNUM_QUALIFIER = Bytes.toBytes("seqnumDuringOpen");
    private static final int MAX_META_SCAN_RESULTS = 10_000;

    private final VirtualRegionManager regionManager;
    private final TableStateManager stateManager;

    public MetaTableEmulator(VirtualRegionManager regionManager, TableStateManager stateManager) {
        this.regionManager = regionManager;
        this.stateManager = stateManager;
    }

    @Override
    public CompletableFuture<HBaseRpcResponse> execute(HBaseRpcRequest request) {
        try {
            String methodName = request.getMethodName();

            if ("Get".equals(methodName)) {
                return executeGet(request);
            } else if ("Scan".equals(methodName)) {
                return executeScan(request);
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported hbase:meta operation: " + methodName);
            }

        } catch (Exception e) {
            LOG.error("Failed to execute meta table request", e);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(request.getCallId(), e));
        }
    }

    private CompletableFuture<HBaseRpcResponse> executeGet(HBaseRpcRequest request) {
        try {
            byte[] requestBytes = (byte[]) request.getRequestParam();
            ClientProtos.GetRequest getRequest = ClientProtos.GetRequest.parseFrom(requestBytes);
            ClientProtos.Get get = getRequest.getGet();

            byte[] rowKey = get.getRow().toByteArray();
            String rowKeyStr = Bytes.toString(rowKey);
            LOG.info("Meta Get: rowKey={}", rowKeyStr);

            boolean isTableStateQuery = false;
            if (get.getColumnCount() > 0) {
                for (ClientProtos.Column column : get.getColumnList()) {
                    byte[] family = column.getFamily().toByteArray();
                    if (Bytes.equals(family, Bytes.toBytes("table"))) {
                        isTableStateQuery = true;
                        LOG.info("Meta Get: detected table:state query");
                        break;
                    }
                }
            }

            TableName tableName;
            Result hbaseResult;

            if (isTableStateQuery) {
                tableName = TableName.valueOf(rowKeyStr);
                LOG.info("Meta Get: table state query for table={}", tableName);
                hbaseResult = buildTableStateResult(tableName, rowKey);
            } else {
                tableName = extractTableNameFromMetaRow(rowKey);
                LOG.info("Meta Get: region query, extracted tableName={}", tableName);

                List<VirtualRegionManager.VirtualRegion> regions =
                        regionManager.getRegions(tableName);
                LOG.info("Meta Get: found {} regions for table {}", regions.size(), tableName);

                if (regions.isEmpty()) {
                    hbaseResult = Result.EMPTY_RESULT;
                    LOG.info("Meta Get: returning EMPTY_RESULT");
                } else {
                    VirtualRegionManager.VirtualRegion region = regions.get(0);
                    hbaseResult = buildMetaRowResult(region);
                    LOG.info(
                            "Meta Get: returning region info for {}",
                            region.getRegionInfo().getRegionNameAsString());
                }
            }

            ClientProtos.GetResponse.Builder responseBuilder =
                    ClientProtos.GetResponse.newBuilder();
            responseBuilder.setResult(convertResultToProto(hbaseResult));

            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.success(request.getCallId(), responseBuilder.build()));

        } catch (Exception e) {
            LOG.error("Failed to execute meta Get request", e);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(request.getCallId(), e));
        }
    }

    private CompletableFuture<HBaseRpcResponse> executeScan(HBaseRpcRequest request) {
        try {
            byte[] requestBytes = (byte[]) request.getRequestParam();
            ClientProtos.ScanRequest scanRequest = ClientProtos.ScanRequest.parseFrom(requestBytes);

            LOG.debug(
                    "Meta Scan: hasScannerId={}, scannerId={}, hasRegion={}, hasCloseScanner={}, numberOfRows={}",
                    scanRequest.hasScannerId(),
                    scanRequest.hasScannerId() ? scanRequest.getScannerId() : "N/A",
                    scanRequest.hasRegion(),
                    scanRequest.hasCloseScanner(),
                    scanRequest.hasNumberOfRows() ? scanRequest.getNumberOfRows() : "N/A");

            // Small scan: hasCloseScanner=true AND scannerId=0 means one-shot scan
            boolean hasCloseScanner =
                    scanRequest.hasCloseScanner() && scanRequest.getCloseScanner();
            boolean noScannerId = !scanRequest.hasScannerId() || scanRequest.getScannerId() == 0;
            boolean isSmallScan = hasCloseScanner && noScannerId;

            LOG.debug(
                    "Scan decision: hasCloseScanner={}, noScannerId={}, isSmallScan={}",
                    hasCloseScanner,
                    noScannerId,
                    isSmallScan);

            if (isSmallScan) {
                LOG.debug("Executing small scan (one-shot scan with immediate close)");
                return executeSmallScan(request, scanRequest);
            } else if (scanRequest.hasCloseScanner() && scanRequest.getCloseScanner()) {
                LOG.debug("Closing scanner: {}", scanRequest.getScannerId());
                return executeCloseScanner(request);
            } else if (!scanRequest.hasScannerId() || scanRequest.getScannerId() == 0) {
                LOG.debug("Opening new scanner");
                return executeOpenScanner(request, scanRequest);
            } else {
                LOG.debug("Fetching next batch for scanner: {}", scanRequest.getScannerId());
                return executeNextScanner(request, scanRequest);
            }

        } catch (Exception e) {
            LOG.error("Failed to execute meta Scan request", e);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(request.getCallId(), e));
        }
    }

    private CompletableFuture<HBaseRpcResponse> executeOpenScanner(
            HBaseRpcRequest request, ClientProtos.ScanRequest scanRequest) {
        try {
            long scannerId = System.currentTimeMillis();

            byte[] scanStartRow = null;
            byte[] scanStopRow = null;
            boolean reversed = false;
            int numberOfRows = Integer.MAX_VALUE;

            if (scanRequest.hasScan()) {
                scanStartRow = scanRequest.getScan().getStartRow().toByteArray();
                scanStopRow = scanRequest.getScan().getStopRow().toByteArray();
                reversed = scanRequest.getScan().getReversed();
                if (scanRequest.getScan().hasCaching()) {
                    numberOfRows = scanRequest.getScan().getCaching();
                }
                LOG.debug(
                        "Scan request - startRow: [{}], stopRow: [{}], reversed: {}, caching: {}",
                        Bytes.toStringBinary(scanStartRow),
                        Bytes.toStringBinary(scanStopRow),
                        reversed,
                        numberOfRows);
            }

            if (scanRequest.hasNumberOfRows()) {
                numberOfRows = Math.min(numberOfRows, scanRequest.getNumberOfRows());
                LOG.debug("ScanRequest has numberOfRows: {}", scanRequest.getNumberOfRows());
            }

            LOG.debug(
                    "Opening meta scanner with ID: {}, will return results in CellBlock",
                    scannerId);

            List<Result> hbaseResults = new ArrayList<>();
            int totalResults = 0;
            for (List<VirtualRegionManager.VirtualRegion> regions : regionManager.getAllRegions()) {
                for (VirtualRegionManager.VirtualRegion region : regions) {
                    Result hbaseResult = buildMetaRowResult(region);
                    if (!hbaseResult.isEmpty()) {
                        byte[] metaRowKey = hbaseResult.getRow();

                        boolean include = true;

                        if (scanStartRow != null && scanStartRow.length > 0) {
                            int cmp = Bytes.compareTo(metaRowKey, scanStartRow);
                            if (reversed) {
                                if (cmp > 0) {
                                    include = false;
                                }
                            } else {
                                if (cmp < 0) {
                                    include = false;
                                }
                            }
                        }

                        if (include && scanStopRow != null && scanStopRow.length > 0) {
                            byte[] tablePrefix = extractTablePrefixFromHBaseMetaStopRow(scanStopRow);

                            if (tablePrefix != null) {
                                include = Bytes.startsWith(metaRowKey, tablePrefix);
                            } else {
                                int cmp = Bytes.compareTo(metaRowKey, scanStopRow);
                                if (reversed) {
                                    include = (cmp > 0);
                                } else {
                                    include = (cmp < 0);
                                }
                            }
                        }

                        if (include) {
                            LOG.debug("Including region: {}", Bytes.toStringBinary(metaRowKey));
                            hbaseResults.add(hbaseResult);
                            totalResults++;
                            if (totalResults > MAX_META_SCAN_RESULTS) {
                                throw new IllegalStateException(
                                        "Meta scan result limit exceeded: " + MAX_META_SCAN_RESULTS);
                            }
                        }
                    }
                }
            }

            if (reversed) {
                LOG.debug("Reversing {} results for reversed scan", hbaseResults.size());                java.util.Collections.reverse(hbaseResults);
            }

            if (hbaseResults.size() > numberOfRows) {
                LOG.debug("Limiting results from {} to {}", hbaseResults.size(), numberOfRows);
                hbaseResults = new ArrayList<>(hbaseResults.subList(0, numberOfRows));
            }

            LOG.debug("Returning {} region entries with CellBlock encoding", hbaseResults.size());

            byte[] cellBlock = CellBlockBuilder.buildCellBlock(hbaseResults);

            ClientProtos.ScanResponse.Builder responseBuilder =
                    ClientProtos.ScanResponse.newBuilder()
                            .setScannerId(scannerId)
                            .setMoreResults(false)
                            .setMoreResultsInRegion(false)
                            .setStale(false);

            for (Result result : hbaseResults) {
                responseBuilder.addCellsPerResult(result.size());
            }

            ClientProtos.ScanResponse response = responseBuilder.build();

            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.success(request.getCallId(), response, cellBlock));

        } catch (Exception e) {
            LOG.error("Failed to open scanner", e);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(request.getCallId(), e));
        }
    }

    private CompletableFuture<HBaseRpcResponse> executeSmallScan(
            HBaseRpcRequest request, ClientProtos.ScanRequest scanRequest) {
        try {
            LOG.debug("Executing small scan (one-shot) for meta table with CellBlock");
            List<Result> hbaseResults = new ArrayList<>();
            int totalResults = 0;

            for (List<VirtualRegionManager.VirtualRegion> regions : regionManager.getAllRegions()) {
                for (VirtualRegionManager.VirtualRegion region : regions) {
                    Result hbaseResult = buildMetaRowResult(region);
                    if (!hbaseResult.isEmpty()) {
                        hbaseResults.add(hbaseResult);
                        totalResults++;
                        if (totalResults > MAX_META_SCAN_RESULTS) {
                            throw new IllegalStateException(
                                    "Meta scan result limit exceeded: " + MAX_META_SCAN_RESULTS);
                        }
                    }
                }
            }

            LOG.debug("Returning {} region entries from small scan", hbaseResults.size());
            byte[] cellBlock = CellBlockBuilder.buildCellBlock(hbaseResults);

            ClientProtos.ScanResponse.Builder responseBuilder =
                    ClientProtos.ScanResponse.newBuilder()
                            .setMoreResults(false)
                            .setMoreResultsInRegion(false)
                            .setStale(false);

            for (Result result : hbaseResults) {
                responseBuilder.addCellsPerResult(result.size());
            }

            ClientProtos.ScanResponse response = responseBuilder.build();

            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.success(request.getCallId(), response, cellBlock));

        } catch (Exception e) {
            LOG.error("Failed to execute small scan", e);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(request.getCallId(), e));
        }
    }

    private CompletableFuture<HBaseRpcResponse> executeNextScanner(
            HBaseRpcRequest request, ClientProtos.ScanRequest scanRequest) {
        try {
            LOG.debug("Executing next on scanner ID: {}", scanRequest.getScannerId());
            List<Result> hbaseResults = new ArrayList<>();
            int totalResults = 0;

            for (List<VirtualRegionManager.VirtualRegion> regions : regionManager.getAllRegions()) {
                for (VirtualRegionManager.VirtualRegion region : regions) {
                    Result hbaseResult = buildMetaRowResult(region);
                    if (!hbaseResult.isEmpty()) {
                        hbaseResults.add(hbaseResult);
                        totalResults++;
                        if (totalResults > MAX_META_SCAN_RESULTS) {
                            throw new IllegalStateException(
                                    "Meta scan result limit exceeded: " + MAX_META_SCAN_RESULTS);
                        }
                    }
                }
            }

            LOG.debug("Returning {} region entries from meta scan", hbaseResults.size());

            byte[] cellBlock = CellBlockBuilder.buildCellBlock(hbaseResults);

            ClientProtos.ScanResponse response =
                    ClientProtos.ScanResponse.newBuilder().setMoreResults(false).build();

            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.success(request.getCallId(), response, cellBlock));

        } catch (Exception e) {
            LOG.error("Failed to execute next scanner", e);
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(request.getCallId(), e));
        }
    }

    private CompletableFuture<HBaseRpcResponse> executeCloseScanner(HBaseRpcRequest request) {
        try {
            ClientProtos.ScanResponse response = ClientProtos.ScanResponse.newBuilder().build();
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.success(request.getCallId(), response));
        } catch (Exception e) {
            return CompletableFuture.completedFuture(
                    HBaseRpcResponse.failure(request.getCallId(), e));
        }
    }

    private TableName extractTableNameFromMetaRow(byte[] rowKey) {
        String rowKeyStr = Bytes.toString(rowKey);
        int commaIdx = rowKeyStr.indexOf(',');
        if (commaIdx > 0) {
            String tableNameStr = rowKeyStr.substring(0, commaIdx);
            return TableName.valueOf(tableNameStr);
        }
        return TableName.valueOf(rowKeyStr);
    }

    private Result buildTableStateResult(TableName tableName, byte[] rowKey) {
        try {
            List<VirtualRegionManager.VirtualRegion> regions = regionManager.getRegions(tableName);
            if (regions.isEmpty()) {
                LOG.info("Table {} has no regions, returning EMPTY_RESULT", tableName);
                return Result.EMPTY_RESULT;
            }

            org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.TableState.State state =
                    stateManager.isTableDisabled(tableName)
                            ? org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos
                                    .TableState.State.DISABLED
                            : org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos
                                    .TableState.State.ENABLED;

            org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.TableState
                    tableStateProto =
                            org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.TableState
                                    .newBuilder()
                                    .setState(state)
                                    .build();
            byte[] stateBytes = tableStateProto.toByteArray();
            long timestamp = System.currentTimeMillis();

            List<org.apache.hadoop.hbase.Cell> cells = new ArrayList<>();
            cells.add(
                    CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
                            .setRow(rowKey)
                            .setFamily(Bytes.toBytes("table"))
                            .setQualifier(Bytes.toBytes("state"))
                            .setTimestamp(timestamp)
                            .setType(org.apache.hadoop.hbase.Cell.Type.Put)
                            .setValue(stateBytes)
                            .build());

            Result result = Result.create(cells);
            LOG.info("Built table state result for table {}: state={}", tableName, state);
            return result;

        } catch (Exception e) {
            LOG.error("Failed to build table state result", e);
            return Result.EMPTY_RESULT;
        }
    }

    private Result buildMetaRowResult(VirtualRegionManager.VirtualRegion region) {
        try {
            HRegionInfo regionInfo = region.getRegionInfo();
            byte[] regionInfoBytes = regionInfo.toByteArray();

            String serverAddress =
                    region.getServerName().getHostname() + ":" + region.getServerName().getPort();
            byte[] serverBytes = Bytes.toBytes(serverAddress);

            byte[] seqNumBytes = Bytes.toBytes(0L);
            byte[] serverStartCodeBytes = Bytes.toBytes(region.getServerName().getStartcode());

            byte[] metaRowKey =
                    buildMetaRowKey(
                            regionInfo.getTable(),
                            regionInfo.getStartKey(),
                            regionInfo.getRegionId());

            long timestamp = System.currentTimeMillis();

            LOG.info(
                    "Building region entry - table={}, startKey=[{}], endKey=[{}], regionId={}, server={}, metaRowKey=[{}]",
                    regionInfo.getTable().getNameAsString(),
                    Bytes.toStringBinary(regionInfo.getStartKey()),
                    Bytes.toStringBinary(regionInfo.getEndKey()),
                    regionInfo.getRegionId(),
                    serverAddress,
                    Bytes.toStringBinary(metaRowKey));

            KeyValue kvRegionInfo =
                    new KeyValue(
                            metaRowKey,
                            META_FAMILY,
                            REGIONINFO_QUALIFIER,
                            timestamp,
                            regionInfoBytes);
            KeyValue kvServer =
                    new KeyValue(metaRowKey, META_FAMILY, SERVER_QUALIFIER, timestamp, serverBytes);
            KeyValue kvServerStartCode =
                    new KeyValue(
                            metaRowKey,
                            META_FAMILY,
                            SERVERSTARTCODE_QUALIFIER,
                            timestamp,
                            serverStartCodeBytes);
            KeyValue kvServerName =
                    new KeyValue(
                            metaRowKey,
                            META_FAMILY,
                            SERVERNAME_QUALIFIER,
                            timestamp,
                            Bytes.toBytes(region.getServerName().getServerName()));
            KeyValue kvSeqNum =
                    new KeyValue(metaRowKey, META_FAMILY, SEQNUM_QUALIFIER, timestamp, seqNumBytes);

            List<org.apache.hadoop.hbase.Cell> cells = new ArrayList<>();
            cells.add(kvRegionInfo);
            cells.add(kvServer);
            cells.add(kvServerStartCode);
            cells.add(kvServerName);
            cells.add(kvSeqNum);

            return Result.create(cells.toArray(new org.apache.hadoop.hbase.Cell[0]));

        } catch (Exception e) {
            LOG.error("Failed to build meta row result", e);
            return Result.EMPTY_RESULT;
        }
    }

    private byte[] buildMetaRowKey(TableName tableName, byte[] startKey, long regionId) {
        StringBuilder sb = new StringBuilder();
        sb.append(tableName.getNameAsString());
        sb.append(',');

        if (startKey.length > 0) {
            sb.append(Bytes.toStringBinary(startKey));
        }

        sb.append(',');
        sb.append(String.format("%016d", regionId));
        sb.append('.');
        sb.append(HConstants.META_ROW_DELIMITER);

        return Bytes.toBytes(sb.toString());
    }

    private byte[] extractTablePrefixFromHBaseMetaStopRow(byte[] stopRow) {
        if (stopRow.length < 3) {
            return null;
        }

        for (int i = 0; i < stopRow.length - 2; i++) {
            boolean matchesPattern =
                    (stopRow[i] == ' ' && stopRow[i + 1] == ',' && stopRow[i + 2] == ',');

            if (matchesPattern) {
                byte[] prefix = new byte[i + 1];
                System.arraycopy(stopRow, 0, prefix, 0, i);
                prefix[i] = ',';
                LOG.info(
                        "Extracted table prefix from stopRow: {}",
                        Bytes.toStringBinary(prefix));
                return prefix;
            }
        }

        LOG.info("No table prefix pattern found in stopRow: {}", Bytes.toStringBinary(stopRow));
        return null;
    }

    private ClientProtos.Result convertResultToProto(Result result) {
        ClientProtos.Result.Builder resultBuilder = ClientProtos.Result.newBuilder();

        // Handle empty results - rawCells() can return null for EMPTY_RESULT
        if (result == null || result.isEmpty()) {
            return resultBuilder.build();
        }

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
}
