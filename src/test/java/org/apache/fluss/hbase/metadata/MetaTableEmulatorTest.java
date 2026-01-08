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

package org.apache.fluss.hbase.metadata;

import org.apache.fluss.hbase.protocol.HBaseRpcRequest;
import org.apache.fluss.hbase.protocol.HBaseRpcResponse;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CellProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class MetaTableEmulatorTest {

    private static final String TEST_HOSTNAME = "localhost";
    private static final int TEST_PORT = 16020;
    private static final TableName TEST_TABLE = TableName.valueOf("test_table");
    private static final byte[] META_FAMILY = Bytes.toBytes("info");

    private VirtualRegionManager regionManager;
    private MetaTableEmulator metaEmulator;

    @BeforeEach
    void setup() {
        regionManager = new VirtualRegionManager(TEST_HOSTNAME, TEST_PORT);
        regionManager.registerTable(TEST_TABLE, 3);
        metaEmulator = new MetaTableEmulator(regionManager);
    }

    @Test
    void testGetMetaRequest() throws Exception {
        byte[] metaRowKey = buildMetaRowKey(TEST_TABLE.getNameAsString(), new byte[0], 0);

        ClientProtos.Get get =
                ClientProtos.Get.newBuilder().setRow(ByteString.copyFrom(metaRowKey)).build();

        ClientProtos.GetRequest getRequest =
                ClientProtos.GetRequest.newBuilder()
                        .setRegion(buildRegionSpecifier())
                        .setGet(get)
                        .build();

        HBaseRpcRequest request = new HBaseRpcRequest(1, "Get", getRequest.toByteArray(), null);

        CompletableFuture<HBaseRpcResponse> future = metaEmulator.execute(request);
        HBaseRpcResponse response = future.get();

        assertThat(response.isSuccess()).isTrue();
        assertThat(response.getCallId()).isEqualTo(1);

        ClientProtos.GetResponse getResponse =
                ClientProtos.GetResponse.parseFrom(
                        ((Message) response.getResponseMessage()).toByteArray());
        assertThat(getResponse.hasResult()).isTrue();

        ClientProtos.Result protoResult = getResponse.getResult();
        assertThat(protoResult.getCellCount()).isGreaterThan(0);
    }

    @Test
    void testGetMetaRequestForNonExistentTable() throws Exception {
        TableName nonExistent = TableName.valueOf("nonexistent");
        byte[] metaRowKey = buildMetaRowKey(nonExistent.getNameAsString(), new byte[0], 0);

        ClientProtos.Get get =
                ClientProtos.Get.newBuilder().setRow(ByteString.copyFrom(metaRowKey)).build();

        ClientProtos.GetRequest getRequest =
                ClientProtos.GetRequest.newBuilder()
                        .setRegion(buildRegionSpecifier())
                        .setGet(get)
                        .build();

        HBaseRpcRequest request = new HBaseRpcRequest(2, "Get", getRequest.toByteArray(), null);

        CompletableFuture<HBaseRpcResponse> future = metaEmulator.execute(request);
        HBaseRpcResponse response = future.get();

        assertThat(response.isSuccess()).isTrue();

        ClientProtos.GetResponse getResponse =
                ClientProtos.GetResponse.parseFrom(
                        ((Message) response.getResponseMessage()).toByteArray());
        assertThat(getResponse.hasResult()).isFalse();
    }

    @Test
    void testOpenScanner() throws Exception {
        ClientProtos.Scan scan =
                ClientProtos.Scan.newBuilder()
                        .setStartRow(ByteString.copyFrom(new byte[0]))
                        .build();

        ClientProtos.ScanRequest scanRequest =
                ClientProtos.ScanRequest.newBuilder()
                        .setRegion(buildRegionSpecifier())
                        .setScan(scan)
                        .build();

        HBaseRpcRequest request = new HBaseRpcRequest(3, "Scan", scanRequest.toByteArray(), null);

        CompletableFuture<HBaseRpcResponse> future = metaEmulator.execute(request);
        HBaseRpcResponse response = future.get();

        assertThat(response.isSuccess()).isTrue();

        ClientProtos.ScanResponse scanResponse =
                ClientProtos.ScanResponse.parseFrom(
                        ((Message) response.getResponseMessage()).toByteArray());
        assertThat(scanResponse.hasScannerId()).isTrue();
        assertThat(scanResponse.getScannerId()).isGreaterThan(0);
    }

    @Test
    void testNextScanner() throws Exception {
        long scannerId = 12345L;

        ClientProtos.ScanRequest scanRequest =
                ClientProtos.ScanRequest.newBuilder()
                        .setScannerId(scannerId)
                        .setNumberOfRows(10)
                        .build();

        HBaseRpcRequest request = new HBaseRpcRequest(4, "Scan", scanRequest.toByteArray(), null);

        CompletableFuture<HBaseRpcResponse> future = metaEmulator.execute(request);
        HBaseRpcResponse response = future.get();

        assertThat(response.isSuccess()).isTrue();

        ClientProtos.ScanResponse scanResponse =
                ClientProtos.ScanResponse.parseFrom(
                        ((Message) response.getResponseMessage()).toByteArray());
        assertThat(scanResponse.hasMoreResults()).isTrue();
        assertThat(scanResponse.getMoreResults()).isFalse();
    }

    @Test
    void testCloseScanner() throws Exception {
        long scannerId = 12345L;

        ClientProtos.ScanRequest scanRequest =
                ClientProtos.ScanRequest.newBuilder()
                        .setScannerId(scannerId)
                        .setCloseScanner(true)
                        .build();

        HBaseRpcRequest request = new HBaseRpcRequest(5, "Scan", scanRequest.toByteArray(), null);

        CompletableFuture<HBaseRpcResponse> future = metaEmulator.execute(request);
        HBaseRpcResponse response = future.get();

        assertThat(response.isSuccess()).isTrue();

        ClientProtos.ScanResponse scanResponse =
                ClientProtos.ScanResponse.parseFrom(
                        ((Message) response.getResponseMessage()).toByteArray());
        assertThat(scanResponse).isNotNull();
    }

    @Test
    void testUnsupportedOperation() throws Exception {
        HBaseRpcRequest request = new HBaseRpcRequest(6, "Put", new byte[0], null);

        CompletableFuture<HBaseRpcResponse> future = metaEmulator.execute(request);
        HBaseRpcResponse response = future.get();

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getException()).isNotNull();
    }

    @Test
    void testMetaResultContainsRegionInfo() throws Exception {
        byte[] metaRowKey = buildMetaRowKey(TEST_TABLE.getNameAsString(), new byte[0], 0);

        ClientProtos.Get get =
                ClientProtos.Get.newBuilder().setRow(ByteString.copyFrom(metaRowKey)).build();

        ClientProtos.GetRequest getRequest =
                ClientProtos.GetRequest.newBuilder()
                        .setRegion(buildRegionSpecifier())
                        .setGet(get)
                        .build();

        HBaseRpcRequest request = new HBaseRpcRequest(7, "Get", getRequest.toByteArray(), null);

        CompletableFuture<HBaseRpcResponse> future = metaEmulator.execute(request);
        HBaseRpcResponse response = future.get();

        ClientProtos.GetResponse getResponse =
                ClientProtos.GetResponse.parseFrom(
                        ((Message) response.getResponseMessage()).toByteArray());
        ClientProtos.Result protoResult = getResponse.getResult();

        boolean hasRegionInfo = false;
        boolean hasServer = false;
        boolean hasSeqNum = false;

        for (CellProtos.Cell cell : protoResult.getCellList()) {
            byte[] family = cell.getFamily().toByteArray();
            byte[] qualifier = cell.getQualifier().toByteArray();

            if (Bytes.equals(family, META_FAMILY)) {
                if (Bytes.equals(qualifier, Bytes.toBytes("regioninfo"))) {
                    hasRegionInfo = true;
                } else if (Bytes.equals(qualifier, Bytes.toBytes("server"))) {
                    hasServer = true;
                    String serverAddress = Bytes.toString(cell.getValue().toByteArray());
                    assertThat(serverAddress).contains(TEST_HOSTNAME);
                } else if (Bytes.equals(qualifier, Bytes.toBytes("seqnumDuringOpen"))) {
                    hasSeqNum = true;
                }
            }
        }

        assertThat(hasRegionInfo).isTrue();
        assertThat(hasServer).isTrue();
        assertThat(hasSeqNum).isTrue();
    }

    @Test
    void testMetaRowKeyFormat() throws Exception {
        byte[] metaRowKey = buildMetaRowKey(TEST_TABLE.getNameAsString(), new byte[0], 12345);
        String rowKeyStr = Bytes.toString(metaRowKey);

        assertThat(rowKeyStr).startsWith(TEST_TABLE.getNameAsString());
        assertThat(rowKeyStr).contains(",");
        assertThat(rowKeyStr).contains("0000000000012345");
    }

    @Test
    void testMetaRowKeyWithStartKey() throws Exception {
        byte[] startKey = Bytes.toBytes("row100");
        byte[] metaRowKey = buildMetaRowKey(TEST_TABLE.getNameAsString(), startKey, 99);
        String rowKeyStr = Bytes.toString(metaRowKey);

        assertThat(rowKeyStr).startsWith(TEST_TABLE.getNameAsString());
        assertThat(rowKeyStr).contains("row100");
        assertThat(rowKeyStr).contains("0000000000000099");
    }

    private byte[] buildMetaRowKey(String tableName, byte[] startKey, long regionId) {
        StringBuilder sb = new StringBuilder();
        sb.append(tableName);
        sb.append(',');

        if (startKey.length > 0) {
            sb.append(Bytes.toStringBinary(startKey));
        }

        sb.append(',');
        sb.append(String.format("%016d", regionId));
        sb.append('.');
        sb.append((char) HConstants.META_ROW_DELIMITER);

        return Bytes.toBytes(sb.toString());
    }

    private HBaseProtos.RegionSpecifier buildRegionSpecifier() {
        return HBaseProtos.RegionSpecifier.newBuilder()
                .setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME)
                .setValue(ByteString.copyFrom(Bytes.toBytes("hbase:meta,,1")))
                .build();
    }
}
