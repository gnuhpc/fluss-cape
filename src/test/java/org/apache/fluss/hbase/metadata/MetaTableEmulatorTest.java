package org.gnuhpc.fluss.cape.hbase.metadata;

import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcRequest;
import org.gnuhpc.fluss.cape.hbase.protocol.HBaseRpcResponse;

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
        metaEmulator = new MetaTableEmulator(regionManager, new TableStateManager());
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
        assertThat(protoResult.getCellCount()).isEqualTo(1);

        CellProtos.Cell cell = protoResult.getCell(0);
        assertThat(cell.getFamily()).isEqualTo(ByteString.copyFrom(META_FAMILY));
        assertThat(cell.getQualifier().toStringUtf8()).isEqualTo("regioninfo");
    }

    private byte[] buildMetaRowKey(String tableName, byte[] startKey, int regionId) {
        byte[] table = Bytes.toBytes(tableName);
        byte[] regionName =
                org.apache.hadoop.hbase.HRegionInfo.createRegionName(
                        TableName.valueOf(table), startKey, regionId, false);
        return Bytes.add(regionName, Bytes.toBytes(HConstants.LATEST_TIMESTAMP));
    }

    private HBaseProtos.RegionSpecifier buildRegionSpecifier() {
        return HBaseProtos.RegionSpecifier.newBuilder()
                .setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME)
                .setValue(ByteString.copyFrom(Bytes.toBytes("hbase:meta")))
                .build();
    }
}
