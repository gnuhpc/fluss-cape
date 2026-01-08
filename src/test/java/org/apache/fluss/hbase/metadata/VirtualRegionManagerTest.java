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

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class VirtualRegionManagerTest {

    private static final String TEST_HOSTNAME = "localhost";
    private static final int TEST_PORT = 16020;
    private static final TableName TEST_TABLE = TableName.valueOf("test_table");

    private VirtualRegionManager regionManager;

    @BeforeEach
    void setup() {
        regionManager = new VirtualRegionManager(TEST_HOSTNAME, TEST_PORT);
    }

    @Test
    void testRegisterTable() {
        regionManager.registerTable(TEST_TABLE, 3);

        List<VirtualRegionManager.VirtualRegion> regions = regionManager.getRegions(TEST_TABLE);

        assertThat(regions).hasSize(3);
        assertThat(regions.get(0).getBucketId()).isEqualTo(0);
        assertThat(regions.get(1).getBucketId()).isEqualTo(1);
        assertThat(regions.get(2).getBucketId()).isEqualTo(2);
    }

    @Test
    void testGetRegionsForNonExistentTable() {
        List<VirtualRegionManager.VirtualRegion> regions =
                regionManager.getRegions(TableName.valueOf("nonexistent"));

        assertThat(regions).isEmpty();
    }

    @Test
    void testGetRegionForRow() {
        regionManager.registerTable(TEST_TABLE, 3);

        byte[] rowKey = Bytes.toBytes("test_key");
        VirtualRegionManager.VirtualRegion region =
                regionManager.getRegionForRow(TEST_TABLE, rowKey);

        assertThat(region).isNotNull();
        assertThat(region.getBucketId()).isBetween(0, 2);
    }

    @Test
    void testGetRegionForRowInFirstBucket() {
        regionManager.registerTable(TEST_TABLE, 3);

        byte[] rowKey = Bytes.toBytes("aaa");
        VirtualRegionManager.VirtualRegion region =
                regionManager.getRegionForRow(TEST_TABLE, rowKey);

        assertThat(region).isNotNull();
        assertThat(region.getBucketId()).isEqualTo(0);
    }

    @Test
    void testGetRegionForRowInMiddleBucket() {
        regionManager.registerTable(TEST_TABLE, 3);

        byte[] rowKey = Bytes.toBytes("bucket_0001");
        VirtualRegionManager.VirtualRegion region =
                regionManager.getRegionForRow(TEST_TABLE, rowKey);

        assertThat(region).isNotNull();
        assertThat(region.getBucketId()).isEqualTo(1);
    }

    @Test
    void testGetRegionForRowInLastBucket() {
        regionManager.registerTable(TEST_TABLE, 3);

        byte[] rowKey = Bytes.toBytes("zzz");
        VirtualRegionManager.VirtualRegion region =
                regionManager.getRegionForRow(TEST_TABLE, rowKey);

        assertThat(region).isNotNull();
        assertThat(region.getBucketId()).isEqualTo(2);
    }

    @Test
    void testGetRegionForRowWithNonExistentTable() {
        VirtualRegionManager.VirtualRegion region =
                regionManager.getRegionForRow(
                        TableName.valueOf("nonexistent"), Bytes.toBytes("key"));

        assertThat(region).isNull();
    }

    @Test
    void testGetBucketForRow() {
        regionManager.registerTable(TEST_TABLE, 5);

        byte[] rowKey1 = Bytes.toBytes("aaa");
        int bucket1 = regionManager.getBucketForRow(TEST_TABLE, rowKey1);
        assertThat(bucket1).isEqualTo(0);

        byte[] rowKey2 = Bytes.toBytes("bucket_0002");
        int bucket2 = regionManager.getBucketForRow(TEST_TABLE, rowKey2);
        assertThat(bucket2).isEqualTo(2);

        byte[] rowKey3 = Bytes.toBytes("zzz");
        int bucket3 = regionManager.getBucketForRow(TEST_TABLE, rowKey3);
        assertThat(bucket3).isEqualTo(4);
    }

    @Test
    void testGetBucketForRowWithNonExistentTable() {
        int bucket =
                regionManager.getBucketForRow(
                        TableName.valueOf("nonexistent"), Bytes.toBytes("key"));

        assertThat(bucket).isEqualTo(0);
    }

    @Test
    void testRegionBoundaries() {
        regionManager.registerTable(TEST_TABLE, 3);

        List<VirtualRegionManager.VirtualRegion> regions = regionManager.getRegions(TEST_TABLE);

        VirtualRegionManager.VirtualRegion region0 = regions.get(0);
        HRegionInfo info0 = region0.getRegionInfo();
        assertThat(info0.getStartKey()).isEmpty();
        assertThat(Bytes.toString(info0.getEndKey())).isEqualTo("bucket_0001");

        VirtualRegionManager.VirtualRegion region1 = regions.get(1);
        HRegionInfo info1 = region1.getRegionInfo();
        assertThat(Bytes.toString(info1.getStartKey())).isEqualTo("bucket_0001");
        assertThat(Bytes.toString(info1.getEndKey())).isEqualTo("bucket_0002");

        VirtualRegionManager.VirtualRegion region2 = regions.get(2);
        HRegionInfo info2 = region2.getRegionInfo();
        assertThat(Bytes.toString(info2.getStartKey())).isEqualTo("bucket_0002");
        assertThat(info2.getEndKey()).isEmpty();
    }

    @Test
    void testRegionServerInfo() {
        regionManager.registerTable(TEST_TABLE, 2);

        List<VirtualRegionManager.VirtualRegion> regions = regionManager.getRegions(TEST_TABLE);

        for (VirtualRegionManager.VirtualRegion region : regions) {
            assertThat(region.getServerName().getHostname()).isEqualTo(TEST_HOSTNAME);
            assertThat(region.getServerName().getPort()).isEqualTo(TEST_PORT);
            assertThat(region.getRegionName()).isNotEmpty();
            assertThat(region.getEncodedNameAsBytes()).isNotEmpty();
        }
    }

    @Test
    void testSingleBucketTable() {
        regionManager.registerTable(TEST_TABLE, 1);

        List<VirtualRegionManager.VirtualRegion> regions = regionManager.getRegions(TEST_TABLE);

        assertThat(regions).hasSize(1);

        VirtualRegionManager.VirtualRegion region = regions.get(0);
        assertThat(region.getBucketId()).isEqualTo(0);
        assertThat(region.getRegionInfo().getStartKey()).isEmpty();
        assertThat(region.getRegionInfo().getEndKey()).isEmpty();

        byte[] anyKey = Bytes.toBytes("any_key");
        VirtualRegionManager.VirtualRegion foundRegion =
                regionManager.getRegionForRow(TEST_TABLE, anyKey);
        assertThat(foundRegion).isEqualTo(region);
    }

    @Test
    void testMultipleTablesIndependence() {
        TableName table1 = TableName.valueOf("table1");
        TableName table2 = TableName.valueOf("table2");

        regionManager.registerTable(table1, 2);
        regionManager.registerTable(table2, 3);

        List<VirtualRegionManager.VirtualRegion> regions1 = regionManager.getRegions(table1);
        List<VirtualRegionManager.VirtualRegion> regions2 = regionManager.getRegions(table2);

        assertThat(regions1).hasSize(2);
        assertThat(regions2).hasSize(3);

        byte[] rowKey = Bytes.toBytes("test");
        int bucket1 = regionManager.getBucketForRow(table1, rowKey);
        int bucket2 = regionManager.getBucketForRow(table2, rowKey);

        assertThat(bucket1).isBetween(0, 1);
        assertThat(bucket2).isBetween(0, 2);
    }
}
