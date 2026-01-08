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

import org.apache.fluss.utils.MapUtils;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Manages the mapping between Fluss buckets and HBase virtual regions.
 *
 * <p>Each Fluss bucket is represented as an HBase region with calculated start/end keys.
 */
public class VirtualRegionManager {

    private final Map<TableName, List<VirtualRegion>> tableRegions;
    private final String serverHostname;
    private final int serverPort;

    public VirtualRegionManager(String serverHostname, int serverPort) {
        this.tableRegions = MapUtils.newConcurrentHashMap();
        this.serverHostname = serverHostname;
        this.serverPort = serverPort;
    }

    public void registerTable(TableName tableName, int bucketCount) {
        // Resolve 0.0.0.0 to localhost for client connections
        String clientConnectableHostname = serverHostname;
        if ("0.0.0.0".equals(serverHostname)) {
            clientConnectableHostname = "localhost";
        }

        List<VirtualRegion> regions = new ArrayList<>();

        for (int bucketId = 0; bucketId < bucketCount; bucketId++) {
            byte[] startKey = createStartKey(bucketId);
            byte[] endKey = createEndKey(bucketId, bucketCount);

            HRegionInfo regionInfo =
                    new HRegionInfo(
                            tableName,
                            startKey,
                            endKey,
                            false,
                            System.currentTimeMillis(),
                            bucketId);

            ServerName serverName = ServerName.valueOf(clientConnectableHostname, serverPort, 0);

            VirtualRegion region = new VirtualRegion(regionInfo, serverName, bucketId);
            regions.add(region);
        }

        tableRegions.put(tableName, regions);
    }

    public List<VirtualRegion> getRegions(TableName tableName) {
        return tableRegions.getOrDefault(tableName, new ArrayList<>());
    }

    public List<List<VirtualRegion>> getAllRegions() {
        return new ArrayList<>(tableRegions.values());
    }

    public VirtualRegion getRegionForRow(TableName tableName, byte[] rowKey) {
        List<VirtualRegion> regions = tableRegions.get(tableName);
        if (regions == null || regions.isEmpty()) {
            return null;
        }

        for (VirtualRegion region : regions) {
            if (isRowInRegion(rowKey, region.getRegionInfo())) {
                return region;
            }
        }

        return regions.get(0);
    }

    public int getBucketForRow(TableName tableName, byte[] rowKey) {
        VirtualRegion region = getRegionForRow(tableName, rowKey);
        return region != null ? region.getBucketId() : 0;
    }

    private boolean isRowInRegion(byte[] rowKey, HRegionInfo regionInfo) {
        byte[] startKey = regionInfo.getStartKey();
        byte[] endKey = regionInfo.getEndKey();

        if (startKey.length == 0 && endKey.length == 0) {
            return true;
        }

        if (startKey.length == 0) {
            return Bytes.compareTo(rowKey, endKey) < 0;
        }

        if (endKey.length == 0) {
            return Bytes.compareTo(rowKey, startKey) >= 0;
        }

        return Bytes.compareTo(rowKey, startKey) >= 0 && Bytes.compareTo(rowKey, endKey) < 0;
    }

    private byte[] createStartKey(int bucketId) {
        if (bucketId == 0) {
            return new byte[0];
        }
        return Bytes.toBytes(String.format("bucket_%04d", bucketId));
    }

    private byte[] createEndKey(int bucketId, int totalBuckets) {
        if (bucketId == totalBuckets - 1) {
            return new byte[0];
        }
        return Bytes.toBytes(String.format("bucket_%04d", bucketId + 1));
    }

    /** Represents a virtual HBase region mapped to a Fluss bucket. */
    public static class VirtualRegion {
        private final HRegionInfo regionInfo;
        private final ServerName serverName;
        private final int bucketId;

        public VirtualRegion(HRegionInfo regionInfo, ServerName serverName, int bucketId) {
            this.regionInfo = regionInfo;
            this.serverName = serverName;
            this.bucketId = bucketId;
        }

        public HRegionInfo getRegionInfo() {
            return regionInfo;
        }

        public ServerName getServerName() {
            return serverName;
        }

        public int getBucketId() {
            return bucketId;
        }

        public byte[] getRegionName() {
            return regionInfo.getRegionName();
        }

        public byte[] getEncodedNameAsBytes() {
            return Bytes.toBytes(regionInfo.getEncodedName());
        }
    }
}
