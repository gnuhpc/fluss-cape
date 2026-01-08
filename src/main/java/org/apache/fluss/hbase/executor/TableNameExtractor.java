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

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/** Extracts table names from HBase RPC request protobufs for routing purposes. */
public class TableNameExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(TableNameExtractor.class);

    @Nullable
    public static String extractTableName(String methodName, byte[] requestBytes) {
        LOG.info("[EXTRACTOR] START extractTableName: method={}", methodName);
        try {
            switch (methodName) {
                case "Get":
                    ClientProtos.GetRequest getRequest =
                            ClientProtos.GetRequest.parseFrom(requestBytes);
                    LOG.info("[EXTRACTOR] Get request: hasRegion={}", getRequest.hasRegion());
                    if (getRequest.hasRegion()) {
                        String tableName = getTableNameFromRegion(getRequest.getRegion());
                        LOG.info("[EXTRACTOR] Extracted from Get: {}", tableName);
                        return tableName;
                    }
                    break;
                case "Mutate":
                    ClientProtos.MutateRequest mutateRequest =
                            ClientProtos.MutateRequest.parseFrom(requestBytes);
                    LOG.info("[EXTRACTOR] Mutate request: hasRegion={}", mutateRequest.hasRegion());
                    if (mutateRequest.hasRegion()) {
                        String tableName = getTableNameFromRegion(mutateRequest.getRegion());
                        LOG.info("[EXTRACTOR] Extracted from Mutate: {}", tableName);
                        return tableName;
                    }
                    break;
                case "Scan":
                    ClientProtos.ScanRequest scanRequest =
                            ClientProtos.ScanRequest.parseFrom(requestBytes);
                    LOG.info("[EXTRACTOR] Scan request: hasRegion={}", scanRequest.hasRegion());
                    if (scanRequest.hasRegion()) {
                        String tableName = getTableNameFromRegion(scanRequest.getRegion());
                        LOG.info("[EXTRACTOR] Extracted from Scan: {}", tableName);
                        return tableName;
                    }
                    break;
                case "Multi":
                    ClientProtos.MultiRequest multiRequest =
                            ClientProtos.MultiRequest.parseFrom(requestBytes);
                    LOG.info(
                            "[EXTRACTOR] Multi request: regionActionCount={}",
                            multiRequest.getRegionActionCount());
                    if (multiRequest.getRegionActionCount() > 0) {
                        ClientProtos.RegionAction regionAction = multiRequest.getRegionAction(0);
                        if (regionAction.hasRegion()) {
                            String tableName = getTableNameFromRegion(regionAction.getRegion());
                            LOG.info("[EXTRACTOR] Extracted from Multi: {}", tableName);
                            return tableName;
                        }
                    }
                    break;
            }
        } catch (Exception e) {
            LOG.warn("[EXTRACTOR] Failed to extract table name from {} request", methodName, e);
        }
        LOG.info("[EXTRACTOR] No table name extracted for method={}", methodName);
        return null;
    }

    private static String getTableNameFromRegion(HBaseProtos.RegionSpecifier region) {
        if (region.hasValue()) {
            String regionName = region.getValue().toStringUtf8();
            int firstComma = regionName.indexOf(',');
            if (firstComma > 0) {
                return regionName.substring(0, firstComma);
            }
            return regionName;
        }
        return null;
    }
}
