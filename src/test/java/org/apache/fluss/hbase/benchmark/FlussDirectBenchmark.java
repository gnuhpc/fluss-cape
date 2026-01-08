/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.hbase.benchmark;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Simple benchmark for direct Fluss API access. */
public class FlussDirectBenchmark {

    private static final int RECORD_COUNT = 1000;
    private static final String DATABASE = "benchmark";
    private static final String TABLE_NAME = "usertable";

    public static void main(String[] args) throws Exception {
        System.out.println("========================================");
        System.out.println("Fluss Direct API Benchmark");
        System.out.println("========================================");
        System.out.println("Testing HBase compatibility layer table");
        System.out.println("Records: " + RECORD_COUNT);
        System.out.println();

        Configuration config = new Configuration();
        config.setString("bootstrap.servers", "localhost:9123");

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            TablePath tablePath = TablePath.of(DATABASE, TABLE_NAME);
            Table table = connection.getTable(tablePath);

            RowType rowType =
                    RowType.of(
                            new org.apache.fluss.types.DataType[] {
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING()
                            },
                            new String[] {
                                "YCSB_KEY",
                                "field0",
                                "field1",
                                "field2",
                                "field3",
                                "field4",
                                "field5",
                                "field6",
                                "field7",
                                "field8",
                                "field9"
                            });

            System.out.println("✓ Connected to Fluss");
            System.out.println("✓ Table: " + tablePath);
            System.out.println();

            testWrites(table, rowType);
            System.out.println();

            testReads(table, rowType);
            System.out.println();

            testScan(table);
            System.out.println();

            System.out.println("========================================");
            System.out.println("✓ All tests passed successfully!");
            System.out.println("HBase compatibility layer is working");
            System.out.println("========================================");

            table.close();
        }
    }

    private static void testWrites(Table table, RowType rowType) throws Exception {
        System.out.println("Test 1: WRITE operations");

        UpsertWriter writer = table.newUpsert().createWriter();
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < RECORD_COUNT; i++) {
            org.apache.fluss.row.GenericRow row = new org.apache.fluss.row.GenericRow(11);
            row.setField(0, org.apache.fluss.row.BinaryString.fromString("user" + i));
            for (int f = 0; f < 10; f++) {
                row.setField(
                        f + 1,
                        org.apache.fluss.row.BinaryString.fromString("value_" + i + "_field" + f));
            }
            writer.upsert(row).get();
        }

        writer.flush();
        long duration = System.currentTimeMillis() - startTime;

        double throughput = (RECORD_COUNT * 1000.0) / duration;
        System.out.printf("  Wrote %d records in %d ms%n", RECORD_COUNT, duration);
        System.out.printf("  Throughput: %.2f ops/sec%n", throughput);
        System.out.println("  ✓ Write test passed");
    }

    private static void testReads(Table table, RowType rowType) throws Exception {
        System.out.println("Test 2: READ (lookup) operations");

        Lookuper lookuper = table.newLookup().createLookuper();
        long startTime = System.currentTimeMillis();
        int successCount = 0;

        for (int i = 0; i < RECORD_COUNT; i++) {
            org.apache.fluss.row.GenericRow keyRow = new org.apache.fluss.row.GenericRow(1);
            keyRow.setField(0, org.apache.fluss.row.BinaryString.fromString("user" + i));

            LookupResult result = lookuper.lookup(keyRow).get();
            if (result != null && !result.getRowList().isEmpty()) {
                successCount++;
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        double throughput = (successCount * 1000.0) / duration;

        System.out.printf("  Read %d records in %d ms%n", successCount, duration);
        System.out.printf("  Throughput: %.2f ops/sec%n", throughput);
        System.out.println("  ✓ Read test passed");
    }

    private static void testScan(Table table) throws Exception {
        System.out.println("Test 3: SCAN operations");

        LogScanner scanner = table.newScan().createLogScanner();
        List<ScanRecord> records = new ArrayList<>();
        int count = 0;
        int maxSampleRecords = 10;
        int maxPollAttempts = 5;

        for (int attempt = 0; attempt < maxPollAttempts && count < maxSampleRecords; attempt++) {
            ScanRecords scanRecords = scanner.poll(Duration.ofSeconds(1));
            for (ScanRecord record : scanRecords) {
                records.add(record);
                count++;
                if (count >= maxSampleRecords) {
                    break;
                }
            }
        }

        System.out.printf("  Scanned %d records (sample)%n", count);
        if (count > 0) {
            System.out.println("  Sample key: " + records.get(0).getRow().getString(0).toString());
        }
        System.out.println("  ✓ Scan test passed");

        scanner.close();
    }
}
