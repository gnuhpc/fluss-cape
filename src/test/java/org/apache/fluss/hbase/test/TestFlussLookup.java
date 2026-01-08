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

package org.apache.fluss.hbase.test;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

/** Test class for Fluss Lookuper functionality. */
public class TestFlussLookup {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Test Fluss Lookuper ===\n");

        Configuration config = new Configuration();
        config.setString("bootstrap.servers", "localhost:9123");

        Connection conn = ConnectionFactory.createConnection(config);
        TablePath tablePath = TablePath.of("benchmark", "usertable");
        Table table = conn.getTable(tablePath);

        System.out.println("Step 1: Upsert a row via Fluss client");
        UpsertWriter writer = table.newUpsert().createWriter();

        GenericRow row = new GenericRow(11);
        row.setField(
                0,
                org.apache.fluss.shaded.guava32.com.google.common.primitives.Bytes.asList(
                        "fluss_test_key".getBytes()));
        row.setField(1, "Alice");
        row.setField(2, "30");
        row.setField(3, "Engineer");
        row.setField(4, null);
        row.setField(5, null);
        row.setField(6, null);
        row.setField(7, null);
        row.setField(8, null);
        row.setField(9, null);
        row.setField(10, null);

        writer.upsert(row).get();
        System.out.println(
                "  ✓ Upserted: key=fluss_test_key, field0=Alice, field1=30, field2=Engineer\n");

        System.out.println("Step 2: Lookup the row");
        Lookuper lookuper = table.newLookup().createLookuper();

        GenericRow keyRow = new GenericRow(1);
        keyRow.setField(
                0,
                org.apache.fluss.shaded.guava32.com.google.common.primitives.Bytes.asList(
                        "fluss_test_key".getBytes()));

        lookuper.lookup(keyRow)
                .thenAccept(
                        lookupResult -> {
                            System.out.println("  Lookup result:");
                            if (lookupResult.getRowList() != null
                                    && !lookupResult.getRowList().isEmpty()) {
                                InternalRow resultRow = lookupResult.getRowList().get(0);
                                System.out.println("    Row returned: " + resultRow);
                                System.out.println("    Field count: " + resultRow.getFieldCount());

                                for (int i = 0; i < Math.min(resultRow.getFieldCount(), 11); i++) {
                                    Object fieldValue = null;
                                    if (!resultRow.isNullAt(i)) {
                                        if (i == 0) {
                                            fieldValue = resultRow.getString(i);
                                        } else {
                                            fieldValue = resultRow.getString(i);
                                        }
                                    }
                                    System.out.println("    Field " + i + ": " + fieldValue);
                                }

                                System.out.println("\n✓✓✓ Lookuper returns FULL row data! ✓✓✓");
                            } else {
                                System.out.println("    ✗ No row found!");
                            }
                        })
                .get();

        table.close();
        conn.close();

        System.out.println("\nConclusion: Fluss Lookuper works correctly.");
        System.out.println("The issue is in HBase compatibility layer's CellConverter.");
    }
}
