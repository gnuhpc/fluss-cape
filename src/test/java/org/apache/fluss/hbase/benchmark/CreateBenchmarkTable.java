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

package org.apache.fluss.hbase.benchmark;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

/** Utility class to create YCSB benchmark database and table in Fluss. */
public class CreateBenchmarkTable {
    public static void main(String[] args) throws Exception {
        System.out.println("Creating benchmark database and table for YCSB...");

        Configuration config = new Configuration();
        config.setString("bootstrap.servers", "localhost:9123");

        try (Connection conn = ConnectionFactory.createConnection(config)) {
            Admin admin = conn.getAdmin();

            String databaseName = "benchmark";
            System.out.println("Creating database: " + databaseName);
            try {
                admin.createDatabase(databaseName, DatabaseDescriptor.EMPTY, false).get();
                System.out.println("✓ Database created: " + databaseName);
            } catch (Exception e) {
                if (e.getMessage() != null && e.getMessage().contains("already exists")) {
                    System.out.println("✓ Database already exists: " + databaseName);
                } else {
                    throw e;
                }
            }

            TablePath tablePath = TablePath.of(databaseName, "usertable");
            System.out.println("Creating table: " + tablePath);

            Schema schema =
                    Schema.newBuilder()
                            .column("YCSB_KEY", DataTypes.STRING())
                            .column("field0", DataTypes.STRING())
                            .column("field1", DataTypes.STRING())
                            .column("field2", DataTypes.STRING())
                            .column("field3", DataTypes.STRING())
                            .column("field4", DataTypes.STRING())
                            .column("field5", DataTypes.STRING())
                            .column("field6", DataTypes.STRING())
                            .column("field7", DataTypes.STRING())
                            .column("field8", DataTypes.STRING())
                            .column("field9", DataTypes.STRING())
                            .primaryKey("YCSB_KEY")
                            .build();

            TableDescriptor tableDescriptor =
                    TableDescriptor.builder().schema(schema).distributedBy(8, "YCSB_KEY").build();

            try {
                admin.createTable(tablePath, tableDescriptor, false).get();
                System.out.println("✓ Table created: " + tablePath);
            } catch (Exception e) {
                if (e.getMessage() != null && e.getMessage().contains("already exists")) {
                    System.out.println("✓ Table already exists: " + tablePath);
                } else {
                    throw e;
                }
            }

            System.out.println("\nBenchmark table setup complete!");
            System.out.println("Database: " + databaseName);
            System.out.println("Table: usertable");
            System.out.println("Primary Key: YCSB_KEY");
            System.out.println("Buckets: 8");
            System.out.println("Fields: field0-field9 (10 STRING columns)");
        }
    }
}
