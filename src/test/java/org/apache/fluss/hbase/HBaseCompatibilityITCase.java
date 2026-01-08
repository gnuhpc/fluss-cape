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

package org.apache.fluss.hbase;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.hbase.executor.GetExecutor;
import org.apache.fluss.hbase.executor.HBaseRequestRouter;
import org.apache.fluss.hbase.executor.MultiExecutor;
import org.apache.fluss.hbase.executor.PutExecutor;
import org.apache.fluss.hbase.executor.ScanExecutor;
import org.apache.fluss.hbase.mapping.CellConverter;
import org.apache.fluss.hbase.mapping.RowKeyEncoder;
import org.apache.fluss.hbase.server.HBaseCompatServer;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HBaseCompatibilityITCase extends ClientToServerITCaseBase {

    private static final String TEST_DATABASE = "test_db";
    private static final String TEST_TABLE = "test_table";
    private static final TablePath TABLE_PATH = TablePath.of(TEST_DATABASE, TEST_TABLE);
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf");
    private static final int HBASE_SERVER_PORT = 16020;

    private HBaseCompatServer hbaseServer;
    private org.apache.hadoop.hbase.client.Connection hbaseConn;

    @BeforeEach
    protected void setup() throws Exception {
        super.setup();

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .withComment("Test table for HBase compatibility")
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3).build();

        createTable(TABLE_PATH, tableDescriptor, false);

        RowType rowType =
                RowType.of(
                        new org.apache.fluss.types.DataType[] {
                            DataTypes.INT(), DataTypes.STRING(), DataTypes.INT()
                        },
                        new String[] {"id", "name", "age"});

        RowKeyEncoder rowKeyEncoder = new RowKeyEncoder(rowType, Collections.singletonList("id"));

        CellConverter cellConverter =
                new CellConverter(
                        rowType, CellConverter.ColumnFamilyMapping.createDefault(rowType, "cf"));

        HBaseRequestRouter router = new HBaseRequestRouter();
        router.registerExecutor(
                "Get", new GetExecutor(conn, TABLE_PATH, rowKeyEncoder, cellConverter));
        router.registerExecutor(
                "Mutate", new PutExecutor(conn, TABLE_PATH, rowType, rowKeyEncoder, cellConverter));
        router.registerExecutor(
                "Scan", new ScanExecutor(conn, TABLE_PATH, rowKeyEncoder, cellConverter));
        router.registerExecutor(
                "Multi",
                new MultiExecutor(conn, TABLE_PATH, rowType, rowKeyEncoder, cellConverter));

        hbaseServer =
                new HBaseCompatServer(new Configuration(), "localhost", HBASE_SERVER_PORT, router);
        hbaseServer.start();

        org.apache.hadoop.conf.Configuration hbaseConfig =
                new org.apache.hadoop.conf.Configuration();
        hbaseConfig.set("hbase.zookeeper.quorum", "localhost:" + HBASE_SERVER_PORT);
        hbaseConn = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbaseConfig);
    }

    @AfterEach
    protected void teardown() throws Exception {
        if (hbaseConn != null) {
            hbaseConn.close();
        }
        if (hbaseServer != null) {
            hbaseServer.close();
        }
    }

    @Test
    void testPutAndGet() throws Exception {
        Table hbaseTable = hbaseConn.getTable(TableName.valueOf(TEST_TABLE));

        byte[] rowKey = Bytes.toBytes("1");
        Put put = new Put(rowKey);
        put.addColumn(COLUMN_FAMILY, Bytes.toBytes("id"), Bytes.toBytes(1));
        put.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("Alice"));
        put.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), Bytes.toBytes(25));
        hbaseTable.put(put);

        Get get = new Get(rowKey);
        Result result = hbaseTable.get(get);

        assertThat(result.isEmpty()).isFalse();
        assertThat(Bytes.toInt(result.getValue(COLUMN_FAMILY, Bytes.toBytes("id")))).isEqualTo(1);
        assertThat(Bytes.toString(result.getValue(COLUMN_FAMILY, Bytes.toBytes("name"))))
                .isEqualTo("Alice");
        assertThat(Bytes.toInt(result.getValue(COLUMN_FAMILY, Bytes.toBytes("age")))).isEqualTo(25);

        hbaseTable.close();
    }

    @Test
    void testPutAndDelete() throws Exception {
        Table hbaseTable = hbaseConn.getTable(TableName.valueOf(TEST_TABLE));

        byte[] rowKey = Bytes.toBytes("2");
        Put put = new Put(rowKey);
        put.addColumn(COLUMN_FAMILY, Bytes.toBytes("id"), Bytes.toBytes(2));
        put.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("Bob"));
        put.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), Bytes.toBytes(30));
        hbaseTable.put(put);

        Get getBeforeDelete = new Get(rowKey);
        Result resultBefore = hbaseTable.get(getBeforeDelete);
        assertThat(resultBefore.isEmpty()).isFalse();

        Delete delete = new Delete(rowKey);
        hbaseTable.delete(delete);

        Get getAfterDelete = new Get(rowKey);
        Result resultAfter = hbaseTable.get(getAfterDelete);
        assertThat(resultAfter.isEmpty()).isTrue();

        hbaseTable.close();
    }

    @Test
    void testScan() throws Exception {
        Table hbaseTable = hbaseConn.getTable(TableName.valueOf(TEST_TABLE));

        for (int i = 10; i < 15; i++) {
            byte[] rowKey = Bytes.toBytes(String.valueOf(i));
            Put put = new Put(rowKey);
            put.addColumn(COLUMN_FAMILY, Bytes.toBytes("id"), Bytes.toBytes(i));
            put.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("User" + i));
            put.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), Bytes.toBytes(20 + i));
            hbaseTable.put(put);
        }

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("10"));
        scan.setStopRow(Bytes.toBytes("15"));

        ResultScanner scanner = hbaseTable.getScanner(scan);
        int count = 0;
        for (Result result : scanner) {
            assertThat(result.isEmpty()).isFalse();
            count++;
        }
        assertThat(count).isEqualTo(5);
        scanner.close();

        hbaseTable.close();
    }

    @Test
    void testMultiGet() throws Exception {
        Table hbaseTable = hbaseConn.getTable(TableName.valueOf(TEST_TABLE));

        for (int i = 20; i < 23; i++) {
            byte[] rowKey = Bytes.toBytes(String.valueOf(i));
            Put put = new Put(rowKey);
            put.addColumn(COLUMN_FAMILY, Bytes.toBytes("id"), Bytes.toBytes(i));
            put.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("Multi" + i));
            put.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), Bytes.toBytes(30 + i));
            hbaseTable.put(put);
        }

        List<Get> gets =
                Arrays.asList(
                        new Get(Bytes.toBytes("20")),
                        new Get(Bytes.toBytes("21")),
                        new Get(Bytes.toBytes("22")));

        Result[] results = hbaseTable.get(gets);

        assertThat(results).hasSize(3);
        for (int i = 0; i < 3; i++) {
            Result result = results[i];
            assertThat(result.isEmpty()).isFalse();
            int expectedId = 20 + i;
            assertThat(Bytes.toInt(result.getValue(COLUMN_FAMILY, Bytes.toBytes("id"))))
                    .isEqualTo(expectedId);
            assertThat(Bytes.toString(result.getValue(COLUMN_FAMILY, Bytes.toBytes("name"))))
                    .isEqualTo("Multi" + expectedId);
        }

        hbaseTable.close();
    }

    @Test
    void testUpdate() throws Exception {
        Table hbaseTable = hbaseConn.getTable(TableName.valueOf(TEST_TABLE));

        byte[] rowKey = Bytes.toBytes("100");
        Put put1 = new Put(rowKey);
        put1.addColumn(COLUMN_FAMILY, Bytes.toBytes("id"), Bytes.toBytes(100));
        put1.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("Original"));
        put1.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), Bytes.toBytes(40));
        hbaseTable.put(put1);

        Get get1 = new Get(rowKey);
        Result result1 = hbaseTable.get(get1);
        assertThat(Bytes.toString(result1.getValue(COLUMN_FAMILY, Bytes.toBytes("name"))))
                .isEqualTo("Original");

        Put put2 = new Put(rowKey);
        put2.addColumn(COLUMN_FAMILY, Bytes.toBytes("id"), Bytes.toBytes(100));
        put2.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("Updated"));
        put2.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), Bytes.toBytes(41));
        hbaseTable.put(put2);

        Get get2 = new Get(rowKey);
        Result result2 = hbaseTable.get(get2);
        assertThat(Bytes.toString(result2.getValue(COLUMN_FAMILY, Bytes.toBytes("name"))))
                .isEqualTo("Updated");
        assertThat(Bytes.toInt(result2.getValue(COLUMN_FAMILY, Bytes.toBytes("age"))))
                .isEqualTo(41);

        hbaseTable.close();
    }

    @Test
    void testPartialUpdate() throws Exception {
        Table hbaseTable = hbaseConn.getTable(TableName.valueOf(TEST_TABLE));

        byte[] rowKey = Bytes.toBytes("200");
        Put initialPut = new Put(rowKey);
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("id"), Bytes.toBytes(200));
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("InitialName"));
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), Bytes.toBytes(50));
        hbaseTable.put(initialPut);

        Get getInitial = new Get(rowKey);
        Result resultInitial = hbaseTable.get(getInitial);
        assertThat(Bytes.toString(resultInitial.getValue(COLUMN_FAMILY, Bytes.toBytes("name"))))
                .isEqualTo("InitialName");
        assertThat(Bytes.toInt(resultInitial.getValue(COLUMN_FAMILY, Bytes.toBytes("age"))))
                .isEqualTo(50);

        Put partialUpdate = new Put(rowKey);
        partialUpdate.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("UpdatedName"));
        hbaseTable.put(partialUpdate);

        Get getAfterUpdate = new Get(rowKey);
        Result resultAfterUpdate = hbaseTable.get(getAfterUpdate);
        assertThat(Bytes.toString(resultAfterUpdate.getValue(COLUMN_FAMILY, Bytes.toBytes("name"))))
                .isEqualTo("UpdatedName");
        assertThat(Bytes.toInt(resultAfterUpdate.getValue(COLUMN_FAMILY, Bytes.toBytes("age"))))
                .isEqualTo(50);

        hbaseTable.close();
    }

    @Test
    void testUpdateToEmptyString() throws Exception {
        Table hbaseTable = hbaseConn.getTable(TableName.valueOf(TEST_TABLE));

        byte[] rowKey = Bytes.toBytes("201");
        Put initialPut = new Put(rowKey);
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("id"), Bytes.toBytes(201));
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("SomeName"));
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), Bytes.toBytes(30));
        hbaseTable.put(initialPut);

        Put emptyStringUpdate = new Put(rowKey);
        emptyStringUpdate.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes(""));
        hbaseTable.put(emptyStringUpdate);

        Get getAfterUpdate = new Get(rowKey);
        Result resultAfterUpdate = hbaseTable.get(getAfterUpdate);
        byte[] nameValue = resultAfterUpdate.getValue(COLUMN_FAMILY, Bytes.toBytes("name"));
        assertThat(nameValue).isNotNull();
        assertThat(Bytes.toString(nameValue)).isEmpty();
        assertThat(Bytes.toInt(resultAfterUpdate.getValue(COLUMN_FAMILY, Bytes.toBytes("age"))))
                .isEqualTo(30);

        hbaseTable.close();
    }

    @Test
    void testMultiplePartialUpdates() throws Exception {
        Table hbaseTable = hbaseConn.getTable(TableName.valueOf(TEST_TABLE));

        byte[] rowKey = Bytes.toBytes("202");
        Put initialPut = new Put(rowKey);
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("id"), Bytes.toBytes(202));
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("FirstName"));
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), Bytes.toBytes(25));
        hbaseTable.put(initialPut);

        Put updateName = new Put(rowKey);
        updateName.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("SecondName"));
        hbaseTable.put(updateName);

        Put updateAge = new Put(rowKey);
        updateAge.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), Bytes.toBytes(26));
        hbaseTable.put(updateAge);

        Put updateBoth = new Put(rowKey);
        updateBoth.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("FinalName"));
        updateBoth.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), Bytes.toBytes(27));
        hbaseTable.put(updateBoth);

        Get getFinal = new Get(rowKey);
        Result resultFinal = hbaseTable.get(getFinal);
        assertThat(Bytes.toString(resultFinal.getValue(COLUMN_FAMILY, Bytes.toBytes("name"))))
                .isEqualTo("FinalName");
        assertThat(Bytes.toInt(resultFinal.getValue(COLUMN_FAMILY, Bytes.toBytes("age"))))
                .isEqualTo(27);

        hbaseTable.close();
    }

    @Test
    void testIncrement() throws Exception {
        Table hbaseTable = hbaseConn.getTable(TableName.valueOf(TEST_TABLE));

        byte[] rowKey = Bytes.toBytes("300");
        Put initialPut = new Put(rowKey);
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("id"), Bytes.toBytes(300));
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("Counter"));
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), Bytes.toBytes(10));
        hbaseTable.put(initialPut);

        org.apache.hadoop.hbase.client.Increment increment =
                new org.apache.hadoop.hbase.client.Increment(rowKey);
        increment.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), 5L);
        Result result = hbaseTable.increment(increment);

        assertThat(Bytes.toInt(result.getValue(COLUMN_FAMILY, Bytes.toBytes("age")))).isEqualTo(15);

        Get getAfterIncrement = new Get(rowKey);
        Result verifyResult = hbaseTable.get(getAfterIncrement);
        assertThat(Bytes.toInt(verifyResult.getValue(COLUMN_FAMILY, Bytes.toBytes("age"))))
                .isEqualTo(15);

        hbaseTable.close();
    }

    @Test
    void testAppend() throws Exception {
        Table hbaseTable = hbaseConn.getTable(TableName.valueOf(TEST_TABLE));

        byte[] rowKey = Bytes.toBytes("301");
        Put initialPut = new Put(rowKey);
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("id"), Bytes.toBytes(301));
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("Hello"));
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), Bytes.toBytes(20));
        hbaseTable.put(initialPut);

        org.apache.hadoop.hbase.client.Append append =
                new org.apache.hadoop.hbase.client.Append(rowKey);
        append.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes(" World"));
        Result result = hbaseTable.append(append);

        assertThat(Bytes.toString(result.getValue(COLUMN_FAMILY, Bytes.toBytes("name"))))
                .isEqualTo("Hello World");

        Get getAfterAppend = new Get(rowKey);
        Result verifyResult = hbaseTable.get(getAfterAppend);
        assertThat(Bytes.toString(verifyResult.getValue(COLUMN_FAMILY, Bytes.toBytes("name"))))
                .isEqualTo("Hello World");

        hbaseTable.close();
    }

    @Test
    void testCheckAndMutateSuccess() throws Exception {
        Table hbaseTable = hbaseConn.getTable(TableName.valueOf(TEST_TABLE));

        byte[] rowKey = Bytes.toBytes("302");
        Put initialPut = new Put(rowKey);
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("id"), Bytes.toBytes(302));
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("TestUser"));
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), Bytes.toBytes(30));
        hbaseTable.put(initialPut);

        Put conditionalPut = new Put(rowKey);
        conditionalPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), Bytes.toBytes(35));

        boolean success =
                hbaseTable
                        .checkAndMutate(rowKey, COLUMN_FAMILY)
                        .qualifier(Bytes.toBytes("name"))
                        .ifEquals(Bytes.toBytes("TestUser"))
                        .thenPut(conditionalPut);

        assertThat(success).isTrue();

        Get getAfterMutate = new Get(rowKey);
        Result result = hbaseTable.get(getAfterMutate);
        assertThat(Bytes.toInt(result.getValue(COLUMN_FAMILY, Bytes.toBytes("age")))).isEqualTo(35);

        hbaseTable.close();
    }

    @Test
    void testCheckAndMutateFailure() throws Exception {
        Table hbaseTable = hbaseConn.getTable(TableName.valueOf(TEST_TABLE));

        byte[] rowKey = Bytes.toBytes("303");
        Put initialPut = new Put(rowKey);
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("id"), Bytes.toBytes(303));
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("User"));
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), Bytes.toBytes(40));
        hbaseTable.put(initialPut);

        Put conditionalPut = new Put(rowKey);
        conditionalPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), Bytes.toBytes(45));

        boolean success =
                hbaseTable
                        .checkAndMutate(rowKey, COLUMN_FAMILY)
                        .qualifier(Bytes.toBytes("name"))
                        .ifEquals(Bytes.toBytes("WrongName"))
                        .thenPut(conditionalPut);

        assertThat(success).isFalse();

        Get getAfterMutate = new Get(rowKey);
        Result result = hbaseTable.get(getAfterMutate);
        assertThat(Bytes.toInt(result.getValue(COLUMN_FAMILY, Bytes.toBytes("age")))).isEqualTo(40);

        hbaseTable.close();
    }

    @Test
    void testRowMutations() throws Exception {
        Table hbaseTable = hbaseConn.getTable(TableName.valueOf(TEST_TABLE));

        byte[] rowKey = Bytes.toBytes("304");
        Put initialPut = new Put(rowKey);
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("id"), Bytes.toBytes(304));
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("Original"));
        initialPut.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), Bytes.toBytes(50));
        hbaseTable.put(initialPut);

        org.apache.hadoop.hbase.client.RowMutations mutations =
                new org.apache.hadoop.hbase.client.RowMutations(rowKey);

        Put put = new Put(rowKey);
        put.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("Updated"));
        mutations.add(put);

        org.apache.hadoop.hbase.client.Increment increment =
                new org.apache.hadoop.hbase.client.Increment(rowKey);
        increment.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), 10L);
        mutations.add(increment);

        hbaseTable.mutateRow(mutations);

        Get getAfterMutations = new Get(rowKey);
        Result result = hbaseTable.get(getAfterMutations);
        assertThat(Bytes.toString(result.getValue(COLUMN_FAMILY, Bytes.toBytes("name"))))
                .isEqualTo("Updated");
        assertThat(Bytes.toInt(result.getValue(COLUMN_FAMILY, Bytes.toBytes("age")))).isEqualTo(60);

        hbaseTable.close();
    }

    @Test
    void testSaslPlainAuthentication() throws Exception {
        // Setup server with SASL PLAIN authentication
        Configuration saslConfig = new Configuration();
        saslConfig.setString(
                "security.sasl.plain.jaas.config",
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required "
                        + "user_testuser=\"testpass\" "
                        + "user_admin=\"admin-secret\";");
        saslConfig.setString("security.sasl.mechanism", "PLAIN");

        Connection flussConnection = ConnectionFactory.createConnection(saslConfig);

        RowType rowType =
                RowType.of(
                        new org.apache.fluss.types.DataType[] {
                            DataTypes.INT(), DataTypes.STRING(), DataTypes.INT()
                        },
                        new String[] {"id", "name", "age"});

        RowKeyEncoder rowKeyEncoder = new RowKeyEncoder(rowType, Collections.singletonList("id"));

        CellConverter cellConverter =
                new CellConverter(
                        rowType, CellConverter.ColumnFamilyMapping.createDefault(rowType, "cf"));

        HBaseRequestRouter saslRouter = new HBaseRequestRouter();
        saslRouter.registerExecutor(
                "Get", new GetExecutor(flussConnection, TABLE_PATH, rowKeyEncoder, cellConverter));
        saslRouter.registerExecutor(
                "Mutate",
                new PutExecutor(
                        flussConnection, TABLE_PATH, rowType, rowKeyEncoder, cellConverter));

        HBaseCompatServer saslServer =
                new HBaseCompatServer(saslConfig, "localhost", 16021, saslRouter);
        saslServer.start();

        try {
            // Configure HBase client with SASL authentication
            org.apache.hadoop.conf.Configuration hbaseConfig =
                    new org.apache.hadoop.conf.Configuration();
            hbaseConfig.set("hbase.zookeeper.quorum", "localhost:16021");
            hbaseConfig.set("hadoop.security.authentication", "simple");
            hbaseConfig.set("hbase.security.authentication", "simple");

            // Create connection with authentication
            org.apache.hadoop.hbase.client.Connection authConn =
                    org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbaseConfig);

            Table hbaseTable = authConn.getTable(TableName.valueOf(TEST_TABLE));

            // Test authenticated operations
            byte[] rowKey = Bytes.toBytes("auth-test");
            Put put = new Put(rowKey);
            put.addColumn(COLUMN_FAMILY, Bytes.toBytes("id"), Bytes.toBytes(999));
            put.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("AuthUser"));
            put.addColumn(COLUMN_FAMILY, Bytes.toBytes("age"), Bytes.toBytes(35));
            hbaseTable.put(put);

            Get get = new Get(rowKey);
            Result result = hbaseTable.get(get);

            assertThat(result.isEmpty()).isFalse();
            assertThat(Bytes.toInt(result.getValue(COLUMN_FAMILY, Bytes.toBytes("id"))))
                    .isEqualTo(999);
            assertThat(Bytes.toString(result.getValue(COLUMN_FAMILY, Bytes.toBytes("name"))))
                    .isEqualTo("AuthUser");

            hbaseTable.close();
            authConn.close();
        } finally {
            saslServer.close();
            flussConnection.close();
        }
    }

    @Test
    void testSaslAuthenticationFailure() throws Exception {
        // Setup server with SASL authentication
        Configuration saslConfig = new Configuration();
        saslConfig.setString(
                "security.sasl.plain.jaas.config",
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required "
                        + "user_validuser=\"validpass\";");
        saslConfig.setString("security.sasl.mechanism", "PLAIN");

        Connection flussConnection = ConnectionFactory.createConnection(saslConfig);

        RowType rowType =
                RowType.of(
                        new org.apache.fluss.types.DataType[] {
                            DataTypes.INT(), DataTypes.STRING(), DataTypes.INT()
                        },
                        new String[] {"id", "name", "age"});

        RowKeyEncoder rowKeyEncoder = new RowKeyEncoder(rowType, Collections.singletonList("id"));

        CellConverter cellConverter =
                new CellConverter(
                        rowType, CellConverter.ColumnFamilyMapping.createDefault(rowType, "cf"));

        HBaseRequestRouter saslRouter = new HBaseRequestRouter();
        saslRouter.registerExecutor(
                "Get", new GetExecutor(flussConnection, TABLE_PATH, rowKeyEncoder, cellConverter));

        HBaseCompatServer saslServer =
                new HBaseCompatServer(saslConfig, "localhost", 16022, saslRouter);
        saslServer.start();

        try {
            // Configure HBase client with wrong credentials
            org.apache.hadoop.conf.Configuration hbaseConfig =
                    new org.apache.hadoop.conf.Configuration();
            hbaseConfig.set("hbase.zookeeper.quorum", "localhost:16022");
            hbaseConfig.set("hadoop.security.authentication", "simple");

            // Attempt connection with invalid credentials should fail
            assertThatThrownBy(
                            () -> {
                                org.apache.hadoop.hbase.client.Connection badConn =
                                        org.apache.hadoop.hbase.client.ConnectionFactory
                                                .createConnection(hbaseConfig);
                                Table hbaseTable = badConn.getTable(TableName.valueOf(TEST_TABLE));
                                hbaseTable.get(new Get(Bytes.toBytes("test")));
                            })
                    .isInstanceOf(IOException.class);
        } finally {
            saslServer.close();
            flussConnection.close();
        }
    }
}
