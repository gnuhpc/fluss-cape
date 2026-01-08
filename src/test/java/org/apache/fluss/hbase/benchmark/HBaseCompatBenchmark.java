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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.hbase.executor.GetExecutor;
import org.apache.fluss.hbase.executor.HBaseRequestRouter;
import org.apache.fluss.hbase.executor.MultiExecutor;
import org.apache.fluss.hbase.executor.PutExecutor;
import org.apache.fluss.hbase.mapping.CellConverter;
import org.apache.fluss.hbase.mapping.RowKeyEncoder;
import org.apache.fluss.hbase.server.HBaseCompatServer;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/** JMH benchmark for HBase compatibility layer performance testing. */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 10)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.Throughput)
public class HBaseCompatBenchmark {

    private static final String TEST_DATABASE = "benchmark_db";
    private static final String TEST_TABLE = "benchmark_table";
    private static final TablePath TABLE_PATH = TablePath.of(TEST_DATABASE, TEST_TABLE);
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf");
    private static final int HBASE_SERVER_PORT = 16020;
    private static final int DATASET_SIZE = 10000;

    private final FlussClusterExtension flussCluster =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    private HBaseCompatServer hbaseServer;
    private org.apache.hadoop.hbase.client.Connection hbaseConn;
    private org.apache.hadoop.hbase.client.Table hbaseTable;
    private Random random;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        flussCluster.start();

        Configuration clientConf = flussCluster.getClientConfig();
        Connection flussConnection = ConnectionFactory.createConnection(clientConf);
        Admin admin = flussConnection.getAdmin();

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("value", DataTypes.BIGINT())
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3).build();

        admin.createDatabase(TEST_DATABASE, DatabaseDescriptor.EMPTY, false).get();
        admin.createTable(TABLE_PATH, tableDescriptor, false).get();

        RowType rowType =
                RowType.of(
                        new org.apache.fluss.types.DataType[] {
                            DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT()
                        },
                        new String[] {"id", "name", "value"});

        RowKeyEncoder rowKeyEncoder = new RowKeyEncoder(rowType, Collections.singletonList("id"));

        CellConverter cellConverter =
                new CellConverter(
                        rowType, CellConverter.ColumnFamilyMapping.createDefault(rowType, "cf"));

        HBaseRequestRouter router = new HBaseRequestRouter();
        router.registerExecutor(
                "Get", new GetExecutor(flussConnection, TABLE_PATH, rowKeyEncoder, cellConverter));
        router.registerExecutor(
                "Mutate",
                new PutExecutor(
                        flussConnection, TABLE_PATH, rowType, rowKeyEncoder, cellConverter));
        router.registerExecutor(
                "Multi",
                new MultiExecutor(
                        flussConnection, TABLE_PATH, rowType, rowKeyEncoder, cellConverter));

        hbaseServer =
                new HBaseCompatServer(new Configuration(), "localhost", HBASE_SERVER_PORT, router);
        hbaseServer.start();

        org.apache.hadoop.conf.Configuration hbaseConfig =
                new org.apache.hadoop.conf.Configuration();
        hbaseConfig.set("hbase.zookeeper.quorum", "localhost:" + HBASE_SERVER_PORT);
        hbaseConn = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbaseConfig);
        hbaseTable = hbaseConn.getTable(TableName.valueOf(TEST_TABLE));

        Table flussTable = flussConnection.getTable(TABLE_PATH);
        UpsertWriter writer = flussTable.newUpsert().createWriter();
        for (int i = 0; i < DATASET_SIZE; i++) {
            GenericRow row = new GenericRow(3);
            row.setField(0, i);
            row.setField(1, "user_" + i);
            row.setField(2, (long) i * 100);
            writer.upsert(row).get();
        }
        writer.flush();

        random = new Random(12345);
    }

    @TearDown(Level.Trial)
    public void teardown() throws Exception {
        if (hbaseTable != null) {
            hbaseTable.close();
        }
        if (hbaseConn != null) {
            hbaseConn.close();
        }
        if (hbaseServer != null) {
            hbaseServer.close();
        }
        flussCluster.close();
    }

    @Benchmark
    @Threads(1)
    public Result benchmarkGet() throws Exception {
        int id = random.nextInt(DATASET_SIZE);
        Get get = new Get(Bytes.toBytes(String.valueOf(id)));
        return hbaseTable.get(get);
    }

    @Benchmark
    @Threads(4)
    public Result benchmarkGetMultiThreaded() throws Exception {
        int id = random.nextInt(DATASET_SIZE);
        Get get = new Get(Bytes.toBytes(String.valueOf(id)));
        return hbaseTable.get(get);
    }

    @Benchmark
    @Threads(1)
    public void benchmarkPut() throws Exception {
        int id = random.nextInt(DATASET_SIZE);
        byte[] rowKey = Bytes.toBytes(String.valueOf(id));
        Put put = new Put(rowKey);
        put.addColumn(COLUMN_FAMILY, Bytes.toBytes("id"), Bytes.toBytes(id));
        put.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("updated_" + id));
        put.addColumn(COLUMN_FAMILY, Bytes.toBytes("value"), Bytes.toBytes((long) id * 200));
        hbaseTable.put(put);
    }

    @Benchmark
    @Threads(4)
    public void benchmarkPutMultiThreaded() throws Exception {
        int id = random.nextInt(DATASET_SIZE);
        byte[] rowKey = Bytes.toBytes(String.valueOf(id));
        Put put = new Put(rowKey);
        put.addColumn(COLUMN_FAMILY, Bytes.toBytes("id"), Bytes.toBytes(id));
        put.addColumn(COLUMN_FAMILY, Bytes.toBytes("name"), Bytes.toBytes("updated_" + id));
        put.addColumn(COLUMN_FAMILY, Bytes.toBytes("value"), Bytes.toBytes((long) id * 200));
        hbaseTable.put(put);
    }

    @Benchmark
    @Threads(1)
    public Result[] benchmarkMultiGet() throws Exception {
        List<Get> gets = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            int id = random.nextInt(DATASET_SIZE);
            gets.add(new Get(Bytes.toBytes(String.valueOf(id))));
        }
        return hbaseTable.get(gets);
    }

    @Benchmark
    @Threads(4)
    public Result[] benchmarkMultiGetMultiThreaded() throws Exception {
        List<Get> gets = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            int id = random.nextInt(DATASET_SIZE);
            gets.add(new Get(Bytes.toBytes(String.valueOf(id))));
        }
        return hbaseTable.get(gets);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + HBaseCompatBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(opt).run();
    }
}
