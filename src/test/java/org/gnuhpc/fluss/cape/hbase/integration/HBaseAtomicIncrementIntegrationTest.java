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

package org.gnuhpc.fluss.cape.hbase.integration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("HBase Atomic Increment Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class HBaseAtomicIncrementIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseAtomicIncrementIntegrationTest.class);

    private static final String ZOOKEEPER_QUORUM = 
            System.getProperty("hbase.zookeeper.quorum", "localhost:2181");
    private static final String TEST_TABLE_PREFIX = "test_incr_";
    private static final byte[] CF = Bytes.toBytes("cf");
    private static final byte[] COUNTER_COL = Bytes.toBytes("counter");

    private static Connection connection;
    private Table table;
    private TableName tableName;

    @BeforeAll
    static void setupConnection() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
        conf.setInt("hbase.client.retries.number", 3);
        conf.setInt("hbase.rpc.timeout", 10000);
        
        try {
            connection = ConnectionFactory.createConnection(conf);
            LOG.info("Connected to HBase via CAPE at {}", ZOOKEEPER_QUORUM);
        } catch (Exception e) {
            LOG.warn("Could not connect to HBase - tests will be skipped: {}", e.getMessage());
            Assumptions.assumeTrue(false, "HBase not available: " + e.getMessage());
        }
    }

    @AfterAll
    static void teardownConnection() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @BeforeEach
    void setup() throws Exception {
        tableName = TableName.valueOf(TEST_TABLE_PREFIX + UUID.randomUUID().toString().replace("-", ""));
        
        TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
        tableBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF));
        
        connection.getAdmin().createTable(tableBuilder.build());
        table = connection.getTable(tableName);
        
        LOG.info("Created test table: {}", tableName);
    }

    @AfterEach
    void teardown() throws Exception {
        if (table != null) {
            table.close();
        }
        if (connection != null && tableName != null) {
            try {
                connection.getAdmin().disableTable(tableName);
                connection.getAdmin().deleteTable(tableName);
                LOG.info("Deleted test table: {}", tableName);
            } catch (Exception e) {
                LOG.warn("Failed to cleanup table {}: {}", tableName, e.getMessage());
            }
        }
    }

    @Test
    @Order(1)
    @DisplayName("Single increment on new row returns delta value")
    void testSingleIncrementNewRow() throws Exception {
        byte[] row = Bytes.toBytes("row1");
        
        Increment incr = new Increment(row);
        incr.addColumn(CF, COUNTER_COL, 1L);
        
        Result result = table.increment(incr);
        
        long value = Bytes.toLong(result.getValue(CF, COUNTER_COL));
        assertThat(value).isEqualTo(1L);
    }

    @Test
    @Order(2)
    @DisplayName("Multiple sequential increments accumulate correctly")
    void testSequentialIncrements() throws Exception {
        byte[] row = Bytes.toBytes("row2");
        
        for (int i = 1; i <= 10; i++) {
            Increment incr = new Increment(row);
            incr.addColumn(CF, COUNTER_COL, 5L);
            Result result = table.increment(incr);
            
            long value = Bytes.toLong(result.getValue(CF, COUNTER_COL));
            assertThat(value).isEqualTo(i * 5L);
        }
        
        Get get = new Get(row);
        Result finalResult = table.get(get);
        long finalValue = Bytes.toLong(finalResult.getValue(CF, COUNTER_COL));
        assertThat(finalValue).isEqualTo(50L);
    }

    @Test
    @Order(3)
    @DisplayName("Negative increment (decrement) works correctly")
    void testNegativeIncrement() throws Exception {
        byte[] row = Bytes.toBytes("row3");
        
        Increment incr1 = new Increment(row);
        incr1.addColumn(CF, COUNTER_COL, 100L);
        table.increment(incr1);
        
        Increment incr2 = new Increment(row);
        incr2.addColumn(CF, COUNTER_COL, -30L);
        Result result = table.increment(incr2);
        
        long value = Bytes.toLong(result.getValue(CF, COUNTER_COL));
        assertThat(value).isEqualTo(70L);
    }

    @Test
    @Order(4)
    @DisplayName("Multi-column increment in single request")
    void testMultiColumnIncrement() throws Exception {
        byte[] row = Bytes.toBytes("row4");
        byte[] col1 = Bytes.toBytes("counter1");
        byte[] col2 = Bytes.toBytes("counter2");
        byte[] col3 = Bytes.toBytes("counter3");
        
        Increment incr = new Increment(row);
        incr.addColumn(CF, col1, 10L);
        incr.addColumn(CF, col2, 20L);
        incr.addColumn(CF, col3, 30L);
        
        Result result = table.increment(incr);
        
        assertThat(Bytes.toLong(result.getValue(CF, col1))).isEqualTo(10L);
        assertThat(Bytes.toLong(result.getValue(CF, col2))).isEqualTo(20L);
        assertThat(Bytes.toLong(result.getValue(CF, col3))).isEqualTo(30L);
    }

    @Test
    @Order(5)
    @DisplayName("Concurrent increments are atomic - no lost updates")
    void testConcurrentIncrementsAtomic() throws Exception {
        byte[] row = Bytes.toBytes("row5");
        int numThreads = 10;
        int incrementsPerThread = 100;
        long expectedTotal = (long) numThreads * incrementsPerThread;
        
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicLong successCount = new AtomicLong(0);
        List<Exception> errors = new CopyOnWriteArrayList<>();
        
        for (int t = 0; t < numThreads; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < incrementsPerThread; i++) {
                        Increment incr = new Increment(row);
                        incr.addColumn(CF, COUNTER_COL, 1L);
                        table.increment(incr);
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    errors.add(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }
        
        long startTime = System.currentTimeMillis();
        startLatch.countDown();
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        long duration = System.currentTimeMillis() - startTime;
        
        executor.shutdown();
        
        assertThat(completed).isTrue();
        assertThat(errors).isEmpty();
        assertThat(successCount.get()).isEqualTo(expectedTotal);
        
        Get get = new Get(row);
        Result result = table.get(get);
        long finalValue = Bytes.toLong(result.getValue(CF, COUNTER_COL));
        
        LOG.info("Concurrent test: {} increments in {}ms, final value: {}", 
                expectedTotal, duration, finalValue);
        
        assertThat(finalValue).isEqualTo(expectedTotal);
    }

    @Test
    @Order(6)
    @DisplayName("Increment on existing row with non-zero value")
    void testIncrementExistingValue() throws Exception {
        byte[] row = Bytes.toBytes("row6");
        
        Put put = new Put(row);
        put.addColumn(CF, COUNTER_COL, Bytes.toBytes(100L));
        table.put(put);
        
        Increment incr = new Increment(row);
        incr.addColumn(CF, COUNTER_COL, 50L);
        Result result = table.increment(incr);
        
        long value = Bytes.toLong(result.getValue(CF, COUNTER_COL));
        assertThat(value).isEqualTo(150L);
    }

    @Test
    @Order(7)
    @DisplayName("Large increment value")
    void testLargeIncrementValue() throws Exception {
        byte[] row = Bytes.toBytes("row7");
        long largeValue = 1_000_000_000_000L;
        
        Increment incr = new Increment(row);
        incr.addColumn(CF, COUNTER_COL, largeValue);
        Result result = table.increment(incr);
        
        long value = Bytes.toLong(result.getValue(CF, COUNTER_COL));
        assertThat(value).isEqualTo(largeValue);
    }

    @Test
    @Order(8)
    @DisplayName("Increment performance - 1000 sequential increments")
    void testIncrementPerformance() throws Exception {
        byte[] row = Bytes.toBytes("perf_row");
        int numIncrements = 1000;
        
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < numIncrements; i++) {
            Increment incr = new Increment(row);
            incr.addColumn(CF, COUNTER_COL, 1L);
            table.increment(incr);
        }
        
        long duration = System.currentTimeMillis() - startTime;
        double avgLatency = (double) duration / numIncrements;
        double throughput = numIncrements * 1000.0 / duration;
        
        LOG.info("Performance: {} increments in {}ms (avg: {:.2f}ms, throughput: {:.0f}/sec)",
                numIncrements, duration, avgLatency, throughput);
        
        Get get = new Get(row);
        Result result = table.get(get);
        long finalValue = Bytes.toLong(result.getValue(CF, COUNTER_COL));
        assertThat(finalValue).isEqualTo(numIncrements);
        
        assertThat(avgLatency).isLessThan(50.0);
    }

    @Test
    @Order(9)
    @DisplayName("Increment with zero delta returns current value")
    void testIncrementZeroDelta() throws Exception {
        byte[] row = Bytes.toBytes("row9");
        
        Increment incr1 = new Increment(row);
        incr1.addColumn(CF, COUNTER_COL, 42L);
        table.increment(incr1);
        
        Increment incr2 = new Increment(row);
        incr2.addColumn(CF, COUNTER_COL, 0L);
        Result result = table.increment(incr2);
        
        long value = Bytes.toLong(result.getValue(CF, COUNTER_COL));
        assertThat(value).isEqualTo(42L);
    }

    @Test
    @Order(10)
    @DisplayName("Multiple rows incremented independently")
    void testMultipleRowsIndependent() throws Exception {
        byte[] row1 = Bytes.toBytes("multi_row1");
        byte[] row2 = Bytes.toBytes("multi_row2");
        
        Increment incr1 = new Increment(row1);
        incr1.addColumn(CF, COUNTER_COL, 100L);
        table.increment(incr1);
        
        Increment incr2 = new Increment(row2);
        incr2.addColumn(CF, COUNTER_COL, 200L);
        table.increment(incr2);
        
        Increment incr3 = new Increment(row1);
        incr3.addColumn(CF, COUNTER_COL, 50L);
        Result result1 = table.increment(incr3);
        
        Increment incr4 = new Increment(row2);
        incr4.addColumn(CF, COUNTER_COL, 50L);
        Result result2 = table.increment(incr4);
        
        assertThat(Bytes.toLong(result1.getValue(CF, COUNTER_COL))).isEqualTo(150L);
        assertThat(Bytes.toLong(result2.getValue(CF, COUNTER_COL))).isEqualTo(250L);
    }
}
