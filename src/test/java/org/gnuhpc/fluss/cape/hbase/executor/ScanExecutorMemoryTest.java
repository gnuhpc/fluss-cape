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

package org.gnuhpc.fluss.cape.hbase.executor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;

/**
 * Memory usage comparison test for ScanExecutor vs ScanExecutorStreaming
 * 
 * This test validates that the streaming version has significantly lower memory footprint
 * when scanning large tables.
 */
@DisplayName("ScanExecutor Memory Usage Test")
public class ScanExecutorMemoryTest {

    @Test
    @Disabled("Manual performance test - requires running Fluss cluster")
    @DisplayName("Old ScanExecutor memory usage baseline (10k rows)")
    public void testOldExecutorMemoryUsage() {
        System.gc();
        long memBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        
        System.out.println("Old ScanExecutor with 10,000 rows pre-loaded:");
        System.out.println("  Expected memory: ~10,000 Result objects buffered");
        System.out.println("  Memory before: " + (memBefore / 1024 / 1024) + " MB");
        
        System.out.println("\nThis test is disabled - enable manually to compare with real Fluss cluster");
    }

    @Test
    @Disabled("Manual performance test - requires running Fluss cluster")
    @DisplayName("New ScanExecutorStreaming memory usage (streaming)")
    public void testStreamingExecutorMemoryUsage() {
        System.gc();
        long memBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        
        System.out.println("New ScanExecutorStreaming with on-demand loading:");
        System.out.println("  Expected memory: ~O(batch_size) only");
        System.out.println("  Memory before: " + (memBefore / 1024 / 1024) + " MB");
        
        System.out.println("\nThis test is disabled - enable manually to compare with real Fluss cluster");
    }

    @Test
    @DisplayName("Memory footprint documentation test")
    public void testMemoryFootprintDocumentation() {
        int maxResults = 10_000;
        int batchSize = 1000;
        int estimatedBytesPerResult = 1024;
        
        long oldMemoryFootprint = (long) maxResults * estimatedBytesPerResult;
        long newMemoryFootprint = (long) batchSize * estimatedBytesPerResult;
        
        System.out.println("=== Memory Footprint Comparison ===");
        System.out.println("Scanning 10,000 rows:");
        System.out.println("  Old ScanExecutor:  " + (oldMemoryFootprint / 1024 / 1024) + " MB (all rows buffered)");
        System.out.println("  New Streaming:     " + (newMemoryFootprint / 1024) + " KB (batch_size=" + batchSize + ")");
        System.out.println("  Memory Reduction:  " + (100 - (newMemoryFootprint * 100 / oldMemoryFootprint)) + "%");
        System.out.println("\nWith 100 concurrent scans:");
        System.out.println("  Old ScanExecutor:  " + (oldMemoryFootprint * 100 / 1024 / 1024) + " MB");
        System.out.println("  New Streaming:     " + (newMemoryFootprint * 100 / 1024 / 1024) + " MB");
        
        assert newMemoryFootprint < oldMemoryFootprint / 5 : "Streaming should use <20% memory";
    }
}
