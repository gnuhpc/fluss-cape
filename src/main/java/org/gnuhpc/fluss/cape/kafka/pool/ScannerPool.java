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

package org.gnuhpc.fluss.cape.kafka.pool;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.metadata.TablePath;
import org.gnuhpc.fluss.cape.kafka.converter.TablePathResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ScannerPool implements AutoCloseable {
    
    private static final Logger LOG = LoggerFactory.getLogger(ScannerPool.class);
    
    private final Connection flussConnection;
    private final TablePathResolver tablePathResolver;
    private final Map<ScannerKey, PooledScanner> scanners;
    private final int maxPoolSize;
    private final Duration idleTimeout;
    private final ScheduledExecutorService cleanupExecutor;
    private volatile boolean closed = false;
    
    public ScannerPool(
            Connection flussConnection,
            TablePathResolver tablePathResolver,
            int maxPoolSize,
            Duration idleTimeout) {
        this.flussConnection = flussConnection;
        this.tablePathResolver = tablePathResolver;
        this.scanners = new ConcurrentHashMap<>();
        this.maxPoolSize = maxPoolSize;
        this.idleTimeout = idleTimeout;
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "scanner-pool-cleanup");
            t.setDaemon(true);
            return t;
        });
        
        this.cleanupExecutor.scheduleAtFixedRate(
            this::evictIdle,
            idleTimeout.toMillis(),
            idleTimeout.toMillis(),
            TimeUnit.MILLISECONDS
        );
        
        LOG.info("ScannerPool initialized: maxSize={}, idleTimeout={}", maxPoolSize, idleTimeout);
    }
    
    public LogScanner getOrCreate(String topic, int partition) {
        if (closed) {
            throw new IllegalStateException("ScannerPool is closed");
        }
        
        ScannerKey key = new ScannerKey(topic, partition);
        
        PooledScanner pooled = scanners.compute(key, (k, existing) -> {
            if (existing != null && existing.isHealthy()) {
                existing.updateLastUsed();
                return existing;
            }
            
            if (existing != null) {
                LOG.warn("Scanner for {}-{} is unhealthy, recreating", topic, partition);
                try {
                    existing.getScanner().close();
                } catch (Exception e) {
                    LOG.warn("Error closing unhealthy scanner for {}-{}", topic, partition, e);
                }
            }
            
            evictIfNeeded();
            
            try {
                TablePath tablePath = tablePathResolver.resolve(topic);
                Table table = flussConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner();
                scanner.subscribe(partition, 0L);
                
                LOG.debug("Created new scanner for {}-{}", topic, partition);
                return new PooledScanner(scanner, Instant.now(), 0L);
            } catch (Exception e) {
                LOG.error("Failed to create scanner for {}-{}", topic, partition, e);
                throw new RuntimeException("Failed to create scanner", e);
            }
        });
        
        return pooled.getScanner();
    }
    
    public boolean needsReposition(String topic, int partition, long requestedOffset) {
        ScannerKey key = new ScannerKey(topic, partition);
        PooledScanner pooled = scanners.get(key);
        if (pooled == null) {
            return true;
        }
        return pooled.getExpectedNextOffset() != requestedOffset;
    }
    
    public void updateExpectedOffset(String topic, int partition, long nextOffset) {
        ScannerKey key = new ScannerKey(topic, partition);
        PooledScanner pooled = scanners.get(key);
        if (pooled != null) {
            pooled.setExpectedNextOffset(nextOffset);
        }
    }
    
    public void invalidate(String topic, int partition) {
        ScannerKey key = new ScannerKey(topic, partition);
        PooledScanner pooled = scanners.remove(key);
        
        if (pooled != null) {
            try {
                pooled.getScanner().close();
                LOG.debug("Invalidated scanner for {}-{}", topic, partition);
            } catch (Exception e) {
                LOG.warn("Error closing scanner during invalidation for {}-{}", topic, partition, e);
            }
        }
    }
    
    public void evictIdle() {
        if (closed) {
            return;
        }
        
        Instant now = Instant.now();
        List<ScannerKey> toEvict = new ArrayList<>();
        
        for (Map.Entry<ScannerKey, PooledScanner> entry : scanners.entrySet()) {
            PooledScanner pooled = entry.getValue();
            Duration idleDuration = Duration.between(pooled.getLastUsed(), now);
            
            if (idleDuration.compareTo(idleTimeout) > 0) {
                toEvict.add(entry.getKey());
            }
        }
        
        int evicted = 0;
        for (ScannerKey key : toEvict) {
            PooledScanner pooled = scanners.remove(key);
            if (pooled != null) {
                try {
                    pooled.getScanner().close();
                    evicted++;
                } catch (Exception e) {
                    LOG.warn("Error closing idle scanner for {}", key, e);
                }
            }
        }
        
        if (evicted > 0) {
            LOG.info("Evicted {} idle scanners (current pool size: {})", evicted, scanners.size());
        }
    }
    
    private void evictIfNeeded() {
        if (scanners.size() >= maxPoolSize) {
            ScannerKey oldestKey = null;
            Instant oldest = Instant.now();
            
            for (Map.Entry<ScannerKey, PooledScanner> entry : scanners.entrySet()) {
                Instant lastUsed = entry.getValue().getLastUsed();
                if (lastUsed.isBefore(oldest)) {
                    oldest = lastUsed;
                    oldestKey = entry.getKey();
                }
            }
            
            if (oldestKey != null) {
                PooledScanner evicted = scanners.remove(oldestKey);
                if (evicted != null) {
                    try {
                        evicted.getScanner().close();
                        LOG.info("Evicted LRU scanner {} to make room (pool size: {})", 
                                oldestKey, scanners.size());
                    } catch (Exception e) {
                        LOG.warn("Error closing evicted scanner {}", oldestKey, e);
                    }
                }
            }
        }
    }
    
    @Override
    public void close() {
        if (closed) {
            return;
        }
        
        closed = true;
        cleanupExecutor.shutdownNow();
        
        LOG.info("Closing ScannerPool with {} active scanners", scanners.size());
        
        for (Map.Entry<ScannerKey, PooledScanner> entry : scanners.entrySet()) {
            try {
                entry.getValue().getScanner().close();
            } catch (Exception e) {
                LOG.warn("Error closing scanner {} during pool shutdown", entry.getKey(), e);
            }
        }
        
        scanners.clear();
        LOG.info("ScannerPool closed");
    }
    
    public int size() {
        return scanners.size();
    }
    
    private static class PooledScanner {
        private final LogScanner scanner;
        private final Instant created;
        private volatile Instant lastUsed;
        private final AtomicLong useCount;
        private volatile long expectedNextOffset;
        
        PooledScanner(LogScanner scanner, Instant created, long initialOffset) {
            this.scanner = scanner;
            this.created = created;
            this.lastUsed = created;
            this.useCount = new AtomicLong(0);
            this.expectedNextOffset = initialOffset;
        }
        
        LogScanner getScanner() {
            return scanner;
        }
        
        Instant getLastUsed() {
            return lastUsed;
        }
        
        void updateLastUsed() {
            this.lastUsed = Instant.now();
            this.useCount.incrementAndGet();
        }
        
        long getExpectedNextOffset() {
            return expectedNextOffset;
        }
        
        void setExpectedNextOffset(long offset) {
            this.expectedNextOffset = offset;
        }
        
        boolean isHealthy() {
            return true;
        }
    }
    
    private static class ScannerKey {
        private final String topic;
        private final int partition;
        
        ScannerKey(String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ScannerKey that = (ScannerKey) o;
            return partition == that.partition && Objects.equals(topic, that.topic);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(topic, partition);
        }
        
        @Override
        public String toString() {
            return topic + "-" + partition;
        }
    }
}
