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
import org.apache.fluss.client.table.writer.AppendWriter;
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

public class WriterPool implements AutoCloseable {
    
    private static final Logger LOG = LoggerFactory.getLogger(WriterPool.class);
    
    private final Connection flussConnection;
    private final TablePathResolver tablePathResolver;
    private final Map<TablePath, PooledWriter> writers;
    private final int maxPoolSize;
    private final Duration idleTimeout;
    private final ScheduledExecutorService cleanupExecutor;
    private volatile boolean closed = false;
    
    public WriterPool(
            Connection flussConnection,
            TablePathResolver tablePathResolver,
            int maxPoolSize,
            Duration idleTimeout) {
        this.flussConnection = flussConnection;
        this.tablePathResolver = tablePathResolver;
        this.writers = new ConcurrentHashMap<>();
        this.maxPoolSize = maxPoolSize;
        this.idleTimeout = idleTimeout;
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "writer-pool-cleanup");
            t.setDaemon(true);
            return t;
        });
        
        this.cleanupExecutor.scheduleAtFixedRate(
            this::evictIdle,
            idleTimeout.toMillis(),
            idleTimeout.toMillis(),
            TimeUnit.MILLISECONDS
        );
        
        LOG.info("WriterPool initialized: maxSize={}, idleTimeout={}", maxPoolSize, idleTimeout);
    }
    
    public AppendWriter getOrCreate(String topic) {
        if (closed) {
            throw new IllegalStateException("WriterPool is closed");
        }
        
        TablePath tablePath = tablePathResolver.resolve(topic);
        
        PooledWriter pooled = writers.compute(tablePath, (k, existing) -> {
            if (existing != null && existing.isHealthy()) {
                existing.updateLastUsed();
                return existing;
            }
            
            // Fluss 0.8 AppendWriter doesn't need explicit close - just drop reference
            if (existing != null) {
                LOG.warn("Writer for {} is unhealthy, recreating", tablePath);
            }
            
            evictIfNeeded();
            
            try {
                LOG.info("Creating new AppendWriter for {}", tablePath);
                Table table = flussConnection.getTable(tablePath);
                LOG.info("Got table, calling newAppend().createWriter() for {}", tablePath);
                AppendWriter writer = table.newAppend().createWriter();
                LOG.info("Successfully created new writer for {}", tablePath);
                
                return new PooledWriter(writer, Instant.now());
            } catch (Exception e) {
                LOG.error("Failed to create writer for {}", tablePath, e);
                throw new RuntimeException("Failed to create writer", e);
            }
        });
        
        return pooled.getWriter();
    }
    
    public void invalidate(String topic) {
        TablePath tablePath = tablePathResolver.resolve(topic);
        PooledWriter pooled = writers.remove(tablePath);
        
        if (pooled != null) {
            // Fluss 0.8 AppendWriter doesn't need explicit close
            LOG.debug("Invalidated writer for {}", tablePath);
        }
    }
    
    public void evictIdle() {
        if (closed) {
            return;
        }
        
        Instant now = Instant.now();
        List<TablePath> toEvict = new ArrayList<>();
        
        for (Map.Entry<TablePath, PooledWriter> entry : writers.entrySet()) {
            PooledWriter pooled = entry.getValue();
            Duration idleDuration = Duration.between(pooled.getLastUsed(), now);
            
            if (idleDuration.compareTo(idleTimeout) > 0) {
                toEvict.add(entry.getKey());
            }
        }
        
        int evicted = 0;
        for (TablePath key : toEvict) {
            if (writers.remove(key) != null) {
                evicted++;
            }
        }
        
        if (evicted > 0) {
            LOG.info("Evicted {} idle writers (current pool size: {})", evicted, writers.size());
        }
    }
    
    private void evictIfNeeded() {
        if (writers.size() >= maxPoolSize) {
            TablePath oldestKey = null;
            Instant oldest = Instant.now();
            
            for (Map.Entry<TablePath, PooledWriter> entry : writers.entrySet()) {
                Instant lastUsed = entry.getValue().getLastUsed();
                if (lastUsed.isBefore(oldest)) {
                    oldest = lastUsed;
                    oldestKey = entry.getKey();
                }
            }
            
            if (oldestKey != null) {
                if (writers.remove(oldestKey) != null) {
                    LOG.info("Evicted LRU writer: {}", oldestKey);
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
        
        LOG.info("Closing WriterPool with {} active writers", writers.size());
        writers.clear();
        LOG.info("WriterPool closed");
    }
    
    public int size() {
        return writers.size();
    }
    
    private static class PooledWriter {
        private final AppendWriter writer;
        private final Instant created;
        private volatile Instant lastUsed;
        private final AtomicLong useCount;
        
        PooledWriter(AppendWriter writer, Instant created) {
            this.writer = writer;
            this.created = created;
            this.lastUsed = created;
            this.useCount = new AtomicLong(0);
        }
        
        AppendWriter getWriter() {
            return writer;
        }
        
        Instant getLastUsed() {
            return lastUsed;
        }
        
        void updateLastUsed() {
            this.lastUsed = Instant.now();
            this.useCount.incrementAndGet();
        }
        
        boolean isHealthy() {
            return true;
        }
    }
}
