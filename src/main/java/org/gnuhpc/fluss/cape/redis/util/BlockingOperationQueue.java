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

package org.gnuhpc.fluss.cape.redis.util;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * True async blocking queue manager for Redis blocking operations (BLPOP, BRPOP, etc.).
 * 
 * <p>Uses Netty ChannelHandlerContext for true async I/O without thread blocking:
 * - Clients register blocking requests with timeout
 * - When data becomes available, waiting clients are notified immediately
 * - Zero latency wakeup (no polling overhead)
 * - Timeout handled via Netty's scheduler
 */
public class BlockingOperationQueue {

    private static final Logger LOG = LoggerFactory.getLogger(BlockingOperationQueue.class);

    /**
     * Represents a client waiting for data on specific keys.
     */
    public static class BlockingRequest {
        public final ChannelHandlerContext ctx;
        public final List<String> keys;
        public final long timeoutSeconds;
        public final long registeredAt;
        public ScheduledFuture<?> timeoutTask;

        public BlockingRequest(
                ChannelHandlerContext ctx,
                List<String> keys,
                long timeoutSeconds) {
            this.ctx = ctx;
            this.keys = keys;
            this.timeoutSeconds = timeoutSeconds;
            this.registeredAt = System.currentTimeMillis();
        }
    }

    // Map: key -> list of waiting clients
    private final Map<String, List<BlockingRequest>> waitingClients;

    public BlockingOperationQueue() {
        this.waitingClients = new ConcurrentHashMap<>();
    }

    /**
     * Register a client to wait for data on any of the specified keys.
     *
     * @param ctx Netty channel context
     * @param keys List of keys to monitor (checked in order)
     * @param timeoutSeconds Timeout in seconds (0 = infinite)
     */
    public void registerBlockingRequest(
            ChannelHandlerContext ctx,
            List<String> keys,
            long timeoutSeconds) {

        BlockingRequest request = new BlockingRequest(ctx, keys, timeoutSeconds);

        for (String key : keys) {
            waitingClients.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>())
                    .add(request);
        }

        LOG.debug("Client {} registered blocking request on keys {} with timeout {}s",
                ctx.channel().id(), keys, timeoutSeconds);

        if (timeoutSeconds > 0) {
            request.timeoutTask = ctx.executor().schedule(() -> {
                handleTimeout(request);
            }, timeoutSeconds, TimeUnit.SECONDS);
        }
    }

    /**
     * Notify waiting clients when data becomes available for a key.
     *
     * @param key The key that now has data
     * @param responseSupplier Function to generate response for the first waiter
     * @return true if a client was notified, false if no one was waiting
     */
    public boolean notifyDataAvailable(
            String key,
            java.util.function.Supplier<RedisMessage> responseSupplier) {

        List<BlockingRequest> waiters = waitingClients.get(key);
        if (waiters == null || waiters.isEmpty()) {
            return false;
        }

        BlockingRequest request = null;
        while (!waiters.isEmpty()) {
            BlockingRequest candidate = waiters.remove(0);
            if (candidate.ctx.channel().isActive()) {
                request = candidate;
                break;
            }
        }

        if (request == null) {
            if (waiters.isEmpty()) {
                waitingClients.remove(key);
            }
            return false;
        }

        for (String k : request.keys) {
            List<BlockingRequest> list = waitingClients.get(k);
            if (list != null) {
                list.remove(request);
            }
        }

        if (request.timeoutTask != null) {
            request.timeoutTask.cancel(false);
        }

        RedisMessage response = responseSupplier.get();
        request.ctx.writeAndFlush(response);

        LOG.debug("Notified client {} with data for key {}", 
                request.ctx.channel().id(), key);

        return true;
    }

    /**
     * Handle timeout for a blocking request.
     */
    private void handleTimeout(BlockingRequest request) {
        LOG.debug("Blocking request timed out for client {} on keys {}",
                request.ctx.channel().id(), request.keys);

        for (String key : request.keys) {
            List<BlockingRequest> list = waitingClients.get(key);
            if (list != null) {
                list.remove(request);
            }
        }

        if (request.ctx.channel().isActive()) {
            request.ctx.writeAndFlush(
                    org.gnuhpc.fluss.cape.redis.protocol.RedisResponse.nullBulkString());
        }
    }

    /**
     * Cancel all blocking requests for a channel (on disconnect).
     */
    public void cancelAllForChannel(ChannelHandlerContext ctx) {
        List<String> keysToClean = new ArrayList<>();

        for (Map.Entry<String, List<BlockingRequest>> entry : waitingClients.entrySet()) {
            List<BlockingRequest> waiters = entry.getValue();
            waiters.removeIf(req -> {
                if (req.ctx == ctx) {
                    if (req.timeoutTask != null) {
                        req.timeoutTask.cancel(false);
                    }
                    return true;
                }
                return false;
            });

            if (waiters.isEmpty()) {
                keysToClean.add(entry.getKey());
            }
        }

        for (String key : keysToClean) {
            waitingClients.remove(key);
        }

        LOG.debug("Cancelled all blocking requests for channel {}", ctx.channel().id());
    }

    /**
     * Get count of waiting clients for a key (for monitoring).
     */
    public int getWaitingCount(String key) {
        List<BlockingRequest> waiters = waitingClients.get(key);
        return waiters != null ? waiters.size() : 0;
    }

    /**
     * Get total count of all waiting clients (for monitoring).
     */
    public int getTotalWaitingCount() {
        return waitingClients.values().stream()
                .mapToInt(List::size)
                .sum();
    }
}
