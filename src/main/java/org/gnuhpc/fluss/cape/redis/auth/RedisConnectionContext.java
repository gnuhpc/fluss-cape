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

package org.gnuhpc.fluss.cape.redis.auth;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

/**
 * Stores per-connection authentication state for Redis protocol.
 *
 * <p>This context is attached to each Netty channel and tracks:
 * <ul>
 *   <li>Authentication status (whether client has provided valid credentials)</li>
 *   <li>Connection creation time (for rate limiting and timeout)</li>
 *   <li>Request count (for rate limiting)</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 * // In connection handler:
 * RedisConnectionContext ctx = RedisConnectionContext.get(channel);
 * if (!ctx.isAuthenticated()) {
 *     return RedisResponse.error("NOAUTH Authentication required");
 * }
 * </pre>
 */
public class RedisConnectionContext {

    // Channel attribute key for storing context
    private static final AttributeKey<RedisConnectionContext> CONTEXT_KEY =
            AttributeKey.valueOf("redis.connection.context");

    private volatile boolean authenticated;
    private final long connectionTime;
    private volatile long requestCount;
    private volatile long lastRequestTime;
    
    // Rate limiting for AUTH command (brute force prevention)
    private volatile int failedAuthAttempts;
    private volatile long lastFailedAuthTime;
    private volatile long authBlockedUntil;

    private RedisConnectionContext() {
        this.authenticated = false;
        this.connectionTime = System.currentTimeMillis();
        this.requestCount = 0;
        this.lastRequestTime = connectionTime;
        this.failedAuthAttempts = 0;
        this.lastFailedAuthTime = 0;
        this.authBlockedUntil = 0;
    }

    /**
     * Get or create connection context for a channel.
     *
     * @param channel the Netty channel
     * @return the connection context
     */
    public static RedisConnectionContext get(Channel channel) {
        RedisConnectionContext ctx = channel.attr(CONTEXT_KEY).get();
        if (ctx == null) {
            ctx = new RedisConnectionContext();
            RedisConnectionContext existing = channel.attr(CONTEXT_KEY).setIfAbsent(ctx);
            if (existing != null) {
                ctx = existing;
            }
        }
        return ctx;
    }

    /**
     * Check if the connection is authenticated.
     *
     * @return true if authenticated, false otherwise
     */
    public boolean isAuthenticated() {
        return authenticated;
    }

    /**
     * Mark the connection as authenticated.
     */
    public void setAuthenticated(boolean authenticated) {
        this.authenticated = authenticated;
    }

    /**
     * Get the connection creation time in milliseconds.
     *
     * @return connection time
     */
    public long getConnectionTime() {
        return connectionTime;
    }

    /**
     * Increment and get the request count.
     *
     * @return the new request count
     */
    public long incrementRequestCount() {
        this.lastRequestTime = System.currentTimeMillis();
        return ++this.requestCount;
    }

    /**
     * Get the current request count.
     *
     * @return request count
     */
    public long getRequestCount() {
        return requestCount;
    }

    /**
     * Get the last request time in milliseconds.
     *
     * @return last request time
     */
    public long getLastRequestTime() {
        return lastRequestTime;
    }

    /**
     * Calculate the connection age in milliseconds.
     *
     * @return connection age
     */
    public long getConnectionAge() {
        return System.currentTimeMillis() - connectionTime;
    }

    /**
     * Calculate the idle time since last request in milliseconds.
     *
     * @return idle time
     */
    public long getIdleTime() {
        return System.currentTimeMillis() - lastRequestTime;
    }

    /**
     * Check if authentication is currently rate-limited (blocked).
     *
     * @return true if blocked, false otherwise
     */
    public boolean isAuthRateLimited() {
        long now = System.currentTimeMillis();
        return authBlockedUntil > now;
    }

    /**
     * Get remaining time in milliseconds until auth is unblocked.
     *
     * @return milliseconds until unblocked, or 0 if not blocked
     */
    public long getAuthBlockedRemaining() {
        long now = System.currentTimeMillis();
        long remaining = authBlockedUntil - now;
        return Math.max(0, remaining);
    }

    /**
     * Record a failed authentication attempt.
     * Implements exponential backoff for brute force protection.
     *
     * <p>Backoff strategy:
     * <ul>
     *   <li>Attempts 1-2: No blocking</li>
     *   <li>Attempt 3: Block for 1 second</li>
     *   <li>Attempt 4: Block for 2 seconds</li>
     *   <li>Attempt 5: Block for 4 seconds</li>
     *   <li>Attempt 6+: Block for 8 seconds</li>
     * </ul>
     *
     * @return milliseconds to wait before next attempt
     */
    public long recordFailedAuthAttempt() {
        long now = System.currentTimeMillis();
        
        // Reset counter if last failure was more than 60 seconds ago
        if (now - lastFailedAuthTime > 60000) {
            failedAuthAttempts = 0;
        }
        
        failedAuthAttempts++;
        lastFailedAuthTime = now;
        
        // Calculate exponential backoff: 2^(attempts-3) seconds, capped at 8 seconds
        long blockDurationMs = 0;
        if (failedAuthAttempts >= 3) {
            int exponent = Math.min(failedAuthAttempts - 3, 3); // Cap at 2^3 = 8 seconds
            blockDurationMs = (long) Math.pow(2, exponent) * 1000;
            authBlockedUntil = now + blockDurationMs;
        }
        
        return blockDurationMs;
    }

    /**
     * Reset failed authentication attempts (called on successful auth).
     */
    public void resetFailedAuthAttempts() {
        failedAuthAttempts = 0;
        lastFailedAuthTime = 0;
        authBlockedUntil = 0;
    }

    /**
     * Get the number of failed authentication attempts.
     *
     * @return failed attempt count
     */
    public int getFailedAuthAttempts() {
        return failedAuthAttempts;
    }

    @Override
    public String toString() {
        return "RedisConnectionContext{"
                + "authenticated="
                + authenticated
                + ", connectionTime="
                + connectionTime
                + ", requestCount="
                + requestCount
                + ", connectionAge="
                + getConnectionAge()
                + "ms, idleTime="
                + getIdleTime()
                + "ms}";
    }
}
