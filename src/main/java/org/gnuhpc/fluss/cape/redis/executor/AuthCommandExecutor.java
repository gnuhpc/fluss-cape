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

package org.gnuhpc.fluss.cape.redis.executor;

import org.gnuhpc.fluss.cape.redis.auth.RedisConnectionContext;
import org.gnuhpc.fluss.cape.redis.protocol.RedisCommand;
import org.gnuhpc.fluss.cape.redis.protocol.RedisResponse;
import org.gnuhpc.fluss.cape.redis.util.RedisErrorSanitizer;

import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;

/**
 * Executor for Redis AUTH command.
 *
 * <p>Implements password-based authentication for Redis protocol connections.
 *
 * <p>Supported commands:
 * <ul>
 *   <li>AUTH password - Authenticate with password (Redis &lt;= 5.x)</li>
 *   <li>AUTH username password - Authenticate with username and password (Redis &gt;= 6.x)</li>
 * </ul>
 *
 * <p>Configuration:
 * <ul>
 *   <li>Set REDIS_PASSWORD environment variable to enable authentication</li>
 *   <li>If REDIS_PASSWORD is not set, authentication is disabled (all connections allowed)</li>
 *   <li>Password is hashed with SHA-256 for secure comparison</li>
 * </ul>
 *
 * <p>Security notes:
 * <ul>
 *   <li>Use this in combination with TLS/SSL for production (passwords sent in plain text)</li>
 *   <li>Consider implementing connection rate limiting to prevent brute force attacks</li>
 *   <li>Recommend strong passwords (16+ characters, alphanumeric + symbols)</li>
 * </ul>
 */
public class AuthCommandExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(AuthCommandExecutor.class);

    private final String requiredPassword;
    private final byte[] requiredPasswordHash;
    private final boolean authenticationEnabled;

    /**
     * Creates an AUTH command executor.
     *
     * <p>Authentication is enabled if REDIS_PASSWORD environment variable is set.
     */
    public AuthCommandExecutor() {
        this.requiredPassword = System.getenv("REDIS_PASSWORD");
        this.authenticationEnabled = (requiredPassword != null && !requiredPassword.isEmpty());

        if (authenticationEnabled) {
            this.requiredPasswordHash = hashPassword(requiredPassword);
            LOG.info("Redis authentication ENABLED (password configured)");
        } else {
            this.requiredPasswordHash = null;
            LOG.warn(
                    "Redis authentication DISABLED - all connections allowed. "
                            + "Set REDIS_PASSWORD environment variable to enable authentication.");
        }
    }

    /**
     * Creates an AUTH command executor with explicit password.
     *
     * @param password the required password (null or empty to disable authentication)
     */
    public AuthCommandExecutor(String password) {
        this.requiredPassword = password;
        this.authenticationEnabled = (password != null && !password.isEmpty());

        if (authenticationEnabled) {
            this.requiredPasswordHash = hashPassword(password);
            LOG.info("Redis authentication ENABLED (password configured)");
        } else {
            this.requiredPasswordHash = null;
            LOG.warn("Redis authentication DISABLED - all connections allowed");
        }
    }

    @Override
    public RedisMessage execute(RedisCommand command) {
        String cmd = command.getCommand();

        if (!"AUTH".equals(cmd)) {
            return RedisResponse.error("ERR unknown command '" + cmd + "'");
        }

        try {
            return executeAuth(command);
        } catch (Exception e) {
            return RedisErrorSanitizer.sanitizeAuthError(e);
        }
    }

    /**
     * Execute AUTH command.
     *
     * <p>Syntax:
     * <ul>
     *   <li>AUTH password - Redis &lt;= 5.x style</li>
     *   <li>AUTH username password - Redis &gt;= 6.x style (username ignored)</li>
     * </ul>
     *
     * @param command the AUTH command
     * @return OK if authentication succeeds, error otherwise
     */
    private RedisMessage executeAuth(RedisCommand command) {
        int argCount = command.getArgCount();
        RedisConnectionContext ctx = RedisConnectionContext.get(command.getContext().channel());

        if (ctx.isAuthRateLimited()) {
            long remainingMs = ctx.getAuthBlockedRemaining();
            LOG.warn(
                    "AUTH rate limited from {} - {} failed attempts, blocked for {} more ms",
                    command.getContext().channel().remoteAddress(),
                    ctx.getFailedAuthAttempts(),
                    remainingMs);
            return RedisResponse.error(
                    String.format(
                            "ERR Too many authentication failures. Try again in %.1f seconds.",
                            remainingMs / 1000.0));
        }

        if (!authenticationEnabled) {
            ctx.setAuthenticated(true);
            LOG.debug("AUTH command accepted (authentication disabled)");
            return RedisResponse.ok();
        }

        // Validate argument count
        if (argCount < 1 || argCount > 2) {
            return RedisResponse.error(
                    "ERR wrong number of arguments for 'auth' command");
        }

        // Extract password (last argument, supports both AUTH password and AUTH username password)
        String providedPassword;
        if (argCount == 1) {
            // AUTH password (Redis <= 5.x)
            providedPassword = command.getArgAsString(0);
        } else {
            // AUTH username password (Redis >= 6.x) - ignore username
            providedPassword = command.getArgAsString(1);
            LOG.debug("AUTH with username (ignored): {}", command.getArgAsString(0));
        }

        if (providedPassword == null || providedPassword.isEmpty()) {
            return RedisResponse.error("ERR invalid password");
        }

        byte[] providedHash = hashPassword(providedPassword);
        boolean valid = MessageDigest.isEqual(providedHash, requiredPasswordHash);

        if (valid) {
            ctx.setAuthenticated(true);
            ctx.resetFailedAuthAttempts();
            LOG.info(
                    "Client authenticated successfully from {}",
                    command.getContext().channel().remoteAddress());
            return RedisResponse.ok();
        } else {
            long blockDurationMs = ctx.recordFailedAuthAttempt();
            LOG.warn(
                    "Authentication failed from {} - attempt #{}, blocked for {}ms",
                    command.getContext().channel().remoteAddress(),
                    ctx.getFailedAuthAttempts(),
                    blockDurationMs);
            return RedisResponse.error("WRONGPASS invalid username-password pair or user is disabled.");
        }
    }

    /**
     * Hash password using SHA-256 for secure comparison.
     *
     * @param password the password to hash
     * @return the SHA-256 hash
     */
    private byte[] hashPassword(String password) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(password.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new RuntimeException("Failed to hash password", e);
        }
    }

    /**
     * Check if authentication is enabled.
     *
     * @return true if authentication is enabled, false otherwise
     */
    public boolean isAuthenticationEnabled() {
        return authenticationEnabled;
    }
}
