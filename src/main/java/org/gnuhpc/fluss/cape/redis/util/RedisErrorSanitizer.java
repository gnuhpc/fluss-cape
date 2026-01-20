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

import org.gnuhpc.fluss.cape.redis.protocol.RedisResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.redis.RedisMessage;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Utility class for sanitizing error messages before returning them to Redis clients.
 *
 * <p>This class prevents information disclosure by:
 * <ul>
 *   <li>Logging full exception details server-side for debugging</li>
 *   <li>Returning generic error messages to clients</li>
 *   <li>Preventing stack traces from leaking internal architecture</li>
 * </ul>
 *
 * <p><b>Security Note:</b> Detailed error messages can reveal:
 * <ul>
 *   <li>Internal class names and package structure</li>
 *   <li>File paths and line numbers</li>
 *   <li>Database schema and field names</li>
 *   <li>Framework versions and implementation details</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * try {
 *     return adapter.get(key);
 * } catch (Exception e) {
 *     return RedisErrorSanitizer.sanitizeError(e, "GET", key);
 * }
 * }</pre>
 */
public class RedisErrorSanitizer {

    private static final Logger LOG = LoggerFactory.getLogger(RedisErrorSanitizer.class);

    /** Private constructor to prevent instantiation. */
    private RedisErrorSanitizer() {}

    /**
     * Sanitizes an exception and returns a generic error response suitable for Redis clients.
     *
     * <p>The full exception with stack trace is logged server-side for debugging, but only a
     * generic error message is returned to the client to prevent information disclosure.
     *
     * @param exception the exception to sanitize
     * @param operation the Redis command that failed (e.g., "GET", "SET", "HGETALL")
     * @param key the key involved in the operation (may be null for some operations)
     * @return a RedisMessage with a sanitized error message
     */
    public static RedisMessage sanitizeError(Exception exception, String operation, String key) {
        // Log full details server-side for debugging
        if (key != null) {
            LOG.error("Redis operation failed: {} key={}", operation, key, exception);
        } else {
            LOG.error("Redis operation failed: {}", operation, exception);
        }

        // Return generic error to client based on exception type
        return createGenericError(exception);
    }

    /**
     * Sanitizes an exception and returns a generic error response (overload without key).
     *
     * @param exception the exception to sanitize
     * @param operation the Redis command that failed
     * @return a RedisMessage with a sanitized error message
     */
    public static RedisMessage sanitizeError(Exception exception, String operation) {
        return sanitizeError(exception, operation, null);
    }

    /**
     * Creates a generic error response based on the exception type.
     *
     * <p>This method categorizes exceptions into broad categories and returns appropriate
     * Redis-compatible error messages that don't leak implementation details.
     *
     * @param exception the exception to categorize
     * @return a generic RedisMessage error
     */
    private static RedisMessage createGenericError(Exception exception) {
        if (exception instanceof NumberFormatException) {
            return RedisResponse.error("ERR value is not an integer or out of range");
        } else if (exception instanceof IllegalArgumentException) {
            return RedisResponse.error("ERR invalid argument");
        } else if (exception instanceof NullPointerException) {
            return RedisResponse.error("ERR invalid key or value");
        } else if (exception instanceof TimeoutException) {
            return RedisResponse.error("ERR operation timeout");
        } else if (exception instanceof IOException) {
            return RedisResponse.error("ERR I/O error");
        } else if (exception instanceof ClassCastException) {
            return RedisResponse.error("WRONGTYPE Operation against a key holding the wrong kind of value");
        } else if (exception instanceof UnsupportedOperationException) {
            return RedisResponse.error("ERR operation not supported");
        } else if (exception instanceof IndexOutOfBoundsException) {
            return RedisResponse.error("ERR index out of range");
        }

        // Check exception message for common patterns (without exposing the full message)
        String message = exception.getMessage();
        if (message != null) {
            String lowerMessage = message.toLowerCase();
            
            if (lowerMessage.contains("not found") || lowerMessage.contains("does not exist")) {
                return RedisResponse.error("ERR key not found");
            } else if (lowerMessage.contains("already exists") || lowerMessage.contains("duplicate")) {
                return RedisResponse.error("ERR key already exists");
            } else if (lowerMessage.contains("permission") || lowerMessage.contains("access denied")) {
                return RedisResponse.error("ERR permission denied");
            } else if (lowerMessage.contains("connection") || lowerMessage.contains("network")) {
                return RedisResponse.error("ERR connection error");
            } else if (lowerMessage.contains("timeout")) {
                return RedisResponse.error("ERR operation timeout");
            } else if (lowerMessage.contains("parse") || lowerMessage.contains("invalid")) {
                return RedisResponse.error("ERR syntax error");
            }
        }

        // Default generic error for unknown exception types
        return RedisResponse.error("ERR internal server error");
    }

    /**
     * Creates a sanitized error response for authentication failures.
     *
     * <p>Authentication errors require special handling to prevent user enumeration attacks.
     *
     * @param exception the authentication exception
     * @return a generic authentication error response
     */
    public static RedisMessage sanitizeAuthError(Exception exception) {
        LOG.error("Redis authentication failed", exception);
        // Don't distinguish between "user not found" and "wrong password"
        return RedisResponse.error("ERR invalid password");
    }

    /**
     * Creates a sanitized error response for rate limiting.
     *
     * @param operation the operation that was rate limited
     * @return a rate limit error response
     */
    public static RedisMessage createRateLimitError(String operation) {
        LOG.warn("Rate limit exceeded for operation: {}", operation);
        return RedisResponse.error("ERR rate limit exceeded, try again later");
    }

    /**
     * Creates a sanitized error response for command syntax errors.
     *
     * @param command the command that had a syntax error
     * @return a syntax error response
     */
    public static RedisMessage createSyntaxError(String command) {
        LOG.debug("Syntax error in command: {}", command);
        return RedisResponse.error("ERR syntax error");
    }

    /**
     * Creates a sanitized error response for wrong number of arguments.
     *
     * @param command the command that had wrong number of arguments
     * @return a wrong number of arguments error response
     */
    public static RedisMessage createWrongArgsError(String command) {
        LOG.debug("Wrong number of arguments for command: {}", command);
        return RedisResponse.error("ERR wrong number of arguments for '" + command + "' command");
    }
}
