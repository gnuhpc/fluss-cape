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

package org.gnuhpc.fluss.cape.redis.security;

import org.gnuhpc.fluss.cape.redis.protocol.RedisResponse;
import org.gnuhpc.fluss.cape.redis.util.RedisErrorSanitizer;

import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Redis Error Sanitization Security Tests")
class RedisErrorSanitizationTest {

    @Test
    @DisplayName("NullPointerException should not leak class names or stack traces")
    void testNullPointerExceptionSanitized() {
        Exception exception = new NullPointerException("Cannot invoke \"String.length()\" because \"key\" is null");
        
        RedisMessage response = RedisErrorSanitizer.sanitizeError(exception, "GET");
        
        assertThat(response).isInstanceOf(ErrorRedisMessage.class);
        String errorMsg = ((ErrorRedisMessage) response).content();
        
        assertThat(errorMsg).doesNotContain("NullPointerException");
        assertThat(errorMsg).doesNotContain("String.length");
        assertThat(errorMsg).doesNotContain("invoke");
        assertThat(errorMsg).doesNotContain(".java");
        assertThat(errorMsg).startsWith("ERR");
        assertThat(errorMsg).containsAnyOf("invalid key", "invalid value");
    }

    @Test
    @DisplayName("IllegalArgumentException should return generic invalid argument error")
    void testIllegalArgumentExceptionSanitized() {
        Exception exception = new IllegalArgumentException("Invalid bucket index: -1 for table users_table");
        
        RedisMessage response = RedisErrorSanitizer.sanitizeError(exception, "HSET");
        
        assertThat(response).isInstanceOf(ErrorRedisMessage.class);
        String errorMsg = ((ErrorRedisMessage) response).content();
        
        assertThat(errorMsg).isEqualTo("ERR invalid argument");
        assertThat(errorMsg).doesNotContain("bucket");
        assertThat(errorMsg).doesNotContain("users_table");
        assertThat(errorMsg).doesNotContain("-1");
    }

    @Test
    @DisplayName("IOException should not leak file paths or system details")
    void testIOExceptionSanitized() {
        Exception exception = new IOException("Failed to write to /var/lib/fluss/data/shard_42/segment_1024.log");
        
        RedisMessage response = RedisErrorSanitizer.sanitizeError(exception, "SET");
        
        assertThat(response).isInstanceOf(ErrorRedisMessage.class);
        String errorMsg = ((ErrorRedisMessage) response).content();
        
        assertThat(errorMsg).isEqualTo("ERR I/O error");
        assertThat(errorMsg).doesNotContain("/var/lib");
        assertThat(errorMsg).doesNotContain("shard_42");
        assertThat(errorMsg).doesNotContain(".log");
    }

    @Test
    @DisplayName("TimeoutException should not reveal internal timing details")
    void testTimeoutExceptionSanitized() {
        Exception exception = new TimeoutException("Operation timed out after 5000ms waiting for coordinator lock");
        
        RedisMessage response = RedisErrorSanitizer.sanitizeError(exception, "ZADD");
        
        assertThat(response).isInstanceOf(ErrorRedisMessage.class);
        String errorMsg = ((ErrorRedisMessage) response).content();
        
        assertThat(errorMsg).isEqualTo("ERR operation timeout");
        assertThat(errorMsg).doesNotContain("5000");
        assertThat(errorMsg).doesNotContain("coordinator");
        assertThat(errorMsg).doesNotContain("lock");
    }

    @Test
    @DisplayName("NumberFormatException should not leak parsing details")
    void testNumberFormatExceptionSanitized() {
        Exception exception = new NumberFormatException("For input string: \"invalid_score_value\"");
        
        RedisMessage response = RedisErrorSanitizer.sanitizeError(exception, "ZADD");
        
        assertThat(response).isInstanceOf(ErrorRedisMessage.class);
        String errorMsg = ((ErrorRedisMessage) response).content();
        
        assertThat(errorMsg).isEqualTo("ERR value is not an integer or out of range");
        assertThat(errorMsg).doesNotContain("input string");
        assertThat(errorMsg).doesNotContain("invalid_score_value");
    }

    @Test
    @DisplayName("ClassCastException should return WRONGTYPE error")
    void testClassCastExceptionSanitized() {
        Exception exception = new ClassCastException("Cannot cast org.gnuhpc.fluss.cape.redis.data.StringValue to HashValue");
        
        RedisMessage response = RedisErrorSanitizer.sanitizeError(exception, "HGETALL");
        
        assertThat(response).isInstanceOf(ErrorRedisMessage.class);
        String errorMsg = ((ErrorRedisMessage) response).content();
        
        assertThat(errorMsg).isEqualTo("WRONGTYPE Operation against a key holding the wrong kind of value");
        assertThat(errorMsg).doesNotContain("org.gnuhpc");
        assertThat(errorMsg).doesNotContain("StringValue");
        assertThat(errorMsg).doesNotContain("HashValue");
    }

    @Test
    @DisplayName("UnsupportedOperationException should return operation not supported")
    void testUnsupportedOperationExceptionSanitized() {
        Exception exception = new UnsupportedOperationException("GEO operations require Fluss 0.9+ with spatial index");
        
        RedisMessage response = RedisErrorSanitizer.sanitizeError(exception, "GEOADD");
        
        assertThat(response).isInstanceOf(ErrorRedisMessage.class);
        String errorMsg = ((ErrorRedisMessage) response).content();
        
        assertThat(errorMsg).isEqualTo("ERR operation not supported");
        assertThat(errorMsg).doesNotContain("Fluss");
        assertThat(errorMsg).doesNotContain("spatial index");
    }

    @Test
    @DisplayName("IndexOutOfBoundsException should return index out of range")
    void testIndexOutOfBoundsExceptionSanitized() {
        Exception exception = new IndexOutOfBoundsException("Index: 42, Size: 10");
        
        RedisMessage response = RedisErrorSanitizer.sanitizeError(exception, "LINDEX");
        
        assertThat(response).isInstanceOf(ErrorRedisMessage.class);
        String errorMsg = ((ErrorRedisMessage) response).content();
        
        assertThat(errorMsg).isEqualTo("ERR index out of range");
        assertThat(errorMsg).doesNotContain("42");
        assertThat(errorMsg).doesNotContain("10");
    }

    @Test
    @DisplayName("Generic exception with 'not found' message should return key not found")
    void testNotFoundExceptionSanitized() {
        Exception exception = new RuntimeException("Table 'users_temp_1234' not found in catalog");
        
        RedisMessage response = RedisErrorSanitizer.sanitizeError(exception, "GET");
        
        assertThat(response).isInstanceOf(ErrorRedisMessage.class);
        String errorMsg = ((ErrorRedisMessage) response).content();
        
        assertThat(errorMsg).isEqualTo("ERR key not found");
        assertThat(errorMsg).doesNotContain("users_temp_1234");
        assertThat(errorMsg).doesNotContain("catalog");
    }

    @Test
    @DisplayName("Generic exception with 'already exists' message should return key already exists")
    void testAlreadyExistsExceptionSanitized() {
        Exception exception = new RuntimeException("Key 'session:abc123' already exists in shard 7");
        
        RedisMessage response = RedisErrorSanitizer.sanitizeError(exception, "SETNX");
        
        assertThat(response).isInstanceOf(ErrorRedisMessage.class);
        String errorMsg = ((ErrorRedisMessage) response).content();
        
        assertThat(errorMsg).isEqualTo("ERR key already exists");
        assertThat(errorMsg).doesNotContain("session:abc123");
        assertThat(errorMsg).doesNotContain("shard 7");
    }

    @Test
    @DisplayName("Unknown exception type should return internal server error")
    void testUnknownExceptionSanitized() {
        Exception exception = new RuntimeException("Some completely unknown internal error condition");
        
        RedisMessage response = RedisErrorSanitizer.sanitizeError(exception, "CUSTOM");
        
        assertThat(response).isInstanceOf(ErrorRedisMessage.class);
        String errorMsg = ((ErrorRedisMessage) response).content();
        
        assertThat(errorMsg).isEqualTo("ERR internal server error");
        assertThat(errorMsg).doesNotContain("unknown");
        assertThat(errorMsg).doesNotContain("condition");
    }

    @Test
    @DisplayName("Authentication error should use special sanitization")
    void testAuthenticationErrorSanitized() {
        Exception exception = new SecurityException("User 'admin' authentication failed: invalid credentials");
        
        RedisMessage response = RedisErrorSanitizer.sanitizeAuthError(exception);
        
        assertThat(response).isInstanceOf(ErrorRedisMessage.class);
        String errorMsg = ((ErrorRedisMessage) response).content();
        
        assertThat(errorMsg).isEqualTo("ERR invalid password");
        assertThat(errorMsg).doesNotContain("admin");
        assertThat(errorMsg).doesNotContain("credentials");
        assertThat(errorMsg).doesNotContain("SecurityException");
    }

    @Test
    @DisplayName("Rate limit error should be properly formatted")
    void testRateLimitErrorFormatted() {
        RedisMessage response = RedisErrorSanitizer.createRateLimitError("AUTH");
        
        assertThat(response).isInstanceOf(ErrorRedisMessage.class);
        String errorMsg = ((ErrorRedisMessage) response).content();
        
        assertThat(errorMsg).contains("rate limit");
        assertThat(errorMsg).startsWith("ERR");
    }

    @Test
    @DisplayName("Syntax error should be generic")
    void testSyntaxErrorGeneric() {
        RedisMessage response = RedisErrorSanitizer.createSyntaxError("MALFORMED_COMMAND");
        
        assertThat(response).isInstanceOf(ErrorRedisMessage.class);
        String errorMsg = ((ErrorRedisMessage) response).content();
        
        assertThat(errorMsg).isEqualTo("ERR syntax error");
    }

    @Test
    @DisplayName("Wrong args error should include command name but no sensitive data")
    void testWrongArgsErrorSafe() {
        RedisMessage response = RedisErrorSanitizer.createWrongArgsError("HSET");
        
        assertThat(response).isInstanceOf(ErrorRedisMessage.class);
        String errorMsg = ((ErrorRedisMessage) response).content();
        
        assertThat(errorMsg).contains("HSET");
        assertThat(errorMsg).contains("wrong number of arguments");
        assertThat(errorMsg).startsWith("ERR");
    }

    @Test
    @DisplayName("All error messages should start with ERR or WRONGTYPE")
    void testAllErrorsHaveProperPrefix() {
        Exception[] exceptions = {
            new NullPointerException(),
            new IllegalArgumentException(),
            new IOException(),
            new TimeoutException(),
            new NumberFormatException(),
            new UnsupportedOperationException(),
            new IndexOutOfBoundsException()
        };
        
        for (Exception exception : exceptions) {
            RedisMessage response = RedisErrorSanitizer.sanitizeError(exception, "TEST");
            assertThat(response).isInstanceOf(ErrorRedisMessage.class);
            String errorMsg = ((ErrorRedisMessage) response).content();
            assertThat(errorMsg).matches("^(ERR|WRONGTYPE).*");
        }
    }

    @Test
    @DisplayName("No error message should contain Java package names")
    void testNoJavaPackageNamesInErrors() {
        String[] forbiddenPatterns = {
            "org.gnuhpc",
            "org.apache.fluss",
            "java.lang",
            "java.io",
            "io.netty",
            ".java:",
            "Exception",
            "at com.",
            "at org."
        };
        
        Exception[] exceptions = {
            new NullPointerException("org.gnuhpc.fluss.cape.redis.executor.StringCommandExecutor.get"),
            new IllegalArgumentException("Invalid at org.apache.fluss.client.Connection"),
            new IOException("java.io.FileNotFoundException: /path/to/file"),
            new ClassCastException("Cannot cast org.gnuhpc.Type to java.lang.String")
        };
        
        for (Exception exception : exceptions) {
            RedisMessage response = RedisErrorSanitizer.sanitizeError(exception, "TEST");
            assertThat(response).isInstanceOf(ErrorRedisMessage.class);
            String errorMsg = ((ErrorRedisMessage) response).content();
            
            for (String pattern : forbiddenPatterns) {
                assertThat(errorMsg)
                    .as("Error message should not contain: " + pattern)
                    .doesNotContain(pattern);
            }
        }
    }
}
