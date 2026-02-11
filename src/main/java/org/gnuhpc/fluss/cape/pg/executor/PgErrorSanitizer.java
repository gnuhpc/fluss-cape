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

package org.gnuhpc.fluss.cape.pg.executor;

final class PgErrorSanitizer {

    private static final int MAX_MESSAGE_LENGTH = 512;

    private PgErrorSanitizer() {
    }

    static String sanitize(Exception exception) {
        if (exception instanceof IllegalArgumentException) {
            return sanitizeMessage(exception.getMessage(), "invalid request");
        }
        return "internal error";
    }

    private static String sanitizeMessage(String message, String fallback) {
        if (message == null || message.isBlank()) {
            return fallback;
        }
        String normalized = message.replace("\r", " ").replace("\n", " ").trim();
        if (normalized.length() > MAX_MESSAGE_LENGTH) {
            return normalized.substring(0, MAX_MESSAGE_LENGTH);
        }
        return normalized;
    }
}
