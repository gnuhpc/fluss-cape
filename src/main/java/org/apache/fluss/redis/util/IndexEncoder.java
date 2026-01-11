/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.redis.util;

/**
 * Utility class for encoding List indices as sortable strings.
 *
 * <p>Redis Lists support LPUSH (add to head) and RPUSH (add to tail). To support this with
 * lexicographic sorting in Fluss, we use signed encoding with zero-padding:
 *
 * <ul>
 *   <li>LPUSH indices: -0000000000000003, -0000000000000002, -0000000000000001
 *   <li>RPUSH indices: +0000000000000000, +0000000000000001, +0000000000000002
 * </ul>
 *
 * <p>This ensures that lexicographic sort (string comparison) maintains the correct order.
 */
public class IndexEncoder {

    /** Format: signed (+/-) followed by 16 zero-padded digits. */
    private static final String FORMAT = "%+017d";

    /** Minimum representable index (for boundary checks). */
    private static final String MIN_INDEX = "-9999999999999999";

    /** Maximum representable index (for boundary checks). */
    private static final String MAX_INDEX = "+9999999999999999";

    /**
     * Encodes a long index as a sortable string.
     *
     * @param index the index to encode (can be negative or positive)
     * @return encoded string (e.g., "-0000000000000001", "+0000000000000000")
     */
    public static String encode(long index) {
        return String.format(FORMAT, index);
    }

    /**
     * Decodes a sortable string back to a long index.
     *
     * @param encoded the encoded string (e.g., "-0000000000000001")
     * @return the original index
     * @throws NumberFormatException if the string is not a valid encoded index
     */
    public static long decode(String encoded) {
        if (encoded == null || encoded.isEmpty()) {
            throw new IllegalArgumentException("Encoded index cannot be null or empty");
        }
        return Long.parseLong(encoded);
    }

    /**
     * Returns the minimum representable index string.
     *
     * @return minimum index ("-9999999999999999")
     */
    public static String getMinIndex() {
        return MIN_INDEX;
    }

    /**
     * Returns the maximum representable index string.
     *
     * @return maximum index ("+9999999999999999")
     */
    public static String getMaxIndex() {
        return MAX_INDEX;
    }

    /**
     * Checks if an encoded string is a valid index format.
     *
     * @param encoded the string to validate
     * @return true if valid, false otherwise
     */
    public static boolean isValidIndex(String encoded) {
        if (encoded == null || encoded.length() != 17) {
            return false;
        }
        char sign = encoded.charAt(0);
        if (sign != '+' && sign != '-') {
            return false;
        }
        try {
            Long.parseLong(encoded);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
