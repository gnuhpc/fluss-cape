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

package org.gnuhpc.fluss.cape.redis.util;

/**
 * Utility class for encoding Sorted Set scores as sortable strings.
 *
 * <p>Redis Sorted Sets maintain elements ordered by score. To support this with lexicographic
 * sorting in Fluss, we encode double scores using IEEE 754 bit manipulation to ensure that string
 * comparison produces the same ordering as numeric comparison.
 *
 * <p>The sub_key format for sorted sets is: "score_encoded:member_name"
 *
 * <p>Example: score=100.0, member="alice" → "4059000000000000:alice"
 */
public class ScoreEncoder {

    private static final String DELIMITER = ":";

    /**
     * Encodes a double score as a sortable hexadecimal string.
     *
     * <p>This uses IEEE 754 bit manipulation to ensure lexicographic sort order matches numeric
     * sort order. Negative numbers have all bits flipped, positive numbers have only the sign bit
     * flipped.
     *
     * @param score the score to encode
     * @return 16-character hexadecimal string
     */
    public static String encode(double score) {
        long bits = Double.doubleToRawLongBits(score);

        if (score < 0) {
            bits = ~bits;
        } else {
            bits ^= 0x8000000000000000L;
        }

        return String.format("%016X", bits);
    }

    /**
     * Decodes a hexadecimal string back to a double score.
     *
     * @param encoded the encoded string (16-character hex)
     * @return the original score
     * @throws NumberFormatException if the string is not a valid encoded score
     */
    public static double decode(String encoded) {
        if (encoded == null || encoded.length() != 16) {
            throw new IllegalArgumentException(
                    "Encoded score must be exactly 16 hex characters");
        }

        long bits = Long.parseUnsignedLong(encoded, 16);

        if ((bits & 0x8000000000000000L) == 0) {
            bits = ~bits;
        } else {
            bits ^= 0x8000000000000000L;
        }

        return Double.longBitsToDouble(bits);
    }

    /**
     * Encodes a score and member name into a composite sub_key.
     *
     * @param score the score
     * @param member the member name
     * @return composite key in format "score_encoded:member_name"
     */
    public static String encodeWithMember(double score, String member) {
        return encode(score) + DELIMITER + member;
    }

    /**
     * Decodes a composite sub_key into a ScoreMember object.
     *
     * @param subKey the composite key (e.g., "4059000000000000:alice")
     * @return ScoreMember object containing score and member
     * @throws IllegalArgumentException if the sub_key format is invalid
     */
    public static ScoreMember decodeWithMember(String subKey) {
        if (subKey == null || !subKey.contains(DELIMITER)) {
            throw new IllegalArgumentException(
                    "Invalid sub_key format. Expected: 'score_encoded:member'");
        }

        int delimiterIndex = subKey.indexOf(DELIMITER);
        String scoreEncoded = subKey.substring(0, delimiterIndex);
        String member = subKey.substring(delimiterIndex + 1);

        double score = decode(scoreEncoded);
        return new ScoreMember(score, member);
    }

    /**
     * Container class for score and member pair.
     */
    public static class ScoreMember {
        public final double score;
        public final String member;

        public ScoreMember(double score, String member) {
            this.score = score;
            this.member = member;
        }
    }
}
