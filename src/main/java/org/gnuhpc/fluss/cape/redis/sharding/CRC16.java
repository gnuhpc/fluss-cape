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

package org.gnuhpc.fluss.cape.redis.sharding;

import java.nio.charset.StandardCharsets;

public class CRC16 {

    private static final int[] CRC16_TABLE = new int[256];

    static {
        for (int i = 0; i < 256; i++) {
            int crc = i << 8;
            for (int j = 0; j < 8; j++) {
                crc = ((crc & 0x8000) != 0) ? ((crc << 1) ^ 0x1021) : (crc << 1);
            }
            CRC16_TABLE[i] = crc & 0xFFFF;
        }
    }

    public static int hash(byte[] data) {
        int crc = 0;
        for (byte b : data) {
            crc = ((crc << 8) ^ CRC16_TABLE[((crc >> 8) ^ (b & 0xFF)) & 0xFF]) & 0xFFFF;
        }
        return crc;
    }

    public static int hash(String key) {
        return hash(key.getBytes(StandardCharsets.UTF_8));
    }
}
