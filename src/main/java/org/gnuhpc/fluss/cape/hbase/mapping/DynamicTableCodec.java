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

package org.gnuhpc.fluss.cape.hbase.mapping;

import org.apache.fluss.row.InternalRow;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Codec for encoding/decoding HBase cells into BYTES columns for dynamic tables.
 *
 * <p>Dynamic tables have schema: rowkey BYTES (PK), cf1 BYTES, cf2 BYTES, ...
 *
 * <p>Encoding format for each column family BYTES column: [num_qualifiers:int]
 * [qualifier1_len:int][qualifier1:bytes][value1_len:int][value1:bytes]
 * [qualifier2_len:int][qualifier2:bytes][value2_len:int][value2:bytes] ...
 *
 * <p>This allows storing multiple HBase qualifiers (cf:q1, cf:q2) within a single Fluss BYTES
 * column.
 */
public class DynamicTableCodec {

    /**
     * Encodes HBase cells from a single column family into a BYTES column value.
     *
     * @param familyName the column family name
     * @param qualifiers map of qualifier -> value
     * @return encoded BYTES value, or null if no qualifiers
     */
    public static byte[] encodeColumnFamily(String familyName, Map<String, byte[]> qualifiers) {
        if (qualifiers == null || qualifiers.isEmpty()) {
            return null;
        }

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            // Write number of qualifiers
            dos.writeInt(qualifiers.size());

            // Write each qualifier and value
            for (Map.Entry<String, byte[]> entry : qualifiers.entrySet()) {
                byte[] qualifierBytes = Bytes.toBytes(entry.getKey());
                byte[] valueBytes = entry.getValue();

                // Write qualifier length and bytes
                dos.writeInt(qualifierBytes.length);
                dos.write(qualifierBytes);

                // Write value length and bytes
                dos.writeInt(valueBytes.length);
                dos.write(valueBytes);
            }

            dos.flush();
            return baos.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException("Failed to encode column family: " + familyName, e);
        }
    }

    /**
     * Decodes a BYTES column value into HBase cells.
     *
     * @param familyName the column family name
     * @param rowKey the row key
     * @param encodedData the encoded BYTES data
     * @param timestamp the cell timestamp
     * @return list of HBase cells
     */
    public static List<Cell> decodeColumnFamily(
            String familyName, byte[] rowKey, byte[] encodedData, long timestamp) {
        List<Cell> cells = new ArrayList<>();

        if (encodedData == null || encodedData.length == 0) {
            return cells;
        }

        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(encodedData);
            DataInputStream dis = new DataInputStream(bais);

            byte[] familyBytes = Bytes.toBytes(familyName);

            // Read number of qualifiers
            int numQualifiers = dis.readInt();

            for (int i = 0; i < numQualifiers; i++) {
                // Read qualifier
                int qualifierLen = dis.readInt();
                byte[] qualifierBytes = new byte[qualifierLen];
                dis.readFully(qualifierBytes);

                // Read value
                int valueLen = dis.readInt();
                byte[] valueBytes = new byte[valueLen];
                dis.readFully(valueBytes);

                // Create HBase cell
                Cell cell =
                        CellUtil.createCell(
                                rowKey,
                                familyBytes,
                                qualifierBytes,
                                timestamp,
                                KeyValue.Type.Put.getCode(),
                                valueBytes);
                cells.add(cell);
            }

            return cells;

        } catch (IOException e) {
            throw new RuntimeException("Failed to decode column family: " + familyName, e);
        }
    }

    /**
     * Extracts qualifiers from encoded BYTES data for a single column family.
     *
     * @param encodedData the encoded BYTES data
     * @return map of qualifier -> value, or empty map if data is null/empty
     */
    public static Map<String, byte[]> extractQualifiers(byte[] encodedData) {
        Map<String, byte[]> qualifiers = new HashMap<>();

        if (encodedData == null || encodedData.length == 0) {
            return qualifiers;
        }

        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(encodedData);
            DataInputStream dis = new DataInputStream(bais);

            // Read number of qualifiers
            int numQualifiers = dis.readInt();

            for (int i = 0; i < numQualifiers; i++) {
                // Read qualifier
                int qualifierLen = dis.readInt();
                byte[] qualifierBytes = new byte[qualifierLen];
                dis.readFully(qualifierBytes);

                // Read value
                int valueLen = dis.readInt();
                byte[] valueBytes = new byte[valueLen];
                dis.readFully(valueBytes);

                String qualifier = Bytes.toString(qualifierBytes);
                qualifiers.put(qualifier, valueBytes);
            }

            return qualifiers;

        } catch (IOException e) {
            throw new RuntimeException("Failed to extract qualifiers from encoded data", e);
        }
    }

    /**
     * Merges new qualifiers into existing encoded data.
     *
     * @param existingData existing encoded BYTES data (can be null)
     * @param newQualifiers new qualifiers to add/update
     * @return merged encoded BYTES data
     */
    public static byte[] mergeQualifiers(
            byte[] existingData, Map<String, byte[]> newQualifiers) {
        Map<String, byte[]> merged = extractQualifiers(existingData);
        merged.putAll(newQualifiers);
        return encodeColumnFamily("temp", merged);
    }

    /**
     * Checks if a row has data in any column family (excluding rowkey).
     *
     * @param row the Fluss internal row
     * @param rowkeyIndex the index of the rowkey column (usually 0)
     * @return true if any non-rowkey column has data
     */
    public static boolean hasData(InternalRow row, int rowkeyIndex) {
        for (int i = 0; i < row.getFieldCount(); i++) {
            if (i != rowkeyIndex && !row.isNullAt(i)) {
                return true;
            }
        }
        return false;
    }
}
