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

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converter for bidirectional transformation between HBase Cells and Fluss InternalRows.
 *
 * <p>Handles type mapping for 11 data types including primitives, strings, bytes, and timestamps.
 */
public class CellConverter {

    private final RowType rowType;
    private final ColumnFamilyMapping columnFamilyMapping;

    public CellConverter(RowType rowType, ColumnFamilyMapping columnFamilyMapping) {
        this.rowType = rowType;
        this.columnFamilyMapping = columnFamilyMapping;
    }

    public RowType getRowType() {
        return rowType;
    }

    public InternalRow resultToRow(Result result) {
        if (result.isEmpty()) {
            return null;
        }

        GenericRow row = new GenericRow(rowType.getFieldCount());

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            DataField field = rowType.getFields().get(i);
            String fieldName = field.getName();
            DataType fieldType = field.getType();

            ColumnMapping mapping = columnFamilyMapping.getMapping(fieldName);
            if (mapping == null) {
                row.setField(i, null);
                continue;
            }

            byte[] family = Bytes.toBytes(mapping.getFamily());
            byte[] qualifier = Bytes.toBytes(mapping.getQualifier());

            byte[] value = result.getValue(family, qualifier);
            if (value == null) {
                row.setField(i, null);
            } else {
                row.setField(i, bytesToValue(value, fieldType));
            }
        }

        return row;
    }

    public List<Cell> rowToCells(byte[] rowKey, InternalRow row, long timestamp) {
        List<Cell> cells = new ArrayList<>();

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            DataField field = rowType.getFields().get(i);
            String fieldName = field.getName();

            ColumnMapping mapping = columnFamilyMapping.getMapping(fieldName);
            if (mapping == null) {
                continue;
            }

            if (!row.isNullAt(i)) {
                byte[] family = Bytes.toBytes(mapping.getFamily());
                byte[] qualifier = Bytes.toBytes(mapping.getQualifier());
                byte[] value = valueToBytes(row, i, field.getType());

                Cell cell =
                        CellUtil.createCell(
                                rowKey,
                                family,
                                qualifier,
                                timestamp,
                                KeyValue.Type.Put.getCode(),
                                value);
                cells.add(cell);
            }
        }

        return cells;
    }

    public Map<String, byte[]> rowToFamilyMap(InternalRow row) {
        Map<String, Map<String, byte[]>> familyQualifierMap = new HashMap<>();

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            DataField field = rowType.getFields().get(i);
            String fieldName = field.getName();

            ColumnMapping mapping = columnFamilyMapping.getMapping(fieldName);
            if (mapping == null || row.isNullAt(i)) {
                continue;
            }

            String family = mapping.getFamily();
            String qualifier = mapping.getQualifier();
            byte[] value = valueToBytes(row, i, field.getType());

            familyQualifierMap.computeIfAbsent(family, k -> new HashMap<>()).put(qualifier, value);
        }

        Map<String, byte[]> result = new HashMap<>();
        for (Map.Entry<String, Map<String, byte[]>> entry : familyQualifierMap.entrySet()) {
            result.put(entry.getKey(), serializeQualifierMap(entry.getValue()));
        }

        return result;
    }

    private byte[] serializeQualifierMap(Map<String, byte[]> qualifierMap) {
        int totalSize = 4;
        for (Map.Entry<String, byte[]> entry : qualifierMap.entrySet()) {
            totalSize += 4 + entry.getKey().length();
            totalSize += 4 + entry.getValue().length;
        }

        byte[] result = new byte[totalSize];
        int offset = 0;

        Bytes.putInt(result, offset, qualifierMap.size());
        offset += 4;

        for (Map.Entry<String, byte[]> entry : qualifierMap.entrySet()) {
            byte[] qualifierBytes = Bytes.toBytes(entry.getKey());
            Bytes.putInt(result, offset, qualifierBytes.length);
            offset += 4;
            System.arraycopy(qualifierBytes, 0, result, offset, qualifierBytes.length);
            offset += qualifierBytes.length;

            byte[] value = entry.getValue();
            Bytes.putInt(result, offset, value.length);
            offset += 4;
            System.arraycopy(value, 0, result, offset, value.length);
            offset += value.length;
        }

        return result;
    }

    private byte[] valueToBytes(InternalRow row, int pos, DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return Bytes.toBytes(row.getBoolean(pos));
            case TINYINT:
                return new byte[] {row.getByte(pos)};
            case SMALLINT:
                return Bytes.toBytes(row.getShort(pos));
            case INTEGER:
                return Bytes.toBytes(row.getInt(pos));
            case BIGINT:
                return Bytes.toBytes(row.getLong(pos));
            case FLOAT:
                return Bytes.toBytes(row.getFloat(pos));
            case DOUBLE:
                return Bytes.toBytes(row.getDouble(pos));
            case STRING:
            case CHAR:
                return row.getString(pos).toBytes();
            case BINARY:
                // BINARY(n) requires length parameter
                org.apache.fluss.types.BinaryType binaryType =
                        (org.apache.fluss.types.BinaryType) type;
                return row.getBinary(pos, binaryType.getLength());
            case BYTES:
                // BYTES is variable-length, use getBytes() instead
                return row.getBytes(pos);
            case DECIMAL:
                // DECIMAL requires precision and scale parameters
                org.apache.fluss.types.DecimalType decimalType =
                        (org.apache.fluss.types.DecimalType) type;
                return row.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale())
                        .toBigDecimal()
                        .toString()
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return Bytes.toBytes(row.getTimestampNtz(pos, 3).getMillisecond());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Bytes.toBytes(row.getTimestampLtz(pos, 3).getEpochMillisecond());
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type for HBase conversion: " + type);
        }
    }

    @Nullable
    public Object bytesToValue(byte[] bytes, DataType type) {
        if (bytes == null) {
            return null;
        }

        if (bytes.length == 0) {
            switch (type.getTypeRoot()) {
                case STRING:
                case CHAR:
                    return BinaryString.fromBytes(bytes);
                case BINARY:
                case BYTES:
                    return bytes;
                default:
                    return null;
            }
        }

        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return convertToBoolean(bytes);
            case TINYINT:
                return convertToTinyInt(bytes);
            case SMALLINT:
                return convertToSmallInt(bytes);
            case INTEGER:
                return convertToInt(bytes);
            case BIGINT:
                return convertToBigInt(bytes);
            case FLOAT:
                return convertToFloat(bytes);
            case DOUBLE:
                return convertToDouble(bytes);
            case STRING:
            case CHAR:
                return BinaryString.fromBytes(bytes);
            case BINARY:
            case BYTES:
                return bytes;
            case DECIMAL:
                return new java.math.BigDecimal(
                        new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type for HBase conversion: " + type);
        }
    }

    private Object convertToBoolean(byte[] bytes) {
        if (bytes.length == 1) {
            return Bytes.toBoolean(bytes);
        }
        String str = new String(bytes, java.nio.charset.StandardCharsets.UTF_8).trim();
        return Boolean.parseBoolean(str);
    }

    private Object convertToTinyInt(byte[] bytes) {
        if (bytes.length != 1) {
            throw new IllegalArgumentException(
                    "TINYINT must be exactly 1 byte, got " + bytes.length + " bytes");
        }
        return bytes[0];
    }

    private Object convertToSmallInt(byte[] bytes) {
        if (bytes.length != 2) {
            throw new IllegalArgumentException(
                    "SMALLINT must be exactly 2 bytes, got " + bytes.length + " bytes");
        }
        return Bytes.toShort(bytes);
    }

    private Object convertToInt(byte[] bytes) {
        if (bytes.length != 4) {
            throw new IllegalArgumentException(
                    "INTEGER must be exactly 4 bytes, got " + bytes.length + " bytes");
        }
        return Bytes.toInt(bytes);
    }

    private Object convertToBigInt(byte[] bytes) {
        if (bytes.length != 8) {
            throw new IllegalArgumentException(
                    "BIGINT must be exactly 8 bytes, got " + bytes.length + " bytes");
        }
        return Bytes.toLong(bytes);
    }

    private Object convertToFloat(byte[] bytes) {
        if (bytes.length != 4) {
            throw new IllegalArgumentException(
                    "FLOAT must be exactly 4 bytes, got " + bytes.length + " bytes");
        }
        return Bytes.toFloat(bytes);
    }

    private Object convertToDouble(byte[] bytes) {
        if (bytes.length != 8) {
            throw new IllegalArgumentException(
                    "DOUBLE must be exactly 8 bytes, got " + bytes.length + " bytes");
        }
        return Bytes.toDouble(bytes);
    }

    /** Mapping configuration for column family and qualifier. */
    public static class ColumnMapping {
        private final String family;
        private final String qualifier;

        public ColumnMapping(String family, String qualifier) {
            this.family = family;
            this.qualifier = qualifier;
        }

        public String getFamily() {
            return family;
        }

        public String getQualifier() {
            return qualifier;
        }
    }

    /** Manages mappings between Fluss field names and HBase column families/qualifiers. */
    public static class ColumnFamilyMapping {
        private final Map<String, ColumnMapping> fieldToMapping = new HashMap<>();

        public void addMapping(String fieldName, String family, String qualifier) {
            fieldToMapping.put(fieldName, new ColumnMapping(family, qualifier));
        }

        @Nullable
        public ColumnMapping getMapping(String fieldName) {
            return fieldToMapping.get(fieldName);
        }

        public static ColumnFamilyMapping createDefault(RowType rowType, String defaultFamily) {
            ColumnFamilyMapping mapping = new ColumnFamilyMapping();
            for (DataField field : rowType.getFields()) {
                mapping.addMapping(field.getName(), defaultFamily, field.getName());
            }
            return mapping;
        }
    }
}
