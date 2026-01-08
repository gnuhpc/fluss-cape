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

package org.apache.fluss.hbase.mapping;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

/**
 * Encoder for bidirectional conversion between HBase row keys and Fluss primary keys.
 *
 * <p>Supports two encoding modes: DELIMITER (composite keys with "|") and FIXED_LENGTH (binary
 * concatenation for primitive types).
 */
public class RowKeyEncoder {

    private final RowType rowType;
    private final List<String> primaryKeyFields;
    private final RowKeySeparator separator;

    public RowKeyEncoder(RowType rowType, List<String> primaryKeyFields) {
        this(rowType, primaryKeyFields, RowKeySeparator.DELIMITER);
    }

    public RowKeyEncoder(
            RowType rowType, List<String> primaryKeyFields, RowKeySeparator separator) {
        this.rowType = rowType;
        this.primaryKeyFields = primaryKeyFields;
        this.separator = separator;
    }

    public byte[] encodeRowKey(GenericRow row) {
        if (separator == RowKeySeparator.DELIMITER) {
            return encodeWithDelimiter(row);
        } else {
            return encodeFixedLength(row);
        }
    }

    public GenericRow decodeRowKey(byte[] rowKey) {
        if (separator == RowKeySeparator.DELIMITER) {
            return decodeWithDelimiter(rowKey);
        } else {
            return decodeFixedLength(rowKey);
        }
    }

    private byte[] encodeWithDelimiter(GenericRow row) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < primaryKeyFields.size(); i++) {
            if (i > 0) {
                sb.append("|");
            }

            String fieldName = primaryKeyFields.get(i);
            int fieldIndex = getFieldIndex(fieldName);
            DataField field = rowType.getFields().get(fieldIndex);

            if (!row.isNullAt(fieldIndex)) {
                String valueStr = fieldValueToString(row, fieldIndex, field.getType());
                sb.append(valueStr);
            }
        }

        return Bytes.toBytes(sb.toString());
    }

    private GenericRow decodeWithDelimiter(byte[] rowKey) {
        String rowKeyStr = Bytes.toString(rowKey);
        String[] parts = rowKeyStr.split("\\|", -1);

        GenericRow row = new GenericRow(rowType.getFieldCount());

        for (int i = 0; i < Math.min(parts.length, primaryKeyFields.size()); i++) {
            String fieldName = primaryKeyFields.get(i);
            int fieldIndex = getFieldIndex(fieldName);
            DataField field = rowType.getFields().get(fieldIndex);

            if (!parts[i].isEmpty()) {
                Object value = stringToFieldValue(parts[i], field.getType());
                row.setField(fieldIndex, value);
            }
        }

        return row;
    }

    private byte[] encodeFixedLength(GenericRow row) {
        int totalLength = 0;
        byte[][] fieldBytes = new byte[primaryKeyFields.size()][];

        for (int i = 0; i < primaryKeyFields.size(); i++) {
            String fieldName = primaryKeyFields.get(i);
            int fieldIndex = getFieldIndex(fieldName);
            DataField field = rowType.getFields().get(fieldIndex);

            fieldBytes[i] = fieldToFixedBytes(row, fieldIndex, field.getType());
            totalLength += fieldBytes[i].length;
        }

        byte[] result = new byte[totalLength];
        int offset = 0;
        for (byte[] bytes : fieldBytes) {
            System.arraycopy(bytes, 0, result, offset, bytes.length);
            offset += bytes.length;
        }

        return result;
    }

    private GenericRow decodeFixedLength(byte[] rowKey) {
        GenericRow row = new GenericRow(rowType.getFieldCount());
        int offset = 0;

        for (String fieldName : primaryKeyFields) {
            int fieldIndex = getFieldIndex(fieldName);
            DataField field = rowType.getFields().get(fieldIndex);

            int fieldSize = getFixedFieldSize(field.getType());
            if (offset + fieldSize > rowKey.length) {
                break;
            }

            byte[] fieldBytes = new byte[fieldSize];
            System.arraycopy(rowKey, offset, fieldBytes, 0, fieldSize);

            Object value = fixedBytesToField(fieldBytes, field.getType());
            row.setField(fieldIndex, value);

            offset += fieldSize;
        }

        return row;
    }

    private String fieldValueToString(GenericRow row, int pos, DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return String.valueOf(row.getBoolean(pos));
            case TINYINT:
                return String.valueOf(row.getByte(pos));
            case SMALLINT:
                return String.valueOf(row.getShort(pos));
            case INTEGER:
                return String.valueOf(row.getInt(pos));
            case BIGINT:
                return String.valueOf(row.getLong(pos));
            case FLOAT:
                return String.valueOf(row.getFloat(pos));
            case DOUBLE:
                return String.valueOf(row.getDouble(pos));
            case STRING:
            case CHAR:
                return row.getString(pos).toString();
            case BINARY:
                org.apache.fluss.types.BinaryType binaryType =
                        (org.apache.fluss.types.BinaryType) type;
                return Bytes.toHex(row.getBinary(pos, binaryType.getLength()));
            case BYTES:
                return Bytes.toHex(row.getBytes(pos));
            default:
                throw new UnsupportedOperationException("Unsupported primary key type: " + type);
        }
    }

    private Object stringToFieldValue(String str, DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return Boolean.parseBoolean(str);
            case TINYINT:
                return Byte.parseByte(str);
            case SMALLINT:
                return Short.parseShort(str);
            case INTEGER:
                return Integer.parseInt(str);
            case BIGINT:
                return Long.parseLong(str);
            case FLOAT:
                return Float.parseFloat(str);
            case DOUBLE:
                return Double.parseDouble(str);
            case STRING:
            case CHAR:
                return BinaryString.fromString(str);
            case BINARY:
            case BYTES:
                return Bytes.fromHex(str);
            default:
                throw new UnsupportedOperationException("Unsupported primary key type: " + type);
        }
    }

    private byte[] fieldToFixedBytes(GenericRow row, int pos, DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return new byte[] {(byte) (row.getBoolean(pos) ? 1 : 0)};
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
            default:
                throw new UnsupportedOperationException(
                        "Fixed-length encoding only supports primitive types, got: " + type);
        }
    }

    private Object fixedBytesToField(byte[] bytes, DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return bytes[0] != 0;
            case TINYINT:
                return bytes[0];
            case SMALLINT:
                return Bytes.toShort(bytes);
            case INTEGER:
                return Bytes.toInt(bytes);
            case BIGINT:
                return Bytes.toLong(bytes);
            case FLOAT:
                return Bytes.toFloat(bytes);
            case DOUBLE:
                return Bytes.toDouble(bytes);
            default:
                throw new UnsupportedOperationException(
                        "Fixed-length encoding only supports primitive types, got: " + type);
        }
    }

    private int getFixedFieldSize(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
                return 1;
            case SMALLINT:
                return 2;
            case INTEGER:
            case FLOAT:
                return 4;
            case BIGINT:
            case DOUBLE:
                return 8;
            default:
                throw new UnsupportedOperationException(
                        "Fixed-length encoding only supports primitive types, got: " + type);
        }
    }

    private int getFieldIndex(String fieldName) {
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (rowType.getFields().get(i).getName().equals(fieldName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Field not found: " + fieldName);
    }

    /** Row key encoding mode. */
    public enum RowKeySeparator {
        DELIMITER,
        FIXED_LENGTH
    }
}
