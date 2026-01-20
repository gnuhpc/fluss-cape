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

package org.gnuhpc.fluss.cape.kafka.converter;

import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

public class RecordConverter {
    private static final Logger LOG = LoggerFactory.getLogger(RecordConverter.class);

    public static RowType createDefaultSchema() {
        return RowType.of(
                new DataType[] {
                    DataTypes.BYTES(),
                    DataTypes.BYTES(),
                    DataTypes.BIGINT()
                },
                new String[] {"key", "value", "timestamp"});
    }

    public static InternalRow kafkaRecordToFlussRow(Record kafkaRecord, RowType schema) {
        try {
            int fieldCount = schema.getFieldCount();
            Object[] values = new Object[fieldCount];
            
            List<String> fieldNames = schema.getFieldNames();
            for (int i = 0; i < fieldCount; i++) {
                String fieldName = fieldNames.get(i);
                switch (fieldName) {
                    case "key":
                        values[i] = extractBytes(kafkaRecord.key());
                        break;
                    case "value":
                        values[i] = extractBytes(kafkaRecord.value());
                        break;
                    case "timestamp":
                        values[i] = kafkaRecord.timestamp();
                        break;
                    default:
                        values[i] = null;
                }
            }
            
            return GenericRow.of(values);
        } catch (Exception e) {
            LOG.error("Error converting Kafka record to Fluss row", e);
            throw new RuntimeException("Failed to convert Kafka record", e);
        }
    }

    public static KafkaRecordBuilder flussRowToKafkaRecordBuilder(InternalRow flussRow, RowType schema) {
        try {
            KafkaRecordBuilder builder = new KafkaRecordBuilder();
            
            List<String> fieldNames = schema.getFieldNames();
            LOG.info("Converting Fluss row with schema fields: {}, fieldCount: {}", fieldNames, schema.getFieldCount());
            
            for (int i = 0; i < schema.getFieldCount(); i++) {
                String fieldName = fieldNames.get(i);
                boolean isNull = flussRow.isNullAt(i);
                LOG.info("Field {}: name={}, isNull={}", i, fieldName, isNull);
                
                switch (fieldName) {
                    case "key":
                        if (!flussRow.isNullAt(i)) {
                            byte[] keyBytes = flussRow.getBytes(i);
                            LOG.info("Key bytes: length={}, value={}", 
                                    keyBytes != null ? keyBytes.length : 0,
                                    keyBytes != null ? new String(keyBytes) : "null");
                            builder.key(keyBytes);
                        }
                        break;
                    case "value":
                        if (!flussRow.isNullAt(i)) {
                            byte[] valueBytes = flussRow.getBytes(i);
                            LOG.info("Value bytes: length={}, value={}", 
                                    valueBytes != null ? valueBytes.length : 0,
                                    valueBytes != null ? new String(valueBytes) : "null");
                            builder.value(valueBytes);
                        }
                        break;
                    case "timestamp":
                        if (!flussRow.isNullAt(i)) {
                            builder.timestamp(flussRow.getLong(i));
                        } else {
                            builder.timestamp(System.currentTimeMillis());
                        }
                        break;
                }
            }
            
            return builder;
        } catch (Exception e) {
            LOG.error("Error converting Fluss row to Kafka record", e);
            throw new RuntimeException("Failed to convert Fluss row", e);
        }
    }

    private static byte[] extractBytes(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    public static class KafkaRecordBuilder {
        private byte[] key;
        private byte[] value;
        private long timestamp;
        private int partition;
        private long offset;

        public KafkaRecordBuilder key(byte[] key) {
            this.key = key;
            return this;
        }

        public KafkaRecordBuilder value(byte[] value) {
            this.value = value;
            return this;
        }

        public KafkaRecordBuilder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public KafkaRecordBuilder partition(int partition) {
            this.partition = partition;
            return this;
        }

        public KafkaRecordBuilder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public byte[] getKey() {
            return key;
        }

        public byte[] getValue() {
            return value;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public int getPartition() {
            return partition;
        }

        public long getOffset() {
            return offset;
        }
    }
}
