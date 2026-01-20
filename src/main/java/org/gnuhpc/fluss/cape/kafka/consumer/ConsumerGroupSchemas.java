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

package org.gnuhpc.fluss.cape.kafka.consumer;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.types.DataTypes;

public class ConsumerGroupSchemas {
    
    public static final String CONSUMER_GROUPS_TABLE = "kafka_consumer_groups";
    public static final String CONSUMER_OFFSETS_TABLE = "kafka_consumer_offsets";
    
    public static Schema consumerGroupSchema() {
        return Schema.newBuilder()
                .column("group_id", DataTypes.STRING())
                .column("state", DataTypes.STRING())
                .column("protocol_type", DataTypes.STRING())
                .column("protocol_name", DataTypes.STRING())
                .column("leader_id", DataTypes.STRING())
                .column("generation_id", DataTypes.INT())
                .column("members_json", DataTypes.STRING())
                .primaryKey("group_id")
                .build();
    }
    
    public static Schema consumerOffsetSchema() {
        return Schema.newBuilder()
                .column("group_id", DataTypes.STRING())
                .column("topic", DataTypes.STRING())
                .column("partition", DataTypes.INT())
                .column("offset", DataTypes.BIGINT())
                .column("metadata", DataTypes.STRING())
                .column("commit_timestamp", DataTypes.BIGINT())
                .primaryKey("group_id", "topic", "partition")
                .build();
    }
}
