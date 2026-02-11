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

package org.gnuhpc.fluss.cape.kafka.handler;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.gnuhpc.fluss.cape.kafka.config.KafkaCompatConfig;
import org.gnuhpc.fluss.cape.kafka.converter.TablePathResolver;
import org.gnuhpc.fluss.cape.kafka.protocol.KafkaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CreateTopicsHandler {
    private static final Logger LOG = LoggerFactory.getLogger(CreateTopicsHandler.class);

    private final Connection flussConnection;
    private final KafkaCompatConfig config;
    private final TablePathResolver tablePathResolver;

    public CreateTopicsHandler(Connection flussConnection, KafkaCompatConfig config,
                               TablePathResolver tablePathResolver) {
        this.flussConnection = flussConnection;
        this.config = config;
        this.tablePathResolver = tablePathResolver;
    }

    public void handle(KafkaRequest request) {
        try {
            CreateTopicsRequest createRequest = request.request();
            LOG.info("Handling CREATE_TOPICS request for request ID: {}", request.requestId());

            CreateTopicsResponseData responseData = new CreateTopicsResponseData();

            createRequest.data().topics().forEach(topic -> {
                String topicName = topic.name();
                int numPartitions = topic.numPartitions();
                short replicationFactor = topic.replicationFactor();

                LOG.info("Creating topic: name={}, partitions={}, replicationFactor={}",
                        topicName, numPartitions, replicationFactor);

                CreateTopicsResponseData.CreatableTopicResult result =
                        new CreateTopicsResponseData.CreatableTopicResult()
                                .setName(topicName);

                try {
                    TablePath tablePath = tablePathResolver.resolve(topicName);
                    Admin admin = flussConnection.getAdmin();

                    boolean tableExists = admin.tableExists(tablePath).get();
                    if (tableExists) {
                        LOG.warn("Table already exists: {}", tablePath);
                        result.setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code())
                              .setErrorMessage("Topic '" + topicName + "' already exists.");
                    } else {
                        Schema schema = Schema.newBuilder()
                                .column("key", DataTypes.BYTES())
                                .column("value", DataTypes.BYTES())
                                .build();

                        TableDescriptor.Builder tableBuilder = TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(numPartitions > 0 ? numPartitions : 3);

                        TableDescriptor tableDescriptor = tableBuilder.build();

                        admin.createTable(tablePath, tableDescriptor, false).get();

                        LOG.info("Successfully created table: {}", tablePath);
                        result.setErrorCode(Errors.NONE.code())
                              .setNumPartitions(numPartitions > 0 ? numPartitions : 3)
                              .setReplicationFactor(replicationFactor > 0 ? replicationFactor : (short) 1);
                    }
                } catch (Exception e) {
                    LOG.error("Failed to create topic: {}", topicName, e);
                    result.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                          .setErrorMessage("Failed to create topic: " + e.getMessage());
                }

                responseData.topics().add(result);
            });

            CreateTopicsResponse response = new CreateTopicsResponse(responseData);

            LOG.info("Completed CREATE_TOPICS request with {} results", responseData.topics().size());
            request.complete(response);

        } catch (Exception e) {
            LOG.error("Error handling CREATE_TOPICS request", e);
            request.fail(e);
        }
    }
}
