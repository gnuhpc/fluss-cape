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
import org.apache.fluss.metadata.TablePath;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.gnuhpc.fluss.cape.kafka.config.KafkaCompatConfig;
import org.gnuhpc.fluss.cape.kafka.converter.TablePathResolver;
import org.gnuhpc.fluss.cape.kafka.protocol.KafkaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteTopicsHandler {
    private static final Logger LOG = LoggerFactory.getLogger(DeleteTopicsHandler.class);

    private final Connection flussConnection;
    private final KafkaCompatConfig config;
    private final TablePathResolver tablePathResolver;

    public DeleteTopicsHandler(Connection flussConnection, KafkaCompatConfig config,
                               TablePathResolver tablePathResolver) {
        this.flussConnection = flussConnection;
        this.config = config;
        this.tablePathResolver = tablePathResolver;
    }

    public void handle(KafkaRequest request) {
        try {
            DeleteTopicsRequest deleteRequest = request.request();
            LOG.info("Handling DELETE_TOPICS request for request ID: {}", request.requestId());

            DeleteTopicsResponseData responseData = new DeleteTopicsResponseData();

            if (deleteRequest.data().topics() != null && !deleteRequest.data().topics().isEmpty()) {
                deleteRequest.data().topics().forEach(topic -> {
                    String topicName = topic.name();
                    LOG.info("Deleting topic: name={}", topicName);

                    DeleteTopicsResponseData.DeletableTopicResult result =
                            new DeleteTopicsResponseData.DeletableTopicResult()
                                    .setName(topicName);

                    try {
                        TablePath tablePath = tablePathResolver.resolve(topicName);
                        Admin admin = flussConnection.getAdmin();

                        boolean tableExists = admin.tableExists(tablePath).get();
                        if (!tableExists) {
                            LOG.warn("Table does not exist: {}", tablePath);
                            result.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                                  .setErrorMessage("Topic '" + topicName + "' does not exist.");
                        } else {
                            admin.dropTable(tablePath, false).get();
                            LOG.info("Successfully dropped table: {}", tablePath);
                            result.setErrorCode(Errors.NONE.code());
                        }
                    } catch (Exception e) {
                        LOG.error("Failed to delete topic: {}", topicName, e);
                        result.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                              .setErrorMessage("Failed to delete topic: " + e.getMessage());
                    }

                    responseData.responses().add(result);
                });
            } else if (deleteRequest.data().topicNames() != null && !deleteRequest.data().topicNames().isEmpty()) {
                deleteRequest.data().topicNames().forEach(topicName -> {
                    LOG.info("Deleting topic: name={}", topicName);

                    DeleteTopicsResponseData.DeletableTopicResult result =
                            new DeleteTopicsResponseData.DeletableTopicResult()
                                    .setName(topicName);

                    try {
                        TablePath tablePath = tablePathResolver.resolve(topicName);
                        Admin admin = flussConnection.getAdmin();

                        boolean tableExists = admin.tableExists(tablePath).get();
                        if (!tableExists) {
                            LOG.warn("Table does not exist: {}", tablePath);
                            result.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                                  .setErrorMessage("Topic '" + topicName + "' does not exist.");
                        } else {
                            admin.dropTable(tablePath, false).get();
                            LOG.info("Successfully dropped table: {}", tablePath);
                            result.setErrorCode(Errors.NONE.code());
                        }
                    } catch (Exception e) {
                        LOG.error("Failed to delete topic: {}", topicName, e);
                        result.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                              .setErrorMessage("Failed to delete topic: " + e.getMessage());
                    }

                    responseData.responses().add(result);
                });
            }

            DeleteTopicsResponse response = new DeleteTopicsResponse(responseData);

            LOG.info("Completed DELETE_TOPICS request with {} results", responseData.responses().size());
            request.complete(response);

        } catch (Exception e) {
            LOG.error("Error handling DELETE_TOPICS request", e);
            request.fail(e);
        }
    }
}
