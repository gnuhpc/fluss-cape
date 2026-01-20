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

import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.message.ConsumerProtocolAssignment;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.gnuhpc.fluss.cape.kafka.consumer.ConsumerGroupCoordinatorV2;
import org.gnuhpc.fluss.cape.kafka.consumer.ConsumerGroupState;
import org.gnuhpc.fluss.cape.kafka.consumer.GroupState;
import org.gnuhpc.fluss.cape.kafka.protocol.KafkaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class SyncGroupHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SyncGroupHandler.class);

    private final ConsumerGroupCoordinatorV2 coordinator;
    private final Map<String, Map<String, ByteBuffer>> groupAssignments = new HashMap<>();

    public SyncGroupHandler(ConsumerGroupCoordinatorV2 coordinator) {
        this.coordinator = coordinator;
    }

    public void handle(KafkaRequest request) {
        coordinator.getInitializationFuture()
            .thenCompose(v -> {
                try {
                    org.apache.kafka.common.requests.SyncGroupRequest syncRequest = request.request();
                    SyncGroupRequestData syncRequestData = syncRequest.data();
                    return handleSyncGroupAsync(syncRequestData, request.apiVersion());
                } catch (Exception e) {
                    LOG.error("Error handling sync group request", e);
                    throw new RuntimeException(e);
                }
            })
            .thenAccept(request::complete)
            .exceptionally(ex -> {
                LOG.error("Error handling sync group request", ex);
                request.fail(ex);
                return null;
            });
    }

    private CompletableFuture<SyncGroupResponse> handleSyncGroupAsync(
            SyncGroupRequestData request, 
            short apiVersion) {
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                String groupId = request.groupId();
                String memberId = request.memberId();
                int generationId = request.generationId();
                
                LOG.debug("SyncGroup from member {} in group {} (generation {})", 
                        memberId, groupId, generationId);
                
                ConsumerGroupState groupState = coordinator.getOrCreateGroupState(groupId);
                
                synchronized (groupState) {
                    if (!groupState.hasMember(memberId)) {
                        LOG.warn("SyncGroup from unknown member {} in group {}", memberId, groupId);
                        SyncGroupResponseData errorResponse = new SyncGroupResponseData()
                                .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code());
                        return new SyncGroupResponse(errorResponse);
                    }
                    
                    if (groupState.getGenerationId() != generationId) {
                        LOG.warn("SyncGroup from member {} with wrong generation {} (expected {})",
                                memberId, generationId, groupState.getGenerationId());
                        SyncGroupResponseData errorResponse = new SyncGroupResponseData()
                                .setErrorCode(Errors.ILLEGAL_GENERATION.code());
                        return new SyncGroupResponse(errorResponse);
                    }
                    
                    if (groupState.getState() == GroupState.PREPARING_REBALANCE) {
                        LOG.debug("SyncGroup from member {} while rebalance still preparing", memberId);
                        SyncGroupResponseData errorResponse = new SyncGroupResponseData()
                                .setErrorCode(Errors.REBALANCE_IN_PROGRESS.code());
                        return new SyncGroupResponse(errorResponse);
                    }
                    
                    if (memberId.equals(groupState.getLeaderId())) {
                        Map<String, byte[]> assignments = new HashMap<>();
                        if (!request.assignments().isEmpty()) {
                            for (SyncGroupRequestData.SyncGroupRequestAssignment assignment : request.assignments()) {
                                assignments.put(assignment.memberId(), assignment.assignment());
                            }
                            groupState.setAssignments(assignments);
                            LOG.info("Received {} assignments from leader {} for group {} (generation {})", 
                                    assignments.size(), memberId, groupId, generationId);
                        } else {
                            LOG.warn("Leader {} sent empty assignments for group {}", memberId, groupId);
                        }
                    }
                    
                    groupState.decrementAwaitingMembers();
                    if (groupState.getAwaitingMembers() == 0 && groupState.getState() == GroupState.COMPLETING_REBALANCE) {
                        groupState.setState(GroupState.STABLE);
                        LOG.info("Group {} transitioned to STABLE after all members synced", groupId);
                    }
                    
                    byte[] memberAssignment = groupState.getAssignment(memberId);
                    if (memberAssignment == null || memberAssignment.length == 0) {
                        LOG.warn("No assignment found for member {} in group {}", memberId, groupId);
                        memberAssignment = new byte[0];
                    } else {
                        try {
                            ConsumerProtocolAssignment decoded = ConsumerProtocol.deserializeConsumerProtocolAssignment(ByteBuffer.wrap(memberAssignment));
                            java.util.List<String> assignedTopics = new java.util.ArrayList<>();
                            for (ConsumerProtocolAssignment.TopicPartition tp : decoded.assignedPartitions()) {
                                assignedTopics.add(tp.topic());
                            }
                            LOG.info("Returning assignment to member {}: topics={}, size={} bytes", 
                                    memberId, assignedTopics, memberAssignment.length);
                        } catch (Exception e) {
                            LOG.warn("Failed to decode assignment for logging (will still send it): {}", e.getMessage());
                        }
                    }
                    
                    SyncGroupResponseData responseData = new SyncGroupResponseData()
                            .setErrorCode(Errors.NONE.code())
                            .setProtocolType(groupState.getProtocolType())
                            .setProtocolName(groupState.getProtocolName())
                            .setAssignment(memberAssignment);
                    
                    return new SyncGroupResponse(responseData);
                }
                
            } catch (Exception e) {
                LOG.error("Error handling sync group request", e);
                SyncGroupResponseData errorResponse = new SyncGroupResponseData()
                        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
                return new SyncGroupResponse(errorResponse);
            }
        });
    }

    public void close() {
    }
}
