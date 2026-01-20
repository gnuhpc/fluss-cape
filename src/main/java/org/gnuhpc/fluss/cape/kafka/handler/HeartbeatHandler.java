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

import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.gnuhpc.fluss.cape.kafka.consumer.ConsumerGroupCoordinatorV2;
import org.gnuhpc.fluss.cape.kafka.consumer.ConsumerGroupMember;
import org.gnuhpc.fluss.cape.kafka.consumer.ConsumerGroupState;
import org.gnuhpc.fluss.cape.kafka.consumer.GroupState;
import org.gnuhpc.fluss.cape.kafka.protocol.KafkaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class HeartbeatHandler {
    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatHandler.class);

    private final ConsumerGroupCoordinatorV2 coordinator;

    public HeartbeatHandler(ConsumerGroupCoordinatorV2 coordinator) {
        this.coordinator = coordinator;
    }

    public void handle(KafkaRequest request) {
        coordinator.getInitializationFuture()
            .thenCompose(v -> {
                try {
                    org.apache.kafka.common.requests.HeartbeatRequest heartbeatRequest = request.request();
                    HeartbeatRequestData heartbeatRequestData = heartbeatRequest.data();
                    return handleHeartbeatAsync(heartbeatRequestData);
                } catch (Exception e) {
                    LOG.error("Error handling heartbeat request", e);
                    throw new RuntimeException(e);
                }
            })
            .thenAccept(request::complete)
            .exceptionally(ex -> {
                LOG.error("Error handling heartbeat request", ex);
                request.fail(ex);
                return null;
            });
    }

    private CompletableFuture<HeartbeatResponse> handleHeartbeatAsync(HeartbeatRequestData request) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String groupId = request.groupId();
                String memberId = request.memberId();
                int generationId = request.generationId();
                
                LOG.debug("Heartbeat from member {} in group {} (generation {})", 
                        memberId, groupId, generationId);
                
                ConsumerGroupState groupState = coordinator.getOrCreateGroupState(groupId);
                
                if (!groupState.hasMember(memberId)) {
                    LOG.warn("Heartbeat from unknown member {} in group {}", memberId, groupId);
                    HeartbeatResponseData responseData = new HeartbeatResponseData()
                            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code());
                    return new HeartbeatResponse(responseData);
                }
                
                if (groupState.getGenerationId() != generationId) {
                    LOG.warn("Heartbeat from member {} with wrong generation {} (expected {})",
                            memberId, generationId, groupState.getGenerationId());
                    HeartbeatResponseData responseData = new HeartbeatResponseData()
                            .setErrorCode(Errors.ILLEGAL_GENERATION.code());
                    return new HeartbeatResponse(responseData);
                }
                
                GroupState state = groupState.getState();
                if (state == GroupState.PREPARING_REBALANCE || state == GroupState.COMPLETING_REBALANCE) {
                    LOG.debug("Heartbeat from member {} during rebalance", memberId);
                    HeartbeatResponseData responseData = new HeartbeatResponseData()
                            .setErrorCode(Errors.REBALANCE_IN_PROGRESS.code());
                    return new HeartbeatResponse(responseData);
                }
                
                ConsumerGroupMember member = groupState.getMember(memberId);
                member.updateHeartbeat();
                
                LOG.trace("Updated heartbeat for member {} in group {}", memberId, groupId);
                
                HeartbeatResponseData responseData = new HeartbeatResponseData()
                        .setErrorCode(Errors.NONE.code());
                return new HeartbeatResponse(responseData);
                        
            } catch (Exception e) {
                LOG.error("Error handling heartbeat request", e);
                HeartbeatResponseData responseData = new HeartbeatResponseData()
                        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
                return new HeartbeatResponse(responseData);
            }
        });
    }

    public void close() {
    }
}
