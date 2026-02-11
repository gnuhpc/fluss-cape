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

import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.gnuhpc.fluss.cape.kafka.consumer.ConsumerGroupCoordinatorV2;
import org.gnuhpc.fluss.cape.kafka.consumer.ConsumerGroupState;
import org.gnuhpc.fluss.cape.kafka.consumer.GroupState;
import org.gnuhpc.fluss.cape.kafka.protocol.KafkaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class LeaveGroupHandler {
    private static final Logger LOG = LoggerFactory.getLogger(LeaveGroupHandler.class);

    private final ConsumerGroupCoordinatorV2 coordinator;

    public LeaveGroupHandler(ConsumerGroupCoordinatorV2 coordinator) {
        this.coordinator = coordinator;
    }

    public void handle(KafkaRequest request) {
        coordinator.getInitializationFuture()
            .thenCompose(v -> {
                try {
                    LeaveGroupRequest leaveRequest = request.request();
                    LeaveGroupRequestData leaveRequestData = leaveRequest.data();
                    return handleLeaveGroupAsync(leaveRequestData, request.apiVersion());
                } catch (Exception e) {
                    LOG.error("Error handling leave group request", e);
                    throw new RuntimeException(e);
                }
            })
            .thenAccept(request::complete)
            .exceptionally(ex -> {
                LOG.error("Error handling leave group request", ex);
                request.fail(ex);
                return null;
            });
    }

    private CompletableFuture<LeaveGroupResponse> handleLeaveGroupAsync(
            LeaveGroupRequestData request,
            short apiVersion) {
        
        String groupId = request.groupId();
        LOG.info("LeaveGroup request for group {}", groupId);
        
        ConsumerGroupState groupState = coordinator.getOrCreateGroupState(groupId);
        
        synchronized (groupState) {
            for (LeaveGroupRequestData.MemberIdentity memberIdentity : request.members()) {
                String memberId = memberIdentity.memberId();
                LOG.info("Member {} leaving group {}", memberId, groupId);
                groupState.removeMember(memberId);
            }
            
            if (groupState.getMemberCount() == 0) {
                groupState.setState(GroupState.EMPTY);
                LOG.info("Group {} is now empty", groupId);
            } else if (groupState.getState() == GroupState.STABLE) {
                groupState.setState(GroupState.PREPARING_REBALANCE);
                groupState.incrementGeneration();
                LOG.info("Group {} transitioning to PREPARING_REBALANCE due to member(s) leaving", groupId);
            }
            
            return persistGroupStateAsync(groupState)
                .thenApply(v -> {
                    LeaveGroupResponseData responseData = new LeaveGroupResponseData()
                            .setErrorCode(Errors.NONE.code());
                    return new LeaveGroupResponse(responseData, apiVersion);
                })
                .exceptionally(e -> {
                    LOG.error("Error handling leave group request for group {}", groupId, e);
                    LeaveGroupResponseData errorResponse = new LeaveGroupResponseData()
                            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
                    return new LeaveGroupResponse(errorResponse, apiVersion);
                });
        }
    }

    private CompletableFuture<Void> persistGroupStateAsync(ConsumerGroupState groupState) {
        UpsertWriter groupWriter = coordinator.getGroupWriter();
        
        InternalRow row = GenericRow.of(
            BinaryString.fromString(groupState.getGroupId()),
            BinaryString.fromString(groupState.getState().name()),
            groupState.getProtocolType() != null ? BinaryString.fromString(groupState.getProtocolType()) : null,
            groupState.getProtocolName() != null ? BinaryString.fromString(groupState.getProtocolName()) : null,
            groupState.getLeaderId() != null ? BinaryString.fromString(groupState.getLeaderId()) : null,
            groupState.getGenerationId(),
            BinaryString.fromString("{}")
        );
        
        return groupWriter.upsert(row)
            .thenApply(result -> (Void) null)
            .exceptionally(e -> {
                LOG.error("Error persisting group state", e);
                throw new RuntimeException("Failed to persist group state", e);
            });
    }
}
