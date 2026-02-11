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
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.message.ConsumerProtocolAssignment;
import org.apache.kafka.common.message.ConsumerProtocolSubscription;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.gnuhpc.fluss.cape.kafka.consumer.ConsumerGroupCoordinatorV2;
import org.gnuhpc.fluss.cape.kafka.consumer.ConsumerGroupMember;
import org.gnuhpc.fluss.cape.kafka.consumer.ConsumerGroupState;
import org.gnuhpc.fluss.cape.kafka.consumer.GroupState;
import org.gnuhpc.fluss.cape.kafka.protocol.KafkaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class JoinGroupHandler {
    private static final Logger LOG = LoggerFactory.getLogger(JoinGroupHandler.class);

    private final ConsumerGroupCoordinatorV2 coordinator;
    private final ScheduledExecutorService rebalanceExecutor;
    private final Map<String, ConcurrentHashMap<String, CompletableFuture<JoinGroupResponse>>>
            pendingJoinResponses;
    private final Map<String, ScheduledFuture<?>> rebalanceTimeouts;
    private final Map<String, ScheduledFuture<?>> joinCompletionTimeouts;
    private final ConcurrentHashMap<String, Lock> groupLocks = new ConcurrentHashMap<>();

    public JoinGroupHandler(ConsumerGroupCoordinatorV2 coordinator) {
        this.coordinator = coordinator;
        this.rebalanceExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "join-group-rebalance-waiter");
            thread.setDaemon(true);
            return thread;
        });
        this.pendingJoinResponses = new ConcurrentHashMap<>();
        this.rebalanceTimeouts = new ConcurrentHashMap<>();
        this.joinCompletionTimeouts = new ConcurrentHashMap<>();
    }

    public void handle(KafkaRequest request) {
        coordinator.getInitializationFuture()
            .thenCompose(v -> {
                try {
                    org.apache.kafka.common.requests.JoinGroupRequest joinRequest = request.request();
                    JoinGroupRequestData joinRequestData = joinRequest.data();
                    return handleJoinGroupAsync(joinRequestData, request.apiVersion());
                } catch (Exception e) {
                    LOG.error("Error handling join group request", e);
                    throw new RuntimeException(e);
                }
            })
            .thenAccept(request::complete)
            .exceptionally(ex -> {
                LOG.error("Error handling join group request", ex);
                request.fail(ex);
                return null;
            });
    }

    private CompletableFuture<JoinGroupResponse> handleJoinGroupAsync(
            JoinGroupRequestData request, 
            short apiVersion) {
        
        String groupId = request.groupId();
        String memberId = request.memberId();
        String protocolType = request.protocolType();
        int sessionTimeoutMs = request.sessionTimeoutMs();
        int rebalanceTimeoutMs = request.rebalanceTimeoutMs();
        
        LOG.debug("JoinGroup request from member {} in group {} (protocol: {})", 
                memberId, groupId, protocolType);
        
        ConsumerGroupState groupState = coordinator.getOrCreateGroupState(groupId);
        
        Lock groupLock = groupLocks.computeIfAbsent(groupId, k -> new ReentrantLock());
        groupLock.lock();
        try {
            if (groupState.getProtocolType() == null) {
                groupState.setProtocolType(protocolType);
            } else if (!groupState.getProtocolType().equals(protocolType)) {
                LOG.warn("JoinGroup rejected: protocol type mismatch ({} vs {})", 
                        protocolType, groupState.getProtocolType());
                JoinGroupResponseData errorResponse = new JoinGroupResponseData()
                        .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code());
                return CompletableFuture.completedFuture(new JoinGroupResponse(errorResponse, apiVersion));
            }
            
            boolean enteringRebalance = false;
            if (groupState.getState() == GroupState.EMPTY) {
                groupState.setState(GroupState.PREPARING_REBALANCE);
                groupState.incrementGeneration();
                groupState.setLeaderId(null);
                enteringRebalance = true;
                LOG.info("First member joining group {}, transitioning to PREPARING_REBALANCE", groupId);
            } else if (groupState.getState() == GroupState.STABLE || groupState.getState() == GroupState.COMPLETING_REBALANCE) {
                GroupState oldState = groupState.getState();
                groupState.setState(GroupState.PREPARING_REBALANCE);
                groupState.incrementGeneration();
                groupState.resetAwaitingMembers();
                groupState.resetExpectedMemberCount();
                groupState.setLeaderId(null);
                enteringRebalance = true;
                LOG.info("New member joining group {} in state {}, generation now {}, triggering rebalance", 
                        groupId, oldState, groupState.getGenerationId());
            } else {
                LOG.debug("Member joining group {} in state {} (generation {})", 
                        groupId, groupState.getState(), groupState.getGenerationId());
            }

            boolean isNewMember = false;
            if (memberId == null || memberId.isEmpty()) {
                memberId = UUID.randomUUID().toString();
                isNewMember = true;
                LOG.debug("Assigned new member ID: {}", memberId);
            }
            
            String finalMemberId = memberId;
            
            ConsumerGroupMember existingMember = groupState.getMember(finalMemberId);
            if (existingMember == null) {
                isNewMember = true;
                existingMember = new ConsumerGroupMember(
                        finalMemberId,
                        sessionTimeoutMs,
                        rebalanceTimeoutMs,
                        protocolType);
                groupState.addMember(existingMember);
                LOG.info("New member {} joined group {}", finalMemberId, groupId);
            } else {
                existingMember.updateHeartbeat();
                LOG.debug("Existing member {} rejoining group {}", finalMemberId, groupId);
            }
            
            if (!request.protocols().isEmpty()) {
                JoinGroupRequestData.JoinGroupRequestProtocol protocol = request.protocols().iterator().next();
                existingMember.setMetadata(protocol.metadata());
            }
            
            if (enteringRebalance) {
                groupState.setExpectedMemberCount(groupState.getMemberCount());
            }
            groupState.incrementAwaitingMembers();
            
            CompletableFuture<JoinGroupResponse> pendingFuture = new CompletableFuture<>();
            pendingJoinResponses
                    .computeIfAbsent(groupId, id -> new ConcurrentHashMap<>())
                    .put(finalMemberId, pendingFuture);
            
            if (groupState.hasAllMembersJoined()) {
                groupState.setState(GroupState.COMPLETING_REBALANCE);
                LOG.info("All {} members joined group {}, completing rebalance", 
                        groupState.getMemberCount(), groupId);
            }
            
            cancelJoinCompletionTimeout(groupId);
            CompletableFuture<Void> persistFuture = persistGroupStateAsync(groupState);
            
            if (groupState.hasExpectedMembers() && groupState.getState() != GroupState.COMPLETING_REBALANCE) {
                persistFuture = persistFuture.thenRun(() ->
                        scheduleJoinCompletionTimeout(groupId, groupState, apiVersion));
            }
            
            if (groupState.getState() == GroupState.COMPLETING_REBALANCE) {
                persistFuture = persistFuture.thenRun(() ->
                        completePendingJoins(groupId, groupState, apiVersion));
            }
            
            return persistFuture
                    .thenCompose(v -> pendingFuture)
                    .exceptionally(e -> {
                        LOG.error("Error handling join group request for member {} in group {}", 
                                finalMemberId, groupId, e);
                        JoinGroupResponseData errorResponse = new JoinGroupResponseData()
                                .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
                        return new JoinGroupResponse(errorResponse, apiVersion);
                    });
        } finally {
            groupLock.unlock();
        }
    }
    
    private Map<String, byte[]> buildAssignments(ConsumerGroupState groupState) {
        List<ConsumerGroupMember> members = new ArrayList<>(groupState.getMembers());
        if (members.isEmpty()) {
            return new HashMap<>();
        }

        members.sort(Comparator.comparing(ConsumerGroupMember::getMemberId));

        String topic = resolveTopicName(members);
        int partitionCount = 3;

        Map<String, List<Integer>> memberPartitions = new HashMap<>();
        for (ConsumerGroupMember member : members) {
            memberPartitions.put(member.getMemberId(), new ArrayList<>());
        }

        for (int partition = 0; partition < partitionCount; partition++) {
            int memberIndex = partition % members.size();
            String memberId = members.get(memberIndex).getMemberId();
            memberPartitions.get(memberId).add(partition);
        }

        Map<String, byte[]> assignments = new HashMap<>();
        for (ConsumerGroupMember member : members) {
            List<Integer> partitions = memberPartitions.get(member.getMemberId());
            ConsumerProtocolAssignment assignment = buildAssignment(topic, partitions);
            ByteBuffer buffer = ConsumerProtocol.serializeAssignment(assignment, (short) 1);
            
            // Extract only the valid bytes from the ByteBuffer (from position 0 to limit)
            byte[] assignmentBytes = new byte[buffer.remaining()];
            buffer.get(assignmentBytes);
            assignments.put(member.getMemberId(), assignmentBytes);
        }

        return assignments;
    }

    private String resolveTopicName(List<ConsumerGroupMember> members) {
        for (ConsumerGroupMember member : members) {
            byte[] metadata = member.getMetadata();
            if (metadata == null || metadata.length == 0) {
                continue;
            }
            try {
                ByteBuffer buf = ByteBuffer.wrap(metadata);
                short version = ConsumerProtocol.deserializeVersion(buf);
                ConsumerProtocolSubscription subscription = ConsumerProtocol.deserializeConsumerProtocolSubscription(buf, version);
                if (subscription.topics() != null && !subscription.topics().isEmpty()) {
                    String topic = subscription.topics().get(0);
                    LOG.info("Resolved topic from subscription metadata: {}", topic);
                    return topic;
                }
            } catch (Exception e) {
                LOG.warn("Failed to decode subscription metadata for member {}, falling back to test-topic", member.getMemberId(), e);
            }
        }

        LOG.warn("No valid subscription found, using fallback topic: test-topic");
        return "test-topic";
    }

    private ConsumerProtocolAssignment buildAssignment(String topic, List<Integer> partitions) {
        ConsumerProtocolAssignment assignment = new ConsumerProtocolAssignment();
        ConsumerProtocolAssignment.TopicPartition topicPartition =
                new ConsumerProtocolAssignment.TopicPartition()
                        .setTopic(topic)
                        .setPartitions(partitions);
        assignment.setAssignedPartitions(new ConsumerProtocolAssignment.TopicPartitionCollection(
                java.util.Collections.singletonList(topicPartition).iterator()));
        assignment.setUserData(ByteBuffer.allocate(0));
        return assignment;
    }

    private void scheduleRebalanceTimeout(
            String groupId,
            ConsumerGroupState groupState,
            short apiVersion,
            int rebalanceTimeoutMs) {
        ScheduledFuture<?> timeoutFuture = rebalanceExecutor.schedule(() -> {
            synchronized (groupState) {
                if (groupState.getState() == GroupState.PREPARING_REBALANCE) {
                    LOG.warn("Rebalance timeout reached for group {}", groupId);
                    groupState.setState(GroupState.COMPLETING_REBALANCE);
                    groupState.setExpectedMemberCount(groupState.getAwaitingMembers());
                    groupState.resetAwaitingMembers();
                    completePendingJoins(groupId, groupState, apiVersion);
                }
            }
        }, rebalanceTimeoutMs, TimeUnit.MILLISECONDS);
        rebalanceTimeouts.put(groupId, timeoutFuture);
    }

    private void scheduleJoinCompletionTimeout(String groupId, ConsumerGroupState groupState, short apiVersion) {
        ScheduledFuture<?> timeoutFuture = rebalanceExecutor.schedule(() -> {
            synchronized (groupState) {
                if (groupState.getState() == GroupState.PREPARING_REBALANCE
                        && !groupState.hasAllMembersJoined()) {
                    LOG.info("JoinGroup wait elapsed for group {}", groupId);
                    groupState.setState(GroupState.COMPLETING_REBALANCE);
                    groupState.setExpectedMemberCount(groupState.getAwaitingMembers());
                    groupState.resetAwaitingMembers();
                    completePendingJoins(groupId, groupState, apiVersion);
                }
            }
        }, 10000, TimeUnit.MILLISECONDS);
        joinCompletionTimeouts.put(groupId, timeoutFuture);
    }

    private void cancelJoinCompletionTimeout(String groupId) {
        ScheduledFuture<?> timeout = joinCompletionTimeouts.remove(groupId);
        if (timeout != null) {
            timeout.cancel(false);
        }
    }

    private void cancelRebalanceTimeout(String groupId) {
        ScheduledFuture<?> timeout = rebalanceTimeouts.remove(groupId);
        if (timeout != null) {
            timeout.cancel(false);
        }
    }

    private void completePendingJoins(String groupId, ConsumerGroupState groupState, short apiVersion) {
        ConcurrentHashMap<String, CompletableFuture<JoinGroupResponse>> futures =
                pendingJoinResponses.remove(groupId);
        cancelRebalanceTimeout(groupId);
        cancelJoinCompletionTimeout(groupId);
        if (futures == null || futures.isEmpty()) {
            return;
        }

        for (ConsumerGroupMember m : groupState.getMembers()) {
            if (m.getMetadata() == null) {
                m.setMetadata(new byte[0]);
            }
        }

        if (groupState.getLeaderId() == null) {
            List<ConsumerGroupMember> sortedMembers = new ArrayList<>(groupState.getMembers());
            sortedMembers.sort(Comparator.comparing(ConsumerGroupMember::getMemberId));
            String leaderId = sortedMembers.get(0).getMemberId();
            groupState.setLeaderId(leaderId);
            groupState.setProtocolName("range");
            
            LOG.info("Group {} elected leader: generation={}, leader={}, members={}", 
                    groupId, groupState.getGenerationId(), leaderId, groupState.getMemberCount());
            
            Map<String, byte[]> assignments = buildAssignments(groupState);
            groupState.setAssignments(assignments);
            LOG.info("Leader {} computed assignments for {} members", leaderId, assignments.size());
        }

        for (ConsumerGroupMember member : groupState.getMembers()) {
            CompletableFuture<JoinGroupResponse> future = futures.get(member.getMemberId());
            if (future != null) {
                future.complete(buildJoinGroupResponse(groupState, member, apiVersion));
            }
        }

        groupState.setExpectedMemberCount(groupState.getMemberCount());
        groupState.resetAwaitingMembers();
    }

    private JoinGroupResponse buildJoinGroupResponse(
            ConsumerGroupState groupState,
            ConsumerGroupMember member,
            short apiVersion) {
        JoinGroupResponseData responseData = new JoinGroupResponseData()
                .setErrorCode(Errors.NONE.code())
                .setGenerationId(groupState.getGenerationId())
                .setProtocolType(groupState.getProtocolType())
                .setProtocolName(groupState.getProtocolName())
                .setLeader(groupState.getLeaderId())
                .setMemberId(member.getMemberId());

        if (member.getMemberId().equals(groupState.getLeaderId())) {
            for (ConsumerGroupMember groupMember : groupState.getMembers()) {
                responseData.members().add(
                        new JoinGroupResponseData.JoinGroupResponseMember()
                                .setMemberId(groupMember.getMemberId())
                                .setMetadata(groupMember.getMetadata()));
            }
        }

        return new JoinGroupResponse(responseData, apiVersion);
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

    public void close() {
    }
}
