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

import org.apache.fluss.row.InternalRow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConsumerGroupState {
    private final String groupId;
    private final Lock groupLock = new ReentrantLock();
    private GroupState state;
    private String protocolType;
    private String protocolName;
    private String leaderId;
    private int generationId;
    private int expectedMemberCount;
    private int awaitingMembers;
    private final ConcurrentHashMap<String, ConsumerGroupMember> members;
    private final Map<String, byte[]> assignments;
    
    public ConsumerGroupState(String groupId) {
        this.groupId = groupId;
        this.state = GroupState.EMPTY;
        this.members = new ConcurrentHashMap<>();
        this.assignments = new HashMap<>();
        this.generationId = 0;
        this.expectedMemberCount = 0;
        this.awaitingMembers = 0;
    }
    
    public static ConsumerGroupState fromRow(InternalRow row) {
        ConsumerGroupState state = new ConsumerGroupState(row.getString(0).toString());
        state.setState(GroupState.valueOf(row.getString(1).toString()));
        state.setProtocolType(row.getString(2).toString());
        state.setProtocolName(row.getString(3).toString());
        state.setLeaderId(row.getString(4).toString());
        state.setGenerationId(row.getInt(5));
        return state;
    }
    
    public String getGroupId() {
        return groupId;
    }
    
    public GroupState getState() {
        groupLock.lock();
        try {
            return state;
        } finally {
            groupLock.unlock();
        }
    }
    
    public void setState(GroupState state) {
        groupLock.lock();
        try {
            this.state = state;
        } finally {
            groupLock.unlock();
        }
    }
    
    public String getProtocolType() {
        return protocolType;
    }
    
    public void setProtocolType(String protocolType) {
        this.protocolType = protocolType;
    }
    
    public String getProtocolName() {
        return protocolName;
    }
    
    public void setProtocolName(String protocolName) {
        this.protocolName = protocolName;
    }
    
    public String getLeaderId() {
        groupLock.lock();
        try {
            return leaderId;
        } finally {
            groupLock.unlock();
        }
    }
    
    public void setLeaderId(String leaderId) {
        groupLock.lock();
        try {
            this.leaderId = leaderId;
        } finally {
            groupLock.unlock();
        }
    }
    
    public int getGenerationId() {
        groupLock.lock();
        try {
            return generationId;
        } finally {
            groupLock.unlock();
        }
    }
    
    public void setGenerationId(int generationId) {
        groupLock.lock();
        try {
            this.generationId = generationId;
        } finally {
            groupLock.unlock();
        }
    }
    
    public void incrementGeneration() {
        groupLock.lock();
        try {
            this.generationId++;
        } finally {
            groupLock.unlock();
        }
    }
    
    public ConcurrentHashMap<String, ConsumerGroupMember> getMembersMap() {
        return members;
    }
    
    public List<ConsumerGroupMember> getMembers() {
        return new ArrayList<>(members.values());
    }
    
    public void addMember(ConsumerGroupMember member) {
        groupLock.lock();
        try {
            members.put(member.getMemberId(), member);
        } finally {
            groupLock.unlock();
        }
    }
    
    public void removeMember(String memberId) {
        groupLock.lock();
        try {
            members.remove(memberId);
        } finally {
            groupLock.unlock();
        }
    }
    
    public ConsumerGroupMember getMember(String memberId) {
        return members.get(memberId);
    }
    
    public boolean hasMember(String memberId) {
        return members.containsKey(memberId);
    }
    
    public int getMemberCount() {
        return members.size();
    }

    public int getExpectedMemberCount() {
        groupLock.lock();
        try {
            return expectedMemberCount;
        } finally {
            groupLock.unlock();
        }
    }

    public int getAwaitingMembers() {
        groupLock.lock();
        try {
            return awaitingMembers;
        } finally {
            groupLock.unlock();
        }
    }

    public void setExpectedMemberCount(int count) {
        groupLock.lock();
        try {
            this.expectedMemberCount = count;
        } finally {
            groupLock.unlock();
        }
    }

    public void resetAwaitingMembers() {
        groupLock.lock();
        try {
            this.awaitingMembers = 0;
        } finally {
            groupLock.unlock();
        }
    }

    public void resetExpectedMemberCount() {
        groupLock.lock();
        try {
            this.expectedMemberCount = 0;
        } finally {
            groupLock.unlock();
        }
    }

    public void incrementAwaitingMembers() {
        groupLock.lock();
        try {
            this.awaitingMembers++;
        } finally {
            groupLock.unlock();
        }
    }

    public void decrementAwaitingMembers() {
        groupLock.lock();
        try {
            if (this.awaitingMembers > 0) {
                this.awaitingMembers--;
            }
        } finally {
            groupLock.unlock();
        }
    }

    public boolean hasAllMembersJoined() {
        groupLock.lock();
        try {
            return expectedMemberCount > 0 && awaitingMembers >= expectedMemberCount;
        } finally {
            groupLock.unlock();
        }
    }

    public boolean hasExpectedMembers() {
        groupLock.lock();
        try {
            return expectedMemberCount > 0;
        } finally {
            groupLock.unlock();
        }
    }

    public void setAssignments(Map<String, byte[]> assignments) {
        groupLock.lock();
        try {
            this.assignments.clear();
            this.assignments.putAll(assignments);
        } finally {
            groupLock.unlock();
        }
    }

    public byte[] getAssignment(String memberId) {
        groupLock.lock();
        try {
            return assignments.getOrDefault(memberId, new byte[0]);
        } finally {
            groupLock.unlock();
        }
    }

    /**
     * Atomically transition to rebalance state. This method ensures that state, generation,
     * and counters are updated together without interference from other threads.
     */
    public void transitionToRebalance() {
        groupLock.lock();
        try {
            this.state = GroupState.PREPARING_REBALANCE;
            this.generationId++;
            this.awaitingMembers = 0;
            this.expectedMemberCount = 0;
        } finally {
            groupLock.unlock();
        }
    }

    /**
     * Atomically transition to completing rebalance state. This is used when moving
     * from PREPARING_REBALANCE to COMPLETING_REBALANCE.
     */
    public void transitionToCompletingRebalance(String leaderId, int expectedMemberCount) {
        groupLock.lock();
        try {
            this.state = GroupState.COMPLETING_REBALANCE;
            this.leaderId = leaderId;
            this.expectedMemberCount = expectedMemberCount;
            this.awaitingMembers = 1; // Leader has joined
        } finally {
            groupLock.unlock();
        }
    }

    /**
     * Atomically add member and increment awaiting counter. This ensures consistent
     * state when new members join during rebalance.
     */
    public void addMemberAndIncrement(ConsumerGroupMember member) {
        groupLock.lock();
        try {
            members.put(member.getMemberId(), member);
            this.awaitingMembers++;
        } finally {
            groupLock.unlock();
        }
    }

    /**
     * Atomically check and transition to stable state if all members have synced.
     * Returns true if transition happened, false otherwise.
     */
    public boolean tryTransitionToStable() {
        groupLock.lock();
        try {
            if (awaitingMembers >= expectedMemberCount && expectedMemberCount > 0) {
                this.state = GroupState.STABLE;
                return true;
            }
            return false;
        } finally {
            groupLock.unlock();
        }
    }
}
