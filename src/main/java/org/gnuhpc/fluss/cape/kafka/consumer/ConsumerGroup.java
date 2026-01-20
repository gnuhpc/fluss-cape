/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

package org.gnuhpc.fluss.cape.kafka.consumer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerGroup {
    private final String groupId;
    private int generationId;
    private String protocolType;
    private String protocolName;
    private String leader;
    private GroupState state;
    private final Map<String, ConsumerGroupMember> members = new ConcurrentHashMap<>();
    private final Map<String, byte[]> assignments = new ConcurrentHashMap<>();
    
    public enum GroupState {
        EMPTY,
        PREPARING_REBALANCE,
        COMPLETING_REBALANCE,
        STABLE,
        DEAD
    }
    
    public ConsumerGroup(String groupId) {
        this.groupId = groupId;
        this.generationId = 0;
        this.state = GroupState.EMPTY;
    }
    
    public void addMember(ConsumerGroupMember member) {
        members.put(member.getMemberId(), member);
        if (state == GroupState.EMPTY) {
            state = GroupState.PREPARING_REBALANCE;
        }
    }
    
    public void removeMember(String memberId) {
        members.remove(memberId);
        if (members.isEmpty()) {
            state = GroupState.EMPTY;
        } else {
            state = GroupState.PREPARING_REBALANCE;
        }
    }
    
    public ConsumerGroupMember getMember(String memberId) {
        return members.get(memberId);
    }
    
    public Map<String, ConsumerGroupMember> getMembers() {
        return members;
    }
    
    public boolean isAllMembersJoined() {
        return !members.isEmpty();
    }
    
    public void electLeader() {
        this.leader = members.values().iterator().next().getMemberId();
    }
    
    public void incrementGeneration() {
        this.generationId++;
        this.state = GroupState.COMPLETING_REBALANCE;
    }
    
    public void setAssignments(Map<String, byte[]> assignments) {
        this.assignments.clear();
        this.assignments.putAll(assignments);
        this.state = GroupState.STABLE;
    }
    
    public byte[] getAssignment(String memberId) {
        return assignments.getOrDefault(memberId, new byte[0]);
    }
    
    public boolean needsRebalance() {
        return state == GroupState.PREPARING_REBALANCE;
    }
    
    public String getGroupId() { return groupId; }
    public int getGenerationId() { return generationId; }
    public String getProtocolType() { return protocolType; }
    public String getProtocolName() { return protocolName; }
    public String getLeader() { return leader; }
    public GroupState getState() { return state; }
    
    public void setProtocolType(String protocolType) { this.protocolType = protocolType; }
    public void setProtocolName(String protocolName) { this.protocolName = protocolName; }
}
