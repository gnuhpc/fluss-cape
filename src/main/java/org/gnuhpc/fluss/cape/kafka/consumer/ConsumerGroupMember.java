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

public class ConsumerGroupMember {
    private final String memberId;
    private final int sessionTimeoutMs;
    private final int rebalanceTimeoutMs;
    private final String protocolType;
    private long lastHeartbeat;
    private byte[] metadata;
    
    public ConsumerGroupMember(String memberId, int sessionTimeoutMs, int rebalanceTimeoutMs, String protocolType) {
        this.memberId = memberId;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.protocolType = protocolType;
        this.lastHeartbeat = System.currentTimeMillis();
        this.metadata = new byte[0];
    }
    
    public String getMemberId() {
        return memberId;
    }
    
    public int getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }
    
    public int getRebalanceTimeoutMs() {
        return rebalanceTimeoutMs;
    }
    
    public String getProtocolType() {
        return protocolType;
    }
    
    public long getLastHeartbeat() {
        return lastHeartbeat;
    }
    
    public void updateHeartbeat() {
        this.lastHeartbeat = System.currentTimeMillis();
    }
    
    public byte[] getMetadata() {
        return metadata;
    }
    
    public void setMetadata(byte[] metadata) {
        this.metadata = metadata;
    }
}
