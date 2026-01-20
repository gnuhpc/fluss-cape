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

import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.gnuhpc.fluss.cape.kafka.protocol.KafkaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles INIT_PRODUCER_ID requests for idempotent and transactional producers.
 * 
 * For now, we implement basic support for idempotent producers by:
 * 1. Generating unique producer IDs
 * 2. Setting epoch to 0 (no producer fencing)
 * 3. Not supporting full transactional semantics
 * 
 * This allows Kafka 3.x clients to work (they enable idempotence by default),
 * but doesn't provide true exactly-once semantics.
 */
public class InitProducerIdHandler {
    private static final Logger LOG = LoggerFactory.getLogger(InitProducerIdHandler.class);
    
    private final AtomicLong producerIdGenerator = new AtomicLong(1000);
    private final ConcurrentHashMap<String, Long> transactionalIdToProducerId = new ConcurrentHashMap<>();

    public void handle(KafkaRequest request) {
        try {
            InitProducerIdRequest initRequest = request.request();
            String transactionalId = initRequest.data().transactionalId();
            int transactionTimeoutMs = initRequest.data().transactionTimeoutMs();
            long producerEpoch = initRequest.data().producerEpoch();
            
            LOG.info("Handling INIT_PRODUCER_ID: transactionalId={}, timeout={}ms, epoch={}", 
                    transactionalId, transactionTimeoutMs, producerEpoch);
            
            if (transactionalId != null && !transactionalId.isEmpty()) {
                LOG.warn("Transactional producers not fully supported, transactionalId={}", transactionalId);
            }
            
            long producerId;
            if (transactionalId != null && !transactionalId.isEmpty()) {
                producerId = transactionalIdToProducerId.computeIfAbsent(
                    transactionalId, 
                    k -> producerIdGenerator.getAndIncrement()
                );
            } else {
                producerId = producerIdGenerator.getAndIncrement();
            }
            
            short producerEpochShort = 0;
            
            InitProducerIdResponseData responseData = new InitProducerIdResponseData()
                .setErrorCode(Errors.NONE.code())
                .setProducerId(producerId)
                .setProducerEpoch(producerEpochShort)
                .setThrottleTimeMs(0);
            
            InitProducerIdResponse response = new InitProducerIdResponse(responseData);
            
            LOG.info("Generated producer ID: {} with epoch: {}", producerId, producerEpochShort);
            request.complete(response);
            
        } catch (Exception e) {
            LOG.error("Error handling INIT_PRODUCER_ID request", e);
            request.fail(e);
        }
    }
}
