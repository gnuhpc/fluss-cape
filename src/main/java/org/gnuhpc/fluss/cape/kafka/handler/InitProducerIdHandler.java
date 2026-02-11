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
 * <h3>⚠️ CRITICAL LIMITATION: No Producer ID Persistence</h3>
 * <p>Producer IDs and transactional ID mappings are stored in-memory only using an {@code AtomicLong}
 * counter (starting at 1000) and a {@code ConcurrentHashMap}. This has severe implications:</p>
 * 
 * <h4>Impact of CAPE Server Restart:</h4>
 * <ul>
 *   <li><b>Producer ID Loss</b>: All assigned producer IDs are lost on restart</li>
 *   <li><b>Counter Reset</b>: Producer ID counter resets to 1000, causing ID reuse</li>
 *   <li><b>Broken Idempotence</b>: Kafka's deduplication fails when IDs are reused across restarts</li>
 *   <li><b>No Transactional Support</b>: Cannot maintain transactional state across restarts</li>
 * </ul>
 * 
 * <h4>Current Behavior:</h4>
 * <table border="1">
 *   <tr><th>Feature</th><th>Status</th><th>Notes</th></tr>
 *   <tr><td>Idempotent Producers (acks=all)</td><td>⚠️ Partial</td><td>Works within session, broken across restarts</td></tr>
 *   <tr><td>Transactional Producers</td><td>❌ Not Supported</td><td>Requires persistent state + fencing</td></tr>
 *   <tr><td>Producer ID Recovery</td><td>❌ None</td><td>No persistence layer implemented</td></tr>
 *   <tr><td>Epoch Management</td><td>❌ Fixed at 0</td><td>No producer fencing</td></tr>
 * </table>
 * 
 * <h4>Why This Matters:</h4>
 * <p>Kafka clients in version 3.x+ enable idempotence by default ({@code enable.idempotence=true}).
 * This handler provides <b>basic compatibility</b> to allow these clients to connect, but does NOT
 * provide true exactly-once semantics or durability guarantees across CAPE restarts.</p>
 * 
 * <h4>Production Impact:</h4>
 * <pre>{@code
 * // Scenario: Producer writes with idempotence enabled
 * producer.send(record1);  // Assigned producerId=1000, epoch=0
 * producer.send(record2);  // Uses producerId=1000
 * // CAPE server restarts
 * producer.send(record3);  // Kafka client still uses producerId=1000
 *                          // But CAPE reassigns producerId=1000 to DIFFERENT producer!
 *                          // Result: Duplicate detection fails, data corruption risk
 * }</pre>
 * 
 * <h4>Workarounds:</h4>
 * <ul>
 *   <li><b>Disable Idempotence</b>: Set {@code enable.idempotence=false} on Kafka producers (trade-off: lose deduplication)</li>
 *   <li><b>Use {@code acks=1}</b>: Lower durability guarantee, but avoids idempotence dependency</li>
 *   <li><b>Avoid CAPE Restarts</b>: Use stable infrastructure to minimize restart frequency</li>
 *   <li><b>Client-Side Retry Logic</b>: Implement application-level deduplication</li>
 * </ul>
 * 
 * <h4>Future Enhancement Required:</h4>
 * <p>To fully support idempotent/transactional producers, this handler must:</p>
 * <ol>
 *   <li>Store producer IDs in a Fluss system table (e.g., {@code kafka_producer_ids})</li>
 *   <li>Persist transactional ID → producer ID mappings durably</li>
 *   <li>Implement epoch bumping for producer fencing</li>
 *   <li>Handle producer ID recovery on startup</li>
 * </ol>
 * 
 * @see org.gnuhpc.fluss.cape.kafka.handler.ProduceHandler ProduceHandler for flush behavior
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
