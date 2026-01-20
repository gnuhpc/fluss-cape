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

import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.gnuhpc.fluss.cape.kafka.config.KafkaCompatConfig;
import org.gnuhpc.fluss.cape.kafka.protocol.KafkaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for FIND_COORDINATOR requests.
 * 
 * In Kafka, coordinator discovery is used for consumer groups and transactions.
 * Since we run as a single compatibility layer, we act as the coordinator for all groups.
 * This handler always returns this server as the coordinator.
 */
public class FindCoordinatorHandler {
    private static final Logger LOG = LoggerFactory.getLogger(FindCoordinatorHandler.class);
    
    private final String coordinatorHost;
    private final int coordinatorPort;
    
    public FindCoordinatorHandler(KafkaCompatConfig config) {
        this.coordinatorHost = "localhost";
        this.coordinatorPort = config.getPort();
    }
    
    public void handle(KafkaRequest request) {
        try {
            org.apache.kafka.common.requests.FindCoordinatorRequest findRequest = request.request();
            FindCoordinatorRequestData requestData = findRequest.data();
            
            LOG.info("FIND_COORDINATOR request: key={}, keyType={}, version={}", 
                requestData.key(), requestData.keyType(), request.apiVersion());
            
            FindCoordinatorResponseData responseData = new FindCoordinatorResponseData();
            
            // Version 4+ uses a list of coordinators, older versions use single coordinator fields
            if (request.apiVersion() >= 4) {
                FindCoordinatorResponseData.Coordinator coordinator = 
                    new FindCoordinatorResponseData.Coordinator()
                        .setKey(requestData.key())
                        .setNodeId(0)
                        .setHost(coordinatorHost)
                        .setPort(coordinatorPort)
                        .setErrorCode(Errors.NONE.code())
                        .setErrorMessage(null);
                
                responseData.setCoordinators(java.util.Collections.singletonList(coordinator));
            } else {
                // Legacy format (versions 0-3)
                responseData
                    .setErrorCode(Errors.NONE.code())
                    .setErrorMessage(null)
                    .setNodeId(0)
                    .setHost(coordinatorHost)
                    .setPort(coordinatorPort);
            }
            
            LOG.info("Returning coordinator (v{}): host={}, port={}, nodeId=0", 
                request.apiVersion(), coordinatorHost, coordinatorPort);
            
            request.complete(new FindCoordinatorResponse(responseData));
            
        } catch (Exception e) {
            LOG.error("Error handling FIND_COORDINATOR request", e);
            request.fail(e);
        }
    }
}
