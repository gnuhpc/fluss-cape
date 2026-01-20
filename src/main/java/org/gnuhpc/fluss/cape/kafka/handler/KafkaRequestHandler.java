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

import org.apache.fluss.client.Connection;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.gnuhpc.fluss.cape.kafka.config.KafkaCompatConfig;
import org.gnuhpc.fluss.cape.kafka.consumer.ConsumerGroupCoordinatorV2;
import org.gnuhpc.fluss.cape.kafka.converter.TablePathResolver;
import org.gnuhpc.fluss.cape.kafka.pool.ScannerPool;
import org.gnuhpc.fluss.cape.kafka.pool.WriterPool;
import org.gnuhpc.fluss.cape.kafka.protocol.KafkaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaRequestHandler {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaRequestHandler.class);

    private final Connection flussConnection;
    private final KafkaCompatConfig config;
    private final TablePathResolver tablePathResolver;
    private final ScannerPool scannerPool;
    private final WriterPool writerPool;
    private final ExecutorService executorService;
    private final ConsumerGroupCoordinatorV2 consumerGroupCoordinator;
    private final MetadataHandler metadataHandler;
    private final ProduceHandler produceHandler;
    private final FetchHandler fetchHandler;
    private final ListOffsetsHandler listOffsetsHandler;
    private final JoinGroupHandler joinGroupHandler;
    private final SyncGroupHandler syncGroupHandler;
    private final HeartbeatHandler heartbeatHandler;
    private final OffsetCommitHandler offsetCommitHandler;
    private final OffsetFetchHandler offsetFetchHandler;
    private final InitProducerIdHandler initProducerIdHandler;
    private final FindCoordinatorHandler findCoordinatorHandler;

    public KafkaRequestHandler(Connection flussConnection, KafkaCompatConfig config) {
        this.flussConnection = flussConnection;
        this.config = config;
        this.tablePathResolver = new TablePathResolver(config.getDefaultDatabase());
        
        this.scannerPool = new ScannerPool(
            flussConnection,
            tablePathResolver,
            100,
            Duration.ofMinutes(5)
        );
        
        this.writerPool = new WriterPool(
            flussConnection,
            tablePathResolver,
            50,
            Duration.ofMinutes(5)
        );
        
        this.executorService = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() * 2,
            r -> {
                Thread t = new Thread(r, "kafka-handler-worker");
                t.setDaemon(true);
                return t;
            }
        );
        
        this.consumerGroupCoordinator = new ConsumerGroupCoordinatorV2(flussConnection);
        
        this.consumerGroupCoordinator.getInitializationFuture()
            .thenRun(() -> LOG.info("Consumer group coordinator fully initialized"))
            .exceptionally(e -> {
                LOG.error("Failed to initialize consumer group coordinator", e);
                throw new RuntimeException("Failed to initialize consumer group coordinator", e);
            });
        
        this.metadataHandler = new MetadataHandler(flussConnection, config, tablePathResolver);
        this.produceHandler = new ProduceHandler(flussConnection, writerPool, config, tablePathResolver);
        this.fetchHandler = new FetchHandler(flussConnection, scannerPool, config, tablePathResolver, executorService);
        this.listOffsetsHandler = new ListOffsetsHandler(flussConnection, config, tablePathResolver);
        this.joinGroupHandler = new JoinGroupHandler(consumerGroupCoordinator);
        this.syncGroupHandler = new SyncGroupHandler(consumerGroupCoordinator);
        this.heartbeatHandler = new HeartbeatHandler(consumerGroupCoordinator);
        this.offsetCommitHandler = new OffsetCommitHandler(consumerGroupCoordinator);
        this.offsetFetchHandler = new OffsetFetchHandler(consumerGroupCoordinator);
        this.initProducerIdHandler = new InitProducerIdHandler();
        this.findCoordinatorHandler = new FindCoordinatorHandler(config);
        
        LOG.info("Initialized KafkaRequestHandler with ScannerPool(size=100), WriterPool(size=50), and ConsumerGroupCoordinator");
    }

    public void processRequest(KafkaRequest request) {
        LOG.info("Processing request: apiKey={}, version={}, correlationId={}", 
                request.apiKey(), request.apiVersion(), request.requestId());
        try {
            switch (request.apiKey()) {
                case API_VERSIONS:
                    handleApiVersionsRequest(request);
                    break;
                case METADATA:
                    metadataHandler.handle(request);
                    break;
                case PRODUCE:
                    produceHandler.handle(request);
                    break;
                case FETCH:
                    fetchHandler.handle(request);
                    break;
                case LIST_OFFSETS:
                    LOG.info("Dispatching to ListOffsetsHandler");
                    listOffsetsHandler.handle(request);
                    break;
                case JOIN_GROUP:
                    joinGroupHandler.handle(request);
                    break;
                case SYNC_GROUP:
                    syncGroupHandler.handle(request);
                    break;
                case HEARTBEAT:
                    heartbeatHandler.handle(request);
                    break;
                case OFFSET_COMMIT:
                    offsetCommitHandler.handle(request);
                    break;
                case OFFSET_FETCH:
                    offsetFetchHandler.handle(request);
                    break;
                case INIT_PRODUCER_ID:
                    initProducerIdHandler.handle(request);
                    break;
                case FIND_COORDINATOR:
                    findCoordinatorHandler.handle(request);
                    break;
                default:
                    LOG.warn("Unsupported API key: {}", request.apiKey());
                    handleUnsupportedRequest(request);
            }
        } catch (Exception e) {
            LOG.error("Error processing request {}", request.apiKey(), e);
            request.fail(e);
        }
    }

    private void handleApiVersionsRequest(KafkaRequest request) {
        short apiVersion = request.apiVersion();
        if (!ApiKeys.API_VERSIONS.isVersionSupported(apiVersion)) {
            request.fail(Errors.UNSUPPORTED_VERSION.exception());
            return;
        }
        ApiVersionsResponseData data = new ApiVersionsResponseData();
        for (ApiKeys apiKey : ApiKeys.values()) {
            if (apiKey.minRequiredInterBrokerMagic <= RecordBatch.CURRENT_MAGIC_VALUE) {
                ApiVersionsResponseData.ApiVersion apiVersionData =
                        new ApiVersionsResponseData.ApiVersion()
                                .setApiKey(apiKey.id)
                                .setMinVersion(apiKey.oldestVersion())
                                .setMaxVersion(apiKey.latestVersion());
                if (apiKey.equals(ApiKeys.METADATA)) {
                    short v = apiKey.latestVersion() > 11 ? 11 : apiKey.latestVersion();
                    apiVersionData.setMaxVersion(v);
                } else if (apiKey.equals(ApiKeys.FETCH)) {
                    short v = apiKey.latestVersion() > 12 ? 12 : apiKey.latestVersion();
                    apiVersionData.setMaxVersion(v);
                } else if (apiKey.equals(ApiKeys.LIST_OFFSETS)) {
                    short v = apiKey.latestVersion() > 7 ? 7 : apiKey.latestVersion();
                    apiVersionData.setMaxVersion(v);
                }
                data.apiKeys().add(apiVersionData);
            }
        }
        request.complete(new ApiVersionsResponse(data));
    }

    private void handleUnsupportedRequest(KafkaRequest request) {
        String message = String.format("Unsupported request with api key %s", request.apiKey());
        AbstractRequest abstractRequest = request.request();
        AbstractResponse response = abstractRequest.getErrorResponse(
                new UnsupportedOperationException(message));
        request.complete(response);
    }

    public void close() {
        LOG.info("Closing KafkaRequestHandler");
        
        try {
            consumerGroupCoordinator.close();
        } catch (Exception e) {
            LOG.error("Error closing consumer group coordinator", e);
        }
        
        try {
            scannerPool.close();
        } catch (Exception e) {
            LOG.error("Error closing scanner pool", e);
        }
        
        try {
            writerPool.close();
        } catch (Exception e) {
            LOG.error("Error closing writer pool", e);
        }
        
        try {
            executorService.shutdownNow();
        } catch (Exception e) {
            LOG.error("Error shutting down executor service", e);
        }
        
        produceHandler.close();
        LOG.info("KafkaRequestHandler closed");
    }
}
