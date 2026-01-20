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

package org.gnuhpc.fluss.cape.hbase.server;

import org.apache.fluss.client.Connection;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public class HealthCheckServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HealthCheckServer.class);

    private final HttpServer httpServer;
    private final Connection flussConnection;
    private final ZooKeeper zooKeeper;
    private final long startTimeMillis;

    public HealthCheckServer(int port, Connection flussConnection, ZooKeeper zooKeeper)
            throws IOException {
        this.flussConnection = flussConnection;
        this.zooKeeper = zooKeeper;
        this.startTimeMillis = System.currentTimeMillis();

        this.httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        this.httpServer.createContext("/health", new HealthHandler());
        this.httpServer.createContext("/ready", new ReadyHandler());
        this.httpServer.setExecutor(null);
    }

    public void start() {
        httpServer.start();
        InetSocketAddress address = httpServer.getAddress();
        LOG.info("Health check server started on port {}", address.getPort());
    }

    @Override
    public void close() {
        if (httpServer != null) {
            httpServer.stop(0);
            LOG.info("Health check server stopped");
        }
    }

    private class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            boolean isHealthy = true;
            StringBuilder response = new StringBuilder();
            response.append("{\n");
            response.append("  \"status\": \"");

            try {
                boolean flussHealthy = flussConnection != null;
                boolean zkHealthy = zooKeeper != null && zooKeeper.getState().isConnected();

                if (!flussHealthy) {
                    isHealthy = false;
                    response.append("unhealthy\",\n");
                    response.append("  \"reason\": \"Fluss connection is null\",\n");
                } else if (!zkHealthy) {
                    isHealthy = false;
                    response.append("unhealthy\",\n");
                    response.append("  \"reason\": \"ZooKeeper connection is not connected\",\n");
                } else {
                    response.append("healthy\",\n");
                }

                long uptimeSeconds = (System.currentTimeMillis() - startTimeMillis) / 1000;
                response.append("  \"uptime_seconds\": ").append(uptimeSeconds).append(",\n");
                response.append("  \"fluss_connection\": \"")
                        .append(flussHealthy ? "connected" : "disconnected")
                        .append("\",\n");
                response.append("  \"zookeeper_connection\": \"")
                        .append(zkHealthy ? "connected" : "disconnected")
                        .append("\",\n");
                response.append("  \"timestamp\": ")
                        .append(System.currentTimeMillis())
                        .append("\n");

            } catch (Exception e) {
                isHealthy = false;
                response.append("unhealthy\",\n");
                response.append("  \"reason\": \"").append(e.getMessage()).append("\",\n");
                response.append("  \"timestamp\": ")
                        .append(System.currentTimeMillis())
                        .append("\n");
            }

            response.append("}");

            int statusCode = isHealthy ? 200 : 503;
            byte[] responseBytes = response.toString().getBytes(StandardCharsets.UTF_8);

            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(statusCode, responseBytes.length);

            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        }
    }

    private class ReadyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            boolean isReady = true;
            String response;

            try {
                boolean flussReady = flussConnection != null;
                boolean zkReady = zooKeeper != null && zooKeeper.getState().isConnected();

                isReady = flussReady && zkReady;

                if (isReady) {
                    response = "{\"status\": \"ready\"}";
                } else {
                    response =
                            "{\"status\": \"not_ready\", \"fluss\": \""
                                    + (flussReady ? "ready" : "not_ready")
                                    + "\", \"zookeeper\": \""
                                    + (zkReady ? "ready" : "not_ready")
                                    + "\"}";
                }
            } catch (Exception e) {
                isReady = false;
                response = "{\"status\": \"not_ready\", \"error\": \"" + e.getMessage() + "\"}";
            }

            int statusCode = isReady ? 200 : 503;
            byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);

            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(statusCode, responseBytes.length);

            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        }
    }
}
