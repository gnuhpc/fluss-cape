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

package org.gnuhpc.fluss.cape.kafka.config;

/**
 * Configuration for Kafka compatibility layer.
 */
public class KafkaCompatConfig {
    
    // Server configuration
    private final String host;
    private final int port;
    private final int nodeId;
    
    // Default database for topic mapping
    private final String defaultDatabase;
    
    // Auto-create tables when producing to non-existent topics
    private final boolean autoCreateTables;
    
    // Number of buckets for auto-created log tables
    private final int defaultNumBuckets;
    
    public static final String DEFAULT_HOST = "0.0.0.0";
    public static final int DEFAULT_PORT = 9092;
    public static final int DEFAULT_NODE_ID = 0;
    public static final String DEFAULT_DATABASE = "default";
    public static final boolean DEFAULT_AUTO_CREATE_TABLES = true;
    public static final int DEFAULT_NUM_BUCKETS = 3;
    
    private KafkaCompatConfig(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.nodeId = builder.nodeId;
        this.defaultDatabase = builder.defaultDatabase;
        this.autoCreateTables = builder.autoCreateTables;
        this.defaultNumBuckets = builder.defaultNumBuckets;
    }
    
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }
    
    public int getNodeId() {
        return nodeId;
    }
    
    public String getDefaultDatabase() {
        return defaultDatabase;
    }
    
    public boolean isAutoCreateTables() {
        return autoCreateTables;
    }
    
    public int getDefaultNumBuckets() {
        return defaultNumBuckets;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String host = DEFAULT_HOST;
        private int port = DEFAULT_PORT;
        private int nodeId = DEFAULT_NODE_ID;
        private String defaultDatabase = DEFAULT_DATABASE;
        private boolean autoCreateTables = DEFAULT_AUTO_CREATE_TABLES;
        private int defaultNumBuckets = DEFAULT_NUM_BUCKETS;
        
        public Builder host(String host) {
            this.host = host;
            return this;
        }
        
        public Builder port(int port) {
            this.port = port;
            return this;
        }
        
        public Builder nodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }
        
        public Builder defaultDatabase(String defaultDatabase) {
            this.defaultDatabase = defaultDatabase;
            return this;
        }
        
        public Builder autoCreateTables(boolean autoCreateTables) {
            this.autoCreateTables = autoCreateTables;
            return this;
        }
        
        public Builder defaultNumBuckets(int defaultNumBuckets) {
            this.defaultNumBuckets = defaultNumBuckets;
            return this;
        }
        
        public KafkaCompatConfig build() {
            return new KafkaCompatConfig(this);
        }
    }
}
