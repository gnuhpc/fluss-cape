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

package org.gnuhpc.fluss.cape.common.configuration;

import java.util.Optional;

/**
 * Centralized configuration management for Fluss CAPE.
 * Reads from System properties and Environment variables.
 */
public class CapeConfig {

    // HBase / General Config
    public String getFlussBootstrapServers() {
        return get("fluss.bootstrap.servers", "localhost:9123");
    }

    public String getBindAddress() {
        return get("hbase.compat.bind.address", "0.0.0.0");
    }

    public int getBindPort() {
        return getInt("hbase.compat.bind.port", 16020);
    }

    public String getZkQuorum() {
        return get("hbase.zookeeper.quorum", "localhost:2181");
    }

    public String getServerId() {
        return get("server.id", null);
    }

    public int getHealthCheckPort() {
        return getInt("health.check.port", 8080);
    }

    // Redis Config
    public boolean isRedisEnabled() {
        return getBoolean("redis.enable", true);
    }

    public String getRedisBindAddress() {
        return get("redis.bind.address", "0.0.0.0");
    }

    public int getRedisPort() {
        return getInt("redis.bind.port", 6379);
    }

    public boolean isRedisShardingEnabled() {
        return getBoolean("redis.sharding.enabled", true);
    }

    public int getRedisShards() {
        return getInt("redis.sharding.num.shards", 16);
    }

    // Kafka Config
    public boolean isKafkaEnabled() {
        return getBoolean("kafka.enable", true);
    }

    public String getKafkaBindAddress() {
        return get("kafka.bind.address", "0.0.0.0");
    }

    public int getKafkaPort() {
        return getInt("kafka.bind.port", 9092);
    }

    // PostgreSQL Config
    public boolean isPgEnabled() {
        return getBoolean("pg.enabled", false);
    }

    public String getPgBindAddress() {
        return get("pg.bind.address", "0.0.0.0");
    }

    public int getPgPort() {
        return getInt("pg.port", 5432);
    }

    public String getPgAuthMode() {
        return get("pg.auth.mode", "trust");
    }

    public String getPgAuthUser() {
        return get("pg.auth.user", null);
    }

    public String getPgAuthPassword() {
        return get("pg.auth.password", null);
    }

    public String getPgDatabase() {
        return get("pg.database", getDefaultDatabase());
    }

    // Shared / Default Database
    public String getDefaultDatabase() {
        return get("default.database", "default");
    }

    // Kafka Config
    public String getKafkaDefaultDatabase() {
        return get("kafka.default.database", getDefaultDatabase());
    }

    // ZooKeeper ACL/Auth
    public String getZkAclMode() {
        return get("hbase.zookeeper.acl.mode", "creator-read");
    }

    public String getZkAuthScheme() {
        return get("hbase.zookeeper.auth.scheme", null);
    }

    public String getZkAuthToken() {
        return get("hbase.zookeeper.auth.token", null);
    }

    // S3 Config (Helper method to set system properties as required by Hadoop S3A)
    public void setS3SystemProperties() {
        String s3Endpoint = get("s3.endpoint", null, "S3_ENDPOINT");
        String s3AccessKey = get("s3.access-key", null, "s3.access.key", "S3_ACCESS_KEY");
        String s3SecretKey = get("s3.secret-key", null, "s3.secret.key", "S3_SECRET_KEY");
        String pathStyleAccess = get("s3.path.style.access", null, "s3.path-style-access", "S3_PATH_STYLE_ACCESS");
        String sslEnabled = get("s3.connection.ssl.enabled", null, "S3_CONNECTION_SSL_ENABLED");

        if (s3AccessKey != null) System.setProperty("fs.s3a.access.key", s3AccessKey);
        if (s3SecretKey != null) System.setProperty("fs.s3a.secret.key", s3SecretKey);
        if (s3Endpoint != null) System.setProperty("fs.s3a.endpoint", s3Endpoint);
        if (pathStyleAccess != null) System.setProperty("fs.s3a.path.style.access", pathStyleAccess);
        if (sslEnabled != null) System.setProperty("fs.s3a.connection.ssl.enabled", sslEnabled);
    }
    
    // Auto Discovery
    public boolean isAutoDiscoveryEnabled() {
        return getBoolean("hbase.compat.auto.discover", true);
    }

    // Helper methods for property resolution
    public String get(String key, String defaultValue, String... altKeys) {
        String value = System.getProperty(key);
        if (value != null && !value.isBlank()) return value;
        
        for (String altKey : altKeys) {
            value = System.getProperty(altKey);
            if (value != null && !value.isBlank()) return value;
        }
        
        // Try environment variable convention: keys like 'fluss.bootstrap.servers' -> 'FLUSS_BOOTSTRAP_SERVERS'
        String envKey = key.replace('.', '_').replace('-', '_').toUpperCase();
        value = System.getenv(envKey);
        if (value != null && !value.isBlank()) return value;

        return defaultValue;
    }

    public int getInt(String key, int defaultValue) {
        String val = get(key, null);
        if (val == null) return defaultValue;
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String val = get(key, null);
        if (val == null) return defaultValue;
        return Boolean.parseBoolean(val);
    }
}
