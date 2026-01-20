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
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.gnuhpc.fluss.cape.hbase.executor.GetExecutor;
import org.gnuhpc.fluss.cape.hbase.executor.GetTableNamesExecutor;
import org.gnuhpc.fluss.cape.hbase.executor.GetTableDescriptorsExecutor;
import org.gnuhpc.fluss.cape.hbase.executor.CreateTableExecutor;
import org.gnuhpc.fluss.cape.hbase.executor.DeleteTableExecutor;
import org.gnuhpc.fluss.cape.hbase.executor.DisableTableExecutor;
import org.gnuhpc.fluss.cape.hbase.executor.EnableTableExecutor;
import org.gnuhpc.fluss.cape.hbase.executor.GetProcedureResultExecutor;
import org.gnuhpc.fluss.cape.hbase.executor.HBaseRequestRouter;
import org.gnuhpc.fluss.cape.hbase.executor.IsMasterRunningExecutor;
import org.gnuhpc.fluss.cape.hbase.executor.MultiExecutor;
import org.gnuhpc.fluss.cape.hbase.executor.PutExecutor;
import org.gnuhpc.fluss.cape.hbase.executor.ScanExecutor;
import org.gnuhpc.fluss.cape.hbase.mapping.CellConverter;
import org.gnuhpc.fluss.cape.hbase.mapping.RowKeyEncoder;
import org.gnuhpc.fluss.cape.hbase.metadata.MetaTableEmulator;
import org.gnuhpc.fluss.cape.hbase.metadata.TableStateManager;
import org.gnuhpc.fluss.cape.hbase.metadata.VirtualRegionManager;
import org.gnuhpc.fluss.cape.pg.server.PgAuthConfig;
import org.gnuhpc.fluss.cape.pg.server.PgCompatServer;
import org.gnuhpc.fluss.cape.pg.server.PgServerConfig;
import org.gnuhpc.fluss.cape.kafka.server.KafkaCompatServer;
import org.gnuhpc.fluss.cape.kafka.config.KafkaCompatConfig;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.RowType;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Main launcher for HBase Compatibility Server.
 *
 * <p>Usage: java -jar fluss-hbase-compat.jar \ -Dfluss.bootstrap.servers=localhost:9123 \
 * -Dhbase.compat.bind.address=localhost \ -Dhbase.compat.bind.port=16020
 *
 * <p>Tables are created dynamically via HBase shell or client API.
 */
public class CAPEServerLauncher {
    private static final Logger LOG = LoggerFactory.getLogger(CAPEServerLauncher.class);

    private static ZooKeeper zooKeeper;
    private static String regionServerZnodePath;
    private static String metaRegionServerZnodePath;
    private static HealthCheckServer healthCheckServer;
    
    private static Connection flussConnection;
    private static Admin flussAdmin;
    private static HBaseRequestRouter router;
    private static VirtualRegionManager regionManager;
    private static String bindAddress;

    private static void applyOptionalConfig(Configuration config) {
        applyOptionalString(config, "remote.data.dir");
        applyOptionalString(config, "client.scanner.io.tmpdir");
        applyOptionalString(config, "client.remote-file.download-thread-num");

        applyOptionalString(config, "s3.endpoint");
        applyOptionalString(config, "s3.region");
        applyOptionalString(config, "s3.access-key", "s3.access.key");
        applyOptionalString(config, "s3.secret-key", "s3.secret.key");
        applyOptionalString(config, "s3.path.style.access", "s3.path-style-access");
        applyOptionalString(config, "s3.connection.ssl.enabled");

        // Also set client.fs.* versions for FlussConnection FileSystem initialization
        applyOptionalClientFsString(config, "client.fs.s3.endpoint", "s3.endpoint");
        applyOptionalClientFsString(config, "client.fs.s3.region", "s3.region");
        applyOptionalClientFsString(config, "client.fs.s3.access-key", "s3.access-key", "s3.access.key");
        applyOptionalClientFsString(config, "client.fs.s3.secret-key", "s3.secret-key", "s3.secret.key");
        applyOptionalClientFsString(config, "client.fs.s3.path.style.access", "s3.path-style-access", "s3.path.style.access");
        applyOptionalClientFsString(config, "client.fs.s3.connection.ssl.enabled", "s3.connection.ssl.enabled");

        // Also set fs.s3a.* versions for Hadoop S3A compatibility
        applyOptionalClientFsString(config, "fs.s3a.endpoint", "s3.endpoint");
        applyOptionalClientFsString(config, "fs.s3a.access.key", "s3.access-key", "s3.access.key");
        applyOptionalClientFsString(config, "fs.s3a.secret.key", "s3.secret-key", "s3.secret.key");
        applyOptionalClientFsString(config, "fs.s3a.path.style.access", "s3.path-style-access", "s3.path.style.access");
        applyOptionalClientFsString(config, "fs.s3a.connection.ssl.enabled", "s3.connection.ssl.enabled");
    }

    private static void applyOptionalString(Configuration config, String key, String... altKeys) {
        String value = getConfigValue(key, altKeys);
        if (value == null || value.isBlank()) {
            return;
        }
        config.setString(key, value);
        LOG.info("Applied Fluss config {}={}", key, value);
    }

    private static void applyOptionalClientFsString(
            Configuration config, String clientFsKey, String... sourceKeys) {
        String value = getConfigValue(sourceKeys[0], dropFirst(sourceKeys));
        if (value == null || value.isBlank()) {
            return;
        }
        config.setString(clientFsKey, value);
        LOG.info("Applied Fluss config {}={}", clientFsKey, value);
    }

    private static String[] dropFirst(String[] values) {
        if (values.length <= 1) {
            return new String[0];
        }
        String[] trimmed = new String[values.length - 1];
        System.arraycopy(values, 1, trimmed, 0, trimmed.length);
        return trimmed;
    }

    private static String getConfigValue(String key, String... altKeys) {
        String value = readSystemProperty(key);
        if (value != null) {
            return value;
        }
        for (String altKey : altKeys) {
            value = readSystemProperty(altKey);
            if (value != null) {
                return value;
            }
        }
        value = readEnvironmentVariable(key);
        if (value != null) {
            return value;
        }
        for (String altKey : altKeys) {
            value = readEnvironmentVariable(altKey);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private static String readSystemProperty(String key) {
        String value = System.getProperty(key);
        return value == null || value.isBlank() ? null : value;
    }

    private static String readEnvironmentVariable(String key) {
        String envKey = key.replace('.', '_').replace('-', '_').toUpperCase();
        String value = System.getenv(envKey);
        return value == null || value.isBlank() ? null : value;
    }
 
    public static void main(String[] args) throws Exception {
        String flussBootstrapServers =
                System.getProperty("fluss.bootstrap.servers", "localhost:9123");
        String bindAddress = System.getProperty("hbase.compat.bind.address", "0.0.0.0");
        int bindPort = Integer.parseInt(System.getProperty("hbase.compat.bind.port", "16020"));
        String zkQuorum = System.getProperty("hbase.zookeeper.quorum", "localhost:2181");
        String serverId = System.getProperty("server.id", generateServerId(bindAddress, bindPort));
        int healthCheckPort = Integer.parseInt(System.getProperty("health.check.port", "8080"));

        boolean enableRedis =
                Boolean.parseBoolean(System.getProperty("redis.enable", "true"));
        String redisBindAddress = System.getProperty("redis.bind.address", "0.0.0.0");
        int redisPort = Integer.parseInt(System.getProperty("redis.bind.port", "6379"));
        boolean redisShardingEnabled =
                Boolean.parseBoolean(System.getProperty("redis.sharding.enabled", "true"));
        int redisShards = Integer.parseInt(System.getProperty("redis.sharding.num.shards", "16"));

        PgServerConfig pgConfig = PgServerConfig.fromSystemProperties();

        boolean enableKafka =
                Boolean.parseBoolean(System.getProperty("kafka.enable", "true"));
        String kafkaBindAddress = System.getProperty("kafka.bind.address", "0.0.0.0");
        int kafkaPort = Integer.parseInt(System.getProperty("kafka.bind.port", "9092"));

        LOG.info("========================================");
        LOG.info("Starting CAPE (Compatibility And Protocol Extensions) Server");
        LOG.info("========================================");
        LOG.info("Server ID: {}", serverId);
        LOG.info("Fluss cluster: {}", flussBootstrapServers);
        LOG.info("Bind address: {}:{}", bindAddress, bindPort);
        LOG.info("ZooKeeper: {}", zkQuorum);
        LOG.info("Table Management: Dynamic (via HBase shell/API)");
        LOG.info("Redis enabled: {}", enableRedis);
        if (enableRedis) {
            LOG.info("Redis address: {}:{}", redisBindAddress, redisPort);
            if (redisShardingEnabled) {
                LOG.info("Redis sharding: ENABLED ({} shards, {} slots per shard)",
                         redisShards, 16384 / redisShards);
                LOG.info("Redis tables: redis_shard_00 to redis_shard_{} + 2 index tables",
                         String.format("%02d", redisShards - 1));
            } else {
                LOG.info("Redis sharding: DISABLED");
                LOG.info("Redis tables: Internal (redis_internal_data, redis_internal_subkey_index, redis_internal_zset_index)");
            }
        }
        LOG.info("PostgreSQL enabled: {}", pgConfig.isEnabled());
        if (pgConfig.isEnabled()) {
            LOG.info("PostgreSQL address: {}:{}", pgConfig.getBindAddress(), pgConfig.getPort());
            LOG.info("PostgreSQL database: {}", pgConfig.getDatabase());
            LOG.info("PostgreSQL auth mode: {}", pgConfig.getAuthMode());
        }
        LOG.info("Kafka enabled: {}", enableKafka);
        if (enableKafka) {
            LOG.info("Kafka address: {}:{}", kafkaBindAddress, kafkaPort);
            LOG.info("Kafka default database: default");
        }
        LOG.info("========================================");

        // Set Java system properties for Hadoop S3A FileSystem (bypasses Fluss config filtering)
        String s3Endpoint = getConfigValue("s3.endpoint", "S3_ENDPOINT");
        String s3AccessKey = getConfigValue("s3.access-key", "s3.access.key", "S3_ACCESS_KEY");
        String s3SecretKey = getConfigValue("s3.secret-key", "s3.secret.key", "S3_SECRET_KEY");
        String pathStyleAccess = getConfigValue("s3.path.style.access", "s3.path-style-access", "S3_PATH_STYLE_ACCESS");
        String sslEnabled = getConfigValue("s3.connection.ssl.enabled", "S3_CONNECTION_SSL_ENABLED");

        if (s3AccessKey != null) {
            System.setProperty("fs.s3a.access.key", s3AccessKey);
            LOG.info("Set system property fs.s3a.access.key");
        }
        if (s3SecretKey != null) {
            System.setProperty("fs.s3a.secret.key", s3SecretKey);
            LOG.info("Set system property fs.s3a.secret.key");
        }
        if (s3Endpoint != null) {
            System.setProperty("fs.s3a.endpoint", s3Endpoint);
            LOG.info("Set system property fs.s3a.endpoint: {}", s3Endpoint);
        }
        if (pathStyleAccess != null) {
            System.setProperty("fs.s3a.path.style.access", pathStyleAccess);
            LOG.info("Set system property fs.s3a.path.style.access: {}", pathStyleAccess);
        }
        if (sslEnabled != null) {
            System.setProperty("fs.s3a.connection.ssl.enabled", sslEnabled);
            LOG.info("Set system property fs.s3a.connection.ssl.enabled: {}", sslEnabled);
        }

        Configuration config = new Configuration();
        config.setString("bootstrap.servers", flussBootstrapServers);
        applyOptionalConfig(config);
        flussConnection = ConnectionFactory.createConnection(config);
        flussAdmin = flussConnection.getAdmin();
        
        String database = "default";
        try {
            if (!flussAdmin.databaseExists(database).get()) {
                LOG.info("Creating database '{}'...", database);
                flussAdmin.createDatabase(database, DatabaseDescriptor.EMPTY, false).get();
                LOG.info("Successfully created database '{}'", database);
            } else {
                LOG.info("Database '{}' already exists", database);
            }
        } catch (Exception e) {
            LOG.error("Failed to ensure database '{}' exists", database, e);
            throw new RuntimeException("Failed to initialize database", e);
        }

        regionManager = new VirtualRegionManager(bindAddress, bindPort);
        
        TableStateManager stateManager = new TableStateManager();

        router = new HBaseRequestRouter();

        router.registerExecutor("IsMasterRunning", new IsMasterRunningExecutor());
        LOG.info("Registered IsMasterRunning executor");

        router.registerExecutor("GetTableNames", new GetTableNamesExecutor(flussAdmin));
        LOG.info("Registered GetTableNames executor");

        router.registerExecutor("GetTableDescriptors", new GetTableDescriptorsExecutor(flussAdmin));
        LOG.info("Registered GetTableDescriptors executor");

        router.registerExecutor("CreateTable", new CreateTableExecutor(
                flussConnection, 
                flussAdmin, 
                tablePath -> registerTableDynamically(tablePath)));
        LOG.info("Registered CreateTable executor");

        router.registerExecutor("DeleteTable", new DeleteTableExecutor(
                flussAdmin,
                tablePath -> unregisterTableExecutors(tablePath),
                stateManager));
        LOG.info("Registered DeleteTable executor");

        router.registerExecutor("DisableTable", new DisableTableExecutor(stateManager));
        LOG.info("Registered DisableTable executor");

        router.registerExecutor("EnableTable", new EnableTableExecutor(stateManager));
        LOG.info("Registered EnableTable executor");

        router.registerExecutor("getProcedureResult", new GetProcedureResultExecutor());
        LOG.info("Registered GetProcedureResult executor");

        MetaTableEmulator metaEmulator = new MetaTableEmulator(regionManager, stateManager);
        router.registerExecutor("Get-hbase:meta", metaEmulator);
        router.registerExecutor("Scan-hbase:meta", metaEmulator);
        router.registerExecutor("Multi-hbase:meta", metaEmulator);
        router.registerExecutor("Mutate-hbase:meta", metaEmulator);
        LOG.info("Registered meta table emulator for hbase:meta (Get, Scan, Multi, Mutate)");

        LOG.info("Dynamic table management enabled. Use HBase shell 'create' command to create tables on-demand.");

        HBaseCompatServer server = new HBaseCompatServer(config, bindAddress, bindPort, router);
        server.start();

        int actualPort = server.getBoundPort();
        // Use bindAddress instead of auto-detected hostname to avoid DNS resolution issues
        String hostname = bindAddress.equals("0.0.0.0") ? "localhost" : bindAddress;
        ServerName serverName =
                ServerName.valueOf(hostname, actualPort, System.currentTimeMillis());

        try {
            registerInZooKeeper(zkQuorum, serverName, serverId);
            LOG.info("Successfully registered in ZooKeeper");
        } catch (Exception e) {
            LOG.error("Failed to register in ZooKeeper", e);
            server.close();
            flussConnection.close();
            throw e;
        }

        try {
            healthCheckServer = new HealthCheckServer(healthCheckPort, flussConnection, zooKeeper);
            healthCheckServer.start();
            LOG.info("Health check server started on port {}", healthCheckPort);
        } catch (Exception e) {
            LOG.error("Failed to start health check server", e);
            unregisterFromZooKeeper();
            server.close();
            flussConnection.close();
            throw e;
        }

        PgCompatServer pgServer = null;
        if (pgConfig.isEnabled()) {
            try {
                PgAuthConfig pgAuthConfig = new PgAuthConfig(
                        pgConfig.getAuthMode(),
                        pgConfig.getAuthUser(),
                        pgConfig.getAuthPassword());
                pgServer = new PgCompatServer(
                        pgConfig.getBindAddress(),
                        pgConfig.getPort(),
                        pgAuthConfig,
                        flussConnection,
                        flussAdmin);
                pgServer.start();
            } catch (Exception e) {
                LOG.error("Failed to start PostgreSQL compatibility server", e);
                unregisterFromZooKeeper();
                server.close();
                if (healthCheckServer != null) {
                    healthCheckServer.close();
                }
                flussConnection.close();
                throw e;
            }
        }

        KafkaCompatServer kafkaServer = null;
        if (enableKafka) {
            try {
                KafkaCompatConfig kafkaConfig = KafkaCompatConfig.builder()
                        .host(kafkaBindAddress)
                        .port(kafkaPort)
                        .nodeId(0)
                        .defaultDatabase(database)
                        .autoCreateTables(true)
                        .defaultNumBuckets(3)
                        .build();
                kafkaServer = new KafkaCompatServer(flussConnection, kafkaConfig);
                kafkaServer.start();
                LOG.info("Kafka compatibility server started on {}:{}", kafkaBindAddress, kafkaPort);
            } catch (Exception e) {
                LOG.error("Failed to start Kafka compatibility server", e);
                unregisterFromZooKeeper();
                server.close();
                if (pgServer != null) {
                    pgServer.close();
                }
                if (healthCheckServer != null) {
                    healthCheckServer.close();
                }
                flussConnection.close();
                throw e;
            }
        }

        org.gnuhpc.fluss.cape.redis.server.RedisCompatServer redisServer = null;
        if (enableRedis) {
            try {
                org.gnuhpc.fluss.cape.redis.storage.RedisStorageAdapter redisAdapter;
                
                if (redisShardingEnabled) {
                    LOG.info("Initializing Redis with sharding: {} shards", redisShards);
                    org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager
                            .ensureAllShardedTables(flussAdmin, database, redisShards);
                    
                    LOG.info("Waiting for metadata to propagate...");
                    Thread.sleep(10000);
                    
                    redisAdapter = new org.gnuhpc.fluss.cape.redis.storage.RedisShardedAdapter(
                            flussConnection, database, redisShards);
                } else {
                    LOG.info("Initializing Redis without sharding (legacy mode)");
                    org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager
                            .ensureAllTables(flussAdmin, database);
                    
                    LOG.info("Waiting for metadata to propagate...");
                    Thread.sleep(10000);
                    
                    redisAdapter = new org.gnuhpc.fluss.cape.redis.storage.RedisSingleTableAdapter(
                            flussConnection, database);
                }

                org.gnuhpc.fluss.cape.redis.executor.RedisCommandRouter redisRouter =
                        new org.gnuhpc.fluss.cape.redis.executor.RedisCommandRouter();

                org.gnuhpc.fluss.cape.redis.executor.StringCommandExecutor stringExecutor =
                        new org.gnuhpc.fluss.cape.redis.executor.StringCommandExecutor(redisAdapter);
                org.gnuhpc.fluss.cape.redis.executor.HashCommandExecutor hashExecutor =
                        new org.gnuhpc.fluss.cape.redis.executor.HashCommandExecutor(redisAdapter);
                org.gnuhpc.fluss.cape.redis.executor.SetCommandExecutor setExecutor =
                        new org.gnuhpc.fluss.cape.redis.executor.SetCommandExecutor(redisAdapter);
                org.gnuhpc.fluss.cape.redis.executor.TypeCommandExecutor typeExecutor =
                        new org.gnuhpc.fluss.cape.redis.executor.TypeCommandExecutor(redisAdapter);
                org.gnuhpc.fluss.cape.redis.executor.ListCommandExecutor listExecutor =
                        new org.gnuhpc.fluss.cape.redis.executor.ListCommandExecutor(redisAdapter);
                org.gnuhpc.fluss.cape.redis.executor.SortedSetCommandExecutor zsetExecutor =
                        new org.gnuhpc.fluss.cape.redis.executor.SortedSetCommandExecutor(
                                redisAdapter, flussConnection, 
                                org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager.getZsetIndexTableName());
                org.gnuhpc.fluss.cape.redis.executor.ExpirationCommandExecutor expirationExecutor =
                        new org.gnuhpc.fluss.cape.redis.executor.ExpirationCommandExecutor(redisAdapter);
                org.gnuhpc.fluss.cape.redis.executor.GeoCommandExecutor geoExecutor =
                        new org.gnuhpc.fluss.cape.redis.executor.GeoCommandExecutor(zsetExecutor, redisAdapter);
                org.gnuhpc.fluss.cape.redis.executor.HyperLogLogCommandExecutor hllExecutor =
                        new org.gnuhpc.fluss.cape.redis.executor.HyperLogLogCommandExecutor(redisAdapter);
                org.gnuhpc.fluss.cape.redis.executor.KeyIterationCommandExecutor keyIterExecutor =
                        new org.gnuhpc.fluss.cape.redis.executor.KeyIterationCommandExecutor(redisAdapter);
                org.gnuhpc.fluss.cape.redis.pubsub.PubSubManager pubSubManager =
                        new org.gnuhpc.fluss.cape.redis.pubsub.PubSubManager();
                org.gnuhpc.fluss.cape.redis.executor.PubSubCommandExecutor pubsubExecutor =
                        new org.gnuhpc.fluss.cape.redis.executor.PubSubCommandExecutor(pubSubManager);
                org.gnuhpc.fluss.cape.redis.executor.TransactionCommandExecutor transactionExecutor =
                        new org.gnuhpc.fluss.cape.redis.executor.TransactionCommandExecutor(redisRouter);
                org.gnuhpc.fluss.cape.redis.executor.RedisCommandExecutor streamExecutor;
                
                if (redisShardingEnabled) {
                    org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager
                            .ensureAllStreamShardTables(flussAdmin, database, redisShards);
                    streamExecutor = new org.gnuhpc.fluss.cape.redis.executor.StreamCommandExecutorSharded(
                            flussConnection, database, redisShards);
                    LOG.info("Initialized Stream executor with sharding: {} shards", redisShards);
                } else {
                    org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager
                            .ensureLegacyStreamTable(flussAdmin, database);
                    streamExecutor = new org.gnuhpc.fluss.cape.redis.executor.StreamCommandExecutor(
                            redisAdapter, flussBootstrapServers, "default.redis_stream_data");
                    LOG.info("Initialized Stream executor without sharding (legacy mode)");
                }
                org.gnuhpc.fluss.cape.redis.executor.DatabaseCommandExecutor databaseExecutor =
                        new org.gnuhpc.fluss.cape.redis.executor.DatabaseCommandExecutor(redisAdapter);

                redisRouter.registerExecutor("GET", stringExecutor);
                redisRouter.registerExecutor("SET", stringExecutor);
                redisRouter.registerExecutor("DEL", stringExecutor);
                redisRouter.registerExecutor("EXISTS", stringExecutor);
                redisRouter.registerExecutor("INCR", stringExecutor);
                redisRouter.registerExecutor("INCRBY", stringExecutor);
                redisRouter.registerExecutor("INCRBYFLOAT", stringExecutor);
                redisRouter.registerExecutor("PING", stringExecutor);
                redisRouter.registerExecutor("MGET", stringExecutor);
                redisRouter.registerExecutor("MSET", stringExecutor);
                redisRouter.registerExecutor("SETNX", stringExecutor);
                redisRouter.registerExecutor("SETEX", stringExecutor);
                redisRouter.registerExecutor("PSETEX", stringExecutor);
                redisRouter.registerExecutor("MSETNX", stringExecutor);
                redisRouter.registerExecutor("GETSET", stringExecutor);
                redisRouter.registerExecutor("APPEND", stringExecutor);
                redisRouter.registerExecutor("STRLEN", stringExecutor);
                redisRouter.registerExecutor("DECR", stringExecutor);
                redisRouter.registerExecutor("DECRBY", stringExecutor);
                redisRouter.registerExecutor("GETRANGE", stringExecutor);
                redisRouter.registerExecutor("SETRANGE", stringExecutor);
                redisRouter.registerExecutor("GETEX", stringExecutor);
                redisRouter.registerExecutor("GETDEL", stringExecutor);
                redisRouter.registerExecutor("SETBIT", stringExecutor);
                redisRouter.registerExecutor("GETBIT", stringExecutor);
                redisRouter.registerExecutor("BITCOUNT", stringExecutor);
                redisRouter.registerExecutor("BITPOS", stringExecutor);
                redisRouter.registerExecutor("BITOP", stringExecutor);
                redisRouter.registerExecutor("LCS", stringExecutor);
                redisRouter.registerExecutor("BITFIELD", stringExecutor);
                redisRouter.registerExecutor("BITFIELD_RO", stringExecutor);
                redisRouter.registerExecutor("SUBSTR", stringExecutor);
                redisRouter.registerExecutor("STRALGO", stringExecutor);
                redisRouter.registerExecutor("COPY", stringExecutor);
                redisRouter.registerExecutor("MOVE", stringExecutor);
                redisRouter.registerExecutor("DUMP", stringExecutor);
                redisRouter.registerExecutor("RESTORE", stringExecutor);

                redisRouter.registerExecutor("HGET", hashExecutor);
                redisRouter.registerExecutor("HSET", hashExecutor);
                redisRouter.registerExecutor("HDEL", hashExecutor);
                redisRouter.registerExecutor("HEXISTS", hashExecutor);
                redisRouter.registerExecutor("HGETALL", hashExecutor);
                redisRouter.registerExecutor("HKEYS", hashExecutor);
                redisRouter.registerExecutor("HVALS", hashExecutor);
                redisRouter.registerExecutor("HLEN", hashExecutor);
                redisRouter.registerExecutor("HMGET", hashExecutor);
                redisRouter.registerExecutor("HMSET", hashExecutor);
                redisRouter.registerExecutor("HINCRBY", hashExecutor);
                redisRouter.registerExecutor("HINCRBYFLOAT", hashExecutor);
                redisRouter.registerExecutor("HRANDFIELD", hashExecutor);
                redisRouter.registerExecutor("HSCAN", hashExecutor);
                redisRouter.registerExecutor("HSTRLEN", hashExecutor);

                redisRouter.registerExecutor("SADD", setExecutor);
                redisRouter.registerExecutor("SREM", setExecutor);
                redisRouter.registerExecutor("SISMEMBER", setExecutor);
                redisRouter.registerExecutor("SMEMBERS", setExecutor);
                redisRouter.registerExecutor("SCARD", setExecutor);
                redisRouter.registerExecutor("SMOVE", setExecutor);
                redisRouter.registerExecutor("SDIFF", setExecutor);
                redisRouter.registerExecutor("SDIFFSTORE", setExecutor);
                redisRouter.registerExecutor("SINTER", setExecutor);
                redisRouter.registerExecutor("SINTERSTORE", setExecutor);
                redisRouter.registerExecutor("SINTERCARD", setExecutor);
                redisRouter.registerExecutor("SUNION", setExecutor);
                redisRouter.registerExecutor("SUNIONSTORE", setExecutor);
                redisRouter.registerExecutor("SPOP", setExecutor);
                redisRouter.registerExecutor("SRANDMEMBER", setExecutor);
                redisRouter.registerExecutor("SSCAN", setExecutor);

                redisRouter.registerExecutor("LPUSH", listExecutor);
                redisRouter.registerExecutor("RPUSH", listExecutor);
                redisRouter.registerExecutor("LPOP", listExecutor);
                redisRouter.registerExecutor("RPOP", listExecutor);
                redisRouter.registerExecutor("LRANGE", listExecutor);
                redisRouter.registerExecutor("LLEN", listExecutor);
                redisRouter.registerExecutor("LINDEX", listExecutor);
                redisRouter.registerExecutor("LINSERT", listExecutor);
                redisRouter.registerExecutor("LREM", listExecutor);
                redisRouter.registerExecutor("LSET", listExecutor);
                redisRouter.registerExecutor("LTRIM", listExecutor);
                redisRouter.registerExecutor("LPUSHX", listExecutor);
                redisRouter.registerExecutor("RPUSHX", listExecutor);
                redisRouter.registerExecutor("LPOS", listExecutor);
                redisRouter.registerExecutor("LMOVE", listExecutor);
                redisRouter.registerExecutor("BLPOP", listExecutor);
                redisRouter.registerExecutor("BRPOP", listExecutor);
                redisRouter.registerExecutor("BLMOVE", listExecutor);

                redisRouter.registerExecutor("ZADD", zsetExecutor);
                redisRouter.registerExecutor("ZREM", zsetExecutor);
                redisRouter.registerExecutor("ZRANGE", zsetExecutor);
                redisRouter.registerExecutor("ZSCORE", zsetExecutor);
                redisRouter.registerExecutor("ZCARD", zsetExecutor);
                redisRouter.registerExecutor("ZRANGEBYSCORE", zsetExecutor);
                redisRouter.registerExecutor("ZINCRBY", zsetExecutor);
                redisRouter.registerExecutor("ZCOUNT", zsetExecutor);
                redisRouter.registerExecutor("ZRANK", zsetExecutor);
                redisRouter.registerExecutor("ZREVRANK", zsetExecutor);
                redisRouter.registerExecutor("ZPOPMIN", zsetExecutor);
                redisRouter.registerExecutor("ZPOPMAX", zsetExecutor);
                redisRouter.registerExecutor("ZREVRANGE", zsetExecutor);
                redisRouter.registerExecutor("ZREVRANGEBYSCORE", zsetExecutor);
                redisRouter.registerExecutor("ZREMRANGEBYRANK", zsetExecutor);
                redisRouter.registerExecutor("ZREMRANGEBYSCORE", zsetExecutor);
                redisRouter.registerExecutor("ZINTER", zsetExecutor);
                redisRouter.registerExecutor("ZINTERSTORE", zsetExecutor);
                redisRouter.registerExecutor("ZUNION", zsetExecutor);
                redisRouter.registerExecutor("ZUNIONSTORE", zsetExecutor);
                redisRouter.registerExecutor("ZDIFF", zsetExecutor);
                redisRouter.registerExecutor("ZDIFFSTORE", zsetExecutor);
                redisRouter.registerExecutor("ZRANDMEMBER", zsetExecutor);
                redisRouter.registerExecutor("ZSCAN", zsetExecutor);
                redisRouter.registerExecutor("BZPOPMIN", zsetExecutor);
                redisRouter.registerExecutor("BZPOPMAX", zsetExecutor);

                redisRouter.registerExecutor("GEOADD", geoExecutor);
                redisRouter.registerExecutor("GEODIST", geoExecutor);
                redisRouter.registerExecutor("GEOPOS", geoExecutor);
                redisRouter.registerExecutor("GEORADIUS", geoExecutor);
                redisRouter.registerExecutor("GEORADIUS_RO", geoExecutor);
                redisRouter.registerExecutor("GEORADIUSBYMEMBER", geoExecutor);
                redisRouter.registerExecutor("GEOSEARCH", geoExecutor);
                redisRouter.registerExecutor("GEOSEARCHSTORE", geoExecutor);

                redisRouter.registerExecutor("PFADD", hllExecutor);
                redisRouter.registerExecutor("PFCOUNT", hllExecutor);
                redisRouter.registerExecutor("PFMERGE", hllExecutor);

                redisRouter.registerExecutor("KEYS", keyIterExecutor);
                redisRouter.registerExecutor("SCAN", keyIterExecutor);

                redisRouter.registerExecutor("PUBLISH", pubsubExecutor);
                redisRouter.registerExecutor("SUBSCRIBE", pubsubExecutor);
                redisRouter.registerExecutor("UNSUBSCRIBE", pubsubExecutor);
                redisRouter.registerExecutor("PSUBSCRIBE", pubsubExecutor);
                redisRouter.registerExecutor("PUNSUBSCRIBE", pubsubExecutor);
                redisRouter.registerExecutor("PUBSUB", pubsubExecutor);

                redisRouter.registerExecutor("MULTI", transactionExecutor);
                redisRouter.registerExecutor("EXEC", transactionExecutor);
                redisRouter.registerExecutor("DISCARD", transactionExecutor);
                redisRouter.registerExecutor("WATCH", transactionExecutor);
                redisRouter.registerExecutor("UNWATCH", transactionExecutor);

                redisRouter.registerExecutor("XADD", streamExecutor);
                redisRouter.registerExecutor("XLEN", streamExecutor);
                redisRouter.registerExecutor("XRANGE", streamExecutor);
                redisRouter.registerExecutor("XREVRANGE", streamExecutor);
                redisRouter.registerExecutor("XREAD", streamExecutor);
                redisRouter.registerExecutor("XDEL", streamExecutor);
                redisRouter.registerExecutor("XTRIM", streamExecutor);
                redisRouter.registerExecutor("XGROUP", streamExecutor);
                redisRouter.registerExecutor("XREADGROUP", streamExecutor);
                redisRouter.registerExecutor("XACK", streamExecutor);
                redisRouter.registerExecutor("XPENDING", streamExecutor);

                redisRouter.registerExecutor("TYPE", typeExecutor);
                
                redisRouter.registerExecutor("FLUSHALL", databaseExecutor);
                redisRouter.registerExecutor("FLUSHDB", databaseExecutor);
                
                redisRouter.registerExecutor("EXPIRE", expirationExecutor);
                redisRouter.registerExecutor("EXPIREAT", expirationExecutor);
                redisRouter.registerExecutor("PEXPIRE", expirationExecutor);
                redisRouter.registerExecutor("TTL", expirationExecutor);
                redisRouter.registerExecutor("PTTL", expirationExecutor);
                redisRouter.registerExecutor("PERSIST", expirationExecutor);

                org.gnuhpc.fluss.cape.redis.executor.AuthCommandExecutor authExecutor =
                        new org.gnuhpc.fluss.cape.redis.executor.AuthCommandExecutor();
                redisRouter.registerExecutor("AUTH", authExecutor);
                boolean authEnabled = authExecutor.isAuthenticationEnabled();
                LOG.info("Redis AUTH command registered (authentication {})",
                         authEnabled ? "ENABLED" : "DISABLED");

                org.gnuhpc.fluss.cape.redis.server.RedisSslContextProvider sslProvider =
                        org.gnuhpc.fluss.cape.redis.server.RedisSslContextProvider.create();
                boolean redisSslEnabled = sslProvider.isSslEnabled();
                LOG.info("Redis TLS/SSL {}", redisSslEnabled ? "ENABLED" : "DISABLED");

                redisServer =
                        new org.gnuhpc.fluss.cape.redis.server.RedisCompatServer(
                                redisBindAddress, redisPort, redisRouter, authEnabled, sslProvider);
                redisServer.start();

                LOG.info("========================================");
                LOG.info("Redis compatibility server started on {}:{}", redisBindAddress, redisPort);
                LOG.info("========================================");
            } catch (Exception e) {
                LOG.error("Failed to start Redis compatibility server", e);
                if (redisServer != null) {
                    redisServer.close();
                }
                unregisterFromZooKeeper();
                if (healthCheckServer != null) {
                    healthCheckServer.close();
                }
                server.close();
                flussConnection.close();
                throw e;
            }
        }

        final org.gnuhpc.fluss.cape.redis.server.RedisCompatServer finalRedisServer = redisServer;
        final PgCompatServer finalPgServer = pgServer;
        final KafkaCompatServer finalKafkaServer = kafkaServer;

        LOG.info("========================================");
        LOG.info("CAPE Server started!");
        LOG.info("========================================");
        LOG.info("HBase clients can connect to: {}:{}", hostname, actualPort);
        if (enableRedis) {
            LOG.info("Redis clients can connect to: {}:{}", redisBindAddress, redisPort);
        }
        if (pgConfig.isEnabled()) {
            LOG.info("PostgreSQL clients can connect to: {}:{}", pgConfig.getBindAddress(), pgConfig.getPort());
        }
        if (enableKafka) {
            LOG.info("Kafka clients can connect to: {}:{}", kafkaBindAddress, kafkaPort);
        }
        LOG.info("========================================");

        // Setup shutdown hook
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    LOG.info("Received shutdown signal");
                                    try {
                                        unregisterFromZooKeeper();
                                        if (finalKafkaServer != null) {
                                            finalKafkaServer.close();
                                        }
                                        if (finalPgServer != null) {
                                            finalPgServer.close();
                                        }
                                        if (finalRedisServer != null) {
                                            finalRedisServer.close();
                                        }
                                        if (healthCheckServer != null) {
                                            healthCheckServer.close();
                                        }
                                        server.close();
                                        flussConnection.close();
                                    } catch (Exception e) {
                                        LOG.error("Error during shutdown", e);
                                    }
                                    shutdownLatch.countDown();
                                }));

        // Keep running
        shutdownLatch.await();
        LOG.info("CAPE Server stopped");
    }

    private static String generateServerId(String bindAddress, int port) {
        try {
            String hostname =
                    bindAddress.equals("0.0.0.0") || bindAddress.equals("localhost")
                            ? java.net.InetAddress.getLocalHost().getHostName()
                            : bindAddress;
            long timestamp = System.currentTimeMillis();
            return String.format("%s,%d,%d", hostname, port, timestamp);
        } catch (Exception e) {
            LOG.warn("Failed to get hostname, using localhost", e);
            return String.format("localhost,%d,%d", port, System.currentTimeMillis());
        }
    }

    private static TablePath parseTablePath(String tablePathStr) {
        String[] parts = tablePathStr.split("\\.");
        if (parts.length == 2) {
            return TablePath.of(parts[0], parts[1]);
        } else if (parts.length == 1) {
            // Use "default" database if not specified
            return TablePath.of("default", parts[0]);
        } else {
            throw new IllegalArgumentException(
                    "Invalid table path: "
                            + tablePathStr
                            + ". Expected format: database.table or table");
        }
    }

    private static void registerTableDynamically(TablePath tablePath) throws Exception {
        LOG.info("Dynamically registering table: {}", tablePath);

        TableInfo tableInfo = flussAdmin.getTableInfo(tablePath).get();
        if (tableInfo == null) {
            throw new IllegalArgumentException("Table not found in Fluss: " + tablePath);
        }

        RowType rowType = tableInfo.getRowType();
        List<String> primaryKeys = tableInfo.getPrimaryKeys();

        RowKeyEncoder rowKeyEncoder = new RowKeyEncoder(rowType, primaryKeys);
        CellConverter cellConverter =
                new CellConverter(
                        rowType, CellConverter.ColumnFamilyMapping.createDefault(rowType, "cf"));

        String tableName = tablePath.getTableName();

        router.registerExecutor(
                "Get-" + tableName,
                new GetExecutor(flussConnection, tablePath, rowKeyEncoder, cellConverter));

        router.registerExecutor(
                "Mutate-" + tableName,
                new PutExecutor(flussConnection, tablePath, rowType, rowKeyEncoder, cellConverter));

        router.registerExecutor(
                "Scan-" + tableName,
                new ScanExecutor(flussConnection, tablePath, rowKeyEncoder, cellConverter));

        router.registerExecutor(
                "Multi-" + tableName,
                new MultiExecutor(flussConnection, tablePath, rowType, rowKeyEncoder, cellConverter));

        int bucketCount = tableInfo.getNumBuckets();
        TableName hbaseTableName = TableName.valueOf(tablePath.getTableName());
        regionManager.registerTable(hbaseTableName, bucketCount);

        LOG.info(
                "Successfully registered table: {} (primaryKeys={}, buckets={})",
                tablePath,
                primaryKeys,
                bucketCount);
    }

    private static void unregisterTableExecutors(TablePath tablePath) {
        String tableName = tablePath.getTableName();
        LOG.info("Unregistering executors for table: {}", tablePath);

        router.unregisterExecutors("Get-" + tableName);
        router.unregisterExecutors("Mutate-" + tableName);
        router.unregisterExecutors("Scan-" + tableName);
        router.unregisterExecutors("Multi-" + tableName);

        LOG.info("Successfully unregistered table: {}", tablePath);
    }

    private static void registerTable(
            Connection flussConnection,
            Admin admin,
            HBaseRequestRouter router,
            VirtualRegionManager regionManager,
            TablePath tablePath,
            String serverName)
            throws Exception {

        LOG.info("Registering table: {}", tablePath);

        // Get table info from Fluss
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        if (tableInfo == null) {
            throw new IllegalArgumentException("Table not found in Fluss: " + tablePath);
        }

        RowType rowType = tableInfo.getRowType();
        List<String> primaryKeys = tableInfo.getPrimaryKeys();

        // Create row key encoder
        RowKeyEncoder rowKeyEncoder = new RowKeyEncoder(rowType, primaryKeys);

        // Create cell converter with default column family mapping
        CellConverter cellConverter =
                new CellConverter(
                        rowType, CellConverter.ColumnFamilyMapping.createDefault(rowType, "cf"));

        // Get table instance
        Table table = flussConnection.getTable(tablePath);

        // Register all executors for this table
        String tableName = tablePath.getTableName();

        router.registerExecutor(
                "Get-" + tableName,
                new GetExecutor(flussConnection, tablePath, rowKeyEncoder, cellConverter));

        router.registerExecutor(
                "Mutate-" + tableName,
                new PutExecutor(flussConnection, tablePath, rowType, rowKeyEncoder, cellConverter));

        router.registerExecutor(
                "Scan-" + tableName,
                new ScanExecutor(flussConnection, tablePath, rowKeyEncoder, cellConverter));

        router.registerExecutor(
                "Multi-" + tableName,
                new MultiExecutor(flussConnection, tablePath, rowType, rowKeyEncoder, cellConverter));

        // Register table with region manager
        int bucketCount = tableInfo.getNumBuckets();
        TableName hbaseTableName = TableName.valueOf(tablePath.getTableName());
        regionManager.registerTable(hbaseTableName, bucketCount);

        LOG.info(
                "Successfully registered table: {} (primaryKeys={}, buckets={})",
                tablePath,
                primaryKeys,
                bucketCount);
    }

    private static void registerInZooKeeper(String zkQuorum, ServerName serverName, String serverId)
            throws Exception {
        LOG.info("Connecting to ZooKeeper at: {}", zkQuorum);
        
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        zooKeeper = new ZooKeeper(zkQuorum, 30000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                LOG.info("ZooKeeper connected successfully");
                connectedSignal.countDown();
            }
        });

        // Wait up to 10 seconds for connection
        if (!connectedSignal.await(10, TimeUnit.SECONDS)) {
            throw new RuntimeException("Failed to connect to ZooKeeper within 10 seconds");
        }
        
        LOG.info("ZooKeeper session established, state: {}", zooKeeper.getState());

        ensureZNodeExists("/hbase");
        ensureZNodeExists("/hbase/rs");

        ensureHBaseClusterId();

        regionServerZnodePath = "/hbase/rs/" + serverId;
        byte[] serverInfoBytes = Bytes.toBytes(serverName.getServerName());

        deleteStaleEphemeralNodeIfExists(regionServerZnodePath);

        zooKeeper.create(
                regionServerZnodePath,
                serverInfoBytes,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);

        LOG.info("Registered region server in ZooKeeper: {}", regionServerZnodePath);

        metaRegionServerZnodePath = "/hbase/meta-region-server";
        byte[] metaServerBytes = buildMetaRegionServerProtobuf(serverName);

        try {
            // Try to create first (most common case for first server)
            zooKeeper.create(
                    metaRegionServerZnodePath,
                    metaServerBytes,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            LOG.info("Created meta region server node (first server in cluster)");
        } catch (KeeperException.NodeExistsException e) {
            // Node already exists, update it with our info
            LOG.info(
                    "Meta region server already registered by another server, updating with our info");
            zooKeeper.setData(metaRegionServerZnodePath, metaServerBytes, -1);
        }

        LOG.info(
                "Registered as meta region server in ZooKeeper (protobuf format): {}",
                metaRegionServerZnodePath);

        registerMasterNode(serverName, serverId);
    }

    private static void ensureHBaseClusterId() throws Exception {
        String clusterIdPath = "/hbase/hbaseid";
        try {
            if (zooKeeper.exists(clusterIdPath, false) == null) {
                String clusterId = "fluss-hbase-compat-" + System.currentTimeMillis();
                zooKeeper.create(
                        clusterIdPath,
                        Bytes.toBytes(clusterId),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                LOG.info("Created HBase cluster ID in ZooKeeper: {}", clusterId);
            }
        } catch (KeeperException.NodeExistsException e) {
        }
    }

    private static void ensureZNodeExists(String path) throws Exception {
        try {
            if (zooKeeper.exists(path, false) == null) {
                zooKeeper.create(
                        path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                LOG.info("Created ZNode: {}", path);
            }
        } catch (KeeperException.NodeExistsException e) {
        }
    }

    private static byte[] buildMetaRegionServerProtobuf(ServerName serverName) {
        HBaseProtos.RegionInfo metaRegionInfo =
                HBaseProtos.RegionInfo.newBuilder()
                        .setRegionId(1)
                        .setTableName(
                                HBaseProtos.TableName.newBuilder()
                                        .setNamespace(ByteString.copyFrom(Bytes.toBytes("hbase")))
                                        .setQualifier(ByteString.copyFrom(Bytes.toBytes("meta")))
                                        .build())
                        .setStartKey(ByteString.EMPTY)
                        .setEndKey(ByteString.EMPTY)
                        .build();

        HBaseProtos.ServerName serverNameProto =
                HBaseProtos.ServerName.newBuilder()
                        .setHostName(serverName.getHostname())
                        .setPort(serverName.getPort())
                        .setStartCode(serverName.getStartcode())
                        .build();

        ZooKeeperProtos.MetaRegionServer metaRegionServer =
                ZooKeeperProtos.MetaRegionServer.newBuilder()
                        .setServer(serverNameProto)
                        .setRpcVersion(1)
                        .build();

        byte[] protoBytes = metaRegionServer.toByteArray();

        byte[] pbufMagic = new byte[] {'P', 'B', 'U', 'F'};
        byte[] protoWithMagic = new byte[pbufMagic.length + protoBytes.length];
        System.arraycopy(pbufMagic, 0, protoWithMagic, 0, pbufMagic.length);
        System.arraycopy(protoBytes, 0, protoWithMagic, pbufMagic.length, protoBytes.length);

        byte[] serverNameBytes = (serverName.getHostname() + ":" + serverName.getPort()).getBytes();
        long randomSalt = java.util.concurrent.ThreadLocalRandom.current().nextLong();

        int dataLength = serverNameBytes.length + 8;
        int totalLength = 1 + 4 + dataLength + protoWithMagic.length;

        byte[] result = new byte[totalLength];
        int offset = 0;

        result[offset++] = (byte) 0xFF;

        offset = Bytes.putInt(result, offset, dataLength);

        offset = Bytes.putBytes(result, offset, serverNameBytes, 0, serverNameBytes.length);

        offset = Bytes.putLong(result, offset, randomSalt);

        System.arraycopy(protoWithMagic, 0, result, offset, protoWithMagic.length);

        return result;
    }

    private static void deleteStaleEphemeralNodeIfExists(String znodePath) throws Exception {
        try {
            if (zooKeeper.exists(znodePath, false) != null) {
                LOG.info(
                        "Deleting stale ephemeral node {} from previous crashed instance",
                        znodePath);
                zooKeeper.delete(znodePath, -1);
            }
        } catch (KeeperException.NoNodeException e) {
        }
    }

    private static void registerMasterNode(ServerName serverName, String serverId)
            throws Exception {
        String masterPath = "/hbase/master";

        HBaseProtos.ServerName serverNameProto =
                HBaseProtos.ServerName.newBuilder()
                        .setHostName(serverName.getHostname())
                        .setPort(serverName.getPort())
                        .setStartCode(serverName.getStartcode())
                        .build();

        ZooKeeperProtos.Master masterProto =
                ZooKeeperProtos.Master.newBuilder()
                        .setMaster(serverNameProto)
                        .setRpcVersion(1)
                        .setInfoPort(serverName.getPort())
                        .build();

        byte[] protoBytes = masterProto.toByteArray();
        byte[] pbufMagic = new byte[] {'P', 'B', 'U', 'F'};
        byte[] result = new byte[pbufMagic.length + protoBytes.length];
        System.arraycopy(pbufMagic, 0, result, 0, pbufMagic.length);
        System.arraycopy(protoBytes, 0, result, pbufMagic.length, protoBytes.length);

        try {
            zooKeeper.create(masterPath, result, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            LOG.info("Registered as master node in ZooKeeper (server: {})", serverId);
        } catch (KeeperException.NodeExistsException e) {
            LOG.info("Master node already exists, attempting to reclaim it");
            try {
                zooKeeper.delete(masterPath, -1);
                LOG.info("Deleted stale master node");
                zooKeeper.create(masterPath, result, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                LOG.info("Registered as master node in ZooKeeper (server: {})", serverId);
            } catch (KeeperException ex) {
                LOG.warn(
                        "Failed to reclaim master node, another server likely holds it: {}",
                        ex.getMessage());
            }
        } catch (KeeperException e) {
            LOG.error("Failed to create master node in ZooKeeper: {}", e.getMessage(), e);
            throw e;
        }
    }

    private static void unregisterFromZooKeeper() {
        if (zooKeeper != null) {
            try {
                if (regionServerZnodePath != null) {
                    try {
                        zooKeeper.delete(regionServerZnodePath, -1);
                        LOG.info("Unregistered from ZooKeeper: {}", regionServerZnodePath);
                    } catch (Exception e) {
                        LOG.warn("Failed to delete region server znode", e);
                    }
                }

                try {
                    zooKeeper.delete("/hbase/master", -1);
                    LOG.info("Unregistered master node from ZooKeeper");
                } catch (Exception e) {
                    LOG.warn("Failed to delete master znode", e);
                }

                zooKeeper.close();
                LOG.info("ZooKeeper connection closed");
            } catch (Exception e) {
                LOG.error("Error closing ZooKeeper connection", e);
            }
        }
    }
}
