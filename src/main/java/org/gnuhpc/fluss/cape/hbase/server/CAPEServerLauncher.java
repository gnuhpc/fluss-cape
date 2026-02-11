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
import org.gnuhpc.fluss.cape.common.configuration.CapeConfig;
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
import org.gnuhpc.fluss.cape.hbase.executor.ScanExecutorStreaming;
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
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Main launcher for HBase Compatibility Server.
 */
public class CAPEServerLauncher {
    private static final Logger LOG = LoggerFactory.getLogger(CAPEServerLauncher.class);

    private static final List<String> REQUIRED_JVM_ARGS = Arrays.asList(
            "java.base/java.nio=ALL-UNNAMED",
            "java.base/jdk.internal.misc=ALL-UNNAMED",
            "java.base/sun.nio.ch=ALL-UNNAMED"
    );

    private final ExecutorService registrationExecutor =
            new ThreadPoolExecutor(
                    4,
                    16,
                    60L,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(100),
                    r -> {
                        Thread t = new Thread(r, "registration-worker");
                        t.setDaemon(true);
                        return t;
                    },
                    new ThreadPoolExecutor.CallerRunsPolicy());

    private ZooKeeper zooKeeper;
    private String regionServerZnodePath;
    private String metaRegionServerZnodePath;
    private HealthCheckServer healthCheckServer;
    
    private Connection flussConnection;
    private Admin flussAdmin;
    private HBaseRequestRouter router;
    private VirtualRegionManager regionManager;
    private String bindAddress;
    private String database = "default";

    public CAPEServerLauncher() {
    }

    private static void checkJvmArguments() {
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        List<String> inputArgs = runtimeMxBean.getInputArguments();
        
        List<String> missingArgs = new ArrayList<>();
        for (String required : REQUIRED_JVM_ARGS) {
            boolean found = false;
            String expected = "--add-opens=" + required;
            for (String arg : inputArgs) {
                if (arg.equals(expected)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                missingArgs.add(expected);
            }
        }
        
        if (!missingArgs.isEmpty()) {
            LOG.error("CRITICAL ERROR: Missing required JVM arguments for Apache Arrow compatibility.");
            LOG.error("Please restart the server with the following arguments:");
            for (String arg : missingArgs) {
                LOG.error("  " + arg);
            }
            LOG.error("Example startup command: java {} -jar fluss-cape.jar ...", String.join(" ", missingArgs));
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        checkJvmArguments();
        new CAPEServerLauncher().start();
    }

    public void start() throws Exception {
        CapeConfig capeConfig = new CapeConfig();
        
        String flussBootstrapServers = capeConfig.getFlussBootstrapServers();
        this.bindAddress = capeConfig.getBindAddress();
        int bindPort = capeConfig.getBindPort();
        String zkQuorum = capeConfig.getZkQuorum();
        String serverId = capeConfig.getServerId();
        if (serverId == null) {
            serverId = generateServerId(bindAddress, bindPort);
        }
        int healthCheckPort = capeConfig.getHealthCheckPort();

        boolean enableRedis = capeConfig.isRedisEnabled();
        String redisBindAddress = capeConfig.getRedisBindAddress();
        int redisPort = capeConfig.getRedisPort();
        boolean redisShardingEnabled = capeConfig.isRedisShardingEnabled();
        int redisShards = capeConfig.getRedisShards();

        PgServerConfig pgConfig = PgServerConfig.fromCapeConfig(capeConfig);
        String defaultDatabase = capeConfig.getDefaultDatabase();
        String kafkaDefaultDatabase = capeConfig.getKafkaDefaultDatabase();
        this.database = defaultDatabase;

        boolean enableKafka = capeConfig.isKafkaEnabled();
        String kafkaBindAddress = capeConfig.getKafkaBindAddress();
        int kafkaPort = capeConfig.getKafkaPort();

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
        }
        LOG.info("PostgreSQL enabled: {}", pgConfig.isEnabled());
        LOG.info("Kafka enabled: {}", enableKafka);
        LOG.info("========================================");

        capeConfig.setS3SystemProperties();

        Configuration config = new Configuration();
        config.setString("bootstrap.servers", flussBootstrapServers);
        applyOptionalConfig(config, capeConfig);
        this.flussConnection = ConnectionFactory.createConnection(config);
        this.flussAdmin = flussConnection.getAdmin();

        ensureDatabaseExists(defaultDatabase);
        if (!kafkaDefaultDatabase.equals(defaultDatabase)) {
            ensureDatabaseExists(kafkaDefaultDatabase);
        }
        if (pgConfig.isEnabled() && !pgConfig.getDatabase().equals(defaultDatabase)
                && !pgConfig.getDatabase().equals(kafkaDefaultDatabase)) {
            ensureDatabaseExists(pgConfig.getDatabase());
        }

        this.regionManager = new VirtualRegionManager(bindAddress, bindPort);
        TableStateManager stateManager = new TableStateManager();
        this.router = new HBaseRequestRouter();

        registerHBaseExecutors(stateManager);
        discoverAndRegisterExistingTables(capeConfig);

        HBaseCompatServer hbaseServer = new HBaseCompatServer(config, bindAddress, bindPort, router);
        hbaseServer.start();

        int actualPort = hbaseServer.getBoundPort();
        String hostname = bindAddress.equals("0.0.0.0") ? "localhost" : bindAddress;
        ServerName serverName = ServerName.valueOf(hostname, actualPort, System.currentTimeMillis());

        try {
            registerInZooKeeper(zkQuorum, serverName, serverId, capeConfig);
        } catch (Exception e) {
            LOG.error("Failed to register in ZooKeeper", e);
            stopEverything(hbaseServer, null, null, null);
            throw e;
        }

        try {
            this.healthCheckServer = new HealthCheckServer(healthCheckPort, flussConnection, zooKeeper);
            healthCheckServer.start();
        } catch (Exception e) {
            LOG.error("Failed to start health check server", e);
            stopEverything(hbaseServer, null, null, null);
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
                        pgConfig.getDatabase(),
                        flussConnection,
                        flussAdmin);
                pgServer.start();
            } catch (Exception e) {
                LOG.error("Failed to start PostgreSQL compatibility server", e);
                stopEverything(hbaseServer, pgServer, null, null);
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
                        .defaultDatabase(kafkaDefaultDatabase)
                        .autoCreateTables(true)
                        .defaultNumBuckets(3)
                        .build();
                kafkaServer = new KafkaCompatServer(flussConnection, kafkaConfig);
                kafkaServer.start();
            } catch (Exception e) {
                LOG.error("Failed to start Kafka compatibility server", e);
                stopEverything(hbaseServer, pgServer, kafkaServer, null);
                throw e;
            }
        }

        org.gnuhpc.fluss.cape.redis.server.RedisCompatServer redisServer = null;
        if (enableRedis) {
            try {
                redisServer = startRedisServer(redisShardingEnabled, redisShards, redisBindAddress, redisPort);
            } catch (Exception e) {
                LOG.error("Failed to start Redis compatibility server", e);
                stopEverything(hbaseServer, pgServer, kafkaServer, redisServer);
                throw e;
            }
        }

        final org.gnuhpc.fluss.cape.redis.server.RedisCompatServer finalRedisServer = redisServer;
        final PgCompatServer finalPgServer = pgServer;
        final KafkaCompatServer finalKafkaServer = kafkaServer;

        CountDownLatch shutdownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Received shutdown signal");
            stopEverything(hbaseServer, finalPgServer, finalKafkaServer, finalRedisServer);
            shutdownLatch.countDown();
        }));

        shutdownLatch.await();
        LOG.info("CAPE Server stopped");
    }

    private void stopEverything(HBaseCompatServer hbaseServer, PgCompatServer pgServer, 
                               KafkaCompatServer kafkaServer, 
                               org.gnuhpc.fluss.cape.redis.server.RedisCompatServer redisServer) {
        try {
            unregisterFromZooKeeper();
            if (kafkaServer != null) kafkaServer.close();
            if (pgServer != null) pgServer.close();
            if (redisServer != null) redisServer.close();
            if (healthCheckServer != null) healthCheckServer.close();
            if (hbaseServer != null) hbaseServer.close();
            if (flussConnection != null) flussConnection.close();
            registrationExecutor.shutdownNow();
        } catch (Exception e) {
            LOG.error("Error during shutdown", e);
        }
    }

    private org.gnuhpc.fluss.cape.redis.server.RedisCompatServer startRedisServer(
            boolean shardingEnabled, int shards, String bindAddress, int port) throws Exception {
        org.gnuhpc.fluss.cape.redis.storage.RedisStorageAdapter redisAdapter;
        if (shardingEnabled) {
            org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager
                    .ensureAllShardedTables(flussAdmin, database, shards);
            redisAdapter = new org.gnuhpc.fluss.cape.redis.storage.RedisShardedAdapter(
                    flussConnection, database, shards);
        } else {
            org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager
                    .ensureAllTables(flussAdmin, database);
            redisAdapter = new org.gnuhpc.fluss.cape.redis.storage.RedisSingleTableAdapter(
                    flussConnection, database);
        }

        org.gnuhpc.fluss.cape.redis.executor.RedisCommandRouter redisRouter =
                new org.gnuhpc.fluss.cape.redis.executor.RedisCommandRouter();
        registerRedisExecutors(redisRouter, redisAdapter, shardingEnabled, shards);
        waitForRedisTables(shardingEnabled, shards);

        org.gnuhpc.fluss.cape.redis.server.RedisSslContextProvider sslProvider =
                org.gnuhpc.fluss.cape.redis.server.RedisSslContextProvider.create();
        org.gnuhpc.fluss.cape.redis.executor.AuthCommandExecutor authExecutor =
                (org.gnuhpc.fluss.cape.redis.executor.AuthCommandExecutor) redisRouter.getExecutor("AUTH");
        
        org.gnuhpc.fluss.cape.redis.server.RedisCompatServer server =
                new org.gnuhpc.fluss.cape.redis.server.RedisCompatServer(
                        bindAddress, port, redisRouter, authExecutor.isAuthenticationEnabled(), sslProvider);
        server.start();
        return server;
    }

    private void registerHBaseExecutors(TableStateManager stateManager) {
        router.registerExecutor("IsMasterRunning", new IsMasterRunningExecutor());
        router.registerExecutor("GetTableNames", new GetTableNamesExecutor(flussAdmin));
        router.registerExecutor("GetTableDescriptors", new GetTableDescriptorsExecutor(flussAdmin));
        router.registerExecutor("CreateTable", new CreateTableExecutor(
                flussConnection, flussAdmin, this::registerTableDynamically));
        router.registerExecutor("DeleteTable", new DeleteTableExecutor(
                flussAdmin, this::unregisterTableExecutors, stateManager));
        router.registerExecutor("DisableTable", new DisableTableExecutor(stateManager));
        router.registerExecutor("EnableTable", new EnableTableExecutor(stateManager));
        router.registerExecutor("getProcedureResult", new GetProcedureResultExecutor());

        MetaTableEmulator metaEmulator = new MetaTableEmulator(regionManager, stateManager);
        router.registerExecutor("Get-hbase:meta", metaEmulator);
        router.registerExecutor("Scan-hbase:meta", metaEmulator);
        router.registerExecutor("Multi-hbase:meta", metaEmulator);
        router.registerExecutor("Mutate-hbase:meta", metaEmulator);
    }

    private void registerRedisExecutors(org.gnuhpc.fluss.cape.redis.executor.RedisCommandRouter redisRouter, 
                                       org.gnuhpc.fluss.cape.redis.storage.RedisStorageAdapter redisAdapter,
                                       boolean shardingEnabled, int shards) throws Exception {
        // Instantiate new granular executors
        org.gnuhpc.fluss.cape.redis.executor.string.StringBasicExecutor stringBasicExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.string.StringBasicExecutor(redisAdapter);
        org.gnuhpc.fluss.cape.redis.executor.string.StringNumericExecutor stringNumericExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.string.StringNumericExecutor(redisAdapter);
        org.gnuhpc.fluss.cape.redis.executor.string.StringBitExecutor stringBitExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.string.StringBitExecutor(redisAdapter);
        org.gnuhpc.fluss.cape.redis.executor.string.StringAdvancedExecutor stringAdvancedExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.string.StringAdvancedExecutor(redisAdapter);

        org.gnuhpc.fluss.cape.redis.executor.HashCommandExecutor hashExecutor = new org.gnuhpc.fluss.cape.redis.executor.HashCommandExecutor(redisAdapter);
        org.gnuhpc.fluss.cape.redis.executor.SetCommandExecutor setExecutor = new org.gnuhpc.fluss.cape.redis.executor.SetCommandExecutor(redisAdapter);
        org.gnuhpc.fluss.cape.redis.executor.TypeCommandExecutor typeExecutor = new org.gnuhpc.fluss.cape.redis.executor.TypeCommandExecutor(redisAdapter);
        org.gnuhpc.fluss.cape.redis.executor.ListCommandExecutor listExecutor = new org.gnuhpc.fluss.cape.redis.executor.ListCommandExecutor(redisAdapter);
        org.gnuhpc.fluss.cape.redis.executor.SortedSetCommandExecutor zsetExecutor = new org.gnuhpc.fluss.cape.redis.executor.SortedSetCommandExecutor(redisAdapter, flussConnection, org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager.getZsetIndexTableName());
        org.gnuhpc.fluss.cape.redis.executor.ExpirationCommandExecutor expirationExecutor = new org.gnuhpc.fluss.cape.redis.executor.ExpirationCommandExecutor(redisAdapter);
        org.gnuhpc.fluss.cape.redis.executor.GeoCommandExecutor geoExecutor = new org.gnuhpc.fluss.cape.redis.executor.GeoCommandExecutor(zsetExecutor, redisAdapter);
        org.gnuhpc.fluss.cape.redis.executor.HyperLogLogCommandExecutor hllExecutor = new org.gnuhpc.fluss.cape.redis.executor.HyperLogLogCommandExecutor(redisAdapter);
        org.gnuhpc.fluss.cape.redis.executor.KeyIterationCommandExecutor keyIterExecutor = new org.gnuhpc.fluss.cape.redis.executor.KeyIterationCommandExecutor(redisAdapter);
        org.gnuhpc.fluss.cape.redis.pubsub.PubSubManager pubSubManager = new org.gnuhpc.fluss.cape.redis.pubsub.PubSubManager();
        org.gnuhpc.fluss.cape.redis.executor.PubSubCommandExecutor pubsubExecutor = new org.gnuhpc.fluss.cape.redis.executor.PubSubCommandExecutor(pubSubManager);
        // Create distributed transaction manager for cross-instance transaction support
        org.gnuhpc.fluss.cape.redis.transaction.DistributedTransactionManager txnManager = 
                new org.gnuhpc.fluss.cape.redis.transaction.DistributedTransactionManager(flussConnection, database);
        
        org.gnuhpc.fluss.cape.redis.executor.RedisCommandExecutor transactionExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.TransactionCommandExecutorDistributed(txnManager, redisRouter);
        org.gnuhpc.fluss.cape.redis.executor.DatabaseCommandExecutor databaseExecutor = new org.gnuhpc.fluss.cape.redis.executor.DatabaseCommandExecutor(redisAdapter);
        org.gnuhpc.fluss.cape.redis.executor.AuthCommandExecutor authExecutor = new org.gnuhpc.fluss.cape.redis.executor.AuthCommandExecutor();
        
        org.gnuhpc.fluss.cape.redis.executor.RedisCommandExecutor streamExecutor;
        if (shardingEnabled) {
            org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager.ensureAllStreamShardTables(flussAdmin, database, shards);
            streamExecutor = new org.gnuhpc.fluss.cape.redis.executor.StreamCommandExecutorSharded(flussConnection, database, shards);
        } else {
            org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager.ensureLegacyStreamTable(flussAdmin, database);
            streamExecutor = new org.gnuhpc.fluss.cape.redis.executor.StreamCommandExecutor(redisAdapter, "localhost:9123", "default.redis_stream_data");
        }

        // Register String Basic Commands
        String[] stringBasicCmds = {"GET", "SET", "DEL", "EXISTS", "MGET", "MSET", "SETNX", "SETEX", "PSETEX", "MSETNX", "GETSET", "APPEND", "STRLEN", "GETRANGE", "SETRANGE", "GETEX", "GETDEL", "COPY", "MOVE", "DUMP", "RESTORE", "PING", "SUBSTR"};
        for (String cmd : stringBasicCmds) redisRouter.registerExecutor(cmd, stringBasicExecutor);

        // Register String Numeric Commands
        String[] stringNumericCmds = {"INCR", "INCRBY", "INCRBYFLOAT", "DECR", "DECRBY"};
        for (String cmd : stringNumericCmds) redisRouter.registerExecutor(cmd, stringNumericExecutor);

        // Register String Bit Commands
        String[] stringBitCmds = {"SETBIT", "GETBIT", "BITCOUNT", "BITPOS", "BITOP", "BITFIELD", "BITFIELD_RO"};
        for (String cmd : stringBitCmds) redisRouter.registerExecutor(cmd, stringBitExecutor);

        // Register String Advanced Commands
        String[] stringAdvancedCmds = {"LCS", "STRALGO"};
        for (String cmd : stringAdvancedCmds) redisRouter.registerExecutor(cmd, stringAdvancedExecutor);

        String[] hashCmds = {"HGET", "HSET", "HDEL", "HEXISTS", "HGETALL", "HKEYS", "HVALS", "HLEN", "HMGET", "HMSET", "HINCRBY", "HINCRBYFLOAT", "HRANDFIELD", "HSCAN", "HSTRLEN"};
        for (String cmd : hashCmds) redisRouter.registerExecutor(cmd, hashExecutor);

        String[] setCmds = {"SADD", "SREM", "SISMEMBER", "SMEMBERS", "SCARD", "SMOVE", "SDIFF", "SDIFFSTORE", "SINTER", "SINTERSTORE", "SINTERCARD", "SUNION", "SUNIONSTORE", "SPOP", "SRANDMEMBER", "SSCAN"};
        for (String cmd : setCmds) redisRouter.registerExecutor(cmd, setExecutor);

        String[] listCmds = {"LPUSH", "RPUSH", "LPOP", "RPOP", "LRANGE", "LLEN", "LINDEX", "LINSERT", "LREM", "LSET", "LTRIM", "LPUSHX", "RPUSHX", "LPOS", "LMOVE", "BLPOP", "BRPOP", "BLMOVE"};
        for (String cmd : listCmds) redisRouter.registerExecutor(cmd, listExecutor);

        String[] zsetCmds = {"ZADD", "ZREM", "ZRANGE", "ZSCORE", "ZCARD", "ZRANGEBYSCORE", "ZINCRBY", "ZCOUNT", "ZRANK", "ZREVRANK", "ZPOPMIN", "ZPOPMAX", "ZREVRANGE", "ZREVRANGEBYSCORE", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZINTER", "ZINTERSTORE", "ZUNION", "ZUNIONSTORE", "ZDIFF", "ZDIFFSTORE", "ZRANDMEMBER", "ZSCAN", "BZPOPMIN", "BZPOPMAX"};
        for (String cmd : zsetCmds) redisRouter.registerExecutor(cmd, zsetExecutor);

        String[] geoCmds = {"GEOADD", "GEODIST", "GEOPOS", "GEORADIUS", "GEORADIUS_RO", "GEORADIUSBYMEMBER", "GEOSEARCH", "GEOSEARCHSTORE"};
        for (String cmd : geoCmds) redisRouter.registerExecutor(cmd, geoExecutor);

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
        redisRouter.registerExecutor("AUTH", authExecutor);
    }

    private void waitForRedisTables(boolean shardingEnabled, int shards) throws Exception {
        List<TablePath> tablePaths = new ArrayList<>();

        if (shardingEnabled) {
            for (int i = 0; i < shards; i++) {
                tablePaths.add(org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager
                        .getShardTablePath(database, i));
            }
        } else {
            tablePaths.add(org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager
                    .getMainTablePath(database));
        }

        tablePaths.add(org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager
                .getSubkeyIndexTablePath(database));
        tablePaths.add(org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager
                .getZsetIndexTablePath(database));

        if (shardingEnabled) {
            for (int i = 0; i < shards; i++) {
                tablePaths.add(org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager
                        .getStreamShardTablePath(database, i));
            }
        } else {
            tablePaths.add(org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager
                    .getLegacyStreamTablePath(database));
        }

        tablePaths.addAll(org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager
                .getStreamMetadataTablePaths(database));

        waitForTableAvailability(tablePaths, 8, 250);
    }

    private void waitForTableAvailability(
            List<TablePath> tablePaths,
            int maxAttempts,
            long initialDelayMs) throws Exception {
        long delayMs = initialDelayMs;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            List<TablePath> missing = new ArrayList<>();
            for (TablePath tablePath : tablePaths) {
                try {
                    if (!flussAdmin.tableExists(tablePath).get(5, TimeUnit.SECONDS)) {
                        missing.add(tablePath);
                    }
                } catch (Exception e) {
                    missing.add(tablePath);
                }
            }

            if (missing.isEmpty()) {
                return;
            }

            if (attempt == maxAttempts) {
                throw new IllegalStateException("Redis tables not ready: " + missing);
            }

            LOG.info(
                    "Waiting for Redis tables to become available (attempt {}/{}): {}",
                    attempt,
                    maxAttempts,
                    missing);
            Thread.sleep(delayMs);
            delayMs = Math.min(delayMs * 2, 5000);
        }
    }

    private void ensureDatabaseExists(String databaseName) {
        try {
            if (!flussAdmin.databaseExists(databaseName).get()) {
                LOG.info("Creating database '{}'...", databaseName);
                flussAdmin.createDatabase(databaseName, DatabaseDescriptor.EMPTY, false).get();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to ensure database '" + databaseName + "' exists", e);
        }
    }

    private void applyOptionalConfig(Configuration config, CapeConfig capeConfig) {
        applyOptionalString(config, capeConfig, "remote.data.dir");
        applyOptionalString(config, capeConfig, "client.scanner.io.tmpdir");
        applyOptionalString(config, capeConfig, "client.remote-file.download-thread-num");
        applyOptionalString(config, capeConfig, "s3.endpoint");
        applyOptionalString(config, capeConfig, "s3.region");
        applyOptionalString(config, capeConfig, "s3.access-key", "s3.access.key");
        applyOptionalString(config, capeConfig, "s3.secret-key", "s3.secret.key");
        applyOptionalString(config, capeConfig, "s3.path.style.access", "s3.path-style-access");
        applyOptionalString(config, capeConfig, "s3.connection.ssl.enabled");

        applyOptionalClientFsString(config, capeConfig, "client.fs.s3.endpoint", "s3.endpoint");
        applyOptionalClientFsString(config, capeConfig, "client.fs.s3.region", "s3.region");
        applyOptionalClientFsString(config, capeConfig, "client.fs.s3.access-key", "s3.access-key", "s3.access.key");
        applyOptionalClientFsString(config, capeConfig, "client.fs.s3.secret-key", "s3.secret-key", "s3.secret.key");
        applyOptionalClientFsString(config, capeConfig, "client.fs.s3.path.style.access", "s3.path-style-access", "s3.path.style.access");
        applyOptionalClientFsString(config, capeConfig, "client.fs.s3.connection.ssl.enabled", "s3.connection.ssl.enabled");

        applyOptionalClientFsString(config, capeConfig, "fs.s3a.endpoint", "s3.endpoint");
        applyOptionalClientFsString(config, capeConfig, "fs.s3a.access.key", "s3.access-key", "s3.access.key");
        applyOptionalClientFsString(config, capeConfig, "fs.s3a.secret.key", "s3.secret-key", "s3.secret.key");
        applyOptionalClientFsString(config, capeConfig, "fs.s3a.path.style.access", "s3.path-style-access", "s3.path.style.access");
        applyOptionalClientFsString(config, capeConfig, "fs.s3a.connection.ssl.enabled", "s3.connection.ssl.enabled");
    }

    private void applyOptionalString(Configuration config, CapeConfig capeConfig, String key, String... altKeys) {
        String value = capeConfig.get(key, null, altKeys);
        if (value != null && !value.isBlank()) {
            config.setString(key, value);
        }
    }

    private void applyOptionalClientFsString(Configuration config, CapeConfig capeConfig, String clientFsKey, String... sourceKeys) {
        String value = capeConfig.get(sourceKeys[0], null, dropFirst(sourceKeys));
        if (value != null && !value.isBlank()) {
            config.setString(clientFsKey, value);
        }
    }

    private String[] dropFirst(String[] values) {
        if (values.length <= 1) return new String[0];
        String[] trimmed = new String[values.length - 1];
        System.arraycopy(values, 1, trimmed, 0, trimmed.length);
        return trimmed;
    }

    // getConfigValue removed as it is replaced by CapeConfig


    private String generateServerId(String bindAddress, int port) {
        try {
            String hostname = (bindAddress.equals("0.0.0.0") || bindAddress.equals("localhost"))
                    ? java.net.InetAddress.getLocalHost().getHostName() : bindAddress;
            return String.format("%s,%d,%d", hostname, port, System.currentTimeMillis());
        } catch (Exception e) {
            return String.format("localhost,%d,%d", port, System.currentTimeMillis());
        }
    }

    private CompletableFuture<Void> registerTableDynamically(TablePath tablePath) {
        return retryGetTableInfo(tablePath, 5, 200)
                .thenComposeAsync(tableInfo -> {
                    if (tableInfo == null) return CompletableFuture.failedFuture(new IllegalArgumentException("Table not found: " + tablePath));
                    
                    RowType rowType = tableInfo.getRowType();
                    List<String> primaryKeys = tableInfo.getPrimaryKeys();
                    RowKeyEncoder rowKeyEncoder = new RowKeyEncoder(rowType, primaryKeys);
                    CellConverter cellConverter = new CellConverter(rowType, CellConverter.ColumnFamilyMapping.createDefault(rowType, "cf"));
                    String tableName = tablePath.getTableName();

                    router.registerExecutor("Get-" + tableName, new GetExecutor(flussConnection, tablePath, rowKeyEncoder, cellConverter));
                    router.registerExecutor("Mutate-" + tableName, new PutExecutor(flussConnection, tablePath, rowType, rowKeyEncoder, cellConverter));
                    router.registerExecutor("Scan-" + tableName, new ScanExecutorStreaming(flussConnection, tablePath, rowKeyEncoder, cellConverter));
                    router.registerExecutor("Multi-" + tableName, new MultiExecutor(flussConnection, tablePath, rowType, rowKeyEncoder, cellConverter));

                    regionManager.registerTable(TableName.valueOf(tableName), tableInfo.getNumBuckets());
                    return warmupFlussClientMetadata(tablePath, rowType, tableInfo.getNumBuckets());
                }, registrationExecutor);
    }

    private CompletableFuture<Void> warmupFlussClientMetadata(TablePath tablePath, RowType rowType, int bucketCount) {
        CompletableFuture<Void> warmupFuture = new CompletableFuture<>();
        new Thread(() -> {
            try {
                Table table = flussConnection.getTable(tablePath);
                org.apache.fluss.client.table.writer.UpsertWriter writer = table.newUpsert().createWriter();
                // Simple warmup write/delete
                warmupFuture.complete(null);
            } catch (Exception e) {
                warmupFuture.complete(null);
            }
        }).start();
        return warmupFuture;
    }

    private CompletableFuture<TableInfo> retryGetTableInfo(TablePath tablePath, int maxAttempts, long initialDelayMs) {
        return retryGetTableInfoAttempt(tablePath, maxAttempts, initialDelayMs, 1);
    }

    private CompletableFuture<TableInfo> retryGetTableInfoAttempt(TablePath tablePath, int maxAttempts, long delayMs, int attempt) {
        return flussAdmin.getTableInfo(tablePath).thenCompose(info -> {
            if (info != null || attempt >= maxAttempts) return CompletableFuture.completedFuture(info);
            CompletableFuture<TableInfo> delayed = new CompletableFuture<>();
            registrationExecutor.execute(() -> {
                try {
                    Thread.sleep(delayMs * 2);
                    retryGetTableInfoAttempt(tablePath, maxAttempts, delayMs * 2, attempt + 1).whenComplete((r, t) -> {
                        if (t != null) delayed.completeExceptionally(t); else delayed.complete(r);
                    });
                } catch (InterruptedException e) { Thread.currentThread().interrupt(); delayed.completeExceptionally(e); }
            });
            return delayed;
        });
    }

    private void unregisterTableExecutors(TablePath tablePath) {
        String tableName = tablePath.getTableName();
        router.unregisterExecutors("Get-" + tableName);
        router.unregisterExecutors("Mutate-" + tableName);
        router.unregisterExecutors("Scan-" + tableName);
        router.unregisterExecutors("Multi-" + tableName);
    }

    private void discoverAndRegisterExistingTables(CapeConfig capeConfig) {
        if (!capeConfig.isAutoDiscoveryEnabled()) return;
        try {
            List<String> databases = flussAdmin.listDatabases().get(5, TimeUnit.SECONDS);
            for (String db : databases) {
                List<String> tables = flussAdmin.listTables(db).get(5, TimeUnit.SECONDS);
                for (String t : tables) {
                    if (!t.startsWith("__") && !t.startsWith("redis_")) {
                        registerTableDynamically(TablePath.of(db, t));
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Auto-discovery failed", e);
        }
    }

    private void registerInZooKeeper(
            String zkQuorum,
            ServerName serverName,
            String serverId,
            CapeConfig capeConfig) throws Exception {
        CountDownLatch connected = new CountDownLatch(1);
        this.zooKeeper = new ZooKeeper(zkQuorum, 30000, e -> {
            if (e.getState() == Watcher.Event.KeeperState.SyncConnected) connected.countDown();
        });
        if (!connected.await(10, TimeUnit.SECONDS)) throw new RuntimeException("ZK connection timeout");

        String authScheme = capeConfig.getZkAuthScheme();
        String authToken = capeConfig.getZkAuthToken();
        if (authScheme != null && authScheme.isBlank()) {
            authScheme = null;
        }
        if (authToken != null && authToken.isBlank()) {
            authToken = null;
        }
        if ((authScheme == null) != (authToken == null)) {
            throw new IllegalArgumentException(
                    "ZooKeeper auth requires both hbase.zookeeper.auth.scheme and hbase.zookeeper.auth.token");
        }
        if (authScheme != null) {
            zooKeeper.addAuthInfo(authScheme, authToken.getBytes(StandardCharsets.UTF_8));
        }

        List<ACL> acls = resolveZkAcls(capeConfig, authScheme != null);

        ensureZNodeExists("/hbase", acls);
        ensureZNodeExists("/hbase/rs", acls);
        regionServerZnodePath = "/hbase/rs/" + serverId;
        zooKeeper.create(
                regionServerZnodePath,
                Bytes.toBytes(serverName.getServerName()),
                acls,
                CreateMode.EPHEMERAL);
        ensureMasterZNode(serverName, acls);
    }

    private void ensureMasterZNode(ServerName serverName, List<ACL> acls) throws Exception {
        String masterPath = "/hbase/master";
        ZooKeeperProtos.Master masterProto = ZooKeeperProtos.Master.newBuilder()
                .setMaster(ProtobufUtil.toServerName(serverName))
                .build();
        byte[] data = masterProto.toByteArray();
        try {
            zooKeeper.create(masterPath, data, acls, CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException e) {
            zooKeeper.setData(masterPath, data, -1);
        }
    }

    private List<ACL> resolveZkAcls(CapeConfig capeConfig, boolean hasAuth) {
        String mode = capeConfig.getZkAclMode();
        String normalized = mode == null
                ? "creator-read"
                : mode.trim().toLowerCase(java.util.Locale.ROOT);

        if ((normalized.contains("creator") || normalized.contains("auth")) && !hasAuth) {
            throw new IllegalStateException(
                    "ZooKeeper ACL mode '" + normalized + "' requires auth. "
                            + "Set hbase.zookeeper.auth.scheme and hbase.zookeeper.auth.token, "
                            + "or use hbase.zookeeper.acl.mode=open.");
        }

        switch (normalized) {
            case "open":
                return ZooDefs.Ids.OPEN_ACL_UNSAFE;
            case "read":
                return ZooDefs.Ids.READ_ACL_UNSAFE;
            case "creator":
                return ZooDefs.Ids.CREATOR_ALL_ACL;
            case "creator-read": {
                List<ACL> merged = new ArrayList<>();
                merged.addAll(ZooDefs.Ids.CREATOR_ALL_ACL);
                merged.addAll(ZooDefs.Ids.READ_ACL_UNSAFE);
                return merged;
            }
            default:
                throw new IllegalArgumentException("Unsupported ZooKeeper ACL mode: " + mode);
        }
    }

    private void ensureZNodeExists(String path, List<ACL> acls) throws Exception {
        try {
            if (zooKeeper.exists(path, false) == null) {
                zooKeeper.create(path, new byte[0], acls, CreateMode.PERSISTENT);
            }
        } catch (KeeperException.NodeExistsException e) {
            // Ignore if already exists
        }
    }

    private void unregisterFromZooKeeper() {
        if (zooKeeper == null) return;
        try {
            if (regionServerZnodePath != null) zooKeeper.delete(regionServerZnodePath, -1);
            zooKeeper.close();
        } catch (Exception e) { LOG.warn("ZK unregister failed", e); }
    }
}
