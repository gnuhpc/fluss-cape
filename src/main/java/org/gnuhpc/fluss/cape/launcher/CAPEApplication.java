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

package org.gnuhpc.fluss.cape.launcher;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.hadoop.hbase.ServerName;
import org.gnuhpc.fluss.cape.common.configuration.CapeConfig;
import org.gnuhpc.fluss.cape.hbase.executor.*;
import org.gnuhpc.fluss.cape.hbase.metadata.MetaTableEmulator;
import org.gnuhpc.fluss.cape.hbase.metadata.TableStateManager;
import org.gnuhpc.fluss.cape.hbase.metadata.VirtualRegionManager;
import org.gnuhpc.fluss.cape.hbase.server.HBaseCompatServer;
import org.gnuhpc.fluss.cape.hbase.server.HealthCheckServer;
import org.gnuhpc.fluss.cape.kafka.server.KafkaCompatServer;
import org.gnuhpc.fluss.cape.pg.server.PgCompatServer;
import org.gnuhpc.fluss.cape.pg.server.PgServerConfig;
import org.gnuhpc.fluss.cape.redis.executor.RedisCommandRouter;
import org.gnuhpc.fluss.cape.redis.server.RedisCompatServer;
import org.gnuhpc.fluss.cape.redis.storage.RedisStorageAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CAPEApplication {
    private static final Logger LOG = LoggerFactory.getLogger(CAPEApplication.class);
    
    private static final List<String> REQUIRED_JVM_ARGS = Arrays.asList(
            "java.base/java.nio=ALL-UNNAMED",
            "java.base/jdk.internal.misc=ALL-UNNAMED",
            "java.base/sun.nio.ch=ALL-UNNAMED"
    );
    
    private final CapeConfig capeConfig;
    private final LifecycleManager lifecycleManager;
    private ProtocolServerFactory serverFactory;
    
    private Connection flussConnection;
    private Admin flussAdmin;
    private HBaseRequestRouter hbaseRouter;
    private VirtualRegionManager regionManager;
    private TableStateManager stateManager;
    private TableDiscoveryService discoveryService;
    
    public CAPEApplication() {
        this.capeConfig = new CapeConfig();
        this.lifecycleManager = new LifecycleManager();
    }
    
    public static void main(String[] args) throws Exception {
        checkJvmArguments();
        
        CAPEApplication app = new CAPEApplication();
        app.run();
    }
    
    public void run() throws Exception {
        LOG.info("========================================");
        LOG.info("Starting CAPE (Compatibility And Protocol Extensions)");
        LOG.info("========================================");
        
        try {
            initializeFlussConnection();
            ensureDatabases();
            initializeComponents();
            startServers();
            
            lifecycleManager.registerShutdownHook();
            lifecycleManager.awaitShutdown();
            
            LOG.info("CAPE application stopped");
        } catch (Exception e) {
            LOG.error("CAPE application failed to start", e);
            lifecycleManager.stopAll();
            throw e;
        }
    }
    
    private void initializeFlussConnection() throws Exception {
        String flussBootstrapServers = capeConfig.getFlussBootstrapServers();
        
        Configuration flussConfig = new Configuration();
        flussConfig.setString("bootstrap.servers", flussBootstrapServers);
        applyOptionalConfig(flussConfig);
        capeConfig.setS3SystemProperties();
        
        this.flussConnection = ConnectionFactory.createConnection(flussConfig);
        this.flussAdmin = flussConnection.getAdmin();
        
        this.serverFactory = new ProtocolServerFactory(capeConfig, flussConnection, 
                flussAdmin, flussConfig);
        
        LOG.info("Fluss connection initialized: {}", flussBootstrapServers);
    }
    
    private void ensureDatabases() {
        String defaultDb = capeConfig.getDefaultDatabase();
        String kafkaDb = capeConfig.getKafkaDefaultDatabase();
        PgServerConfig pgConfig = PgServerConfig.fromCapeConfig(capeConfig);
        
        ensureDatabase(defaultDb);
        
        if (!kafkaDb.equals(defaultDb)) {
            ensureDatabase(kafkaDb);
        }
        
        if (pgConfig.isEnabled() && !pgConfig.getDatabase().equals(defaultDb) 
                && !pgConfig.getDatabase().equals(kafkaDb)) {
            ensureDatabase(pgConfig.getDatabase());
        }
    }
    
    private void ensureDatabase(String dbName) {
        try {
            if (!flussAdmin.databaseExists(dbName).get()) {
                LOG.info("Creating database: {}", dbName);
                flussAdmin.createDatabase(dbName, DatabaseDescriptor.EMPTY, false).get();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to ensure database: " + dbName, e);
        }
    }
    
    private void initializeComponents() {
        String bindAddress = capeConfig.getBindAddress();
        int bindPort = capeConfig.getBindPort();
        String serverId = generateServerId(bindAddress, bindPort);
        
        this.regionManager = new VirtualRegionManager(bindAddress, bindPort);
        this.stateManager = new TableStateManager();
        this.hbaseRouter = new HBaseRequestRouter();
        
        registerHBaseExecutors();
        
        this.discoveryService = new TableDiscoveryService(
                flussConnection, flussAdmin, hbaseRouter, regionManager, capeConfig);
        
        LOG.info("Server ID: {}", serverId);
        LOG.info("Components initialized");
    }
    
    private void registerHBaseExecutors() {
        hbaseRouter.registerExecutor("IsMasterRunning", new IsMasterRunningExecutor());
        hbaseRouter.registerExecutor("GetTableNames", new GetTableNamesExecutor(flussAdmin));
        hbaseRouter.registerExecutor("GetTableDescriptors", 
                new GetTableDescriptorsExecutor(flussAdmin));
        hbaseRouter.registerExecutor("CreateTable", 
                new CreateTableExecutor(flussConnection, flussAdmin, 
                        tablePath -> discoveryService.registerTable(tablePath)));
        hbaseRouter.registerExecutor("DeleteTable", 
                new DeleteTableExecutor(flussAdmin, 
                        tablePath -> discoveryService.unregisterTable(tablePath), stateManager));
        hbaseRouter.registerExecutor("DisableTable", new DisableTableExecutor(stateManager));
        hbaseRouter.registerExecutor("EnableTable", new EnableTableExecutor(stateManager));
        hbaseRouter.registerExecutor("getProcedureResult", new GetProcedureResultExecutor());
        
        MetaTableEmulator metaEmulator = new MetaTableEmulator(regionManager, stateManager);
        hbaseRouter.registerExecutor("Get-hbase:meta", metaEmulator);
        hbaseRouter.registerExecutor("Scan-hbase:meta", metaEmulator);
        hbaseRouter.registerExecutor("Multi-hbase:meta", metaEmulator);
        hbaseRouter.registerExecutor("Mutate-hbase:meta", metaEmulator);
    }
    
    private void startServers() throws Exception {
        lifecycleManager.registerComponent(discoveryService);
        
        startHBaseServer();
        startZooKeeperRegistry();
        startHealthCheckServer();
        
        if (capeConfig.isRedisEnabled()) {
            startRedisServer();
        }
        
        PgServerConfig pgConfig = PgServerConfig.fromCapeConfig(capeConfig);
        if (pgConfig.isEnabled()) {
            startPgServer(pgConfig);
        }
        
        if (capeConfig.isKafkaEnabled()) {
            startKafkaServer();
        }
        
        lifecycleManager.startAll();
    }
    
    private void startHBaseServer() throws Exception {
        HBaseCompatServer hbaseServer = serverFactory.createHBaseServer(hbaseRouter);
        lifecycleManager.registerComponent(new ServerComponentAdapter(
                hbaseServer, "HBaseServer"));
    }
    
    private void startZooKeeperRegistry() {
        String bindAddress = capeConfig.getBindAddress();
        int bindPort = capeConfig.getBindPort();
        String zkQuorum = capeConfig.getZkQuorum();
        String serverId = capeConfig.getServerId();
        
        if (serverId == null) {
            serverId = generateServerId(bindAddress, bindPort);
        }
        
        String hostname = bindAddress.equals("0.0.0.0") ? "localhost" : bindAddress;
        ServerName serverName = ServerName.valueOf(hostname, bindPort, System.currentTimeMillis());
        
        ZooKeeperRegistry zkRegistry = new ZooKeeperRegistry(
                zkQuorum, serverName, serverId, capeConfig);
        lifecycleManager.registerComponent(zkRegistry);
    }
    
    private void startHealthCheckServer() throws Exception {
        HealthCheckServer healthServer = serverFactory.createHealthCheckServer(null);
        lifecycleManager.registerComponent(new ServerComponentAdapter(
                healthServer, "HealthCheckServer"));
    }
    
    private void startRedisServer() throws Exception {
        String database = capeConfig.getDefaultDatabase();
        RedisStorageAdapter redisAdapter = serverFactory.createRedisStorageAdapter(database);
        
        RedisCommandRouter redisRouter = new RedisCommandRouter();
        registerRedisExecutors(redisRouter, redisAdapter, database);
        
        RedisCompatServer redisServer = serverFactory.createRedisServer(redisRouter);
        lifecycleManager.registerComponent(new ServerComponentAdapter(
                redisServer, "RedisServer"));
    }
    
    private void registerRedisExecutors(RedisCommandRouter router, RedisStorageAdapter adapter, String database) throws Exception {
        org.gnuhpc.fluss.cape.redis.executor.string.StringBasicExecutor stringBasicExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.string.StringBasicExecutor(adapter);
        org.gnuhpc.fluss.cape.redis.executor.string.StringNumericExecutor stringNumericExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.string.StringNumericExecutor(adapter);
        org.gnuhpc.fluss.cape.redis.executor.string.StringBitExecutor stringBitExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.string.StringBitExecutor(adapter);
        org.gnuhpc.fluss.cape.redis.executor.string.StringAdvancedExecutor stringAdvancedExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.string.StringAdvancedExecutor(adapter);

        org.gnuhpc.fluss.cape.redis.executor.HashCommandExecutor hashExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.HashCommandExecutor(adapter);
        org.gnuhpc.fluss.cape.redis.executor.SetCommandExecutor setExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.SetCommandExecutor(adapter);
        org.gnuhpc.fluss.cape.redis.executor.TypeCommandExecutor typeExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.TypeCommandExecutor(adapter);
        org.gnuhpc.fluss.cape.redis.executor.ListCommandExecutor listExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.ListCommandExecutor(adapter);
        org.gnuhpc.fluss.cape.redis.executor.SortedSetCommandExecutor zsetExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.SortedSetCommandExecutor(
                        adapter, flussConnection, 
                        org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager.getZsetIndexTableName());
        org.gnuhpc.fluss.cape.redis.executor.ExpirationCommandExecutor expirationExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.ExpirationCommandExecutor(adapter);
        org.gnuhpc.fluss.cape.redis.executor.GeoCommandExecutor geoExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.GeoCommandExecutor(zsetExecutor, adapter);
        org.gnuhpc.fluss.cape.redis.executor.HyperLogLogCommandExecutor hllExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.HyperLogLogCommandExecutor(adapter);
        org.gnuhpc.fluss.cape.redis.executor.KeyIterationCommandExecutor keyIterExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.KeyIterationCommandExecutor(adapter);
        org.gnuhpc.fluss.cape.redis.pubsub.PubSubManager pubSubManager = 
                new org.gnuhpc.fluss.cape.redis.pubsub.PubSubManager();
        org.gnuhpc.fluss.cape.redis.executor.PubSubCommandExecutor pubsubExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.PubSubCommandExecutor(pubSubManager);
        // Create distributed transaction manager for cross-instance transaction support
        org.gnuhpc.fluss.cape.redis.transaction.DistributedTransactionManager txnManager = 
                new org.gnuhpc.fluss.cape.redis.transaction.DistributedTransactionManager(flussConnection, database);
        
        org.gnuhpc.fluss.cape.redis.executor.RedisCommandExecutor transactionExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.TransactionCommandExecutorDistributed(txnManager, router);
        org.gnuhpc.fluss.cape.redis.executor.DatabaseCommandExecutor databaseExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.DatabaseCommandExecutor(adapter);
        org.gnuhpc.fluss.cape.redis.executor.AuthCommandExecutor authExecutor = 
                new org.gnuhpc.fluss.cape.redis.executor.AuthCommandExecutor();
        
        boolean shardingEnabled = capeConfig.isRedisShardingEnabled();
        int shards = capeConfig.getRedisShards();
        
        org.gnuhpc.fluss.cape.redis.executor.RedisCommandExecutor streamExecutor;
        if (shardingEnabled) {
            org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager.ensureAllStreamShardTables(flussAdmin, database, shards);
            streamExecutor = new org.gnuhpc.fluss.cape.redis.executor.StreamCommandExecutorSharded(flussConnection, database, shards);
        } else {
            org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager.ensureLegacyStreamTable(flussAdmin, database);
            streamExecutor = new org.gnuhpc.fluss.cape.redis.executor.StreamCommandExecutor(adapter, 
                    capeConfig.getFlussBootstrapServers(), database + ".redis_stream_data");
        }

        String[] stringBasicCmds = {"GET", "SET", "DEL", "EXISTS", "MGET", "MSET", "SETNX", "SETEX", "PSETEX", "MSETNX", "GETSET", "APPEND", "STRLEN", "GETRANGE", "SETRANGE", "GETEX", "GETDEL", "COPY", "MOVE", "DUMP", "RESTORE", "PING", "SUBSTR"};
        for (String cmd : stringBasicCmds) router.registerExecutor(cmd, stringBasicExecutor);

        String[] stringNumericCmds = {"INCR", "INCRBY", "INCRBYFLOAT", "DECR", "DECRBY"};
        for (String cmd : stringNumericCmds) router.registerExecutor(cmd, stringNumericExecutor);

        String[] stringBitCmds = {"SETBIT", "GETBIT", "BITCOUNT", "BITPOS", "BITOP", "BITFIELD", "BITFIELD_RO"};
        for (String cmd : stringBitCmds) router.registerExecutor(cmd, stringBitExecutor);

        String[] stringAdvancedCmds = {"LCS", "STRALGO"};
        for (String cmd : stringAdvancedCmds) router.registerExecutor(cmd, stringAdvancedExecutor);

        String[] hashCmds = {"HGET", "HSET", "HDEL", "HEXISTS", "HGETALL", "HKEYS", "HVALS", "HLEN", "HMGET", "HMSET", "HINCRBY", "HINCRBYFLOAT", "HRANDFIELD", "HSCAN", "HSTRLEN"};
        for (String cmd : hashCmds) router.registerExecutor(cmd, hashExecutor);

        String[] setCmds = {"SADD", "SREM", "SISMEMBER", "SMEMBERS", "SCARD", "SMOVE", "SDIFF", "SDIFFSTORE", "SINTER", "SINTERSTORE", "SINTERCARD", "SUNION", "SUNIONSTORE", "SPOP", "SRANDMEMBER", "SSCAN"};
        for (String cmd : setCmds) router.registerExecutor(cmd, setExecutor);

        String[] listCmds = {"LPUSH", "RPUSH", "LPOP", "RPOP", "LRANGE", "LLEN", "LINDEX", "LINSERT", "LREM", "LSET", "LTRIM", "LPUSHX", "RPUSHX", "LPOS", "LMOVE", "BLPOP", "BRPOP", "BLMOVE"};
        for (String cmd : listCmds) router.registerExecutor(cmd, listExecutor);

        String[] zsetCmds = {"ZADD", "ZREM", "ZRANGE", "ZSCORE", "ZCARD", "ZRANGEBYSCORE", "ZINCRBY", "ZCOUNT", "ZRANK", "ZREVRANK", "ZPOPMIN", "ZPOPMAX", "ZREVRANGE", "ZREVRANGEBYSCORE", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZINTER", "ZINTERSTORE", "ZUNION", "ZUNIONSTORE", "ZDIFF", "ZDIFFSTORE", "ZRANDMEMBER", "ZSCAN", "BZPOPMIN", "BZPOPMAX"};
        for (String cmd : zsetCmds) router.registerExecutor(cmd, zsetExecutor);

        String[] geoCmds = {"GEOADD", "GEODIST", "GEOPOS", "GEORADIUS", "GEORADIUS_RO", "GEORADIUSBYMEMBER", "GEOSEARCH", "GEOSEARCHSTORE"};
        for (String cmd : geoCmds) router.registerExecutor(cmd, geoExecutor);

        router.registerExecutor("PFADD", hllExecutor);
        router.registerExecutor("PFCOUNT", hllExecutor);
        router.registerExecutor("PFMERGE", hllExecutor);
        router.registerExecutor("KEYS", keyIterExecutor);
        router.registerExecutor("SCAN", keyIterExecutor);
        router.registerExecutor("PUBLISH", pubsubExecutor);
        router.registerExecutor("SUBSCRIBE", pubsubExecutor);
        router.registerExecutor("UNSUBSCRIBE", pubsubExecutor);
        router.registerExecutor("PSUBSCRIBE", pubsubExecutor);
        router.registerExecutor("PUNSUBSCRIBE", pubsubExecutor);
        router.registerExecutor("PUBSUB", pubsubExecutor);
        router.registerExecutor("MULTI", transactionExecutor);
        router.registerExecutor("EXEC", transactionExecutor);
        router.registerExecutor("DISCARD", transactionExecutor);
        router.registerExecutor("WATCH", transactionExecutor);
        router.registerExecutor("UNWATCH", transactionExecutor);
        router.registerExecutor("XADD", streamExecutor);
        router.registerExecutor("XLEN", streamExecutor);
        router.registerExecutor("XRANGE", streamExecutor);
        router.registerExecutor("XREVRANGE", streamExecutor);
        router.registerExecutor("XREAD", streamExecutor);
        router.registerExecutor("XDEL", streamExecutor);
        router.registerExecutor("XTRIM", streamExecutor);
        router.registerExecutor("XGROUP", streamExecutor);
        router.registerExecutor("XREADGROUP", streamExecutor);
        router.registerExecutor("XACK", streamExecutor);
        router.registerExecutor("XPENDING", streamExecutor);
        router.registerExecutor("TYPE", typeExecutor);
        router.registerExecutor("FLUSHALL", databaseExecutor);
        router.registerExecutor("FLUSHDB", databaseExecutor);
        router.registerExecutor("EXPIRE", expirationExecutor);
        router.registerExecutor("EXPIREAT", expirationExecutor);
        router.registerExecutor("PEXPIRE", expirationExecutor);
        router.registerExecutor("TTL", expirationExecutor);
        router.registerExecutor("PTTL", expirationExecutor);
        router.registerExecutor("PERSIST", expirationExecutor);
        router.registerExecutor("AUTH", authExecutor);
    }
    
    private void startPgServer(PgServerConfig pgConfig) throws Exception {
        PgCompatServer pgServer = serverFactory.createPgServer();
        lifecycleManager.registerComponent(new ServerComponentAdapter(
                pgServer, "PostgreSQLServer"));
    }
    
    private void startKafkaServer() throws Exception {
        KafkaCompatServer kafkaServer = serverFactory.createKafkaServer();
        lifecycleManager.registerComponent(new ServerComponentAdapter(
                kafkaServer, "KafkaServer"));
    }
    
    private void applyOptionalConfig(Configuration config) {
        applyOptionalString(config, "remote.data.dir");
        applyOptionalString(config, "client.scanner.io.tmpdir");
        applyOptionalString(config, "client.remote-file.download-thread-num");
        applyOptionalString(config, "s3.endpoint");
        applyOptionalString(config, "s3.region");
        applyOptionalString(config, "s3.access-key", "s3.access.key");
        applyOptionalString(config, "s3.secret-key", "s3.secret.key");
        applyOptionalString(config, "s3.path.style.access", "s3.path-style-access");
        applyOptionalString(config, "s3.connection.ssl.enabled");

        applyOptionalClientFsString(config, "client.fs.s3.endpoint", "s3.endpoint");
        applyOptionalClientFsString(config, "client.fs.s3.region", "s3.region");
        applyOptionalClientFsString(config, "client.fs.s3.access-key", "s3.access-key", "s3.access.key");
        applyOptionalClientFsString(config, "client.fs.s3.secret-key", "s3.secret-key", "s3.secret.key");
        applyOptionalClientFsString(config, "client.fs.s3.path.style.access", "s3.path-style-access", "s3.path.style.access");
        applyOptionalClientFsString(config, "client.fs.s3.connection.ssl.enabled", "s3.connection.ssl.enabled");

        applyOptionalClientFsString(config, "fs.s3a.endpoint", "s3.endpoint");
        applyOptionalClientFsString(config, "fs.s3a.access.key", "s3.access-key", "s3.access.key");
        applyOptionalClientFsString(config, "fs.s3a.secret.key", "s3.secret-key", "s3.secret.key");
        applyOptionalClientFsString(config, "fs.s3a.path.style.access", "s3.path-style-access", "s3.path.style.access");
        applyOptionalClientFsString(config, "fs.s3a.connection.ssl.enabled", "s3.connection.ssl.enabled");
    }

    private void applyOptionalString(Configuration config, String key, String... altKeys) {
        String value = capeConfig.get(key, null, altKeys);
        if (value != null && !value.isBlank()) {
            config.setString(key, value);
        }
    }

    private void applyOptionalClientFsString(Configuration config, String clientFsKey, String... sourceKeys) {
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
    
    private String generateServerId(String bindAddress, int port) {
        try {
            String hostname = (bindAddress.equals("0.0.0.0") || bindAddress.equals("localhost"))
                    ? java.net.InetAddress.getLocalHost().getHostName() : bindAddress;
            return String.format("%s,%d,%d", hostname, port, System.currentTimeMillis());
        } catch (Exception e) {
            return String.format("localhost,%d,%d", port, System.currentTimeMillis());
        }
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
                LOG.error("  {}", arg);
            }
            LOG.error("Example startup command: java {} -jar fluss-cape.jar ...", 
                    String.join(" ", missingArgs));
            System.exit(1);
        }
    }
    
    private static class ServerComponentAdapter implements ServerComponent {
        private final AutoCloseable server;
        private final String name;
        private volatile boolean running = false;
        
        ServerComponentAdapter(AutoCloseable server, String name) {
            this.server = server;
            this.name = name;
        }
        
        @Override
        public void start() throws Exception {
            if (server instanceof org.gnuhpc.fluss.cape.common.server.ProtocolServer) {
                ((org.gnuhpc.fluss.cape.common.server.ProtocolServer) server).start();
            }
            running = true;
        }
        
        @Override
        public void close() throws Exception {
            running = false;
            server.close();
        }
        
        @Override
        public String getName() {
            return name;
        }
        
        @Override
        public boolean isRunning() {
            return running;
        }
    }
}
