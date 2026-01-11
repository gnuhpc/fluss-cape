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

package org.apache.fluss.hbase.server;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.hbase.executor.GetExecutor;
import org.apache.fluss.hbase.executor.GetTableNamesExecutor;
import org.apache.fluss.hbase.executor.HBaseRequestRouter;
import org.apache.fluss.hbase.executor.IsMasterRunningExecutor;
import org.apache.fluss.hbase.executor.MultiExecutor;
import org.apache.fluss.hbase.executor.PutExecutor;
import org.apache.fluss.hbase.executor.ScanExecutor;
import org.apache.fluss.hbase.mapping.CellConverter;
import org.apache.fluss.hbase.mapping.RowKeyEncoder;
import org.apache.fluss.hbase.metadata.MetaTableEmulator;
import org.apache.fluss.hbase.metadata.VirtualRegionManager;
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
 * -Dhbase.compat.bind.address=localhost \ -Dhbase.compat.bind.port=16020 \
 * -Dhbase.compat.tables=db.table1,db.table2
 */
public class HBaseCompatServerLauncher {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseCompatServerLauncher.class);

    private static ZooKeeper zooKeeper;
    private static String regionServerZnodePath;
    private static String metaRegionServerZnodePath;
    private static HealthCheckServer healthCheckServer;

    public static void main(String[] args) throws Exception {
        // Parse configuration from system properties
        String flussBootstrapServers =
                System.getProperty("fluss.bootstrap.servers", "localhost:9123");
        String bindAddress = System.getProperty("hbase.compat.bind.address", "0.0.0.0");
        int bindPort = Integer.parseInt(System.getProperty("hbase.compat.bind.port", "16020"));
        String tablesList = System.getProperty("hbase.compat.tables", "");
        String zkQuorum = System.getProperty("hbase.zookeeper.quorum", "localhost:2181");
        String serverId = System.getProperty("server.id", generateServerId(bindAddress, bindPort));
        int healthCheckPort = Integer.parseInt(System.getProperty("health.check.port", "8080"));

        boolean enableRedis =
                Boolean.parseBoolean(System.getProperty("redis.enable", "true"));
        String redisBindAddress = System.getProperty("redis.bind.address", "0.0.0.0");
        int redisPort = Integer.parseInt(System.getProperty("redis.bind.port", "6379"));
        String redisTableName = System.getProperty("redis.table.name", "default.redis_data");

        LOG.info("========================================");
        LOG.info("Starting HBase Compatibility Server");
        LOG.info("========================================");
        LOG.info("Server ID: {}", serverId);
        LOG.info("Fluss cluster: {}", flussBootstrapServers);
        LOG.info("Bind address: {}:{}", bindAddress, bindPort);
        LOG.info("ZooKeeper: {}", zkQuorum);
        LOG.info("Tables: {}", tablesList.isEmpty() ? "none (meta only)" : tablesList);
        LOG.info("Redis enabled: {}", enableRedis);
        if (enableRedis) {
            LOG.info("Redis address: {}:{}", redisBindAddress, redisPort);
            LOG.info("Redis table: {}", redisTableName);
        }
        LOG.info("========================================");

        // Create Fluss connection
        Configuration config = new Configuration();
        config.setString("bootstrap.servers", flussBootstrapServers);
        Connection flussConn = ConnectionFactory.createConnection(config);
        Admin admin = flussConn.getAdmin();

        // Setup virtual region manager
        VirtualRegionManager regionManager = new VirtualRegionManager(bindAddress, bindPort);

        // Create router and register executors
        HBaseRequestRouter router = new HBaseRequestRouter();

        router.registerExecutor("IsMasterRunning", new IsMasterRunningExecutor());
        LOG.info("Registered IsMasterRunning executor");

        router.registerExecutor("GetTableNames", new GetTableNamesExecutor(admin));
        LOG.info("Registered GetTableNames executor");

        MetaTableEmulator metaEmulator = new MetaTableEmulator(regionManager);
        router.registerExecutor("Get-hbase:meta", metaEmulator);
        router.registerExecutor("Scan-hbase:meta", metaEmulator);
        router.registerExecutor("Multi-hbase:meta", metaEmulator);
        router.registerExecutor("Mutate-hbase:meta", metaEmulator);
        LOG.info("Registered meta table emulator for hbase:meta (Get, Scan, Multi, Mutate)");

        // Register user tables if specified
        if (!tablesList.isEmpty()) {
            String[] tables = tablesList.split(",");
            for (String tablePathStr : tables) {
                tablePathStr = tablePathStr.trim();
                if (tablePathStr.isEmpty()) {
                    continue;
                }

                try {
                    TablePath tablePath = parseTablePath(tablePathStr);
                    registerTable(flussConn, admin, router, regionManager, tablePath, bindAddress);
                } catch (Exception e) {
                    LOG.error("Failed to register table: {}", tablePathStr, e);
                    throw e;
                }
            }
        } else {
            LOG.warn(
                    "No tables configured. Only hbase:meta will be available. "
                            + "Set -Dhbase.compat.tables=db.table1,db.table2 to expose Fluss tables.");
        }

        // Start server
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
            flussConn.close();
            throw e;
        }

        try {
            healthCheckServer = new HealthCheckServer(healthCheckPort, flussConn, zooKeeper);
            healthCheckServer.start();
            LOG.info("Health check server started on port {}", healthCheckPort);
        } catch (Exception e) {
            LOG.error("Failed to start health check server", e);
            unregisterFromZooKeeper();
            server.close();
            flussConn.close();
            throw e;
        }

        org.apache.fluss.redis.server.RedisCompatServer redisServer = null;
        if (enableRedis) {
            try {
                TablePath redisTablePath = parseTablePath(redisTableName);

                org.apache.fluss.redis.storage.RedisFlussAdapter redisAdapter =
                        new org.apache.fluss.redis.storage.RedisFlussAdapter(
                                flussConn, redisTablePath);

                org.apache.fluss.redis.executor.RedisCommandRouter redisRouter =
                        new org.apache.fluss.redis.executor.RedisCommandRouter();

                org.apache.fluss.redis.executor.StringCommandExecutor stringExecutor =
                        new org.apache.fluss.redis.executor.StringCommandExecutor(redisAdapter);
                org.apache.fluss.redis.executor.HashCommandExecutor hashExecutor =
                        new org.apache.fluss.redis.executor.HashCommandExecutor(redisAdapter);
                org.apache.fluss.redis.executor.SetCommandExecutor setExecutor =
                        new org.apache.fluss.redis.executor.SetCommandExecutor(redisAdapter);
                org.apache.fluss.redis.executor.TypeCommandExecutor typeExecutor =
                        new org.apache.fluss.redis.executor.TypeCommandExecutor(redisAdapter);
                org.apache.fluss.redis.executor.ListCommandExecutor listExecutor =
                        new org.apache.fluss.redis.executor.ListCommandExecutor(redisAdapter);
                org.apache.fluss.redis.executor.SortedSetCommandExecutor zsetExecutor =
                        new org.apache.fluss.redis.executor.SortedSetCommandExecutor(
                                redisAdapter, flussConn, "redis_zset_members");
                org.apache.fluss.redis.executor.ExpirationCommandExecutor expirationExecutor =
                        new org.apache.fluss.redis.executor.ExpirationCommandExecutor(redisAdapter);

                redisRouter.registerExecutor("GET", stringExecutor);
                redisRouter.registerExecutor("SET", stringExecutor);
                redisRouter.registerExecutor("DEL", stringExecutor);
                redisRouter.registerExecutor("EXISTS", stringExecutor);
                redisRouter.registerExecutor("INCR", stringExecutor);
                redisRouter.registerExecutor("INCRBY", stringExecutor);
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

                redisRouter.registerExecutor("SADD", setExecutor);
                redisRouter.registerExecutor("SREM", setExecutor);
                redisRouter.registerExecutor("SISMEMBER", setExecutor);
                redisRouter.registerExecutor("SMEMBERS", setExecutor);
                redisRouter.registerExecutor("SCARD", setExecutor);
                redisRouter.registerExecutor("SMOVE", setExecutor);

                redisRouter.registerExecutor("LPUSH", listExecutor);
                redisRouter.registerExecutor("RPUSH", listExecutor);
                redisRouter.registerExecutor("LPOP", listExecutor);
                redisRouter.registerExecutor("RPOP", listExecutor);
                redisRouter.registerExecutor("LRANGE", listExecutor);
                redisRouter.registerExecutor("LLEN", listExecutor);
                redisRouter.registerExecutor("LINDEX", listExecutor);

                redisRouter.registerExecutor("ZADD", zsetExecutor);
                redisRouter.registerExecutor("ZREM", zsetExecutor);
                redisRouter.registerExecutor("ZRANGE", zsetExecutor);
                redisRouter.registerExecutor("ZSCORE", zsetExecutor);
                redisRouter.registerExecutor("ZCARD", zsetExecutor);
                redisRouter.registerExecutor("ZRANGEBYSCORE", zsetExecutor);

                redisRouter.registerExecutor("TYPE", typeExecutor);
                
                redisRouter.registerExecutor("EXPIRE", expirationExecutor);
                redisRouter.registerExecutor("EXPIREAT", expirationExecutor);
                redisRouter.registerExecutor("PEXPIRE", expirationExecutor);
                redisRouter.registerExecutor("TTL", expirationExecutor);
                redisRouter.registerExecutor("PTTL", expirationExecutor);
                redisRouter.registerExecutor("PERSIST", expirationExecutor);

                redisServer =
                        new org.apache.fluss.redis.server.RedisCompatServer(
                                redisBindAddress, redisPort, redisRouter);
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
                flussConn.close();
                throw e;
            }
        }

        final org.apache.fluss.redis.server.RedisCompatServer finalRedisServer = redisServer;

        LOG.info("========================================");
        LOG.info("HBase Compatibility Server started!");
        LOG.info("========================================");
        LOG.info("HBase clients can connect to: {}:{}", hostname, actualPort);
        LOG.info("Connection string: {}:{}", hostname, actualPort);
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
                                        if (finalRedisServer != null) {
                                            finalRedisServer.close();
                                        }
                                        if (healthCheckServer != null) {
                                            healthCheckServer.close();
                                        }
                                        server.close();
                                        flussConn.close();
                                    } catch (Exception e) {
                                        LOG.error("Error during shutdown", e);
                                    }
                                    shutdownLatch.countDown();
                                }));

        // Keep running
        shutdownLatch.await();
        LOG.info("HBase Compatibility Server stopped");
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

    private static void registerTable(
            Connection flussConn,
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
        Table table = flussConn.getTable(tablePath);

        // Register all executors for this table
        String tableName = tablePath.getTableName();

        router.registerExecutor(
                "Get-" + tableName,
                new GetExecutor(flussConn, tablePath, rowKeyEncoder, cellConverter));

        router.registerExecutor(
                "Mutate-" + tableName,
                new PutExecutor(flussConn, tablePath, rowType, rowKeyEncoder, cellConverter));

        router.registerExecutor(
                "Scan-" + tableName,
                new ScanExecutor(flussConn, tablePath, rowKeyEncoder, cellConverter));

        router.registerExecutor(
                "Multi-" + tableName,
                new MultiExecutor(flussConn, tablePath, rowType, rowKeyEncoder, cellConverter));

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
            LOG.info("Master node already exists (another server is master), updating data");
            try {
                zooKeeper.setData(masterPath, result, -1);
                LOG.info("Updated master node data (server: {})", serverId);
            } catch (KeeperException ex) {
                LOG.warn(
                        "Failed to update master node, another server likely holds it: {}",
                        ex.getMessage());
            }
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
