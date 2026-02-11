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
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.gnuhpc.fluss.cape.common.configuration.CapeConfig;
import org.gnuhpc.fluss.cape.hbase.server.HBaseCompatServer;
import org.gnuhpc.fluss.cape.hbase.server.HealthCheckServer;
import org.gnuhpc.fluss.cape.hbase.executor.HBaseRequestRouter;
import org.gnuhpc.fluss.cape.kafka.config.KafkaCompatConfig;
import org.gnuhpc.fluss.cape.kafka.server.KafkaCompatServer;
import org.gnuhpc.fluss.cape.pg.server.PgAuthConfig;
import org.gnuhpc.fluss.cape.pg.server.PgCompatServer;
import org.gnuhpc.fluss.cape.pg.server.PgServerConfig;
import org.gnuhpc.fluss.cape.redis.executor.RedisCommandRouter;
import org.gnuhpc.fluss.cape.redis.server.RedisCompatServer;
import org.gnuhpc.fluss.cape.redis.storage.RedisStorageAdapter;
import org.gnuhpc.fluss.cape.redis.storage.RedisDynamicTableManager;
import org.gnuhpc.fluss.cape.redis.storage.RedisShardedAdapter;
import org.gnuhpc.fluss.cape.redis.storage.RedisSingleTableAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtocolServerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ProtocolServerFactory.class);
    
    private final CapeConfig capeConfig;
    private final Connection flussConnection;
    private final Admin flussAdmin;
    private final Configuration flussConfig;
    
    public ProtocolServerFactory(CapeConfig capeConfig, Connection flussConnection, 
                                Admin flussAdmin, Configuration flussConfig) {
        this.capeConfig = capeConfig;
        this.flussConnection = flussConnection;
        this.flussAdmin = flussAdmin;
        this.flussConfig = flussConfig;
    }
    
    public HBaseCompatServer createHBaseServer(HBaseRequestRouter router) throws Exception {
        String bindAddress = capeConfig.getBindAddress();
        int bindPort = capeConfig.getBindPort();
        
        HBaseCompatServer server = new HBaseCompatServer(flussConfig, bindAddress, bindPort, router);
        LOG.info("Created HBase server: {}:{}", bindAddress, bindPort);
        return server;
    }
    
    public RedisCompatServer createRedisServer(RedisCommandRouter router) throws Exception {
        String redisBindAddress = capeConfig.getRedisBindAddress();
        int redisPort = capeConfig.getRedisPort();
        
        org.gnuhpc.fluss.cape.redis.executor.AuthCommandExecutor authExecutor =
                (org.gnuhpc.fluss.cape.redis.executor.AuthCommandExecutor) router.getExecutor("AUTH");
        
        org.gnuhpc.fluss.cape.redis.server.RedisSslContextProvider sslProvider =
                org.gnuhpc.fluss.cape.redis.server.RedisSslContextProvider.create();
        
        RedisCompatServer server = new RedisCompatServer(
                redisBindAddress, redisPort, router, 
                authExecutor.isAuthenticationEnabled(), sslProvider);
        
        LOG.info("Created Redis server: {}:{}", redisBindAddress, redisPort);
        return server;
    }
    
    public PgCompatServer createPgServer() throws Exception {
        PgServerConfig pgConfig = PgServerConfig.fromCapeConfig(capeConfig);
        
        PgAuthConfig pgAuthConfig = new PgAuthConfig(
                pgConfig.getAuthMode(),
                pgConfig.getAuthUser(),
                pgConfig.getAuthPassword());
        
        PgCompatServer server = new PgCompatServer(
                pgConfig.getBindAddress(),
                pgConfig.getPort(),
                pgAuthConfig,
                pgConfig.getDatabase(),
                flussConnection,
                flussAdmin);
        
        LOG.info("Created PostgreSQL server: {}:{}", 
                pgConfig.getBindAddress(), pgConfig.getPort());
        return server;
    }
    
    public KafkaCompatServer createKafkaServer() throws Exception {
        String kafkaBindAddress = capeConfig.getKafkaBindAddress();
        int kafkaPort = capeConfig.getKafkaPort();
        String kafkaDefaultDatabase = capeConfig.getKafkaDefaultDatabase();
        
        KafkaCompatConfig kafkaConfig = KafkaCompatConfig.builder()
                .host(kafkaBindAddress)
                .port(kafkaPort)
                .nodeId(0)
                .defaultDatabase(kafkaDefaultDatabase)
                .autoCreateTables(true)
                .defaultNumBuckets(3)
                .build();
        
        KafkaCompatServer server = new KafkaCompatServer(flussConnection, kafkaConfig);
        LOG.info("Created Kafka server: {}:{}", kafkaBindAddress, kafkaPort);
        return server;
    }
    
    public HealthCheckServer createHealthCheckServer(org.apache.zookeeper.ZooKeeper zooKeeper) throws Exception {
        int healthCheckPort = capeConfig.getHealthCheckPort();
        HealthCheckServer server = new HealthCheckServer(healthCheckPort, flussConnection, zooKeeper);
        LOG.info("Created health check server on port: {}", healthCheckPort);
        return server;
    }
    
    public RedisStorageAdapter createRedisStorageAdapter(String database) throws Exception {
        boolean shardingEnabled = capeConfig.isRedisShardingEnabled();
        int shards = capeConfig.getRedisShards();
        
        RedisStorageAdapter adapter;
        if (shardingEnabled) {
            RedisDynamicTableManager.ensureAllShardedTables(flussAdmin, database, shards);
            adapter = new RedisShardedAdapter(flussConnection, database, shards);
            LOG.info("Created Redis sharded adapter: {} shards", shards);
        } else {
            RedisDynamicTableManager.ensureAllTables(flussAdmin, database);
            adapter = new RedisSingleTableAdapter(flussConnection, database);
            LOG.info("Created Redis single-table adapter");
        }
        
        return adapter;
    }
}
