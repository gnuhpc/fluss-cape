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

package org.gnuhpc.fluss.cape.redis.storage;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager for Redis internal tables with `redis_internal_` prefix.
 *
 * <p>Handles dynamic creation of Redis storage tables on-demand without requiring
 * pre-configuration. All internal tables use `redis_internal_` prefix to distinguish them from
 * user tables.
 */
public class RedisDynamicTableManager {

    private static final Logger LOG = LoggerFactory.getLogger(RedisDynamicTableManager.class);

    // Internal table names (with __ prefix)
    private static final String MAIN_TABLE = "redis_internal_data";
    private static final String ZSET_INDEX_TABLE = "redis_internal_zset_index";
    private static final String SUBKEY_INDEX_TABLE = "redis_internal_subkey_index";

    /**
     * Ensures the main Redis data table exists, creating it if needed.
     *
     * <p>Schema: (redis_key STRING, redis_type STRING, sub_key STRING, score DOUBLE, value BYTES)
     * PK: (redis_key, sub_key)
     *
     * @param admin Fluss admin client
     * @param database database name (e.g., "default")
     * @throws Exception if table creation fails
     */
    public static void ensureMainTable(Admin admin, String database) throws Exception {
        TablePath tablePath = getMainTablePath(database);

        if (admin.tableExists(tablePath).get()) {
            LOG.debug("Redis main table '{}' already exists", tablePath);
            return;
        }

        LOG.info("Creating Redis main table '{}'...", tablePath);

        Schema schema =
                Schema.newBuilder()
                        .column("redis_key", DataTypes.STRING())
                        .column("redis_type", DataTypes.STRING())
                        .column("sub_key", DataTypes.STRING())
                        .column("score", DataTypes.DOUBLE())
                        .column("value", DataTypes.BYTES())
                        .primaryKey("redis_key", "sub_key")
                        .build();

        TableDescriptor tableDescriptor = TableDescriptor.builder().schema(schema).build();

        admin.createTable(tablePath, tableDescriptor, false).get();
        LOG.info("Successfully created Redis main table '{}'", tablePath);
    }

    /**
     * Ensures the sorted set reverse index table exists, creating it if needed.
     *
     * <p>Schema: (redis_key STRING, member STRING, score DOUBLE) PK: (redis_key, member)
     *
     * @param admin Fluss admin client
     * @param database database name
     * @throws Exception if table creation fails
     */
    public static void ensureZsetIndexTable(Admin admin, String database) throws Exception {
        TablePath tablePath = getZsetIndexTablePath(database);

        if (admin.tableExists(tablePath).get()) {
            LOG.debug("Redis zset index table '{}' already exists", tablePath);
            return;
        }

        LOG.info("Creating Redis zset index table '{}'...", tablePath);

        Schema schema =
                Schema.newBuilder()
                        .column("redis_key", DataTypes.STRING())
                        .column("member", DataTypes.STRING())
                        .column("score", DataTypes.DOUBLE())
                        .primaryKey("redis_key", "member")
                        .build();

        TableDescriptor tableDescriptor = TableDescriptor.builder().schema(schema).build();

        admin.createTable(tablePath, tableDescriptor, false).get();
        LOG.info("Successfully created Redis zset index table '{}'", tablePath);
    }

    /**
     * Ensures the sub-key index table exists, creating it if needed.
     *
     * <p>Schema: (redis_key STRING, sub_keys STRING) PK: (redis_key)
     *
     * @param admin Fluss admin client
     * @param database database name
     * @throws Exception if table creation fails
     */
    public static void ensureSubkeyIndexTable(Admin admin, String database) throws Exception {
        TablePath tablePath = getSubkeyIndexTablePath(database);

        if (admin.tableExists(tablePath).get()) {
            LOG.debug("Redis subkey index table '{}' already exists", tablePath);
            return;
        }

        LOG.info("Creating Redis subkey index table '{}'...", tablePath);

        Schema schema =
                Schema.newBuilder()
                        .column("redis_key", DataTypes.STRING())
                        .column("sub_keys", DataTypes.STRING())
                        .primaryKey("redis_key")
                        .build();

        TableDescriptor tableDescriptor = TableDescriptor.builder().schema(schema).build();

        admin.createTable(tablePath, tableDescriptor, false).get();
        LOG.info("Successfully created Redis subkey index table '{}'", tablePath);
    }

    /**
     * Ensures all Redis internal tables exist.
     *
     * @param admin Fluss admin client
     * @param database database name
     * @throws Exception if any table creation fails
     */
    public static void ensureAllTables(Admin admin, String database) throws Exception {
        LOG.info("Initializing Redis internal tables in database '{}'", database);
        ensureMainTable(admin, database);
        ensureSubkeyIndexTable(admin, database);
        ensureZsetIndexTable(admin, database);
        LOG.info("Redis internal tables initialization complete");
    }

    /** Returns the TablePath for the main Redis data table. */
    public static TablePath getMainTablePath(String database) {
        return TablePath.of(database, MAIN_TABLE);
    }

    /** Returns the TablePath for the sorted set reverse index table. */
    public static TablePath getZsetIndexTablePath(String database) {
        return TablePath.of(database, ZSET_INDEX_TABLE);
    }

    /** Returns the TablePath for the sub-key index table. */
    public static TablePath getSubkeyIndexTablePath(String database) {
        return TablePath.of(database, SUBKEY_INDEX_TABLE);
    }

    /** Returns the main table name. */
    public static String getMainTableName() {
        return MAIN_TABLE;
    }

    /** Returns the zset index table name. */
    public static String getZsetIndexTableName() {
        return ZSET_INDEX_TABLE;
    }

    /** Returns the subkey index table name. */
    public static String getSubkeyIndexTableName() {
        return SUBKEY_INDEX_TABLE;
    }

    public static void ensureShardedTable(Admin admin, TablePath tablePath) throws Exception {
        if (admin.tableExists(tablePath).get()) {
            LOG.debug("Sharded table '{}' already exists", tablePath);
            return;
        }

        LOG.info("Creating sharded Redis table '{}'...", tablePath);

        Schema schema =
                Schema.newBuilder()
                        .column("redis_key", DataTypes.STRING())
                        .column("redis_type", DataTypes.STRING())
                        .column("sub_key", DataTypes.STRING())
                        .column("score", DataTypes.DOUBLE())
                        .column("value", DataTypes.BYTES())
                        .primaryKey("redis_key", "sub_key")
                        .build();

        TableDescriptor tableDescriptor = TableDescriptor.builder().schema(schema).build();

        admin.createTable(tablePath, tableDescriptor, false).get();
        LOG.info("Successfully created sharded Redis table '{}'", tablePath);
    }

    public static void ensureAllShardedTables(Admin admin, String database, int numberOfShards)
            throws Exception {
        LOG.info(
                "Initializing {} sharded Redis tables in database '{}'",
                numberOfShards,
                database);

        for (int i = 0; i < numberOfShards; i++) {
            String tableName = String.format("redis_shard_%02d", i);
            TablePath tablePath = TablePath.of(database, tableName);
            ensureShardedTable(admin, tablePath);
        }

        ensureSubkeyIndexTable(admin, database);
        ensureZsetIndexTable(admin, database);

        LOG.info(
                "Successfully initialized {} sharded tables + 2 index tables", numberOfShards);
    }

    // ============================================================
    // Stream Sharding Support
    // ============================================================

    /**
     * Ensures a single stream shard table exists, creating it if needed.
     *
     * <p>Schema: (stream_key STRING, entry_id STRING, fields BYTES) 
     * PK: (stream_key, entry_id)
     *
     * @param admin Fluss admin client
     * @param tablePath table path for the shard
     * @throws Exception if table creation fails
     */
    public static void ensureStreamShardTable(Admin admin, TablePath tablePath) throws Exception {
        if (admin.tableExists(tablePath).get()) {
            LOG.debug("Stream shard table '{}' already exists", tablePath);
            return;
        }

        LOG.info("Creating stream shard table '{}'...", tablePath);

        Schema schema =
                Schema.newBuilder()
                        .column("stream_key", DataTypes.STRING())
                        .column("entry_id", DataTypes.STRING())
                        .column("fields", DataTypes.BYTES())
                        .primaryKey("stream_key", "entry_id")
                        .build();

        TableDescriptor tableDescriptor = TableDescriptor.builder().schema(schema).build();

        admin.createTable(tablePath, tableDescriptor, false).get();
        LOG.info("Successfully created stream shard table '{}'", tablePath);
    }

    /**
     * Ensures all stream shard tables exist.
     *
     * @param admin Fluss admin client
     * @param database database name
     * @param numberOfShards number of shards to create
     * @throws Exception if table creation fails
     */
    public static void ensureAllStreamShardTables(Admin admin, String database, int numberOfShards)
            throws Exception {
        LOG.info(
                "Initializing {} stream shard tables in database '{}'",
                numberOfShards,
                database);

        for (int i = 0; i < numberOfShards; i++) {
            String tableName = String.format("redis_stream_shard_%02d", i);
            TablePath tablePath = TablePath.of(database, tableName);
            ensureStreamShardTable(admin, tablePath);
        }

        // Create shared metadata tables (not sharded)
        ensureStreamMetadataTables(admin, database);

        LOG.info(
                "Successfully initialized {} stream shard tables + metadata tables",
                numberOfShards);
    }

    /**
     * Ensures stream metadata tables exist (consumer groups, pending entries, tombstones).
     * These tables are shared across all shards.
     *
     * @param admin Fluss admin client
     * @param database database name
     * @throws Exception if table creation fails
     */
    public static void ensureStreamMetadataTables(Admin admin, String database) throws Exception {
        // Consumer groups table
        TablePath consumerGroupsPath = TablePath.of(database, "redis_stream_consumer_groups");
        if (!admin.tableExists(consumerGroupsPath).get()) {
            LOG.info("Creating stream consumer groups table '{}'...", consumerGroupsPath);
            Schema schema =
                    Schema.newBuilder()
                            .column("stream_key", DataTypes.STRING())
                            .column("group_name", DataTypes.STRING())
                            .column("last_delivered_id", DataTypes.STRING())
                            .column("created_timestamp", DataTypes.BIGINT())
                            .primaryKey("stream_key", "group_name")
                            .build();
            TableDescriptor tableDescriptor = TableDescriptor.builder().schema(schema).build();
            admin.createTable(consumerGroupsPath, tableDescriptor, false).get();
            LOG.info("Successfully created stream consumer groups table");
        }

        // Pending entries table
        TablePath pendingEntriesPath = TablePath.of(database, "redis_stream_pending_entries");
        if (!admin.tableExists(pendingEntriesPath).get()) {
            LOG.info("Creating stream pending entries table '{}'...", pendingEntriesPath);
            Schema schema =
                    Schema.newBuilder()
                            .column("stream_key", DataTypes.STRING())
                            .column("group_name", DataTypes.STRING())
                            .column("entry_id", DataTypes.STRING())
                            .column("consumer_name", DataTypes.STRING())
                            .column("delivery_timestamp", DataTypes.BIGINT())
                            .primaryKey("stream_key", "group_name", "entry_id")
                            .build();
            TableDescriptor tableDescriptor = TableDescriptor.builder().schema(schema).build();
            admin.createTable(pendingEntriesPath, tableDescriptor, false).get();
            LOG.info("Successfully created stream pending entries table");
        }

        // Tombstones table (for XDEL)
        TablePath tombstonesPath = TablePath.of(database, "redis_stream_tombstones");
        if (!admin.tableExists(tombstonesPath).get()) {
            LOG.info("Creating stream tombstones table '{}'...", tombstonesPath);
            Schema schema =
                    Schema.newBuilder()
                            .column("stream_key", DataTypes.STRING())
                            .column("entry_id", DataTypes.STRING())
                            .column("deleted", DataTypes.BOOLEAN())
                            .column("delete_timestamp", DataTypes.BIGINT())
                            .primaryKey("stream_key", "entry_id")
                            .build();
            TableDescriptor tableDescriptor = TableDescriptor.builder().schema(schema).build();
            admin.createTable(tombstonesPath, tableDescriptor, false).get();
            LOG.info("Successfully created stream tombstones table");
        }
    }

    /**
     * Ensures the legacy single-table stream table exists (for backward compatibility).
     *
     * @param admin Fluss admin client
     * @param database database name
     * @throws Exception if table creation fails
     */
    public static void ensureLegacyStreamTable(Admin admin, String database) throws Exception {
        TablePath tablePath = TablePath.of(database, "redis_stream_data");

        if (admin.tableExists(tablePath).get()) {
            LOG.debug("Legacy stream table '{}' already exists", tablePath);
            return;
        }

        LOG.info("Creating legacy stream table '{}'...", tablePath);

        Schema schema =
                Schema.newBuilder()
                        .column("stream_key", DataTypes.STRING())
                        .column("entry_id", DataTypes.STRING())
                        .column("fields", DataTypes.BYTES())
                        .primaryKey("stream_key", "entry_id")
                        .build();

        TableDescriptor tableDescriptor = TableDescriptor.builder().schema(schema).build();

        admin.createTable(tablePath, tableDescriptor, false).get();
        LOG.info("Successfully created legacy stream table '{}'", tablePath);

        // Also create metadata tables
        ensureStreamMetadataTables(admin, database);
    }
}
