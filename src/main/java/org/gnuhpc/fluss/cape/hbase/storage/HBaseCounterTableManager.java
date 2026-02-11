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

package org.gnuhpc.fluss.cape.hbase.storage;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager for HBase counter aggregation tables.
 *
 * <p>Uses Fluss aggregation engine (merge-engine=aggregation with SUM) to implement
 * atomic increment operations for HBase Increment command.
 *
 * <p>This follows the same pattern as Redis's RedisDynamicTableManager for atomic INCR/HINCRBY.
 *
 * <p><b>Counter Table Schema:</b>
 * <pre>
 * For each HBase table "{table_name}", a corresponding counter table is created:
 *   Table: {database}.{table_name}_incr_counters
 *   Schema: (row_key BYTES, column_key STRING, delta BIGINT) 
 *   PK: (row_key, column_key)
 *   Merge Engine: aggregation with SUM on delta
 * </pre>
 *
 * <p>The column_key format is: "{family}:{qualifier}"
 *
 * <p><b>Usage Pattern:</b>
 * <ol>
 *   <li>Increment: Upsert delta to counter table → Lookup aggregated sum → Sync to main table</li>
 *   <li>Delete: Clear counter entries for the row</li>
 *   <li>Put: Optionally seed counter with new value</li>
 * </ol>
 */
public class HBaseCounterTableManager {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseCounterTableManager.class);

    /** Suffix for counter table names */
    public static final String COUNTER_TABLE_SUFFIX = "_incr_counters";

    /**
     * Gets the counter table path for a given HBase table.
     *
     * @param database the database name
     * @param tableName the original HBase table name
     * @return TablePath for the counter table
     */
    public static TablePath getCounterTablePath(String database, String tableName) {
        return TablePath.of(database, tableName + COUNTER_TABLE_SUFFIX);
    }

    /**
     * Checks if a counter table exists for the given HBase table.
     *
     * @param admin Fluss admin client
     * @param database database name
     * @param tableName HBase table name
     * @return true if counter table exists
     * @throws Exception if check fails
     */
    public static boolean counterTableExists(Admin admin, String database, String tableName) 
            throws Exception {
        TablePath counterPath = getCounterTablePath(database, tableName);
        return admin.tableExists(counterPath).get();
    }

    /**
     * Ensures the counter aggregation table exists for the given HBase table.
     *
     * <p>Schema: (row_key BYTES, column_key STRING, delta BIGINT)
     * <ul>
     *   <li>row_key: The HBase row key (as bytes)</li>
     *   <li>column_key: Format "{family}:{qualifier}"</li>
     *   <li>delta: The increment value (with SUM aggregation)</li>
     * </ul>
     *
     * @param admin Fluss admin client
     * @param database database name
     * @param tableName HBase table name
     * @throws Exception if table creation fails
     */
    public static void ensureCounterTable(Admin admin, String database, String tableName) 
            throws Exception {
        TablePath counterPath = getCounterTablePath(database, tableName);

        if (admin.tableExists(counterPath).get()) {
            LOG.debug("HBase counter table '{}' already exists", counterPath);
            return;
        }

        LOG.info("Creating HBase counter aggregation table '{}'...", counterPath);

        Schema schema = Schema.newBuilder()
                .column("row_key", DataTypes.BYTES())
                .column("column_key", DataTypes.STRING())
                .column("delta", DataTypes.BIGINT(), AggFunctions.SUM())
                .primaryKey("row_key", "column_key")
                .build();

        TableDescriptor tableDescriptor = TableDescriptor.builder()
                .schema(schema)
                .property(ConfigOptions.TABLE_MERGE_ENGINE.key(), "aggregation")
                .build();

        admin.createTable(counterPath, tableDescriptor, false).get();
        LOG.info("Successfully created HBase counter aggregation table '{}'", counterPath);
    }

    /**
     * Drops the counter table for the given HBase table.
     *
     * @param admin Fluss admin client
     * @param database database name
     * @param tableName HBase table name
     * @throws Exception if drop fails
     */
    public static void dropCounterTable(Admin admin, String database, String tableName) 
            throws Exception {
        TablePath counterPath = getCounterTablePath(database, tableName);

        if (!admin.tableExists(counterPath).get()) {
            LOG.debug("HBase counter table '{}' does not exist, nothing to drop", counterPath);
            return;
        }

        LOG.info("Dropping HBase counter aggregation table '{}'...", counterPath);
        admin.dropTable(counterPath, false).get();
        LOG.info("Successfully dropped HBase counter aggregation table '{}'", counterPath);
    }

    /**
     * Builds the column_key string from family and qualifier.
     *
     * @param family column family
     * @param qualifier column qualifier
     * @return column key in format "family:qualifier"
     */
    public static String buildColumnKey(String family, String qualifier) {
        return family + ":" + qualifier;
    }

    /**
     * Parses family from column_key.
     *
     * @param columnKey column key in format "family:qualifier"
     * @return family name
     */
    public static String parseFamily(String columnKey) {
        int colonIndex = columnKey.indexOf(':');
        return colonIndex > 0 ? columnKey.substring(0, colonIndex) : columnKey;
    }

    /**
     * Parses qualifier from column_key.
     *
     * @param columnKey column key in format "family:qualifier"
     * @return qualifier name
     */
    public static String parseQualifier(String columnKey) {
        int colonIndex = columnKey.indexOf(':');
        return colonIndex >= 0 ? columnKey.substring(colonIndex + 1) : "";
    }
}
