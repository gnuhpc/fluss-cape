/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gnuhpc.fluss.cape.redis.storage;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Reverse index for Sorted Set operations to enable O(1) member→score lookups.
 *
 * <p>This class manages a separate Fluss table that stores member-to-score mappings,
 * enabling O(1) ZSCORE and ZREM operations instead of O(n) full scans.
 *
 * <p>Table schema:
 * <pre>
 * CREATE TABLE redis_zset_members (
 *   redis_key STRING,
 *   member STRING,
 *   score DOUBLE,
 *   PRIMARY KEY (redis_key, member)
 * );
 * </pre>
 *
 * <p>Usage pattern:
 * <ul>
 *   <li>ZADD: Write to both main table + reverse index</li>
 *   <li>ZREM: Delete from both tables</li>
 *   <li>ZSCORE: Read from reverse index only (O(1))</li>
 *   <li>ZRANGE: Read from main table only (score-sorted)</li>
 * </ul>
 */
public class ZSetReverseIndex {

    private static final Logger LOG = LoggerFactory.getLogger(ZSetReverseIndex.class);

    private final Table table;
    private final UpsertWriter upsertWriter;
    private final Lookuper lookuper;

    /**
     * Creates a new reverse index manager.
     *
     * @param connection Fluss connection
     * @param tablePath Table path for reverse index (e.g., "default.redis_zset_members")
     * @throws Exception if table initialization fails
     */
    public ZSetReverseIndex(Connection connection, TablePath tablePath) throws Exception {
        this.table = connection.getTable(tablePath);
        this.upsertWriter = table.newUpsert().createWriter();
        this.lookuper = table.newLookup().createLookuper();

        LOG.info(
                "Initialized ZSetReverseIndex with table: {}",
                tablePath.getDatabaseName() + "." + tablePath.getTableName());
    }

    /**
     * Get the score for a member in a sorted set.
     *
     * <p>This is an O(1) operation using the reverse index.
     *
     * @param redisKey Redis key (e.g., "myzset")
     * @param member Member name
     * @return Score if member exists, null otherwise
     * @throws Exception if lookup fails
     */
    public Double getMemberScore(String redisKey, String member) throws Exception {
        GenericRow keyRow = GenericRow.of(
                BinaryString.fromString(redisKey), 
                BinaryString.fromString(member));

        List<InternalRow> results = lookuper.lookup(keyRow).get().getRowList();

        if (results == null || results.isEmpty()) {
            return null;
        }

        InternalRow row = results.get(0);

        // Column 2: score (redis_key, member, score)
        if (row.isNullAt(2)) {
            return null;
        }

        return row.getDouble(2);
    }

    /**
     * Set or update the score for a member.
     *
     * <p>This is an upsert operation - inserts new member or updates existing score.
     *
     * @param redisKey Redis key
     * @param member Member name
     * @param score New score
     * @throws Exception if write fails
     */
    public void setMemberScore(String redisKey, String member, double score) throws Exception {
        GenericRow row = GenericRow.of(
                BinaryString.fromString(redisKey), 
                BinaryString.fromString(member), 
                score);
        upsertWriter.upsert(row).get();
    }

    /**
     * Delete a member from the reverse index.
     *
     * @param redisKey Redis key
     * @param member Member name
     * @throws Exception if delete fails
     */
    public void deleteMember(String redisKey, String member) throws Exception {
        GenericRow keyRow = GenericRow.of(
                BinaryString.fromString(redisKey), 
                BinaryString.fromString(member),
                null);  // score column (set to null for delete)
        upsertWriter.delete(keyRow).get();
    }

    /**
     * Check if a member exists in the sorted set.
     *
     * <p>This is an O(1) operation using the reverse index.
     *
     * @param redisKey Redis key
     * @param member Member name
     * @return true if member exists, false otherwise
     * @throws Exception if lookup fails
     */
    public boolean memberExists(String redisKey, String member) throws Exception {
        return getMemberScore(redisKey, member) != null;
    }

    /**
     * Delete all members for a given Redis key.
     *
     * <p>This is used when deleting the entire sorted set.
     * Note: This requires scanning all members for the key (no efficient bulk delete in Fluss).
     *
     * @param redisKey Redis key
     * @throws Exception if delete fails
     */
    public void deleteAllMembers(String redisKey) throws Exception {
        // TODO: Implement bulk delete if Fluss provides efficient API
        // For now, this would require scanning and deleting individually
        LOG.warn(
                "deleteAllMembers() not yet implemented efficiently for key: {}. "
                        + "Consider using individual deleteMember() calls.",
                redisKey);
    }

    /**
     * Close the reverse index and release resources.
     *
     * @throws Exception if close fails
     */
    public void close() throws Exception {
        LOG.info("Closed ZSetReverseIndex");
    }
}
