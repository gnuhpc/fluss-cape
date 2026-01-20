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

package org.gnuhpc.fluss.cape.redis.sharding;

import org.apache.fluss.metadata.TablePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisSlotRouter {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSlotRouter.class);

    private static final int REDIS_CLUSTER_SLOTS = 16384;
    
    private final int numberOfShards;
    private final int slotsPerShard;
    private final String database;
    private final boolean hashTagEnabled;

    public RedisSlotRouter(String database, int numberOfShards) {
        this(database, numberOfShards, true);
    }

    public RedisSlotRouter(String database, int numberOfShards, boolean hashTagEnabled) {
        if (numberOfShards <= 0 || numberOfShards > REDIS_CLUSTER_SLOTS) {
            throw new IllegalArgumentException(
                    "Number of shards must be between 1 and " + REDIS_CLUSTER_SLOTS);
        }
        this.database = database;
        this.numberOfShards = numberOfShards;
        this.slotsPerShard = REDIS_CLUSTER_SLOTS / numberOfShards;
        this.hashTagEnabled = hashTagEnabled;
        
        LOG.info("Initialized RedisSlotRouter: database={}, shards={}, slotsPerShard={}, hashTag={}", 
                 database, numberOfShards, slotsPerShard, hashTagEnabled);
    }

    public TablePath route(String redisKey) {
        int slot = calculateSlot(redisKey);
        int shardIndex = slot / slotsPerShard;
        String tableName = String.format("redis_shard_%02d", shardIndex);
        return TablePath.of(database, tableName);
    }

    public int calculateSlot(String redisKey) {
        String hashKey = extractHashKey(redisKey);
        return CRC16.hash(hashKey) % REDIS_CLUSTER_SLOTS;
    }

    private String extractHashKey(String redisKey) {
        if (!hashTagEnabled) {
            return redisKey;
        }

        int start = redisKey.indexOf('{');
        if (start == -1) {
            return redisKey;
        }

        int end = redisKey.indexOf('}', start + 1);
        if (end == -1 || end == start + 1) {
            return redisKey;
        }

        return redisKey.substring(start + 1, end);
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }

    public TablePath getShardTablePath(int shardIndex) {
        if (shardIndex < 0 || shardIndex >= numberOfShards) {
            throw new IllegalArgumentException(
                    "Shard index must be between 0 and " + (numberOfShards - 1));
        }
        String tableName = String.format("redis_shard_%02d", shardIndex);
        return TablePath.of(database, tableName);
    }

    public boolean isHashTagEnabled() {
        return hashTagEnabled;
    }
}
