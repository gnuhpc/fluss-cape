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

import java.util.List;
import java.util.Set;

/**
 * Common interface for Redis storage adapters.
 * Provides abstraction over different storage implementations (single-table vs sharded).
 */
public interface RedisStorageAdapter {

    // ============================================================
    // Basic Key-Value Operations
    // ============================================================

    byte[] get(byte[] key) throws Exception;

    void set(byte[] key, byte[] value) throws Exception;

    void delete(byte[] key) throws Exception;

    boolean exists(byte[] key) throws Exception;

    boolean keyExists(String key) throws Exception;

    List<String> keys(String pattern) throws Exception;

    long incr(byte[] key) throws Exception;

    long decr(byte[] key) throws Exception;

    long incrBy(byte[] key, long increment) throws Exception;

    long decrBy(byte[] key, long decrement) throws Exception;

    double incrByFloat(byte[] key, double increment) throws Exception;

    long hincrBy(String redisKey, String field, long increment) throws Exception;

    double zincrBy(String redisKey, String member, double increment) throws Exception;

    void close();

    // ============================================================
    // Multi-row Operations (Hash, List, Set, Sorted Set)
    // ============================================================

    List<RedisSingleTableAdapter.KeyValue> scanByKey(String redisKey) throws Exception;

    List<RedisSingleTableAdapter.KeyValue> scanByPrefix(String redisKey, String subKeyPrefix)
            throws Exception;

    long countByKey(String redisKey) throws Exception;

    Set<String> getAllKeys();

    List<RedisSingleTableAdapter.FullKeyValue> scanAll() throws Exception;

    byte[] getByCompositeKey(String redisKey, String subKey) throws Exception;

    RedisSingleTableAdapter.KeyValue getKeyValueByCompositeKey(String redisKey, String subKey)
            throws Exception;

    void setByCompositeKey(
            String redisKey, String redisType, String subKey, byte[] value, Double score)
            throws Exception;

    void deleteByCompositeKey(String redisKey, String subKey) throws Exception;

    void deleteByKey(String redisKey) throws Exception;

    String getType(String redisKey) throws Exception;

    // ============================================================
    // Batch Operations
    // ============================================================

    List<byte[]> multiGet(List<String> keys) throws Exception;

    void multiSet(List<String> keys, List<byte[]> values) throws Exception;

    List<Boolean> multiExists(List<String> keys) throws Exception;
}
