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

package org.apache.fluss.redis.metadata;

import org.apache.fluss.redis.storage.RedisFlussAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MetadataManager {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataManager.class);
    private static final String METADATA_SUB_KEY = "__meta__";

    private final RedisFlussAdapter adapter;

    public MetadataManager(RedisFlussAdapter adapter) {
        this.adapter = adapter;
    }

    public ListMetadata getListMetadata(String redisKey) {
        try {
            byte[] data = adapter.getByCompositeKey(redisKey, METADATA_SUB_KEY);
            if (data == null || data.length == 0) {
                return null;
            }
            return ListMetadata.deserialize(data);
        } catch (Exception e) {
            LOG.error("Failed to deserialize ListMetadata for key: {}", redisKey, e);
            return null;
        }
    }

    public void saveListMetadata(String redisKey, String redisType, ListMetadata metadata) {
        try {
            byte[] data = metadata.serialize();
            adapter.setByCompositeKey(redisKey, redisType, METADATA_SUB_KEY, data, null);
        } catch (Exception e) {
            LOG.error("Failed to serialize ListMetadata for key: {}", redisKey, e);
        }
    }

    public ListMetadata getOrCreateListMetadata(String redisKey) {
        ListMetadata meta = getListMetadata(redisKey);
        if (meta == null) {
            meta = new ListMetadata();
        }
        return meta;
    }

    public ZSetMetadata getZSetMetadata(String redisKey) {
        try {
            byte[] data = adapter.getByCompositeKey(redisKey, METADATA_SUB_KEY);
            if (data == null || data.length == 0) {
                return null;
            }
            return ZSetMetadata.deserialize(data);
        } catch (Exception e) {
            LOG.error("Failed to deserialize ZSetMetadata for key: {}", redisKey, e);
            return null;
        }
    }

    public void saveZSetMetadata(String redisKey, String redisType, ZSetMetadata metadata) {
        try {
            byte[] data = metadata.serialize();
            adapter.setByCompositeKey(redisKey, redisType, METADATA_SUB_KEY, data, null);
        } catch (Exception e) {
            LOG.error("Failed to serialize ZSetMetadata for key: {}", redisKey, e);
        }
    }

    public ZSetMetadata getOrCreateZSetMetadata(String redisKey) {
        ZSetMetadata meta = getZSetMetadata(redisKey);
        if (meta == null) {
            meta = new ZSetMetadata();
        }
        return meta;
    }

    public void deleteMetadata(String redisKey) {
        try {
            adapter.deleteByCompositeKey(redisKey, METADATA_SUB_KEY);
        } catch (Exception e) {
            LOG.error("Failed to delete metadata for key: {}", redisKey, e);
        }
    }
}
