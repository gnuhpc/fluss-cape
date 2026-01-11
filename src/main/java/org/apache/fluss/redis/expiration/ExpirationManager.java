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

package org.apache.fluss.redis.expiration;

import org.apache.fluss.redis.storage.RedisFlussAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ExpirationManager {

    private static final Logger LOG = LoggerFactory.getLogger(ExpirationManager.class);
    private static final String EXPIRATION_SUB_KEY = "__expire__";

    private final RedisFlussAdapter adapter;

    public ExpirationManager(RedisFlussAdapter adapter) {
        this.adapter = adapter;
    }

    public void setExpiration(String redisKey, long expireAtMs) throws Exception {
        byte[] data = serializeExpiration(expireAtMs);
        adapter.setByCompositeKey(redisKey, null, EXPIRATION_SUB_KEY, data, null);
        LOG.debug("Set expiration for key: {} at {}", redisKey, expireAtMs);
    }

    public Long getExpiration(String redisKey) throws Exception {
        byte[] data = adapter.getByCompositeKey(redisKey, EXPIRATION_SUB_KEY);
        if (data == null || data.length == 0) {
            return null;
        }
        return deserializeExpiration(data);
    }

    public void removeExpiration(String redisKey) throws Exception {
        adapter.deleteByCompositeKey(redisKey, EXPIRATION_SUB_KEY);
        LOG.debug("Removed expiration for key: {}", redisKey);
    }

    public boolean isExpired(String redisKey) throws Exception {
        Long expireAt = getExpiration(redisKey);
        if (expireAt == null) {
            return false;
        }
        return System.currentTimeMillis() > expireAt;
    }

    public void deleteExpiredKey(String redisKey) throws Exception {
        try {
            LOG.debug("Deleting expired key: {}", redisKey);
            adapter.deleteByKey(redisKey);
        } catch (Exception e) {
            LOG.warn("Failed to delete expired key (snapshot may not be available yet): {}", redisKey, e);
        }
    }

    public boolean checkAndDeleteIfExpired(String redisKey) throws Exception {
        if (isExpired(redisKey)) {
            deleteExpiredKey(redisKey);
            return true;
        }
        return false;
    }

    private byte[] serializeExpiration(long expireAtMs) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(expireAtMs);
        dos.flush();
        return baos.toByteArray();
    }

    private long deserializeExpiration(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        return dis.readLong();
    }
}
