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

package org.gnuhpc.fluss.cape.redis.executor;

import org.gnuhpc.fluss.cape.redis.protocol.RedisCommand;
import org.gnuhpc.fluss.cape.redis.protocol.RedisResponse;
import org.gnuhpc.fluss.cape.redis.storage.RedisSingleTableAdapter;
import org.gnuhpc.fluss.cape.redis.storage.RedisStorageAdapter;
import org.gnuhpc.fluss.cape.redis.util.HyperLogLogHelper;
import org.gnuhpc.fluss.cape.redis.util.HyperLogLogHelper.HyperLogLog;
import org.gnuhpc.fluss.cape.redis.util.RedisErrorSanitizer;

import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class HyperLogLogCommandExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(HyperLogLogCommandExecutor.class);

    private static final String TYPE_HLL = "hyperloglog";

    private final RedisStorageAdapter adapter;

    public HyperLogLogCommandExecutor(RedisStorageAdapter adapter) {
        this.adapter = adapter;
    }

    @Override
    public RedisMessage execute(RedisCommand command) {
        String cmd = command.getCommand();

        try {
            switch (cmd) {
                case "PFADD":
                    return executePfAdd(command);
                case "PFCOUNT":
                    return executePfCount(command);
                case "PFMERGE":
                    return executePfMerge(command);
                default:
                    return RedisResponse.error("ERR unknown HyperLogLog command '" + cmd + "'");
            }
        } catch (Exception e) {
            LOG.error("Error executing HyperLogLog command: {}", cmd, e);
            return RedisErrorSanitizer.sanitizeError(e, cmd);
        }
    }

    private RedisMessage executePfAdd(RedisCommand command) {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'PFADD' command");
        }

        String key = command.getArgAsString(0);
        byte[] keyBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        byte[] existingData = null;
        try {
            existingData = adapter.get(keyBytes);
        } catch (Exception e) {
            LOG.warn("Error reading HLL key: {}", key, e);
        }

        HyperLogLog hll;

        if (existingData == null) {
            hll = HyperLogLogHelper.create();
        } else {
            String type = null;
            try {
                type = adapter.getType(key);
            } catch (Exception e) {
                LOG.warn("Error getting type for key: {}", key, e);
            }

            if (type != null && !type.equals("string") && !type.equals(TYPE_HLL)) {
                return RedisResponse.error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            try {
                hll = HyperLogLogHelper.fromBytes(existingData);
            } catch (Exception e) {
                hll = HyperLogLogHelper.create();
            }
        }

        boolean modified = false;
        for (int i = 1; i < command.getArgCount(); i++) {
            byte[] element = command.getArg(i);
            if (hll.add(element)) {
                modified = true;
            }
        }

        if (modified) {
            try {
                adapter.set(keyBytes, HyperLogLogHelper.toBytes(hll));
            } catch (Exception e) {
                LOG.error("Error saving HLL to key: {}", key, e);
                return RedisErrorSanitizer.sanitizeError(e, "PFADD");
            }
        }

        return RedisResponse.integer(modified ? 1 : 0);
    }

    private RedisMessage executePfCount(RedisCommand command) {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'PFCOUNT' command");
        }

        if (command.getArgCount() == 1) {
            String key = command.getArgAsString(0);
            byte[] keyBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] data = null;

            try {
                data = adapter.get(keyBytes);
            } catch (Exception e) {
                LOG.warn("Error reading HLL key: {}", key, e);
            }

            if (data == null) {
                return RedisResponse.integer(0);
            }

            String type = null;
            try {
                type = adapter.getType(key);
            } catch (Exception e) {
                LOG.warn("Error getting type for key: {}", key, e);
            }

            if (type != null && !type.equals("string") && !type.equals(TYPE_HLL)) {
                return RedisResponse.error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            try {
                HyperLogLog hll = HyperLogLogHelper.fromBytes(data);
                long cardinality = hll.cardinality();
                return RedisResponse.integer(cardinality);
            } catch (Exception e) {
                LOG.error("Error reading HLL from key: {}", key, e);
                return RedisResponse.integer(0);
            }
        } else {
            List<HyperLogLog> hlls = new ArrayList<>();

            for (int i = 0; i < command.getArgCount(); i++) {
                String key = command.getArgAsString(i);
                byte[] keyBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                byte[] data = null;

                try {
                    data = adapter.get(keyBytes);
                } catch (Exception e) {
                    LOG.warn("Error reading HLL key: {}", key, e);
                }

                if (data != null) {
                    String type = null;
                    try {
                        type = adapter.getType(key);
                    } catch (Exception e) {
                        LOG.warn("Error getting type for key: {}", key, e);
                    }

                    if (type != null && !type.equals("string") && !type.equals(TYPE_HLL)) {
                        return RedisResponse.error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value");
                    }

                    try {
                        hlls.add(HyperLogLogHelper.fromBytes(data));
                    } catch (Exception e) {
                        LOG.warn("Skipping invalid HLL at key: {}", key);
                    }
                }
            }

            if (hlls.isEmpty()) {
                return RedisResponse.integer(0);
            }

            HyperLogLog merged = HyperLogLogHelper.mergeAll(hlls.toArray(new HyperLogLog[0]));
            long cardinality = merged.cardinality();
            return RedisResponse.integer(cardinality);
        }
    }

    private RedisMessage executePfMerge(RedisCommand command) {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'PFMERGE' command");
        }

        String destKey = command.getArgAsString(0);
        byte[] destKeyBytes = destKey.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        List<HyperLogLog> hlls = new ArrayList<>();

        for (int i = 1; i < command.getArgCount(); i++) {
            String key = command.getArgAsString(i);
            byte[] keyBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] data = null;

            try {
                data = adapter.get(keyBytes);
            } catch (Exception e) {
                LOG.warn("Error reading HLL key: {}", key, e);
            }

            if (data != null) {
                String type = null;
                try {
                    type = adapter.getType(key);
                } catch (Exception e) {
                    LOG.warn("Error getting type for key: {}", key, e);
                }

                if (type != null && !type.equals("string") && !type.equals(TYPE_HLL)) {
                    return RedisResponse.error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value");
                }

                try {
                    hlls.add(HyperLogLogHelper.fromBytes(data));
                } catch (Exception e) {
                    LOG.warn("Skipping invalid HLL at key: {}", key);
                }
            }
        }

        HyperLogLog merged;
        if (hlls.isEmpty()) {
            merged = HyperLogLogHelper.create();
        } else {
            merged = HyperLogLogHelper.mergeAll(hlls.toArray(new HyperLogLog[0]));
        }

        try {
            adapter.set(destKeyBytes, HyperLogLogHelper.toBytes(merged));
        } catch (Exception e) {
            LOG.error("Error saving merged HLL to key: {}", destKey, e);
            return RedisErrorSanitizer.sanitizeError(e, "PFMERGE");
        }

        return RedisResponse.ok();
    }
}
