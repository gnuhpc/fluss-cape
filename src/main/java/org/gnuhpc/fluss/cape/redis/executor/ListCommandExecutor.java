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
import java.nio.charset.StandardCharsets;

import org.gnuhpc.fluss.cape.redis.expiration.ExpirationManager;
import org.gnuhpc.fluss.cape.redis.metadata.ListMetadata;
import org.gnuhpc.fluss.cape.redis.metadata.MetadataManager;
import org.gnuhpc.fluss.cape.redis.protocol.RedisCommand;
import org.gnuhpc.fluss.cape.redis.protocol.RedisResponse;
import org.gnuhpc.fluss.cape.redis.storage.RedisSingleTableAdapter;
import org.gnuhpc.fluss.cape.redis.storage.RedisStorageAdapter;
import org.gnuhpc.fluss.cape.redis.util.BlockingOperationQueue;
import org.gnuhpc.fluss.cape.redis.util.IndexEncoder;
import org.gnuhpc.fluss.cape.redis.util.RedisErrorSanitizer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Executor for Redis List commands.
 *
 * <h3>⚠️ Multi-Instance Deployment Limitation</h3>
 * <p>Blocking operations (BLPOP, BRPOP, BLMOVE) store their blocking state in local memory
 * tied to the Netty {@link ChannelHandlerContext}. In multi-instance deployments with load
 * balancers, if a blocking command and the subsequent data-producing command (e.g., LPUSH)
 * route to different CAPE instances, the blocked client will NOT be notified because the
 * blocking state exists only on the first instance.</p>
 *
 * <p><b>Solution</b>: Configure sticky sessions (session affinity) at your load balancer.
 * See {@link TransactionCommandExecutor} class JavaDoc for HAProxy/Nginx configuration examples.</p>
 *
 * @see TransactionCommandExecutor Full explanation and load balancer configuration examples
 */
public class ListCommandExecutor implements BlockingAwareExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ListCommandExecutor.class);
    private static final String REDIS_TYPE = "list";
    private static final String METADATA_SUB_KEY = "__meta__";

    private final RedisStorageAdapter adapter;
    private final MetadataManager metadataManager;
    private final ExpirationManager expirationManager;
    private BlockingOperationQueue currentBlockingQueue;

    public ListCommandExecutor(RedisStorageAdapter adapter) {
        this.adapter = adapter;
        this.metadataManager = new MetadataManager(adapter);
        this.expirationManager = new ExpirationManager(adapter);
    }

    @Override
    public RedisMessage execute(RedisCommand command) {
        return execute(command, null, null);
    }

    @Override
    public RedisMessage execute(RedisCommand command, ChannelHandlerContext ctx, BlockingOperationQueue blockingQueue) {
        String cmd = command.getCommand();
        this.currentBlockingQueue = blockingQueue;

        try {
            switch (cmd) {
                case "LPUSH":
                    return executeLPush(command);
                case "RPUSH":
                    return executeRPush(command);
                case "LPOP":
                    return executeLPop(command);
                case "RPOP":
                    return executeRPop(command);
                case "LRANGE":
                    return executeLRange(command);
                case "LLEN":
                    return executeLLen(command);
                case "LINDEX":
                    return executeLIndex(command);
                case "LINSERT":
                    return executeLInsert(command);
                case "LREM":
                    return executeLRem(command);
                case "LSET":
                    return executeLSet(command);
                case "LTRIM":
                    return executeLTrim(command);
                case "LPUSHX":
                    return executeLPushX(command);
                case "RPUSHX":
                    return executeRPushX(command);
                case "LPOS":
                    return executeLPos(command);
                case "LMOVE":
                    return executeLMove(command);
                case "BLPOP":
                    return executeBLPop(command, ctx, blockingQueue);
                case "BRPOP":
                    return executeBRPop(command, ctx, blockingQueue);
                case "BLMOVE":
                    return executeBLMove(command, ctx, blockingQueue);
                default:
                    return RedisResponse.error("ERR unknown list command '" + cmd + "'");
            }
        } catch (Exception e) {
            LOG.error("Error executing list command: {}", cmd, e);
            return RedisErrorSanitizer.sanitizeError(e, cmd);
        } finally {
            this.currentBlockingQueue = null;
        }
    }

    private RedisMessage executeLPush(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'LPUSH' command");
        }

        String key = command.getArgAsString(0);
        List<byte[]> values = command.getArgs().subList(1, command.getArgs().size());

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        ListMetadata metadata = metadataManager.getOrCreateListMetadata(key);

        for (byte[]  value : values) {
            metadata.decrementHeadIndex();
            String subKey = IndexEncoder.encode(metadata.getHeadIndex());
            adapter.setByCompositeKey(key, REDIS_TYPE, subKey, value, null);
            metadata.incrementCount();
        }

        metadataManager.saveListMetadata(key, REDIS_TYPE, metadata);

        if (currentBlockingQueue != null) {
            currentBlockingQueue.notifyDataAvailable(key, () -> {
                try {
                    return executePopForBlocking(key, true);
                } catch (Exception e) {
                    LOG.error("Error in blocking pop", e);
                    return RedisResponse.nullBulkString();
                }
            });
        }

        return RedisResponse.integer(metadata.getCount());
    }

    private RedisMessage executeRPush(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'RPUSH' command");
        }

        String key = command.getArgAsString(0);
        List<byte[]> values = command.getArgs().subList(1, command.getArgs().size());

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        ListMetadata metadata = metadataManager.getOrCreateListMetadata(key);

        for (byte[] value : values) {
            metadata.incrementTailIndex();
            String subKey = IndexEncoder.encode(metadata.getTailIndex());
            adapter.setByCompositeKey(key, REDIS_TYPE, subKey, value, null);
            metadata.incrementCount();
        }

        metadataManager.saveListMetadata(key, REDIS_TYPE, metadata);

        if (currentBlockingQueue != null) {
            currentBlockingQueue.notifyDataAvailable(key, () -> {
                try {
                    return executePopForBlocking(key, false);
                } catch (Exception e) {
                    LOG.error("Error in blocking pop", e);
                    return RedisResponse.nullBulkString();
                }
            });
        }

        return RedisResponse.integer(metadata.getCount());
    }

    private RedisMessage executeLPop(RedisCommand command) throws Exception {
        if (command.getArgCount() != 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'LPOP' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.nullBulkString();
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.nullBulkString();
        }

        ListMetadata metadata = metadataManager.getListMetadata(key);
        if (metadata == null || metadata.isEmpty()) {
            return RedisResponse.nullBulkString();
        }

        String headSubKey = IndexEncoder.encode(metadata.getHeadIndex());
        byte[] value = adapter.getByCompositeKey(key, headSubKey);

        if (value == null) {
            return RedisResponse.nullBulkString();
        }

        adapter.deleteByCompositeKey(key, headSubKey);

        metadata.incrementHeadIndex();
        metadata.decrementCount();
        metadataManager.saveListMetadata(key, REDIS_TYPE, metadata);

        return RedisResponse.bulkString(value);
    }

    private RedisMessage executeRPop(RedisCommand command) throws Exception {
        if (command.getArgCount() != 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'RPOP' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.nullBulkString();
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.nullBulkString();
        }

        ListMetadata metadata = metadataManager.getListMetadata(key);
        if (metadata == null || metadata.isEmpty()) {
            return RedisResponse.nullBulkString();
        }

        String tailSubKey = IndexEncoder.encode(metadata.getTailIndex());
        byte[] value = adapter.getByCompositeKey(key, tailSubKey);

        if (value == null) {
            return RedisResponse.nullBulkString();
        }

        adapter.deleteByCompositeKey(key, tailSubKey);

        metadata.decrementTailIndex();
        metadata.decrementCount();
        metadataManager.saveListMetadata(key, REDIS_TYPE, metadata);

        return RedisResponse.bulkString(value);
    }

    private RedisMessage executeLRange(RedisCommand command) throws Exception {
        if (command.getArgCount() != 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'LRANGE' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }
        
        long start;
        long stop;

        try {
            start = Long.parseLong(command.getArgAsString(1));
            stop = Long.parseLong(command.getArgAsString(2));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not an integer or out of range");
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }

        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

        if (items.isEmpty()) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }

        items.sort((a, b) -> a.subKey.compareTo(b.subKey));

        long totalCount = items.size();

        if (start < 0) {
            start = totalCount + start;
        }
        if (stop < 0) {
            stop = totalCount + stop;
        }

        start = Math.max(0, start);
        stop = Math.min(totalCount - 1, stop);

        if (start > stop || start >= totalCount) {
            return RedisResponse.bytesArray(Collections.emptyList());
        }

        List<byte[]> results = new ArrayList<>();
        for (long i = start; i <= stop && i < totalCount; i++) {
            results.add(items.get((int) i).value);
        }

        return RedisResponse.bytesArray(results);
    }

    private RedisMessage executeLLen(RedisCommand command) throws Exception {
        if (command.getArgCount() != 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'LLEN' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.integer(0);
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.integer(0);
        }

        ListMetadata metadata = metadataManager.getListMetadata(key);
        if (metadata == null) {
            return RedisResponse.integer(0);
        }

        return RedisResponse.integer(metadata.getCount());
    }

    private RedisMessage executeLIndex(RedisCommand command) throws Exception {
        if (command.getArgCount() != 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'LINDEX' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.nullBulkString();
        }
        
        long index;

        try {
            index = Long.parseLong(command.getArgAsString(1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not an integer or out of range");
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.nullBulkString();
        }

        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));

        if (items.isEmpty()) {
            return RedisResponse.nullBulkString();
        }

        items.sort((a, b) -> a.subKey.compareTo(b.subKey));

        long totalCount = items.size();

        if (index < 0) {
            index = totalCount + index;
        }

        if (index < 0 || index >= totalCount) {
            return RedisResponse.nullBulkString();
        }

        return RedisResponse.bulkString(items.get((int) index).value);
    }

    private RedisMessage executeLInsert(RedisCommand command) throws Exception {
        if (command.getArgCount() != 4) {
            return RedisResponse.error("ERR wrong number of arguments for 'LINSERT' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.integer(0);
        }
        
        String position = command.getArgAsString(1).toUpperCase();
        byte[] pivot = command.getArg(2);
        byte[] newValue = command.getArg(3);

        if (!position.equals("BEFORE") && !position.equals("AFTER")) {
            return RedisResponse.error("ERR syntax error");
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.integer(0);
        }

        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));
        items.sort((a, b) -> a.subKey.compareTo(b.subKey));

        int pivotIndex = -1;
        for (int i = 0; i < items.size(); i++) {
            if (java.util.Arrays.equals(items.get(i).value, pivot)) {
                pivotIndex = i;
                break;
            }
        }

        if (pivotIndex == -1) {
            return RedisResponse.integer(-1);
        }

        ListMetadata metadata = metadataManager.getListMetadata(key);
        if (metadata == null) {
            return RedisResponse.integer(0);
        }

        int insertIndex = position.equals("BEFORE") ? pivotIndex : pivotIndex + 1;

        List<byte[]> allValues = new ArrayList<>();
        for (RedisSingleTableAdapter.KeyValue kv : items) {
            allValues.add(kv.value);
        }
        allValues.add(insertIndex, newValue);

        for (RedisSingleTableAdapter.KeyValue kv : items) {
            adapter.deleteByCompositeKey(key, kv.subKey);
        }

        long currentIndex = metadata.getHeadIndex();
        for (byte[] val : allValues) {
            String subKey = IndexEncoder.encode(currentIndex);
            adapter.setByCompositeKey(key, REDIS_TYPE, subKey, val, null);
            currentIndex++;
        }

        metadata.setTailIndex(currentIndex - 1);
        metadata.setCount(allValues.size());
        metadataManager.saveListMetadata(key, REDIS_TYPE, metadata);

        return RedisResponse.integer(allValues.size());
    }

    private RedisMessage executeLRem(RedisCommand command) throws Exception {
        if (command.getArgCount() != 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'LREM' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.integer(0);
        }
        
        long count;
        try {
            count = Long.parseLong(command.getArgAsString(1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not an integer or out of range");
        }

        byte[] element = command.getArg(2);

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.integer(0);
        }

        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));
        items.sort((a, b) -> a.subKey.compareTo(b.subKey));

        int removed = 0;
        int toRemove = (int) Math.abs(count);
        boolean fromHead = count >= 0;

        if (!fromHead) {
            Collections.reverse(items);
        }

        List<Integer> indicesToRemove = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            if (java.util.Arrays.equals(items.get(i).value, element)) {
                indicesToRemove.add(i);
                removed++;
                if (count != 0 && removed >= toRemove) {
                    break;
                }
            }
        }

        if (!fromHead) {
            Collections.reverse(items);
            for (int i = 0; i < indicesToRemove.size(); i++) {
                indicesToRemove.set(i, items.size() - 1 - indicesToRemove.get(i));
            }
        }

        for (int i = indicesToRemove.size() - 1; i >= 0; i--) {
            RedisSingleTableAdapter.KeyValue kv = items.remove(indicesToRemove.get(i).intValue());
            adapter.deleteByCompositeKey(key, kv.subKey);
        }

        if (removed > 0) {
            ListMetadata metadata = metadataManager.getListMetadata(key);
            if (metadata != null) {
                metadata.setCount(items.size());
                metadataManager.saveListMetadata(key, REDIS_TYPE, metadata);
            }
        }

        return RedisResponse.integer(removed);
    }

    private RedisMessage executeLSet(RedisCommand command) throws Exception {
        if (command.getArgCount() != 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'LSET' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.error("ERR no such key");
        }
        
        long index;
        try {
            index = Long.parseLong(command.getArgAsString(1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not an integer or out of range");
        }

        byte[] newValue = command.getArg(2);

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.error("ERR no such key");
        }

        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));
        items.sort((a, b) -> a.subKey.compareTo(b.subKey));

        long totalCount = items.size();

        if (index < 0) {
            index = totalCount + index;
        }

        if (index < 0 || index >= totalCount) {
            return RedisResponse.error("ERR index out of range");
        }

        RedisSingleTableAdapter.KeyValue target = items.get((int) index);
        adapter.setByCompositeKey(key, REDIS_TYPE, target.subKey, newValue, null);

        return RedisResponse.ok();
    }

    private RedisMessage executeLTrim(RedisCommand command) throws Exception {
        if (command.getArgCount() != 3) {
            return RedisResponse.error("ERR wrong number of arguments for 'LTRIM' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.ok();
        }
        
        long start;
        long stop;
        try {
            start = Long.parseLong(command.getArgAsString(1));
            stop = Long.parseLong(command.getArgAsString(2));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR value is not an integer or out of range");
        }

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.ok();
        }

        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));
        items.sort((a, b) -> a.subKey.compareTo(b.subKey));

        long totalCount = items.size();

        if (start < 0) {
            start = totalCount + start;
        }
        if (stop < 0) {
            stop = totalCount + stop;
        }

        start = Math.max(0, start);
        stop = Math.min(totalCount - 1, stop);

        if (start > stop || start >= totalCount) {
            for (RedisSingleTableAdapter.KeyValue kv : items) {
                adapter.deleteByCompositeKey(key, kv.subKey);
            }
            adapter.deleteByCompositeKey(key, METADATA_SUB_KEY);
            return RedisResponse.ok();
        }

        List<RedisSingleTableAdapter.KeyValue> toKeep = new ArrayList<>();
        for (long i = start; i <= stop && i < totalCount; i++) {
            toKeep.add(items.get((int) i));
        }

        for (RedisSingleTableAdapter.KeyValue kv : items) {
            adapter.deleteByCompositeKey(key, kv.subKey);
        }

        ListMetadata metadata = metadataManager.getOrCreateListMetadata(key);
        long currentIndex = 0;
        for (RedisSingleTableAdapter.KeyValue kv : toKeep) {
            String subKey = IndexEncoder.encode(currentIndex);
            adapter.setByCompositeKey(key, REDIS_TYPE, subKey, kv.value, null);
            currentIndex++;
        }

        metadata.setHeadIndex(0);
        metadata.setTailIndex(currentIndex - 1);
        metadata.setCount(toKeep.size());
        metadataManager.saveListMetadata(key, REDIS_TYPE, metadata);

        return RedisResponse.ok();
    }

    private RedisMessage executeLPushX(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'LPUSHX' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.integer(0);
        }
        
        List<byte[]> values = command.getArgs().subList(1, command.getArgs().size());

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.integer(0);
        }

        ListMetadata metadata = metadataManager.getListMetadata(key);
        if (metadata == null) {
            return RedisResponse.integer(0);
        }

        for (byte[] value : values) {
            metadata.decrementHeadIndex();
            String subKey = IndexEncoder.encode(metadata.getHeadIndex());
            adapter.setByCompositeKey(key, REDIS_TYPE, subKey, value, null);
            metadata.incrementCount();
        }

        metadataManager.saveListMetadata(key, REDIS_TYPE, metadata);

        return RedisResponse.integer(metadata.getCount());
    }

    private RedisMessage executeRPushX(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'RPUSHX' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.integer(0);
        }
        
        List<byte[]> values = command.getArgs().subList(1, command.getArgs().size());

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.integer(0);
        }

        ListMetadata metadata = metadataManager.getListMetadata(key);
        if (metadata == null) {
            return RedisResponse.integer(0);
        }

        for (byte[] value : values) {
            metadata.incrementTailIndex();
            String subKey = IndexEncoder.encode(metadata.getTailIndex());
            adapter.setByCompositeKey(key, REDIS_TYPE, subKey, value, null);
            metadata.incrementCount();
        }

        metadataManager.saveListMetadata(key, REDIS_TYPE, metadata);

        return RedisResponse.integer(metadata.getCount());
    }

    private RedisMessage executeLPos(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'LPOS' command");
        }

        String key = command.getArgAsString(0);
        
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.nullBulkString();
        }
        
        byte[] element = command.getArg(1);

        String existingType = adapter.getType(key);
        if (existingType != null && !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (existingType == null) {
            return RedisResponse.nullBulkString();
        }

        List<RedisSingleTableAdapter.KeyValue> items = adapter.scanByKey(key);
        items.removeIf(kv -> METADATA_SUB_KEY.equals(kv.subKey));
        items.sort((a, b) -> a.subKey.compareTo(b.subKey));

        for (int i = 0; i < items.size(); i++) {
            if (java.util.Arrays.equals(items.get(i).value, element)) {
                return RedisResponse.integer(i);
            }
        }

        return RedisResponse.nullBulkString();
    }

    private RedisMessage executeLMove(RedisCommand command) throws Exception {
        if (command.getArgCount() != 4) {
            return RedisResponse.error("ERR wrong number of arguments for 'LMOVE' command");
        }

        String sourceKey = command.getArgAsString(0);
        String destKey = command.getArgAsString(1);
        String wherefrom = command.getArgAsString(2).toUpperCase();
        String whereto = command.getArgAsString(3).toUpperCase();

        if (expirationManager.checkAndDeleteIfExpired(sourceKey)) {
            return RedisResponse.nullBulkString();
        }

        if (!wherefrom.equals("LEFT") && !wherefrom.equals("RIGHT")) {
            return RedisResponse.error("ERR syntax error");
        }
        if (!whereto.equals("LEFT") && !whereto.equals("RIGHT")) {
            return RedisResponse.error("ERR syntax error");
        }

        String sourceType = adapter.getType(sourceKey);
        if (sourceType != null && !sourceType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (sourceType == null) {
            return RedisResponse.nullBulkString();
        }

        String destType = adapter.getType(destKey);
        if (destType != null && !destType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        ListMetadata sourceMetadata = metadataManager.getListMetadata(sourceKey);
        if (sourceMetadata == null || sourceMetadata.isEmpty()) {
            return RedisResponse.nullBulkString();
        }

        byte[] element;
        if (wherefrom.equals("LEFT")) {
            String headSubKey = IndexEncoder.encode(sourceMetadata.getHeadIndex());
            element = adapter.getByCompositeKey(sourceKey, headSubKey);
            if (element == null) {
                return RedisResponse.nullBulkString();
            }
            adapter.deleteByCompositeKey(sourceKey, headSubKey);
            sourceMetadata.incrementHeadIndex();
            sourceMetadata.decrementCount();
        } else {
            String tailSubKey = IndexEncoder.encode(sourceMetadata.getTailIndex());
            element = adapter.getByCompositeKey(sourceKey, tailSubKey);
            if (element == null) {
                return RedisResponse.nullBulkString();
            }
            adapter.deleteByCompositeKey(sourceKey, tailSubKey);
            sourceMetadata.decrementTailIndex();
            sourceMetadata.decrementCount();
        }

        metadataManager.saveListMetadata(sourceKey, REDIS_TYPE, sourceMetadata);

        ListMetadata destMetadata = metadataManager.getOrCreateListMetadata(destKey);

        if (whereto.equals("LEFT")) {
            destMetadata.decrementHeadIndex();
            String subKey = IndexEncoder.encode(destMetadata.getHeadIndex());
            adapter.setByCompositeKey(destKey, REDIS_TYPE, subKey, element, null);
            destMetadata.incrementCount();
        } else {
            destMetadata.incrementTailIndex();
            String subKey = IndexEncoder.encode(destMetadata.getTailIndex());
            adapter.setByCompositeKey(destKey, REDIS_TYPE, subKey, element, null);
            destMetadata.incrementCount();
        }

        metadataManager.saveListMetadata(destKey, REDIS_TYPE, destMetadata);

        return RedisResponse.bulkString(element);
    }

    private RedisMessage executePopForBlocking(String key, boolean fromLeft) throws Exception {
        if (expirationManager.checkAndDeleteIfExpired(key)) {
            return RedisResponse.nullBulkString();
        }

        String existingType = adapter.getType(key);
        if (existingType == null || !existingType.equals(REDIS_TYPE)) {
            return RedisResponse.nullBulkString();
        }

        ListMetadata metadata = metadataManager.getListMetadata(key);
        if (metadata == null || metadata.isEmpty()) {
            return RedisResponse.nullBulkString();
        }

        byte[] value;
        if (fromLeft) {
            String headSubKey = IndexEncoder.encode(metadata.getHeadIndex());
            value = adapter.getByCompositeKey(key, headSubKey);
            if (value != null) {
                adapter.deleteByCompositeKey(key, headSubKey);
                metadata.incrementHeadIndex();
                metadata.decrementCount();
            }
        } else {
            String tailSubKey = IndexEncoder.encode(metadata.getTailIndex());
            value = adapter.getByCompositeKey(key, tailSubKey);
            if (value != null) {
                adapter.deleteByCompositeKey(key, tailSubKey);
                metadata.decrementTailIndex();
                metadata.decrementCount();
            }
        }

        if (value == null) {
            return RedisResponse.nullBulkString();
        }

        metadataManager.saveListMetadata(key, REDIS_TYPE, metadata);

        List<byte[]> response = new ArrayList<>();
        response.add(key.getBytes(StandardCharsets.UTF_8));
        response.add(value);
        return RedisResponse.bytesArray(response);
    }

    private RedisMessage executeBLPop(RedisCommand command, ChannelHandlerContext ctx, BlockingOperationQueue blockingQueue) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'BLPOP' command");
        }

        int timeout;
        try {
            timeout = Integer.parseInt(command.getArgAsString(command.getArgCount() - 1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR timeout is not an integer or out of range");
        }

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < command.getArgCount() - 1; i++) {
            keys.add(command.getArgAsString(i));
        }

        for (String key : keys) {
            if (expirationManager.checkAndDeleteIfExpired(key)) {
                continue;
            }

            String existingType = adapter.getType(key);
            if (existingType != null && !existingType.equals(REDIS_TYPE)) {
                return RedisResponse.error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            if (existingType != null) {
                ListMetadata metadata = metadataManager.getListMetadata(key);
                if (metadata != null && !metadata.isEmpty()) {
                    String headSubKey = IndexEncoder.encode(metadata.getHeadIndex());
                    byte[] value = adapter.getByCompositeKey(key, headSubKey);

                    if (value != null) {
                        adapter.deleteByCompositeKey(key, headSubKey);
                        metadata.incrementHeadIndex();
                        metadata.decrementCount();
                        metadataManager.saveListMetadata(key, REDIS_TYPE, metadata);

                        List<byte[]> response = new ArrayList<>();
                        response.add(key.getBytes(StandardCharsets.UTF_8));
                        response.add(value);
                        return RedisResponse.bytesArray(response);
                    }
                }
            }
        }

        blockingQueue.registerBlockingRequest(ctx, keys, timeout);
        return null;
    }

    private RedisMessage executeBRPop(RedisCommand command, ChannelHandlerContext ctx, BlockingOperationQueue blockingQueue) throws Exception {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'BRPOP' command");
        }

        int timeout;
        try {
            timeout = Integer.parseInt(command.getArgAsString(command.getArgCount() - 1));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR timeout is not an integer or out of range");
        }

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < command.getArgCount() - 1; i++) {
            keys.add(command.getArgAsString(i));
        }

        for (String key : keys) {
            if (expirationManager.checkAndDeleteIfExpired(key)) {
                continue;
            }

            String existingType = adapter.getType(key);
            if (existingType != null && !existingType.equals(REDIS_TYPE)) {
                return RedisResponse.error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            if (existingType != null) {
                ListMetadata metadata = metadataManager.getListMetadata(key);
                if (metadata != null && !metadata.isEmpty()) {
                    String tailSubKey = IndexEncoder.encode(metadata.getTailIndex());
                    byte[] value = adapter.getByCompositeKey(key, tailSubKey);

                    if (value != null) {
                        adapter.deleteByCompositeKey(key, tailSubKey);
                        metadata.decrementTailIndex();
                        metadata.decrementCount();
                        metadataManager.saveListMetadata(key, REDIS_TYPE, metadata);

                        List<byte[]> response = new ArrayList<>();
                        response.add(key.getBytes(StandardCharsets.UTF_8));
                        response.add(value);
                        return RedisResponse.bytesArray(response);
                    }
                }
            }
        }

        blockingQueue.registerBlockingRequest(ctx, keys, timeout);
        return null;
    }

    private RedisMessage executeBLMove(RedisCommand command, ChannelHandlerContext ctx, BlockingOperationQueue blockingQueue) throws Exception {
        if (command.getArgCount() != 5) {
            return RedisResponse.error("ERR wrong number of arguments for 'BLMOVE' command");
        }

        String sourceKey = command.getArgAsString(0);
        String destKey = command.getArgAsString(1);
        String wherefrom = command.getArgAsString(2).toUpperCase();
        String whereto = command.getArgAsString(3).toUpperCase();
        
        int timeout;
        try {
            timeout = Integer.parseInt(command.getArgAsString(4));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR timeout is not an integer or out of range");
        }

        if (!wherefrom.equals("LEFT") && !wherefrom.equals("RIGHT")) {
            return RedisResponse.error("ERR syntax error");
        }
        if (!whereto.equals("LEFT") && !whereto.equals("RIGHT")) {
            return RedisResponse.error("ERR syntax error");
        }

        if (expirationManager.checkAndDeleteIfExpired(sourceKey)) {
            blockingQueue.registerBlockingRequest(ctx, Collections.singletonList(sourceKey), timeout);
            return null;
        }

        String sourceType = adapter.getType(sourceKey);
        if (sourceType != null && !sourceType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        String destType = adapter.getType(destKey);
        if (destType != null && !destType.equals(REDIS_TYPE)) {
            return RedisResponse.error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        if (sourceType != null) {
            ListMetadata sourceMetadata = metadataManager.getListMetadata(sourceKey);
            if (sourceMetadata != null && !sourceMetadata.isEmpty()) {
                byte[] element;
                if (wherefrom.equals("LEFT")) {
                    String headSubKey = IndexEncoder.encode(sourceMetadata.getHeadIndex());
                    element = adapter.getByCompositeKey(sourceKey, headSubKey);
                    if (element != null) {
                        adapter.deleteByCompositeKey(sourceKey, headSubKey);
                        sourceMetadata.incrementHeadIndex();
                        sourceMetadata.decrementCount();
                    }
                } else {
                    String tailSubKey = IndexEncoder.encode(sourceMetadata.getTailIndex());
                    element = adapter.getByCompositeKey(sourceKey, tailSubKey);
                    if (element != null) {
                        adapter.deleteByCompositeKey(sourceKey, tailSubKey);
                        sourceMetadata.decrementTailIndex();
                        sourceMetadata.decrementCount();
                    }
                }

                if (element != null) {
                    metadataManager.saveListMetadata(sourceKey, REDIS_TYPE, sourceMetadata);

                    ListMetadata destMetadata = metadataManager.getOrCreateListMetadata(destKey);

                    if (whereto.equals("LEFT")) {
                        destMetadata.decrementHeadIndex();
                        String subKey = IndexEncoder.encode(destMetadata.getHeadIndex());
                        adapter.setByCompositeKey(destKey, REDIS_TYPE, subKey, element, null);
                        destMetadata.incrementCount();
                    } else {
                        destMetadata.incrementTailIndex();
                        String subKey = IndexEncoder.encode(destMetadata.getTailIndex());
                        adapter.setByCompositeKey(destKey, REDIS_TYPE, subKey, element, null);
                        destMetadata.incrementCount();
                    }

                    metadataManager.saveListMetadata(destKey, REDIS_TYPE, destMetadata);
                    return RedisResponse.bulkString(element);
                }
            }
        }

        blockingQueue.registerBlockingRequest(ctx, Collections.singletonList(sourceKey), timeout);
        return null;
    }
}
