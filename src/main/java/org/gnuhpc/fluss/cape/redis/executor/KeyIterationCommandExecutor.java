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
import org.gnuhpc.fluss.cape.redis.util.ScanCursorManager;
import org.gnuhpc.fluss.cape.redis.util.RedisErrorSanitizer;

import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class KeyIterationCommandExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(KeyIterationCommandExecutor.class);

    private static final int DEFAULT_SCAN_COUNT = 10;

    private final RedisStorageAdapter adapter;
    private final ScanCursorManager cursorManager;

    public KeyIterationCommandExecutor(RedisStorageAdapter adapter) {
        this.adapter = adapter;
        this.cursorManager = new ScanCursorManager();
    }

    @Override
    public RedisMessage execute(RedisCommand command) {
        String cmd = command.getCommand();

        try {
            switch (cmd) {
                case "KEYS":
                    return executeKeys(command);
                case "SCAN":
                    return executeScan(command);
                default:
                    return RedisResponse.error("ERR unknown key iteration command '" + cmd + "'");
            }
        } catch (Exception e) {
            LOG.error("Error executing key iteration command: {}", cmd, e);
            return RedisErrorSanitizer.sanitizeError(e, cmd);
        }
    }

    private RedisMessage executeKeys(RedisCommand command) {
        if (command.getArgCount() != 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'KEYS' command");
        }

        String pattern = command.getArgAsString(0);

        try {
            Set<String> allKeys = getAllKeys();
            List<String> matchedKeys = matchPattern(allKeys, pattern);

            List<RedisMessage> messages = new ArrayList<>();
            for (String key : matchedKeys) {
                messages.add(
                        new FullBulkStringRedisMessage(
                                io.netty.buffer.Unpooled.wrappedBuffer(
                                        key.getBytes(CharsetUtil.UTF_8))));
            }

            return new ArrayRedisMessage(messages);
        } catch (Exception e) {
            LOG.error("Error executing KEYS command", e);
            return RedisErrorSanitizer.sanitizeError(e, "KEYS");
        }
    }

    private RedisMessage executeScan(RedisCommand command) {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'SCAN' command");
        }

        long cursor;
        try {
            cursor = Long.parseLong(command.getArgAsString(0));
        } catch (NumberFormatException e) {
            return RedisResponse.error("ERR invalid cursor");
        }

        String matchPattern = null;
        int count = DEFAULT_SCAN_COUNT;

        for (int i = 1; i < command.getArgCount(); i++) {
            String arg = command.getArgAsString(i).toUpperCase();
            if (arg.equals("MATCH") && i + 1 < command.getArgCount()) {
                matchPattern = command.getArgAsString(i + 1);
                i++;
            } else if (arg.equals("COUNT") && i + 1 < command.getArgCount()) {
                try {
                    count = Integer.parseInt(command.getArgAsString(i + 1));
                    if (count <= 0) {
                        return RedisResponse.error("ERR COUNT must be positive");
                    }
                } catch (NumberFormatException e) {
                    return RedisResponse.error("ERR value is not an integer or out of range");
                }
                i++;
            }
        }

        try {
            Set<String> allKeys = getAllKeys();
            List<String> keyList;

            if (matchPattern != null) {
                keyList = matchPattern(allKeys, matchPattern);
            } else {
                keyList = new ArrayList<>(allKeys);
            }

            ScanCursorManager.ScanResult result = cursorManager.scan(cursor, keyList, count);

            List<RedisMessage> response = new ArrayList<>();
            response.add(
                    new FullBulkStringRedisMessage(
                            io.netty.buffer.Unpooled.wrappedBuffer(
                                    String.valueOf(result.nextCursor)
                                            .getBytes(CharsetUtil.UTF_8))));

            List<RedisMessage> keys = new ArrayList<>();
            for (String key : result.items) {
                keys.add(
                        new FullBulkStringRedisMessage(
                                io.netty.buffer.Unpooled.wrappedBuffer(
                                        key.getBytes(CharsetUtil.UTF_8))));
            }
            response.add(new ArrayRedisMessage(keys));

            return new ArrayRedisMessage(response);
        } catch (Exception e) {
            LOG.error("Error executing SCAN command", e);
            return RedisErrorSanitizer.sanitizeError(e, "SCAN");
        }
    }

    private Set<String> getAllKeys() throws Exception {
        return adapter.getAllKeys();
    }

    private List<String> matchPattern(Set<String> keys, String pattern) {
        Pattern regex = convertGlobToRegex(pattern);

        return keys.stream()
                .filter(key -> regex.matcher(key).matches())
                .sorted()
                .collect(Collectors.toList());
    }

    private Pattern convertGlobToRegex(String glob) {
        StringBuilder regex = new StringBuilder("^");

        for (int i = 0; i < glob.length(); i++) {
            char c = glob.charAt(i);
            switch (c) {
                case '*':
                    regex.append(".*");
                    break;
                case '?':
                    regex.append(".");
                    break;
                case '[':
                    int closeBracket = glob.indexOf(']', i);
                    if (closeBracket != -1) {
                        String charClass = glob.substring(i + 1, closeBracket);
                        regex.append("[");
                        if (charClass.startsWith("^")) {
                            regex.append("^");
                            charClass = charClass.substring(1);
                        }
                        regex.append(Pattern.quote(charClass).replaceAll("\\\\Q\\\\E", ""));
                        regex.append("]");
                        i = closeBracket;
                    } else {
                        regex.append(Pattern.quote(String.valueOf(c)));
                    }
                    break;
                case '\\':
                    if (i + 1 < glob.length()) {
                        regex.append(Pattern.quote(String.valueOf(glob.charAt(i + 1))));
                        i++;
                    } else {
                        regex.append(Pattern.quote(String.valueOf(c)));
                    }
                    break;
                default:
                    regex.append(Pattern.quote(String.valueOf(c)));
                    break;
            }
        }

        regex.append("$");
        return Pattern.compile(regex.toString());
    }
}
