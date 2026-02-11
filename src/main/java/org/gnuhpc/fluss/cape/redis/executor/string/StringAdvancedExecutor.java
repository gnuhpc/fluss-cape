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

package org.gnuhpc.fluss.cape.redis.executor.string;

import org.gnuhpc.fluss.cape.redis.executor.RedisCommandExecutor;
import org.gnuhpc.fluss.cape.redis.expiration.ExpirationManager;
import org.gnuhpc.fluss.cape.redis.protocol.RedisCommand;
import org.gnuhpc.fluss.cape.redis.protocol.RedisResponse;
import org.gnuhpc.fluss.cape.redis.storage.RedisStorageAdapter;
import org.gnuhpc.fluss.cape.redis.util.RedisErrorSanitizer;

import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Executor for advanced String operations (LCS, STRALGO, etc.)
 */
public class StringAdvancedExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(StringAdvancedExecutor.class);

    private final RedisStorageAdapter adapter;
    private final ExpirationManager expirationManager;

    public StringAdvancedExecutor(RedisStorageAdapter adapter) {
        this.adapter = adapter;
        this.expirationManager = new ExpirationManager(adapter);
    }

    @Override
    public RedisMessage execute(RedisCommand command) {
        String cmd = command.getCommand();

        try {
            switch (cmd) {
                case "LCS": return executeLcs(command);
                case "STRALGO": return executeStralgo(command);
                default:
                    return RedisResponse.error("ERR unknown command '" + cmd + "' in StringAdvancedExecutor");
            }
        } catch (Exception e) {
            return RedisErrorSanitizer.sanitizeError(e, cmd);
        }
    }

    private RedisMessage executeLcs(RedisCommand command) throws Exception {
        if (command.getArgCount() < 2) return RedisResponse.error("ERR wrong number of arguments for 'lcs' command");

        byte[] key1 = command.getArg(0);
        byte[] key2 = command.getArg(1);
        String key1Str = new String(key1);
        String key2Str = new String(key2);

        if (expirationManager.checkAndDeleteIfExpired(key1Str)) return RedisResponse.bulkString("");
        if (expirationManager.checkAndDeleteIfExpired(key2Str)) return RedisResponse.bulkString("");

        byte[] value1 = adapter.get(key1);
        byte[] value2 = adapter.get(key2);
        if (value1 == null) value1 = new byte[0];
        if (value2 == null) value2 = new byte[0];

        boolean lenOnly = false;
        boolean withIdx = false;

        for (int i = 2; i < command.getArgCount(); i++) {
            String option = new String(command.getArg(i)).toUpperCase();
            switch (option) {
                case "LEN": lenOnly = true; break;
                case "IDX": withIdx = true; break;
                case "MINMATCHLEN": if (i + 1 < command.getArgCount()) i++; break;
                case "WITHMATCHLEN": break;
            }
        }

        String lcs = computeLCS(new String(value1), new String(value2));

        if (lenOnly) return RedisResponse.integer(lcs.length());
        else if (withIdx) {
            List<RedisMessage> result = new ArrayList<>();
            result.add(RedisResponse.bulkString("matches"));
            result.add(new ArrayRedisMessage(new ArrayList<>()));
            result.add(RedisResponse.bulkString("len"));
            result.add(RedisResponse.integer(lcs.length()));
            return new ArrayRedisMessage(result);
        } else {
            return RedisResponse.bulkString(lcs);
        }
    }

    private RedisMessage executeStralgo(RedisCommand command) throws Exception {
        if (command.getArgCount() < 1) return RedisResponse.error("ERR wrong number of arguments for 'stralgo' command");
        String algo = new String(command.getArg(0)).toUpperCase();
        if (!"LCS".equals(algo)) return RedisResponse.error("ERR Unknown algorithm '" + algo + "'. Valid algorithms are: LCS");
        if (command.getArgCount() < 4) return RedisResponse.error("ERR wrong number of arguments for 'stralgo lcs' command");

        String inputType = new String(command.getArg(1)).toUpperCase();
        if (!"KEYS".equals(inputType) && !"STRINGS".equals(inputType)) return RedisResponse.error("ERR Expected 'KEYS' or 'STRINGS' but got '" + inputType + "'");

        byte[] input1 = command.getArg(2);
        byte[] input2 = command.getArg(3);
        byte[] value1, value2;

        if ("KEYS".equals(inputType)) {
            String key1Str = new String(input1);
            String key2Str = new String(input2);
            if (expirationManager.checkAndDeleteIfExpired(key1Str)) value1 = new byte[0];
            else {
                value1 = adapter.get(input1);
                if (value1 == null) value1 = new byte[0];
            }
            if (expirationManager.checkAndDeleteIfExpired(key2Str)) value2 = new byte[0];
            else {
                value2 = adapter.get(input2);
                if (value2 == null) value2 = new byte[0];
            }
        } else {
            value1 = input1;
            value2 = input2;
        }

        boolean lenOnly = false;
        boolean withIdx = false;
        for (int i = 4; i < command.getArgCount(); i++) {
            String option = new String(command.getArg(i)).toUpperCase();
            switch (option) {
                case "LEN": lenOnly = true; break;
                case "IDX": withIdx = true; break;
                case "MINMATCHLEN": if (i + 1 < command.getArgCount()) i++; break;
                case "WITHMATCHLEN": break;
            }
        }

        String lcs = computeLCS(new String(value1), new String(value2));

        if (lenOnly) return RedisResponse.integer(lcs.length());
        else if (withIdx) {
            List<RedisMessage> result = new ArrayList<>();
            result.add(RedisResponse.bulkString("matches"));
            result.add(new ArrayRedisMessage(new ArrayList<>()));
            result.add(RedisResponse.bulkString("len"));
            result.add(RedisResponse.integer(lcs.length()));
            return new ArrayRedisMessage(result);
        } else {
            return RedisResponse.bulkString(lcs);
        }
    }

    private String computeLCS(String str1, String str2) {
        int m = str1.length();
        int n = str2.length();
        int[][] dp = new int[m + 1][n + 1];

        for (int i = 1; i <= m; i++) {
            for (int j = 1; j <= n; j++) {
                if (str1.charAt(i - 1) == str2.charAt(j - 1)) {
                    dp[i][j] = dp[i - 1][j - 1] + 1;
                } else {
                    dp[i][j] = Math.max(dp[i - 1][j], dp[i][j - 1]);
                }
            }
        }

        StringBuilder lcs = new StringBuilder();
        int i = m, j = n;
        while (i > 0 && j > 0) {
            if (str1.charAt(i - 1) == str2.charAt(j - 1)) {
                lcs.insert(0, str1.charAt(i - 1));
                i--; j--;
            } else if (dp[i - 1][j] > dp[i][j - 1]) {
                i--;
            } else {
                j--;
            }
        }
        return lcs.toString();
    }
}
