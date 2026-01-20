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

package org.gnuhpc.fluss.cape.redis.protocol;

import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RedisCommand {
    private static final Logger LOG = LoggerFactory.getLogger(RedisCommand.class);
    
    private static final int DEFAULT_MAX_BULK_LENGTH = 512 * 1024 * 1024;
    private static final int DEFAULT_MAX_MULTIBULK_LENGTH = 1024 * 1024;
    private static final int DEFAULT_MAX_COMMAND_LENGTH = 16;
    
    private static final int MAX_BULK_LENGTH;
    private static final int MAX_MULTIBULK_LENGTH;
    private static final int MAX_COMMAND_LENGTH;
    
    static {
        MAX_BULK_LENGTH = parseEnvInt("REDIS_MAX_BULK_LENGTH", DEFAULT_MAX_BULK_LENGTH, 1024, 1024 * 1024 * 1024);
        MAX_MULTIBULK_LENGTH = parseEnvInt("REDIS_MAX_MULTIBULK_LENGTH", DEFAULT_MAX_MULTIBULK_LENGTH, 1, 10 * 1024 * 1024);
        MAX_COMMAND_LENGTH = parseEnvInt("REDIS_MAX_COMMAND_LENGTH", DEFAULT_MAX_COMMAND_LENGTH, 1, 256);
        
        LOG.info("Redis input validation limits configured:");
        LOG.info("  MAX_BULK_LENGTH: {} bytes ({}MB)", MAX_BULK_LENGTH, MAX_BULK_LENGTH / (1024 * 1024));
        LOG.info("  MAX_MULTIBULK_LENGTH: {} arguments", MAX_MULTIBULK_LENGTH);
        LOG.info("  MAX_COMMAND_LENGTH: {} bytes", MAX_COMMAND_LENGTH);
    }
    
    private static int parseEnvInt(String envVar, int defaultValue, int minValue, int maxValue) {
        String value = System.getenv(envVar);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < minValue || parsed > maxValue) {
                LOG.warn("Invalid value for {}: {} (must be between {} and {}). Using default: {}",
                        envVar, parsed, minValue, maxValue, defaultValue);
                return defaultValue;
            }
            LOG.info("Using configured value for {}: {}", envVar, parsed);
            return parsed;
        } catch (NumberFormatException e) {
            LOG.warn("Invalid number format for {}: {}. Using default: {}", envVar, value, defaultValue);
            return defaultValue;
        }
    }

    private final String command;
    private final List<byte[]> args;
    private ChannelHandlerContext context;

    public RedisCommand(String command, List<byte[]> args) {
        this.command = command.toUpperCase();
        this.args = args;
    }

    public String getCommand() {
        return command;
    }

    public List<byte[]> getArgs() {
        return args;
    }

    public int getArgCount() {
        return args.size();
    }

    public byte[] getArg(int index) {
        if (index < 0 || index >= args.size()) {
            return null;
        }
        return args.get(index);
    }

    public String getArgAsString(int index) {
        byte[] arg = getArg(index);
        return arg != null ? new String(arg, StandardCharsets.UTF_8) : null;
    }

    public ChannelHandlerContext getContext() {
        return context;
    }

    public void setContext(ChannelHandlerContext context) {
        this.context = context;
    }

    public static RedisCommand parse(io.netty.handler.codec.redis.ArrayRedisMessage message) {
        List<io.netty.handler.codec.redis.RedisMessage> children = message.children();
        if (children == null || children.isEmpty()) {
            throw new IllegalArgumentException("Empty Redis command");
        }

        if (children.size() > MAX_MULTIBULK_LENGTH) {
            throw new IllegalArgumentException(
                    "Command array too large: "
                            + children.size()
                            + " (max: "
                            + MAX_MULTIBULK_LENGTH
                            + ")");
        }

        io.netty.handler.codec.redis.RedisMessage firstMsg = children.get(0);
        if (!(firstMsg instanceof io.netty.handler.codec.redis.FullBulkStringRedisMessage)) {
            throw new IllegalArgumentException("Invalid command format");
        }

        io.netty.handler.codec.redis.FullBulkStringRedisMessage bulkString =
                (io.netty.handler.codec.redis.FullBulkStringRedisMessage) firstMsg;
        int commandLength = bulkString.content().readableBytes();

        if (commandLength > MAX_COMMAND_LENGTH) {
            throw new IllegalArgumentException(
                    "Command name too long: " + commandLength + " bytes (max: " + MAX_COMMAND_LENGTH + ")");
        }

        byte[] commandBytes = new byte[commandLength];
        bulkString.content().getBytes(bulkString.content().readerIndex(), commandBytes);
        String command = new String(commandBytes, StandardCharsets.UTF_8);

        List<byte[]> args = new ArrayList<>();
        for (int i = 1; i < children.size(); i++) {
            io.netty.handler.codec.redis.RedisMessage argMsg = children.get(i);
            if (argMsg instanceof io.netty.handler.codec.redis.FullBulkStringRedisMessage) {
                io.netty.handler.codec.redis.FullBulkStringRedisMessage argBulk =
                        (io.netty.handler.codec.redis.FullBulkStringRedisMessage) argMsg;
                int argLength = argBulk.content().readableBytes();

                if (argLength > MAX_BULK_LENGTH) {
                    throw new IllegalArgumentException(
                            "Argument too large: "
                                    + argLength
                                    + " bytes (max: "
                                    + MAX_BULK_LENGTH
                                    + ")");
                }

                byte[] argBytes = new byte[argLength];
                argBulk.content().getBytes(argBulk.content().readerIndex(), argBytes);
                args.add(argBytes);
            }
        }

        return new RedisCommand(command, args);
    }

    @Override
    public String toString() {
        return "RedisCommand{" + "command='" + command + '\'' + ", argCount=" + args.size() + '}';
    }
}