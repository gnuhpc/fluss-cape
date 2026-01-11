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

package org.apache.fluss.redis.protocol;

import java.util.ArrayList;
import java.util.List;

/** Represents a parsed Redis command from RESP protocol. */
public class RedisCommand {
    private final String command;
    private final List<byte[]> args;

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
        return arg != null ? new String(arg) : null;
    }

    public static RedisCommand parse(io.netty.handler.codec.redis.ArrayRedisMessage message) {
        List<io.netty.handler.codec.redis.RedisMessage> children = message.children();
        if (children == null || children.isEmpty()) {
            throw new IllegalArgumentException("Empty Redis command");
        }

        // First element is the command name
        io.netty.handler.codec.redis.RedisMessage firstMsg = children.get(0);
        if (!(firstMsg instanceof io.netty.handler.codec.redis.FullBulkStringRedisMessage)) {
            throw new IllegalArgumentException("Invalid command format");
        }

        io.netty.handler.codec.redis.FullBulkStringRedisMessage bulkString =
                (io.netty.handler.codec.redis.FullBulkStringRedisMessage) firstMsg;
        byte[] commandBytes = new byte[bulkString.content().readableBytes()];
        bulkString.content().getBytes(bulkString.content().readerIndex(), commandBytes);
        String command = new String(commandBytes);

        // Remaining elements are arguments
        List<byte[]> args = new ArrayList<>();
        for (int i = 1; i < children.size(); i++) {
            io.netty.handler.codec.redis.RedisMessage argMsg = children.get(i);
            if (argMsg instanceof io.netty.handler.codec.redis.FullBulkStringRedisMessage) {
                io.netty.handler.codec.redis.FullBulkStringRedisMessage argBulk =
                        (io.netty.handler.codec.redis.FullBulkStringRedisMessage) argMsg;
                byte[] argBytes = new byte[argBulk.content().readableBytes()];
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
