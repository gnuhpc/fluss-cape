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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;

import java.util.ArrayList;
import java.util.List;

/** Helper class to create Redis responses in RESP protocol format. */
public class RedisResponse {

    public static RedisMessage ok() {
        return new SimpleStringRedisMessage("OK");
    }

    public static RedisMessage error(String message) {
        return new ErrorRedisMessage(message);
    }

    public static RedisMessage integer(long value) {
        return new IntegerRedisMessage(value);
    }

    public static RedisMessage bulkString(byte[] data) {
        if (data == null) {
            return FullBulkStringRedisMessage.NULL_INSTANCE;
        }
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        return new FullBulkStringRedisMessage(buf);
    }

    public static RedisMessage bulkString(String data) {
        if (data == null) {
            return FullBulkStringRedisMessage.NULL_INSTANCE;
        }
        return bulkString(data.getBytes());
    }

    public static RedisMessage nullBulkString() {
        return FullBulkStringRedisMessage.NULL_INSTANCE;
    }

    public static RedisMessage array(List<RedisMessage> elements) {
        if (elements == null) {
            return ArrayRedisMessage.NULL_INSTANCE;
        }
        return new ArrayRedisMessage(elements);
    }

    public static RedisMessage emptyArray() {
        return ArrayRedisMessage.EMPTY_INSTANCE;
    }

    public static RedisMessage stringArray(List<String> strings) {
        if (strings == null) {
            return ArrayRedisMessage.NULL_INSTANCE;
        }
        List<RedisMessage> messages = new ArrayList<>();
        for (String s : strings) {
            messages.add(bulkString(s));
        }
        return new ArrayRedisMessage(messages);
    }

    public static RedisMessage bytesArray(List<byte[]> bytesList) {
        if (bytesList == null) {
            return ArrayRedisMessage.NULL_INSTANCE;
        }
        List<RedisMessage> messages = new ArrayList<>();
        for (byte[] bytes : bytesList) {
            messages.add(bulkString(bytes));
        }
        return new ArrayRedisMessage(messages);
    }
}
