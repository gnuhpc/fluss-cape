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

package org.gnuhpc.fluss.cape.redis.pubsub;

import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.regex.Pattern;

public class PubSubManager {

    private final Map<String, Set<ChannelHandlerContext>> channelSubscriptions;
    private final Map<String, Set<ChannelHandlerContext>> patternSubscriptions;
    
    public PubSubManager() {
        this.channelSubscriptions = new ConcurrentHashMap<>();
        this.patternSubscriptions = new ConcurrentHashMap<>();
    }

    public void subscribe(ChannelHandlerContext ctx, String channel) {
        channelSubscriptions
                .computeIfAbsent(channel, k -> new CopyOnWriteArraySet<>())
                .add(ctx);
    }

    public void unsubscribe(ChannelHandlerContext ctx, String channel) {
        Set<ChannelHandlerContext> subscribers = channelSubscriptions.get(channel);
        if (subscribers != null) {
            subscribers.remove(ctx);
            if (subscribers.isEmpty()) {
                channelSubscriptions.remove(channel);
            }
        }
    }

    public void psubscribe(ChannelHandlerContext ctx, String pattern) {
        patternSubscriptions
                .computeIfAbsent(pattern, k -> new CopyOnWriteArraySet<>())
                .add(ctx);
    }

    public void punsubscribe(ChannelHandlerContext ctx, String pattern) {
        Set<ChannelHandlerContext> subscribers = patternSubscriptions.get(pattern);
        if (subscribers != null) {
            subscribers.remove(ctx);
            if (subscribers.isEmpty()) {
                patternSubscriptions.remove(pattern);
            }
        }
    }

    public int publish(String channel, byte[] message) {
        int recipientCount = 0;

        Set<ChannelHandlerContext> directSubscribers = channelSubscriptions.get(channel);
        if (directSubscribers != null) {
            for (ChannelHandlerContext ctx : directSubscribers) {
                if (ctx.channel().isActive()) {
                    sendMessage(ctx, "message", channel, message);
                    recipientCount++;
                }
            }
        }

        for (Map.Entry<String, Set<ChannelHandlerContext>> entry : patternSubscriptions.entrySet()) {
            String pattern = entry.getKey();
            if (matchesPattern(channel, pattern)) {
                for (ChannelHandlerContext ctx : entry.getValue()) {
                    if (ctx.channel().isActive()) {
                        sendPatternMessage(ctx, pattern, channel, message);
                        recipientCount++;
                    }
                }
            }
        }

        return recipientCount;
    }

    public int getSubscriptionCount(ChannelHandlerContext ctx) {
        int count = 0;
        for (Set<ChannelHandlerContext> subscribers : channelSubscriptions.values()) {
            if (subscribers.contains(ctx)) {
                count++;
            }
        }
        return count;
    }

    public int getPatternSubscriptionCount(ChannelHandlerContext ctx) {
        int count = 0;
        for (Set<ChannelHandlerContext> subscribers : patternSubscriptions.values()) {
            if (subscribers.contains(ctx)) {
                count++;
            }
        }
        return count;
    }

    public List<String> getChannels(String pattern) {
        List<String> result = new ArrayList<>();
        if (pattern == null || pattern.isEmpty()) {
            result.addAll(channelSubscriptions.keySet());
        } else {
            for (String channel : channelSubscriptions.keySet()) {
                if (matchesPattern(channel, pattern)) {
                    result.add(channel);
                }
            }
        }
        return result;
    }

    public int getNumSub(String channel) {
        Set<ChannelHandlerContext> subscribers = channelSubscriptions.get(channel);
        return subscribers != null ? subscribers.size() : 0;
    }

    public int getNumPat() {
        int total = 0;
        for (Set<ChannelHandlerContext> subscribers : patternSubscriptions.values()) {
            total += subscribers.size();
        }
        return total;
    }

    private void sendMessage(ChannelHandlerContext ctx, String type, String channel, byte[] message) {
        List<Object> response = new ArrayList<>();
        response.add(type.getBytes());
        response.add(channel.getBytes());
        response.add(message);
        
        io.netty.handler.codec.redis.ArrayRedisMessage arrayMsg = 
            new io.netty.handler.codec.redis.ArrayRedisMessage(
                toRedisMessages(response)
            );
        
        ctx.writeAndFlush(arrayMsg);
    }

    private void sendPatternMessage(ChannelHandlerContext ctx, String pattern, String channel, byte[] message) {
        List<Object> response = new ArrayList<>();
        response.add("pmessage".getBytes());
        response.add(pattern.getBytes());
        response.add(channel.getBytes());
        response.add(message);
        
        io.netty.handler.codec.redis.ArrayRedisMessage arrayMsg = 
            new io.netty.handler.codec.redis.ArrayRedisMessage(
                toRedisMessages(response)
            );
        
        ctx.writeAndFlush(arrayMsg);
    }

    private List<io.netty.handler.codec.redis.RedisMessage> toRedisMessages(List<Object> objects) {
        List<io.netty.handler.codec.redis.RedisMessage> messages = new ArrayList<>();
        for (Object obj : objects) {
            if (obj instanceof byte[]) {
                io.netty.buffer.ByteBuf buf = io.netty.buffer.Unpooled.wrappedBuffer((byte[]) obj);
                messages.add(new io.netty.handler.codec.redis.FullBulkStringRedisMessage(buf));
            }
        }
        return messages;
    }

    private boolean matchesPattern(String channel, String pattern) {
        String regex = pattern
                .replace("*", ".*")
                .replace("?", ".")
                .replace("[", "\\[")
                .replace("]", "\\]");
        return Pattern.matches(regex, channel);
    }
}
