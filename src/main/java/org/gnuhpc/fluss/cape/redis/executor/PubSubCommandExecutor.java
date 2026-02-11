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
import org.gnuhpc.fluss.cape.redis.pubsub.PubSubManager;
import org.gnuhpc.fluss.cape.redis.util.RedisErrorSanitizer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PubSubCommandExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubCommandExecutor.class);

    private final PubSubManager pubSubManager;

    public PubSubCommandExecutor(PubSubManager pubSubManager) {
        this.pubSubManager = pubSubManager;
    }

    public PubSubManager getPubSubManager() {
        return pubSubManager;
    }

    @Override
    public RedisMessage execute(RedisCommand command) {
        String cmd = command.getCommand();

        try {
            switch (cmd) {
                case "PUBLISH":
                    return executePublish(command);
                case "SUBSCRIBE":
                    return executeSubscribe(command);
                case "UNSUBSCRIBE":
                    return executeUnsubscribe(command);
                case "PSUBSCRIBE":
                    return executePSubscribe(command);
                case "PUNSUBSCRIBE":
                    return executePUnsubscribe(command);
                case "PUBSUB":
                    return executePubSub(command);
                default:
                    return RedisResponse.error("ERR unknown pubsub command '" + cmd + "'");
            }
        } catch (Exception e) {
            LOG.error("Error executing pubsub command: {}", cmd, e);
            return RedisErrorSanitizer.sanitizeError(e, cmd);
        }
    }

    private RedisMessage executePublish(RedisCommand command) {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'publish' command");
        }

        String channel = command.getArgAsString(0);
        byte[] message = command.getArg(1);

        int recipientCount = pubSubManager.publish(channel, message);

        return RedisResponse.integer(recipientCount);
    }

    private RedisMessage executeSubscribe(RedisCommand command) {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'subscribe' command");
        }

        ChannelHandlerContext ctx = command.getContext();
        if (ctx == null) {
            return RedisResponse.error("ERR no channel context available");
        }

        List<RedisMessage> responses = new ArrayList<>();

        for (int i = 0; i < command.getArgCount(); i++) {
            String channel = command.getArgAsString(i);
            pubSubManager.subscribe(ctx, channel);

            int subscriptionCount = pubSubManager.getSubscriptionCount(ctx) + 
                                   pubSubManager.getPatternSubscriptionCount(ctx);

            List<RedisMessage> subscribeResponse = new ArrayList<>();
            subscribeResponse.add(RedisResponse.bulkString("subscribe"));
            subscribeResponse.add(RedisResponse.bulkString(channel));
            subscribeResponse.add(RedisResponse.integer(subscriptionCount));
            responses.add(new ArrayRedisMessage(subscribeResponse));
        }

        return new ArrayRedisMessage(responses);
    }

    private RedisMessage executeUnsubscribe(RedisCommand command) {
        ChannelHandlerContext ctx = command.getContext();
        if (ctx == null) {
            return RedisResponse.error("ERR no channel context available");
        }

        List<RedisMessage> responses = new ArrayList<>();

        if (command.getArgCount() == 0) {
            int subscriptionCount = 0;
            List<RedisMessage> unsubscribeResponse = new ArrayList<>();
            unsubscribeResponse.add(RedisResponse.bulkString("unsubscribe"));
            unsubscribeResponse.add(RedisResponse.nullBulkString());
            unsubscribeResponse.add(RedisResponse.integer(subscriptionCount));
            responses.add(new ArrayRedisMessage(unsubscribeResponse));
        } else {
            for (int i = 0; i < command.getArgCount(); i++) {
                String channel = command.getArgAsString(i);
                pubSubManager.unsubscribe(ctx, channel);

                int subscriptionCount = pubSubManager.getSubscriptionCount(ctx) +
                                       pubSubManager.getPatternSubscriptionCount(ctx);

                List<RedisMessage> unsubscribeResponse = new ArrayList<>();
                unsubscribeResponse.add(RedisResponse.bulkString("unsubscribe"));
                unsubscribeResponse.add(RedisResponse.bulkString(channel));
                unsubscribeResponse.add(RedisResponse.integer(subscriptionCount));
                responses.add(new ArrayRedisMessage(unsubscribeResponse));
            }
        }

        return new ArrayRedisMessage(responses);
    }

    private RedisMessage executePSubscribe(RedisCommand command) {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'psubscribe' command");
        }

        ChannelHandlerContext ctx = command.getContext();
        if (ctx == null) {
            return RedisResponse.error("ERR no channel context available");
        }

        List<RedisMessage> responses = new ArrayList<>();

        for (int i = 0; i < command.getArgCount(); i++) {
            String pattern = command.getArgAsString(i);
            pubSubManager.psubscribe(ctx, pattern);

            int subscriptionCount = pubSubManager.getSubscriptionCount(ctx) +
                                   pubSubManager.getPatternSubscriptionCount(ctx);

            List<RedisMessage> psubscribeResponse = new ArrayList<>();
            psubscribeResponse.add(RedisResponse.bulkString("psubscribe"));
            psubscribeResponse.add(RedisResponse.bulkString(pattern));
            psubscribeResponse.add(RedisResponse.integer(subscriptionCount));
            responses.add(new ArrayRedisMessage(psubscribeResponse));
        }

        return new ArrayRedisMessage(responses);
    }

    private RedisMessage executePUnsubscribe(RedisCommand command) {
        ChannelHandlerContext ctx = command.getContext();
        if (ctx == null) {
            return RedisResponse.error("ERR no channel context available");
        }

        List<RedisMessage> responses = new ArrayList<>();

        if (command.getArgCount() == 0) {
            int subscriptionCount = 0;
            List<RedisMessage> punsubscribeResponse = new ArrayList<>();
            punsubscribeResponse.add(RedisResponse.bulkString("punsubscribe"));
            punsubscribeResponse.add(RedisResponse.nullBulkString());
            punsubscribeResponse.add(RedisResponse.integer(subscriptionCount));
            responses.add(new ArrayRedisMessage(punsubscribeResponse));
        } else {
            for (int i = 0; i < command.getArgCount(); i++) {
                String pattern = command.getArgAsString(i);
                pubSubManager.punsubscribe(ctx, pattern);

                int subscriptionCount = pubSubManager.getSubscriptionCount(ctx) +
                                       pubSubManager.getPatternSubscriptionCount(ctx);

                List<RedisMessage> punsubscribeResponse = new ArrayList<>();
                punsubscribeResponse.add(RedisResponse.bulkString("punsubscribe"));
                punsubscribeResponse.add(RedisResponse.bulkString(pattern));
                punsubscribeResponse.add(RedisResponse.integer(subscriptionCount));
                responses.add(new ArrayRedisMessage(punsubscribeResponse));
            }
        }

        return new ArrayRedisMessage(responses);
    }

    private RedisMessage executePubSub(RedisCommand command) {
        if (command.getArgCount() < 1) {
            return RedisResponse.error("ERR wrong number of arguments for 'pubsub' command");
        }

        String subcommand = command.getArgAsString(0).toUpperCase();

        switch (subcommand) {
            case "CHANNELS":
                String pattern = command.getArgCount() > 1 ? command.getArgAsString(1) : null;
                List<String> channels = pubSubManager.getChannels(pattern);
                List<RedisMessage> channelMessages = new ArrayList<>();
                for (String ch : channels) {
                    channelMessages.add(RedisResponse.bulkString(ch));
                }
                return new ArrayRedisMessage(channelMessages);

            case "NUMSUB":
                List<RedisMessage> numsubResponse = new ArrayList<>();
                for (int i = 1; i < command.getArgCount(); i++) {
                    String channel = command.getArgAsString(i);
                    int count = pubSubManager.getNumSub(channel);
                    numsubResponse.add(RedisResponse.bulkString(channel));
                    numsubResponse.add(RedisResponse.integer(count));
                }
                return new ArrayRedisMessage(numsubResponse);

            case "NUMPAT":
                int numPat = pubSubManager.getNumPat();
                return RedisResponse.integer(numPat);

            default:
                return RedisResponse.error("ERR unknown PUBSUB subcommand '" + subcommand + "'");
        }
    }
}
