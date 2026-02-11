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

import org.gnuhpc.fluss.cape.redis.protocol.RedisCommand;
import org.gnuhpc.fluss.cape.redis.protocol.RedisResponse;
import org.gnuhpc.fluss.cape.redis.storage.RedisSingleTableAdapter;
import org.gnuhpc.fluss.cape.redis.storage.RedisStorageAdapter;
import org.gnuhpc.fluss.cape.redis.util.GeoHashHelper;
import org.gnuhpc.fluss.cape.redis.util.RedisErrorSanitizer;

import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class GeoCommandExecutor implements RedisCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(GeoCommandExecutor.class);

    private final SortedSetCommandExecutor zsetExecutor;
    private final RedisStorageAdapter adapter;

    public GeoCommandExecutor(SortedSetCommandExecutor zsetExecutor, RedisStorageAdapter adapter) {
        this.zsetExecutor = zsetExecutor;
        this.adapter = adapter;
    }

    @Override
    public RedisMessage execute(RedisCommand command) {
        String cmd = command.getCommand();

        try {
            switch (cmd) {
                case "GEOADD":
                    return executeGeoAdd(command);
                case "GEODIST":
                    return executeGeoDist(command);
                case "GEOPOS":
                    return executeGeoPos(command);
                case "GEORADIUS":
                    return executeGeoRadius(command);
                case "GEORADIUS_RO":
                    return executeGeoRadiusRo(command);
                case "GEORADIUSBYMEMBER":
                    return executeGeoRadiusByMember(command);
                case "GEOSEARCH":
                    return executeGeoSearch(command);
                case "GEOSEARCHSTORE":
                    return executeGeoSearchStore(command);
                default:
                    return RedisResponse.error("ERR unknown geo command '" + cmd + "'");
            }
        } catch (Exception e) {
            LOG.error("Error executing geo command: {}", cmd, e);
            return RedisErrorSanitizer.sanitizeError(e, cmd);
        }
    }

    private RedisMessage executeGeoAdd(RedisCommand command) {
        if (command.getArgCount() < 4 || (command.getArgCount() - 1) % 3 != 0) {
            return RedisResponse.error("ERR wrong number of arguments for 'GEOADD' command");
        }

        String key = command.getArgAsString(0);

        boolean nxOption = false;
        boolean xxOption = false;
        boolean chOption = false;
        int startIndex = 1;

        while (startIndex < command.getArgCount()) {
            String arg = command.getArgAsString(startIndex);
            String upperArg = arg.toUpperCase();

            if (upperArg.equals("NX")) {
                nxOption = true;
                startIndex++;
            } else if (upperArg.equals("XX")) {
                xxOption = true;
                startIndex++;
            } else if (upperArg.equals("CH")) {
                chOption = true;
                startIndex++;
            } else {
                break;
            }
        }

        if (nxOption && xxOption) {
            return RedisResponse.error("ERR XX and NX options at the same time are not compatible");
        }

        if ((command.getArgCount() - startIndex) % 3 != 0) {
            return RedisResponse.error("ERR syntax error");
        }

        List<byte[]> zaddArgs = new ArrayList<>();
        zaddArgs.add(key.getBytes(StandardCharsets.UTF_8));

        if (xxOption) {
            zaddArgs.add("XX".getBytes(StandardCharsets.UTF_8));
        } else if (nxOption) {
            zaddArgs.add("NX".getBytes(StandardCharsets.UTF_8));
        }

        if (chOption) {
            zaddArgs.add("CH".getBytes(StandardCharsets.UTF_8));
        }

        int addedCount = 0;
        for (int i = startIndex; i < command.getArgCount(); i += 3) {
            double longitude;
            double latitude;
            try {
                longitude = Double.parseDouble(command.getArgAsString(i));
                latitude = Double.parseDouble(command.getArgAsString(i + 1));
            } catch (NumberFormatException e) {
                return RedisResponse.error("ERR invalid longitude or latitude");
            }

            if (!GeoHashHelper.isValidCoordinate(longitude, latitude)) {
                return RedisResponse.error("ERR invalid longitude or latitude");
            }

            String member = command.getArgAsString(i + 2);

            long geohash = GeoHashHelper.encode(longitude, latitude);
            double score = (double) geohash;

            zaddArgs.add(String.valueOf(score).getBytes(StandardCharsets.UTF_8));
            zaddArgs.add(member.getBytes(StandardCharsets.UTF_8));
        }

        RedisCommand zaddCommand = new RedisCommand("ZADD", zaddArgs);
        return zsetExecutor.execute(zaddCommand);
    }

    private RedisMessage executeGeoDist(RedisCommand command) {
        if (command.getArgCount() < 3 || command.getArgCount() > 4) {
            return RedisResponse.error("ERR wrong number of arguments for 'GEODIST' command");
        }

        String key = command.getArgAsString(0);
        String member1 = command.getArgAsString(1);
        String member2 = command.getArgAsString(2);
        String unit = command.getArgCount() == 4 ? command.getArgAsString(3) : "m";

        try {
            double[] pos1 = getMemberPosition(key, member1);
            if (pos1 == null) {
                return RedisResponse.nullBulkString();
            }

            double[] pos2 = getMemberPosition(key, member2);
            if (pos2 == null) {
                return RedisResponse.nullBulkString();
            }

            double distanceInMeters = GeoHashHelper.calculateDistance(pos1[0], pos1[1], pos2[0], pos2[1]);
            double result = GeoHashHelper.convertFromMeters(distanceInMeters, unit);

            return RedisResponse.bulkString(String.format("%.4f", result).getBytes(StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) {
            return RedisErrorSanitizer.sanitizeError(e, "GEODIST");
        }
    }

    private RedisMessage executeGeoPos(RedisCommand command) {
        if (command.getArgCount() < 2) {
            return RedisResponse.error("ERR wrong number of arguments for 'GEOPOS' command");
        }

        String key = command.getArgAsString(0);

        List<RedisMessage> results = new ArrayList<>();

        for (int i = 1; i < command.getArgCount(); i++) {
            String member = command.getArgAsString(i);
            double[] position = getMemberPosition(key, member);

            if (position == null) {
                results.add(RedisResponse.nullBulkString());
            } else {
                List<byte[]> coords = new ArrayList<>();
                coords.add(String.format("%.17g", position[0]).getBytes(StandardCharsets.UTF_8));
                coords.add(String.format("%.17g", position[1]).getBytes(StandardCharsets.UTF_8));
                results.add(RedisResponse.bytesArray(coords));
            }
        }

        return RedisResponse.array(results);
    }

    private RedisMessage executeGeoRadius(RedisCommand command) {
        return RedisResponse.error("ERR GEORADIUS is deprecated, use GEOSEARCH instead");
    }

    private RedisMessage executeGeoSearch(RedisCommand command) {
        if (command.getArgCount() < 4) {
            return RedisResponse.error("ERR wrong number of arguments for 'GEOSEARCH' command");
        }

        String key = command.getArgAsString(0);

        boolean fromMember = false;
        boolean fromLonLat = false;
        double centerLon = 0;
        double centerLat = 0;
        String sourceMember = null;

        boolean byRadius = false;
        boolean byBox = false;
        double radius = 0;
        double width = 0;
        double height = 0;
        String unit = "m";

        boolean withCoord = false;
        boolean withDist = false;
        boolean withHash = false;
        long count = 0;
        boolean asc = false;
        boolean desc = false;

        int i = 1;
        while (i < command.getArgCount()) {
            String arg = command.getArgAsString(i).toUpperCase();

            if (arg.equals("FROMMEMBER") && i + 1 < command.getArgCount()) {
                sourceMember = command.getArgAsString(i + 1);
                fromMember = true;
                i += 2;
            } else if (arg.equals("FROMLONLAT") && i + 2 < command.getArgCount()) {
                try {
                    centerLon = Double.parseDouble(command.getArgAsString(i + 1));
                    centerLat = Double.parseDouble(command.getArgAsString(i + 2));
                } catch (NumberFormatException e) {
                    return RedisResponse.error("ERR invalid longitude or latitude");
                }
                fromLonLat = true;
                i += 3;
            } else if (arg.equals("BYRADIUS") && i + 2 < command.getArgCount()) {
                try {
                    radius = Double.parseDouble(command.getArgAsString(i + 1));
                } catch (NumberFormatException e) {
                    return RedisResponse.error("ERR invalid radius");
                }
                unit = command.getArgAsString(i + 2);
                byRadius = true;
                i += 3;
            } else if (arg.equals("BYBOX") && i + 3 < command.getArgCount()) {
                try {
                    width = Double.parseDouble(command.getArgAsString(i + 1));
                    height = Double.parseDouble(command.getArgAsString(i + 2));
                } catch (NumberFormatException e) {
                    return RedisResponse.error("ERR invalid box dimensions");
                }
                unit = command.getArgAsString(i + 3);
                byBox = true;
                i += 4;
            } else if (arg.equals("WITHCOORD")) {
                withCoord = true;
                i++;
            } else if (arg.equals("WITHDIST")) {
                withDist = true;
                i++;
            } else if (arg.equals("WITHHASH")) {
                withHash = true;
                i++;
            } else if (arg.equals("COUNT") && i + 1 < command.getArgCount()) {
                try {
                    count = Long.parseLong(command.getArgAsString(i + 1));
                    if (count <= 0) {
                        return RedisResponse.error("ERR COUNT must be > 0");
                    }
                } catch (NumberFormatException e) {
                    return RedisResponse.error("ERR invalid count");
                }
                i += 2;
            } else if (arg.equals("ASC")) {
                asc = true;
                i++;
            } else if (arg.equals("DESC")) {
                desc = true;
                i++;
            } else {
                return RedisResponse.error("ERR syntax error");
            }
        }

        if (!fromMember && !fromLonLat) {
            return RedisResponse.error("ERR exactly one of FROMMEMBER or FROMLONLAT is required");
        }

        if (fromMember && fromLonLat) {
            return RedisResponse.error("ERR FROMMEMBER and FROMLONLAT can't be used together");
        }

        if (!byRadius && !byBox) {
            return RedisResponse.error("ERR exactly one of BYRADIUS or BYBOX is required");
        }

        if (byRadius && byBox) {
            return RedisResponse.error("ERR BYRADIUS and BYBOX can't be used together");
        }

        if (asc && desc) {
            return RedisResponse.error("ERR ASC and DESC can't be used together");
        }

        if (fromMember) {
            double[] pos = getMemberPosition(key, sourceMember);
            if (pos == null) {
                return RedisResponse.error("ERR could not decode requested zset member");
            }
            centerLon = pos[0];
            centerLat = pos[1];
        }

        double radiusMeters = byRadius ? GeoHashHelper.convertToMeters(radius, unit) : 0;
        double widthMeters = byBox ? GeoHashHelper.convertToMeters(width, unit) : 0;
        double heightMeters = byBox ? GeoHashHelper.convertToMeters(height, unit) : 0;

        List<byte[]> zrangeArgs = new ArrayList<>();
        zrangeArgs.add(key.getBytes(StandardCharsets.UTF_8));
        zrangeArgs.add("0".getBytes(StandardCharsets.UTF_8));
        zrangeArgs.add("-1".getBytes(StandardCharsets.UTF_8));
        zrangeArgs.add("WITHSCORES".getBytes(StandardCharsets.UTF_8));
        RedisCommand zrangeCommand = new RedisCommand("ZRANGE", zrangeArgs);
        RedisMessage zrangeResponse = zsetExecutor.execute(zrangeCommand);

        List<GeoHashHelper.GeoSearchResult> results = new ArrayList<>();

        if (zrangeResponse instanceof io.netty.handler.codec.redis.ArrayRedisMessage) {
            io.netty.handler.codec.redis.ArrayRedisMessage arrayMsg =
                    (io.netty.handler.codec.redis.ArrayRedisMessage) zrangeResponse;

            for (int idx = 0; idx < arrayMsg.children().size(); idx += 2) {
                if (idx + 1 >= arrayMsg.children().size()) break;

                RedisMessage memberMsg = arrayMsg.children().get(idx);
                RedisMessage scoreMsg = arrayMsg.children().get(idx + 1);

                if (memberMsg instanceof io.netty.handler.codec.redis.FullBulkStringRedisMessage &&
                    scoreMsg instanceof io.netty.handler.codec.redis.FullBulkStringRedisMessage) {

                    io.netty.handler.codec.redis.FullBulkStringRedisMessage memberBulk =
                            (io.netty.handler.codec.redis.FullBulkStringRedisMessage) memberMsg;
                    io.netty.handler.codec.redis.FullBulkStringRedisMessage scoreBulk =
                            (io.netty.handler.codec.redis.FullBulkStringRedisMessage) scoreMsg;

                    String member = memberBulk.content().toString(io.netty.util.CharsetUtil.UTF_8);
                    String scoreStr = scoreBulk.content().toString(io.netty.util.CharsetUtil.UTF_8);

                    try {
                        long geohash = (long) Double.parseDouble(scoreStr);
                        double[] pos = GeoHashHelper.decode(geohash);
                        double pointLon = pos[0];
                        double pointLat = pos[1];

                        boolean inRange = false;
                        if (byRadius) {
                            inRange = GeoHashHelper.isInRadius(centerLon, centerLat, pointLon, pointLat, radiusMeters);
                        } else if (byBox) {
                            inRange = GeoHashHelper.isInBox(centerLon, centerLat, pointLon, pointLat, widthMeters, heightMeters);
                        }

                        if (inRange) {
                            double distance = GeoHashHelper.calculateDistance(centerLon, centerLat, pointLon, pointLat);
                            results.add(new GeoHashHelper.GeoSearchResult(member, pointLon, pointLat, distance, geohash));
                        }
                    } catch (Exception e) {
                        LOG.warn("Failed to decode geohash for member {}", member, e);
                    }
                }
            }
        }

        if (asc) {
            results.sort((a, b) -> Double.compare(a.distance, b.distance));
        } else if (desc) {
            results.sort((a, b) -> Double.compare(b.distance, a.distance));
        }

        if (count > 0 && results.size() > count) {
            results = results.subList(0, (int) count);
        }

        List<RedisMessage> finalResults = new ArrayList<>();
        for (GeoHashHelper.GeoSearchResult result : results) {
            if (!withCoord && !withDist && !withHash) {
                finalResults.add(RedisResponse.bulkString(result.member.getBytes(StandardCharsets.UTF_8)));
            } else {
                List<RedisMessage> itemData = new ArrayList<>();
                itemData.add(RedisResponse.bulkString(result.member.getBytes(StandardCharsets.UTF_8)));

                if (withDist) {
                    double distInUnit = GeoHashHelper.convertFromMeters(result.distance, unit);
                    itemData.add(RedisResponse.bulkString(String.format("%.4f", distInUnit).getBytes(StandardCharsets.UTF_8)));
                }

                if (withHash) {
                    itemData.add(RedisResponse.bulkString(String.valueOf(result.geohash).getBytes(StandardCharsets.UTF_8)));
                }

                if (withCoord) {
                    List<byte[]> coords = new ArrayList<>();
                    coords.add(String.format("%.17g", result.longitude).getBytes(StandardCharsets.UTF_8));
                    coords.add(String.format("%.17g", result.latitude).getBytes(StandardCharsets.UTF_8));
                    itemData.add(RedisResponse.bytesArray(coords));
                }

                finalResults.add(RedisResponse.array(itemData));
            }
        }

        return RedisResponse.array(finalResults);
    }

    private double[] getMemberPosition(String key, String member) {
        List<byte[]> args = new ArrayList<>();
        args.add(key.getBytes(StandardCharsets.UTF_8));
        args.add(member.getBytes(StandardCharsets.UTF_8));
        RedisCommand zscoreCommand = new RedisCommand("ZSCORE", args);
        RedisMessage response = zsetExecutor.execute(zscoreCommand);

        if (response instanceof io.netty.handler.codec.redis.FullBulkStringRedisMessage) {
            io.netty.handler.codec.redis.FullBulkStringRedisMessage bulkMsg =
                    (io.netty.handler.codec.redis.FullBulkStringRedisMessage) response;

            if (bulkMsg.isNull()) {
                return null;
            }

            try {
                String scoreStr = bulkMsg.content().toString(io.netty.util.CharsetUtil.UTF_8);
                long geohash = (long) Double.parseDouble(scoreStr);
                return GeoHashHelper.decode(geohash);
            } catch (Exception e) {
                LOG.warn("Failed to decode geohash for member {}", member, e);
                return null;
            } finally {
                bulkMsg.release();
            }
        }

        return null;
    }

    private RedisMessage executeGeoSearchStore(RedisCommand command) {
        if (command.getArgCount() < 5) {
            return RedisResponse.error("ERR wrong number of arguments for 'GEOSEARCHSTORE' command");
        }

        String destKey = command.getArgAsString(0);
        String sourceKey = command.getArgAsString(1);

        boolean storeDist = false;
        List<String> searchArgs = new ArrayList<>();
        searchArgs.add(sourceKey);

        for (int i = 2; i < command.getArgCount(); i++) {
            String arg = command.getArgAsString(i);
            if (arg.toUpperCase().equals("STOREDIST")) {
                storeDist = true;
            } else {
                searchArgs.add(arg);
            }
        }

        List<byte[]> searchCmdArgs = new ArrayList<>();
        for (String arg : searchArgs) {
            searchCmdArgs.add(arg.getBytes(StandardCharsets.UTF_8));
        }
        RedisCommand searchCommand = new RedisCommand("GEOSEARCH", searchCmdArgs);

        RedisMessage searchResponse = executeGeoSearch(searchCommand);

        if (searchResponse instanceof io.netty.handler.codec.redis.ErrorRedisMessage) {
            return searchResponse;
        }

        if (!(searchResponse instanceof io.netty.handler.codec.redis.ArrayRedisMessage)) {
            return RedisResponse.error("ERR unexpected response from GEOSEARCH");
        }

        io.netty.handler.codec.redis.ArrayRedisMessage arrayResponse =
                (io.netty.handler.codec.redis.ArrayRedisMessage) searchResponse;

        List<byte[]> zaddArgs = new ArrayList<>();
        zaddArgs.add(destKey.getBytes(StandardCharsets.UTF_8));

        int storedCount = 0;

        for (RedisMessage itemMsg : arrayResponse.children()) {
            if (itemMsg instanceof io.netty.handler.codec.redis.FullBulkStringRedisMessage) {
                io.netty.handler.codec.redis.FullBulkStringRedisMessage bulkMsg =
                        (io.netty.handler.codec.redis.FullBulkStringRedisMessage) itemMsg;
                String member = bulkMsg.content().toString(io.netty.util.CharsetUtil.UTF_8);

                double[] pos = getMemberPosition(sourceKey, member);
                if (pos != null) {
                    long geohash = GeoHashHelper.encode(pos[0], pos[1]);
                    double score = storeDist ? 0.0 : (double) geohash;

                    zaddArgs.add(String.valueOf(score).getBytes(StandardCharsets.UTF_8));
                    zaddArgs.add(member.getBytes(StandardCharsets.UTF_8));
                    storedCount++;
                }
            } else if (itemMsg instanceof io.netty.handler.codec.redis.ArrayRedisMessage) {
                io.netty.handler.codec.redis.ArrayRedisMessage itemArray =
                        (io.netty.handler.codec.redis.ArrayRedisMessage) itemMsg;

                if (itemArray.children().isEmpty()) continue;

                RedisMessage firstChild = itemArray.children().get(0);
                if (!(firstChild instanceof io.netty.handler.codec.redis.FullBulkStringRedisMessage)) continue;

                io.netty.handler.codec.redis.FullBulkStringRedisMessage memberBulk =
                        (io.netty.handler.codec.redis.FullBulkStringRedisMessage) firstChild;
                String member = memberBulk.content().toString(io.netty.util.CharsetUtil.UTF_8);

                double score;
                if (storeDist && itemArray.children().size() > 1) {
                    RedisMessage distChild = itemArray.children().get(1);
                    if (distChild instanceof io.netty.handler.codec.redis.FullBulkStringRedisMessage) {
                        io.netty.handler.codec.redis.FullBulkStringRedisMessage distBulk =
                                (io.netty.handler.codec.redis.FullBulkStringRedisMessage) distChild;
                        score = Double.parseDouble(distBulk.content().toString(io.netty.util.CharsetUtil.UTF_8));
                    } else {
                        continue;
                    }
                } else {
                    double[] pos = getMemberPosition(sourceKey, member);
                    if (pos == null) continue;
                    long geohash = GeoHashHelper.encode(pos[0], pos[1]);
                    score = (double) geohash;
                }

                zaddArgs.add(String.valueOf(score).getBytes(StandardCharsets.UTF_8));
                zaddArgs.add(member.getBytes(StandardCharsets.UTF_8));
                storedCount++;
            }
        }

        if (storedCount == 0) {
            return RedisResponse.integer(0);
        }

        RedisCommand zaddCommand = new RedisCommand("ZADD", zaddArgs);
        RedisMessage zaddResponse = zsetExecutor.execute(zaddCommand);

        if (zaddResponse instanceof io.netty.handler.codec.redis.IntegerRedisMessage) {
            return RedisResponse.integer(storedCount);
        }

        return RedisResponse.integer(storedCount);
    }

    private RedisMessage executeGeoRadiusRo(RedisCommand command) {
        if (command.getArgCount() < 5) {
            return RedisResponse.error("ERR wrong number of arguments for 'GEORADIUS_RO' command");
        }

        List<byte[]> geosearchArgs = new ArrayList<>();
        geosearchArgs.add(command.getArg(0));
        geosearchArgs.add("FROMLONLAT".getBytes(StandardCharsets.UTF_8));
        geosearchArgs.add(command.getArg(1));
        geosearchArgs.add(command.getArg(2));
        geosearchArgs.add("BYRADIUS".getBytes(StandardCharsets.UTF_8));
        geosearchArgs.add(command.getArg(3));
        geosearchArgs.add(command.getArg(4));
        
        for (int i = 5; i < command.getArgCount(); i++) {
            geosearchArgs.add(command.getArg(i));
        }
        
        RedisCommand geosearchCommand = new RedisCommand("GEOSEARCH", geosearchArgs);
        return executeGeoSearch(geosearchCommand);
    }

    private RedisMessage executeGeoRadiusByMember(RedisCommand command) {
        if (command.getArgCount() < 4) {
            return RedisResponse.error("ERR wrong number of arguments for 'GEORADIUSBYMEMBER' command");
        }

        List<byte[]> geosearchArgs = new ArrayList<>();
        geosearchArgs.add(command.getArg(0));
        geosearchArgs.add("FROMMEMBER".getBytes(StandardCharsets.UTF_8));
        geosearchArgs.add(command.getArg(1));
        geosearchArgs.add("BYRADIUS".getBytes(StandardCharsets.UTF_8));
        geosearchArgs.add(command.getArg(2));
        geosearchArgs.add(command.getArg(3));
        
        for (int i = 4; i < command.getArgCount(); i++) {
            geosearchArgs.add(command.getArg(i));
        }
        
        RedisCommand geosearchCommand = new RedisCommand("GEOSEARCH", geosearchArgs);
        return executeGeoSearch(geosearchCommand);
    }
}
