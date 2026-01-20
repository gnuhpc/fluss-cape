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

package org.gnuhpc.fluss.cape.redis.util;

/**
 * Geohash encoding and decoding utilities for Redis geospatial commands.
 *
 * <p>Based on Redis/Valkey geohash implementation using 52-bit precision (26 bits per coordinate).
 * Coordinates are stored as sorted set scores using geohash encoding.
 *
 * <p>References:
 * - /root/valkey/src/geohash.c
 * - /root/valkey/src/geohash_helper.c
 */
public class GeoHashHelper {

    // Earth radius in meters (from WGS84)
    public static final double EARTH_RADIUS_IN_METERS = 6372797.560856;

    // Coordinate ranges (WGS84 limits)
    public static final double MIN_LATITUDE = -85.05112878;
    public static final double MAX_LATITUDE = 85.05112878;
    public static final double MIN_LONGITUDE = -180.0;
    public static final double MAX_LONGITUDE = 180.0;

    // Geohash precision (26 bits per coordinate = 52 bits total)
    public static final int GEO_STEP_MAX = 26;

    // Unit conversion constants
    private static final double TO_METERS = 1.0;
    private static final double TO_KILOMETERS = 1000.0;
    private static final double TO_MILES = 1609.34;
    private static final double TO_FEET = 0.3048;

    /**
     * Encode latitude and longitude into a 52-bit geohash.
     *
     * <p>Algorithm:
     * 1. Normalize lat/lon to [0, 1] range
     * 2. Convert to 26-bit integers
     * 3. Interleave bits (lon in even positions, lat in odd positions)
     *
     * @param longitude in degrees [-180, 180]
     * @param latitude in degrees [-85.05112878, 85.05112878]
     * @return 52-bit geohash as a long
     * @throws IllegalArgumentException if coordinates are out of range
     */
    public static long encode(double longitude, double latitude) {
        if (latitude < MIN_LATITUDE || latitude > MAX_LATITUDE) {
            throw new IllegalArgumentException(
                    "Latitude must be in range [" + MIN_LATITUDE + ", " + MAX_LATITUDE + "]");
        }
        if (longitude < MIN_LONGITUDE || longitude > MAX_LONGITUDE) {
            throw new IllegalArgumentException(
                    "Longitude must be in range [" + MIN_LONGITUDE + ", " + MAX_LONGITUDE + "]");
        }

        // Normalize to [0, 1]
        double latOffset = (latitude - MIN_LATITUDE) / (MAX_LATITUDE - MIN_LATITUDE);
        double lonOffset = (longitude - MIN_LONGITUDE) / (MAX_LONGITUDE - MIN_LONGITUDE);

        // Convert to 26-bit integers
        long latBits = (long) (latOffset * 0x3FFFFFF); // 2^26 - 1
        long lonBits = (long) (lonOffset * 0x3FFFFFF);

        // Interleave bits
        long hash = 0;
        for (int i = 0; i < GEO_STEP_MAX; i++) {
            hash |= ((lonBits >> i) & 1) << (2 * i);      // lon in even positions
            hash |= ((latBits >> i) & 1) << (2 * i + 1);  // lat in odd positions
        }

        return hash;
    }

    /**
     * Decode a 52-bit geohash back to latitude and longitude.
     *
     * @param geohash 52-bit geohash
     * @return [longitude, latitude] in degrees
     */
    public static double[] decode(long geohash) {
        long latBits = 0;
        long lonBits = 0;

        // De-interleave bits
        for (int i = 0; i < GEO_STEP_MAX; i++) {
            lonBits |= ((geohash >> (2 * i)) & 1) << i;      // extract lon from even positions
            latBits |= ((geohash >> (2 * i + 1)) & 1) << i;  // extract lat from odd positions
        }

        // Convert back to [0, 1] range
        double latOffset = (double) latBits / 0x3FFFFFF;
        double lonOffset = (double) lonBits / 0x3FFFFFF;

        // Denormalize to actual coordinates
        double latitude = MIN_LATITUDE + latOffset * (MAX_LATITUDE - MIN_LATITUDE);
        double longitude = MIN_LONGITUDE + lonOffset * (MAX_LONGITUDE - MIN_LONGITUDE);

        return new double[]{longitude, latitude};
    }

    /**
     * Calculate great-circle distance between two points using Haversine formula.
     *
     * @param lon1 first point longitude in degrees
     * @param lat1 first point latitude in degrees
     * @param lon2 second point longitude in degrees
     * @param lat2 second point latitude in degrees
     * @return distance in meters
     */
    public static double calculateDistance(double lon1, double lat1, double lon2, double lat2) {
        // Convert to radians
        double lat1r = Math.toRadians(lat1);
        double lon1r = Math.toRadians(lon1);
        double lat2r = Math.toRadians(lat2);
        double lon2r = Math.toRadians(lon2);

        double dlat = lat2r - lat1r;
        double dlon = lon2r - lon1r;

        // Haversine formula
        double a = Math.sin(dlat / 2) * Math.sin(dlat / 2)
                + Math.cos(lat1r) * Math.cos(lat2r)
                * Math.sin(dlon / 2) * Math.sin(dlon / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return EARTH_RADIUS_IN_METERS * c;
    }

    /**
     * Convert distance from specified unit to meters.
     *
     * @param distance distance value
     * @param unit unit string (m, km, mi, ft)
     * @return distance in meters
     * @throws IllegalArgumentException if unit is invalid
     */
    public static double convertToMeters(double distance, String unit) {
        String lowerUnit = unit.toLowerCase();
        switch (lowerUnit) {
            case "m":
                return distance * TO_METERS;
            case "km":
                return distance * TO_KILOMETERS;
            case "mi":
                return distance * TO_MILES;
            case "ft":
                return distance * TO_FEET;
            default:
                throw new IllegalArgumentException(
                        "Invalid unit '" + unit + "'. Supported: m, km, mi, ft");
        }
    }

    /**
     * Convert distance from meters to specified unit.
     *
     * @param distanceInMeters distance in meters
     * @param unit unit string (m, km, mi, ft)
     * @return distance in specified unit
     * @throws IllegalArgumentException if unit is invalid
     */
    public static double convertFromMeters(double distanceInMeters, String unit) {
        String lowerUnit = unit.toLowerCase();
        switch (lowerUnit) {
            case "m":
                return distanceInMeters / TO_METERS;
            case "km":
                return distanceInMeters / TO_KILOMETERS;
            case "mi":
                return distanceInMeters / TO_MILES;
            case "ft":
                return distanceInMeters / TO_FEET;
            default:
                throw new IllegalArgumentException(
                        "Invalid unit '" + unit + "'. Supported: m, km, mi, ft");
        }
    }

    /**
     * Validate coordinate ranges.
     *
     * @param longitude in degrees
     * @param latitude in degrees
     * @return true if coordinates are valid
     */
    public static boolean isValidCoordinate(double longitude, double latitude) {
        return latitude >= MIN_LATITUDE && latitude <= MAX_LATITUDE
                && longitude >= MIN_LONGITUDE && longitude <= MAX_LONGITUDE;
    }

    /**
     * Result container for decoded coordinates with member name.
     */
    public static class GeoPosition {
        public final String member;
        public final double longitude;
        public final double latitude;

        public GeoPosition(String member, double longitude, double latitude) {
            this.member = member;
            this.longitude = longitude;
            this.latitude = latitude;
        }
    }

    /**
     * Result container for distance calculations.
     */
    public static class GeoDistanceResult {
        public final double distance;
        public final String unit;

        public GeoDistanceResult(double distance, String unit) {
            this.distance = distance;
            this.unit = unit;
        }
    }

    public static class GeoSearchResult {
        public final String member;
        public final double longitude;
        public final double latitude;
        public final double distance;
        public final long geohash;

        public GeoSearchResult(String member, double longitude, double latitude, double distance, long geohash) {
            this.member = member;
            this.longitude = longitude;
            this.latitude = latitude;
            this.distance = distance;
            this.geohash = geohash;
        }
    }

    public static boolean isInRadius(double centerLon, double centerLat, double pointLon, double pointLat, double radiusMeters) {
        double distance = calculateDistance(centerLon, centerLat, pointLon, pointLat);
        return distance <= radiusMeters;
    }

    public static boolean isInBox(double centerLon, double centerLat, double pointLon, double pointLat, double widthMeters, double heightMeters) {
        double latDiff = Math.abs(pointLat - centerLat);
        double lonDiff = Math.abs(pointLon - centerLon);
        
        double latDistance = latDiff * 111320.0;
        double lonDistance = lonDiff * 111320.0 * Math.cos(Math.toRadians(centerLat));
        
        return lonDistance <= widthMeters / 2 && latDistance <= heightMeters / 2;
    }
}
