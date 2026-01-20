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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ScanCursorManager {

    private static final long CURSOR_EXPIRATION_MS = 300000;

    private final AtomicLong cursorIdGenerator = new AtomicLong(0);
    private final Map<Long, CursorState> cursors = new ConcurrentHashMap<>();

    public static class CursorState {
        public final List<String> items;
        public final int currentIndex;
        public final long createdAt;

        public CursorState(List<String> items, int currentIndex) {
            this.items = items;
            this.currentIndex = currentIndex;
            this.createdAt = System.currentTimeMillis();
        }

        public boolean isExpired() {
            return System.currentTimeMillis() - createdAt > CURSOR_EXPIRATION_MS;
        }
    }

    public static class ScanResult {
        public final long nextCursor;
        public final List<String> items;

        public ScanResult(long nextCursor, List<String> items) {
            this.nextCursor = nextCursor;
            this.items = items;
        }
    }

    public ScanResult scan(long cursor, List<String> allItems, int count) {
        cleanExpiredCursors();

        if (cursor == 0) {
            if (allItems.isEmpty()) {
                return new ScanResult(0, new ArrayList<>());
            }

            int batchSize = Math.min(count, allItems.size());
            List<String> batch = allItems.subList(0, batchSize);

            if (batchSize >= allItems.size()) {
                return new ScanResult(0, new ArrayList<>(batch));
            } else {
                long newCursor = cursorIdGenerator.incrementAndGet();
                cursors.put(newCursor, new CursorState(allItems, batchSize));
                return new ScanResult(newCursor, new ArrayList<>(batch));
            }
        } else {
            CursorState state = cursors.get(cursor);
            if (state == null || state.isExpired()) {
                cursors.remove(cursor);
                return new ScanResult(0, new ArrayList<>());
            }

            int startIndex = state.currentIndex;
            int endIndex = Math.min(startIndex + count, state.items.size());
            List<String> batch = state.items.subList(startIndex, endIndex);

            if (endIndex >= state.items.size()) {
                cursors.remove(cursor);
                return new ScanResult(0, new ArrayList<>(batch));
            } else {
                cursors.put(cursor, new CursorState(state.items, endIndex));
                return new ScanResult(cursor, new ArrayList<>(batch));
            }
        }
    }

    private void cleanExpiredCursors() {
        cursors.entrySet().removeIf(entry -> entry.getValue().isExpired());
    }

    public void clear() {
        cursors.clear();
    }
}
