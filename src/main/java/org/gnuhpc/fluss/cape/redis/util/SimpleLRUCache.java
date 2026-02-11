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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SimpleLRUCache<K, V> {
    private final int maxSize;
    private final Map<K, V> cache;
    private final ReadWriteLock lock;

    public SimpleLRUCache(int maxSize) {
        this.maxSize = maxSize;
        this.lock = new ReentrantReadWriteLock();
        this.cache = new LinkedHashMap<K, V>(maxSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > SimpleLRUCache.this.maxSize;
            }
        };
    }

    public V get(K key) {
        lock.readLock().lock();
        try {
            return cache.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void put(K key, V value) {
        lock.writeLock().lock();
        try {
            cache.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public V computeIfAbsent(K key, java.util.function.Function<? super K, ? extends V> mappingFunction) {
        lock.readLock().lock();
        try {
            V value = cache.get(key);
            if (value != null) {
                return value;
            }
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            return cache.computeIfAbsent(key, mappingFunction);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void remove(K key) {
        lock.writeLock().lock();
        try {
            cache.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return cache.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            cache.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean containsKey(K key) {
        lock.readLock().lock();
        try {
            return cache.containsKey(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public V compute(K key, java.util.function.BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        lock.writeLock().lock();
        try {
            return cache.compute(key, remappingFunction);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public V computeIfPresent(K key, java.util.function.BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        lock.writeLock().lock();
        try {
            return cache.computeIfPresent(key, remappingFunction);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
