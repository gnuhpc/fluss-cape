# Phase 2.3: TTL/Expiration Support - Architecture Design

## Overview

Implement Redis TTL (Time To Live) functionality for automatic key expiration in Fluss-backed Redis compatibility layer.

## Current Schema

```sql
CREATE TABLE default.redis_data (
  redis_key STRING,
  redis_type STRING,
  sub_key STRING,
  score DOUBLE,
  value BYTES,
  PRIMARY KEY (redis_key, sub_key)
);
```

## Proposed Schema Update

```sql
CREATE TABLE default.redis_data (
  redis_key STRING,
  redis_type STRING,
  sub_key STRING,
  score DOUBLE,
  value BYTES,
  expire_at BIGINT,  -- NEW: Unix timestamp in milliseconds (NULL = no expiration)
  PRIMARY KEY (redis_key, sub_key)
);
```

**Why milliseconds?**
- Redis uses seconds for EXPIRE, milliseconds for PEXPIRE
- Internal storage in milliseconds allows both granularities
- Smaller storage overhead than storing both seconds and milliseconds

---

## TTL Architecture

### Two-Tier Expiration Strategy

**1. Lazy Deletion (Primary)**
- Check expiration on every read operation
- If expired: delete immediately and return null/empty
- No additional background overhead for reads
- Guarantees expired keys never returned to clients

**2. Background Scanner (Secondary)**
- Periodic scan to clean up expired keys that are never read
- Prevents unbounded storage growth from write-only keys
- Configurable scan interval (default: 60 seconds)
- Scans in batches to avoid long pauses

### Design Rationale

This matches Redis's expiration model:
- Redis also uses lazy deletion + periodic sampling
- Lazy deletion is fast and deterministic
- Background scanner prevents memory leaks

---

## Implementation Components

### 1. TTL Storage Layer

**New Interface: `ExpirationManager`**

```java
public class ExpirationManager {
    private final RedisFlussAdapter adapter;
    
    // Set expiration for a key (all sub-keys inherit same expiration)
    void setExpiration(String redisKey, long expireAtMs) throws Exception;
    
    // Get expiration timestamp (null if no expiration)
    Long getExpiration(String redisKey) throws Exception;
    
    // Remove expiration (make key persistent)
    void removeExpiration(String redisKey) throws Exception;
    
    // Check if key is expired (lazy deletion check)
    boolean isExpired(String redisKey) throws Exception;
    
    // Delete expired key (call from lazy deletion)
    void deleteExpiredKey(String redisKey) throws Exception;
}
```

**Storage Strategy:**
- Expiration stored in `sub_key = "__expire__"` row (similar to metadata)
- Single expiration timestamp per redis_key (all sub-keys expire together)
- Metadata row structure:
  ```
  redis_key: "mykey"
  sub_key: "__expire__"
  expire_at: 1704985200000  (Unix timestamp in ms)
  ```

### 2. Command Executors

**New: `ExpirationCommandExecutor`**

```java
public class ExpirationCommandExecutor implements RedisCommandExecutor {
    private final ExpirationManager expirationManager;
    
    RedisMessage execute(RedisCommand command) {
        switch (command.getCommand()) {
            case "EXPIRE":   return executeExpire(command);
            case "EXPIREAT": return executeExpireAt(command);
            case "PEXPIRE":  return executePExpire(command);
            case "TTL":      return executeTTL(command);
            case "PTTL":     return executePTTL(command);
            case "PERSIST":  return executePersist(command);
        }
    }
}
```

**Commands to implement:**

| Command | Description | Return |
|---------|-------------|--------|
| `EXPIRE key seconds` | Set expiration in seconds | 1 if set, 0 if key doesn't exist |
| `EXPIREAT key timestamp` | Set expiration at Unix timestamp (seconds) | 1 if set, 0 if key doesn't exist |
| `PEXPIRE key milliseconds` | Set expiration in milliseconds | 1 if set, 0 if key doesn't exist |
| `TTL key` | Get remaining time in seconds | Seconds, -1 if no expiration, -2 if doesn't exist |
| `PTTL key` | Get remaining time in milliseconds | Milliseconds, -1 if no expiration, -2 if doesn't exist |
| `PERSIST key` | Remove expiration | 1 if removed, 0 if no expiration or key doesn't exist |

### 3. Lazy Deletion Integration

**Modify all executors to check expiration:**

```java
// In StringCommandExecutor.executeGet()
public RedisMessage executeGet(RedisCommand command) throws Exception {
    String key = command.getArgAsString(0);
    
    // Lazy deletion check
    if (expirationManager.isExpired(key)) {
        expirationManager.deleteExpiredKey(key);
        return RedisResponse.nullBulkString();
    }
    
    // Normal GET logic
    byte[] value = adapter.get(key.getBytes());
    return value != null ? RedisResponse.bulkString(value) : RedisResponse.nullBulkString();
}
```

**Executors to update:**
- ✅ `StringCommandExecutor` (GET, EXISTS, INCR, INCRBY)
- ✅ `HashCommandExecutor` (HGET, HGETALL, HKEYS, HVALS, HLEN, HMGET, HEXISTS)
- ✅ `SetCommandExecutor` (SMEMBERS, SISMEMBER, SCARD)
- ✅ `ListCommandExecutor` (LRANGE, LLEN, LINDEX, LPOP, RPOP)
- ✅ `SortedSetCommandExecutor` (ZRANGE, ZSCORE, ZCARD, ZRANGEBYSCORE)
- ✅ `TypeCommandExecutor` (TYPE)

### 4. Background Expiration Scanner

**New Thread: `ExpirationScanner`**

```java
public class ExpirationScanner implements Runnable {
    private final RedisFlussAdapter adapter;
    private final ExpirationManager expirationManager;
    private final long scanIntervalMs;
    private final int batchSize;
    
    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(scanIntervalMs);
                scanAndDeleteExpiredKeys();
            } catch (InterruptedException e) {
                break;
            }
        }
    }
    
    private void scanAndDeleteExpiredKeys() {
        // Scan redis_data table for __expire__ rows
        // Check if expired
        // Delete expired keys in batches
    }
}
```

**Scanner Configuration:**
- **Scan interval**: 60 seconds (default)
- **Batch size**: 1000 keys per scan cycle
- **Throttling**: Sleep 10ms between batches to avoid CPU spikes

**Scanner Algorithm:**
1. Scan table for rows with `sub_key = "__expire__"`
2. For each expiration entry:
   - Check if `expire_at < System.currentTimeMillis()`
   - If expired: delete all rows with same `redis_key`
3. Process in batches (1000 keys at a time)
4. Sleep between batches to throttle

---

## Performance Considerations

### Read Operations

**Before (without TTL check):**
```
GET mykey → 1 Fluss lookup → 50μs
```

**After (with lazy deletion):**
```
GET mykey → 
  1. Lookup __expire__ (1 Fluss lookup) → 50μs
  2. Check expiration (in-memory comparison) → <1μs
  3. If not expired, lookup value (1 Fluss lookup) → 50μs
Total: ~100μs (2x overhead)
```

**Optimization: Cache expiration timestamps**
```java
// In-memory LRU cache for expiration timestamps
private final Cache<String, Long> expirationCache = 
    Caffeine.newBuilder()
        .maximumSize(10_000)
        .expireAfterWrite(Duration.ofSeconds(10))
        .build();

// Optimized isExpired check
boolean isExpired(String redisKey) {
    Long expireAt = expirationCache.get(redisKey, k -> {
        // Cache miss: fetch from Fluss
        return getExpirationFromFluss(k);
    });
    
    if (expireAt == null) return false; // No expiration
    return System.currentTimeMillis() > expireAt;
}
```

**With cache (hot path):**
```
GET mykey → 
  1. Check cache for expiration (in-memory) → <1μs
  2. Lookup value (1 Fluss lookup) → 50μs
Total: ~50μs (no overhead for cached expirations!)
```

### Write Operations

**EXPIRE command overhead:**
```
EXPIRE mykey 60 →
  1. Check key exists (1 Fluss lookup) → 50μs
  2. Write __expire__ row (1 Fluss upsert) → 100μs
Total: ~150μs
```

**SET with EXPIRE:**
```
SET mykey value
EXPIRE mykey 60
→ 2 Fluss writes (~200μs)

Future optimization: SETEX (single write)
```

### Background Scanner Impact

**Worst case (1M keys with expiration):**
- Scan 1M expiration entries → ~30 seconds (at 30k scans/sec)
- Delete 10k expired keys/minute → ~1 second
- Total overhead: ~31 seconds every 60 seconds = **~50% CPU**

**Mitigation:**
1. **Throttling**: Sleep 10ms between 1000-key batches → Reduces to ~10% CPU
2. **Lazy-only mode**: Disable scanner for read-heavy workloads
3. **Adaptive scanning**: Scan faster if many keys expiring, slower if few

---

## Migration Path

### For Existing Deployments

**Option 1: Schema migration (recommended)**

```sql
-- Add expire_at column to existing table
ALTER TABLE default.redis_data ADD COLUMN expire_at BIGINT;

-- No data migration needed (NULL = no expiration)
```

**Option 2: Create new table + migrate**

```sql
-- Create new table with expire_at
CREATE TABLE default.redis_data_v2 (
  redis_key STRING,
  redis_type STRING,
  sub_key STRING,
  score DOUBLE,
  value BYTES,
  expire_at BIGINT,
  PRIMARY KEY (redis_key, sub_key)
);

-- Copy data (all keys get NULL expiration)
INSERT INTO redis_data_v2 
SELECT redis_key, redis_type, sub_key, score, value, NULL 
FROM redis_data;

-- Rename tables
DROP TABLE redis_data;
ALTER TABLE redis_data_v2 RENAME TO redis_data;
```

### For New Deployments

Use new schema from day 1:

```sql
CREATE TABLE default.redis_data (
  redis_key STRING,
  redis_type STRING,
  sub_key STRING,
  score DOUBLE,
  value BYTES,
  expire_at BIGINT,
  PRIMARY KEY (redis_key, sub_key)
);
```

---

## Configuration

### Environment Variables

```bash
# Enable/disable TTL support (default: true)
REDIS_TTL_ENABLED=true

# Background scanner interval in seconds (default: 60)
REDIS_EXPIRATION_SCAN_INTERVAL=60

# Background scanner batch size (default: 1000)
REDIS_EXPIRATION_BATCH_SIZE=1000

# Enable/disable background scanner (default: true)
REDIS_EXPIRATION_SCANNER_ENABLED=true

# Expiration cache size (default: 10000)
REDIS_EXPIRATION_CACHE_SIZE=10000

# Expiration cache TTL in seconds (default: 10)
REDIS_EXPIRATION_CACHE_TTL=10
```

### Command Line Arguments

```bash
java -jar fluss-hbase-compat.jar \
  --redis.ttl.enabled=true \
  --redis.expiration.scan.interval=60 \
  --redis.expiration.batch.size=1000 \
  --redis.expiration.scanner.enabled=true \
  --redis.expiration.cache.size=10000 \
  --redis.expiration.cache.ttl=10
```

---

## Testing Strategy

### Functional Tests

1. **Basic expiration**
   ```bash
   SET mykey value
   EXPIRE mykey 1
   sleep 2
   GET mykey  # Should return null
   ```

2. **TTL accuracy**
   ```bash
   SET mykey value
   EXPIRE mykey 60
   TTL mykey  # Should return ~60
   sleep 10
   TTL mykey  # Should return ~50
   ```

3. **PERSIST**
   ```bash
   SET mykey value
   EXPIRE mykey 10
   PERSIST mykey
   TTL mykey  # Should return -1
   sleep 15
   GET mykey  # Should still exist
   ```

4. **Expiration on different data types**
   ```bash
   HSET myhash field value
   EXPIRE myhash 1
   # Wait 2 seconds
   HGETALL myhash  # Should return empty
   ```

5. **Background scanner**
   ```bash
   # Create 10k keys with 1-second expiration
   for i in {1..10000}; do
     SET key$i value$i
     EXPIRE key$i 1
   done
   
   # Wait 65 seconds (1s expire + 60s scanner + 4s margin)
   sleep 65
   
   # Check all keys deleted
   for i in {1..10000}; do
     EXISTS key$i  # Should return 0
   done
   ```

### Performance Tests

1. **Lazy deletion overhead**
   - Benchmark GET with/without TTL
   - Expected: <2x overhead, <100μs with cache

2. **EXPIRE command throughput**
   - Benchmark EXPIRE operations
   - Expected: >5000 ops/sec

3. **Background scanner impact**
   - Monitor CPU usage during scan
   - Expected: <10% CPU with throttling

---

## Implementation Order

1. ✅ **Design architecture** (this document)
2. ⏳ **Update schema** - Add expire_at column support
3. ⏳ **Implement ExpirationManager** - Core TTL logic
4. ⏳ **Implement ExpirationCommandExecutor** - EXPIRE/TTL commands
5. ⏳ **Update all executors** - Add lazy deletion checks
6. ⏳ **Implement ExpirationScanner** - Background cleanup
7. ⏳ **Add caching** - Optimize hot path performance
8. ⏳ **Create tests** - Functional + performance validation
9. ⏳ **Documentation** - Update README and create usage guide

---

## Known Limitations

### 1. Per-Key Expiration Only

**Limitation**: All sub-keys of a redis_key expire together.

**Example:**
```bash
HSET myhash field1 value1
HSET myhash field2 value2
EXPIRE myhash 60  # Both fields expire together
```

**Why**: Fluss table design groups sub-keys under single redis_key.

**Workaround**: Redis also expires entire hash/set/list together, so this matches Redis semantics.

### 2. Expiration Precision

**Limitation**: Background scanner has 60-second granularity by default.

**Impact**: Keys may exist up to 60 seconds after expiration if not read (lazy deletion).

**Mitigation**: 
- Lazy deletion always triggers on read (deterministic)
- Reduce scan interval for time-sensitive applications
- Most applications tolerate this (Redis also has sampling delay)

### 3. Large Key Deletion

**Limitation**: Deleting large collections (10k+ sub-keys) may cause latency spike.

**Example:**
```bash
# Large sorted set with 100k members
ZADD largeset $(for i in {1..100000}; do echo "$i member$i"; done)
EXPIRE largeset 1
# After 1 second: Deletion of 100k rows may take ~1 second
```

**Mitigation**:
- Batch deletions in background scanner
- Consider "soft delete" flag for very large keys (future optimization)

### 4. No Cross-Table Consistency

**Limitation**: Main table and reverse index may temporarily diverge during expiration.

**Example**: Sorted set members in `redis_zset_members` may exist briefly after main table expires.

**Mitigation**: Expiration manager deletes from all tables atomically (best effort).

---

## Future Enhancements

### Phase 2.3.9 - SETEX / PSETEX

Atomic SET + EXPIRE in single operation:

```bash
SETEX mykey 60 value  # SET + EXPIRE in one Fluss write
```

### Phase 2.3.10 - GETEX

Get value and update expiration:

```bash
GETEX mykey EX 60  # Get value and extend expiration to 60 seconds
```

### Phase 2.3.11 - Active Expiration

Instead of periodic scanning, subscribe to Fluss change events:

```java
// Listen to expiration events from Fluss
flussConnection.subscribeToExpirations(event -> {
    if (event.isExpired()) {
        deleteExpiredKey(event.getKey());
    }
});
```

### Phase 2.3.12 - Expiration Metrics

Expose Prometheus metrics:

```
redis_keys_expired_total{type="lazy"}
redis_keys_expired_total{type="scanner"}
redis_expiration_scan_duration_seconds
redis_expiration_cache_hit_rate
```

---

## Summary

**Architecture Decision:**
- ✅ Two-tier expiration: Lazy deletion (primary) + Background scanner (secondary)
- ✅ Store expiration as `__expire__` metadata row per redis_key
- ✅ In-memory LRU cache for expiration timestamps (10k entries, 10s TTL)
- ✅ Configurable background scanner (60s interval, 1000 batch size)

**Performance Target:**
- Read overhead: <2x (with cache: ~1.05x)
- EXPIRE throughput: >5000 ops/sec
- Scanner CPU: <10% with throttling

**Next Step:** Implement schema update and ExpirationManager class.
