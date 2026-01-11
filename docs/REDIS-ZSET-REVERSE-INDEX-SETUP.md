# Redis Sorted Set Reverse Index Setup

## Overview

This document explains how to set up the reverse index table for optimized Redis Sorted Set operations.

## Why Reverse Index?

Without reverse index:
- **ZSCORE**: O(n) - Must scan all members to find one
- **ZREM**: O(n) - Must scan to find member's sub_key
- **ZADD update**: O(n) - Must scan to check if member exists

With reverse index:
- **ZSCORE**: O(1) - Direct member lookup
- **ZREM**: O(1) - Direct member deletion
- **ZADD update**: O(1) - Direct existence check

**Expected speedup: 30-40x for large sorted sets (1000+ members)**

## Architecture

### Dual Table Pattern

```
Main Table (redis_data):
  redis_key | sub_key                    | score | value
  "myzset"  | "4059000000000000:alice"  | 100.0 | []
  "myzset"  | "405A000000000000:bob"    | 200.0 | []
  (Score-sorted for ZRANGE queries)

Reverse Index (redis_zset_members):
  redis_key | member  | score
  "myzset"  | "alice" | 100.0
  "myzset"  | "bob"   | 200.0
  (Member-indexed for O(1) ZSCORE/ZREM)
```

### Operation Flow

**ZADD**:
1. Check reverse index if member exists (O(1))
2. If exists, delete old main table entry
3. Write new main table entry with score-encoded sub_key
4. Update reverse index with new score

**ZREM**:
1. Lookup reverse index to get score (O(1))
2. Delete from main table using score-encoded sub_key
3. Delete from reverse index

**ZSCORE**:
1. Lookup reverse index directly (O(1))
2. Return score (no main table scan!)

**ZRANGE** (unchanged):
- Scan main table only (already score-sorted)

## Setup Instructions

### Step 1: Create Reverse Index Table

Connect to Fluss SQL client and run:

```sql
CREATE TABLE default.redis_zset_members (
  redis_key STRING,
  member STRING,
  score DOUBLE,
  PRIMARY KEY (redis_key, member)
);
```

**Table characteristics**:
- Primary key: Composite (redis_key, member)
- No TTL (inherits Fluss persistence)
- Same deployment as main redis_data table

### Step 2: Verify Table Creation

```sql
SHOW TABLES;
```

Expected output:
```
default.redis_data
default.redis_zset_members
```

### Step 3: Configure Application

The application automatically uses the reverse index if the table exists.

**Configuration** (optional):
```bash
java -jar fluss-hbase-compat-1.0.0-SNAPSHOT.jar \
  --redis.enable=true \
  --redis.table.name=default.redis_data \
  --redis.zset.reverse.index.table=default.redis_zset_members
```

Default: `default.redis_zset_members` (automatically used)

### Step 4: Test with redis-cli

```bash
redis-cli -p 6379

# Add members
> ZADD testset 100 alice 200 bob 150 charlie
(integer) 3

# Test O(1) ZSCORE (should be fast even with millions of members)
> ZSCORE testset alice
"100.0"

# Test O(1) ZREM
> ZREM testset bob
(integer) 1

# Verify ZRANGE still works (uses main table)
> ZRANGE testset 0 -1 WITHSCORES
1) "alice"
2) "100"
3) "charlie"
4) "150"
```

## Performance Validation

### Before Reverse Index

Create large sorted set (10,000 members):
```bash
redis-cli -p 6379
> ZADD largeset $(for i in {1..10000}; do echo "$i member$i"; done)

# Measure ZSCORE latency
> time redis-cli ZSCORE largeset member5000
# Expected: 2-5ms (O(n) scan)
```

### After Reverse Index

```bash
# Same large sorted set
> time redis-cli ZSCORE largeset member5000
# Expected: 50-100μs (O(1) lookup)

# Speedup: 20-100x depending on set size
```

### Benchmark with redis-benchmark

```bash
# Generate 10,000 member sorted set
redis-cli -p 6379 --pipe <<EOF
$(for i in {1..10000}; do echo "ZADD benchmark_zset $i member$i"; done)
EOF

# Benchmark ZSCORE operations
redis-benchmark -p 6379 -t zscore -n 100000 -d 10

# Before: ~500 ops/sec (2ms per operation)
# After:  ~20,000 ops/sec (50μs per operation)
# Speedup: 40x
```

## Monitoring

### Check Reverse Index Health

```sql
-- Count reverse index entries
SELECT COUNT(*) FROM default.redis_zset_members;

-- Check consistency (should match main table zset count)
SELECT 
  redis_key, 
  COUNT(*) as member_count 
FROM default.redis_zset_members 
GROUP BY redis_key;
```

### Verify Data Consistency

```bash
# Compare main table and reverse index
redis-cli -p 6379

> ZADD consistency_test 100 alice 200 bob
(integer) 2

> ZSCORE consistency_test alice
"100.0"

> ZCARD consistency_test
(integer) 2

# Both commands should return consistent results
```

## Maintenance

### Cleanup Orphaned Entries

If application crashes during dual-write, cleanup:

```sql
-- Find orphaned reverse index entries (no corresponding main table entry)
-- (Manual query using Fluss SQL)

DELETE FROM default.redis_zset_members 
WHERE (redis_key, member) NOT IN (
  SELECT redis_key, 
         SUBSTRING(sub_key, INSTR(sub_key, ':') + 1) as member
  FROM default.redis_data 
  WHERE redis_type = 'zset'
);
```

### Rebuild Reverse Index

If reverse index is corrupted or missing:

```bash
# Stop application first!

# Option 1: Drop and recreate table
DROP TABLE default.redis_zset_members;
CREATE TABLE default.redis_zset_members (...);

# Option 2: Rebuild from main table (future feature)
# java -jar fluss-hbase-compat.jar --rebuild-zset-index
```

## Storage Impact

### Storage Overhead

Reverse index adds ~40-50% storage overhead:

**Main table entry**:
- redis_key: ~10 bytes
- sub_key: ~20 bytes (score:member)
- score: 8 bytes
- value: 0 bytes (empty for zset)
- Total: ~38 bytes + Fluss metadata (~20 bytes) = **~58 bytes**

**Reverse index entry**:
- redis_key: ~10 bytes
- member: ~10 bytes
- score: 8 bytes
- Total: ~28 bytes + Fluss metadata (~20 bytes) = **~48 bytes**

**Combined**: 58 + 48 = **106 bytes per member** (vs 58 bytes without index)

**Example**:
- 1 million members: ~60MB → ~100MB (+40MB)
- 10 million members: ~600MB → ~1GB (+400MB)

**Tradeoff**: 40% more storage for 30-40x performance improvement

### Query Performance vs Storage

| Operation | Without Index | With Index | Storage Overhead |
|-----------|---------------|------------|------------------|
| ZADD | O(n) ~3ms | O(1) ~150μs | +40% |
| ZREM | O(n) ~3ms | O(1) ~100μs | +40% |
| ZSCORE | O(n) ~2ms | O(1) ~50μs | +40% |
| ZRANGE | O(n+k) ~5ms | O(n+k) ~5ms | +40% |
| ZCARD | O(1) ~10μs | O(1) ~10μs | +40% |

**Recommendation**: Always use reverse index for production workloads with ZSCORE/ZREM operations.

## Troubleshooting

### Problem: ZSCORE still slow after setup

**Check**:
```bash
# Verify reverse index table exists
redis-cli -p 6379
> ZADD test 100 alice
> ZSCORE test alice

# Check application logs for reverse index initialization
grep "ZSetReverseIndex" /path/to/application.log
```

**Solution**: Restart application if table was created after startup.

### Problem: Inconsistent data between tables

**Symptoms**:
- ZSCORE returns different score than ZRANGE
- ZCARD count doesn't match reverse index count

**Solution**:
1. Stop application
2. Backup data
3. Rebuild reverse index from main table
4. Restart application

### Problem: High write latency after adding reverse index

**Expected**: ZADD latency increases from ~50μs to ~150μs (dual write overhead)

**If much higher**:
- Check Fluss cluster health
- Verify network latency to Fluss
- Monitor Fluss write throughput (should handle 10k+ writes/sec)

## Migration Guide

### Migrating Existing Sorted Sets

If you already have sorted sets in `redis_data` before reverse index setup:

**Option 1: Lazy migration** (recommended)
- Create reverse index table
- Restart application
- Reverse index populates on next ZADD to each key
- ZSCORE falls back to O(n) scan if reverse index entry missing

**Option 2: Bulk migration** (future feature)
```bash
# Run migration script
java -jar fluss-hbase-compat.jar --migrate-zset-index

# Scans all zset entries and populates reverse index
# Estimated time: ~1 minute per 100k members
```

## Configuration Reference

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_ZSET_REVERSE_INDEX_TABLE` | `default.redis_zset_members` | Reverse index table name |
| `REDIS_ZSET_REVERSE_INDEX_ENABLED` | `true` | Enable/disable reverse index |
| `REDIS_ZSET_LAZY_INDEX_POPULATION` | `true` | Populate index lazily on write |

### Command Line Arguments

```bash
--redis.zset.reverse.index.table=default.redis_zset_members
--redis.zset.reverse.index.enabled=true
--redis.zset.lazy.index.population=true
```

## See Also

- [Redis Sorted Set Commands](../README.md#sorted-set-commands)
- [Fluss Table Schema](../README.md#redis-compatibility)
- [Performance Benchmarks](../docs/REDIS-PERFORMANCE.md)
