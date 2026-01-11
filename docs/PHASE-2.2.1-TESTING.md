# Phase 2.2.1 Metadata Layer Testing Guide

**Date**: January 9, 2026  
**Phase**: Redis Optimization Phase 2.2.1  
**Feature**: Metadata caching for List and Sorted Set operations

---

## Overview

This document provides a comprehensive testing guide for the metadata layer optimization implemented in Phase 2.2.1. The optimization introduces cached metadata to eliminate O(n) operations for LLEN, LPUSH, RPUSH, ZCARD commands.

---

## Prerequisites

### 1. Start Fluss Cluster

```bash
# Ensure Fluss is running
$FLUSS_HOME/bin/fluss-daemon.sh start zookeeper
$FLUSS_HOME/bin/fluss-daemon.sh start coordinator
$FLUSS_HOME/bin/fluss-daemon.sh start tablet-server
```

### 2. Create Redis Table

```sql
-- Connect to Fluss SQL client
$FLUSS_HOME/bin/sql-client.sh

-- Create table (if not already exists)
CREATE TABLE IF NOT EXISTS default.redis_data (
  redis_key STRING,
  redis_type STRING,
  sub_key STRING,
  score DOUBLE,
  value BYTES,
  PRIMARY KEY (redis_key, sub_key)
);
```

### 3. Start HBase Compatibility Server with Redis

```bash
cd /root/fluss-hbase-compat-standalone

java -jar target/fluss-hbase-compat-1.0.0-SNAPSHOT.jar \
  --fluss.bootstrap.servers=localhost:9123 \
  --zookeeper.quorum=localhost:2181 \
  --hbase.compat.bind.port=16020 \
  --redis.enable=true \
  --redis.bind.port=6379 \
  --redis.table.name=default.redis_data \
  --tables=default.test_table
```

---

## Test Suite

### Test 1: List Metadata Creation and LLEN

**Objective**: Verify metadata is created on first LPUSH/RPUSH and LLEN uses it.

```bash
redis-cli -p 6379

# Test RPUSH creates metadata
> RPUSH testlist1 a b c
(integer) 3

# Verify LLEN uses metadata (should be instant)
> LLEN testlist1
(integer) 3

# Add more items
> RPUSH testlist1 d e
(integer) 5

> LLEN testlist1
(integer) 5
```

**Expected Fluss Data**:
```sql
-- Check in Fluss SQL client
SELECT redis_key, redis_type, sub_key FROM default.redis_data WHERE redis_key = 'testlist1';

-- Should show:
-- testlist1, list, __meta__           (metadata row)
-- testlist1, list, +0000000000000000  (item a)
-- testlist1, list, +0000000000000001  (item b)
-- testlist1, list, +0000000000000002  (item c)
-- testlist1, list, +0000000000000003  (item d)
-- testlist1, list, +0000000000000004  (item e)
```

**Validation**:
- ✅ Metadata row exists with `sub_key = "__meta__"`
- ✅ LLEN returns correct count without scanning all items
- ✅ Index encoding is sequential for RPUSH

---

### Test 2: LPUSH with Negative Indices

**Objective**: Verify LPUSH uses metadata to find head index.

```bash
> LPUSH testlist2 z
(integer) 1

> LPUSH testlist2 y x
(integer) 3

> LRANGE testlist2 0 -1
1) "x"
2) "y"
3) "z"

> LLEN testlist2
(integer) 3
```

**Expected Fluss Data**:
```sql
SELECT redis_key, sub_key FROM default.redis_data WHERE redis_key = 'testlist2' ORDER BY sub_key;

-- Should show:
-- testlist2, __meta__           (metadata: headIndex=-2, tailIndex=0)
-- testlist2, -0000000000000002  (item x)
-- testlist2, -0000000000000001  (item y)
-- testlist2, +0000000000000000  (item z)
```

**Validation**:
- ✅ LPUSH creates negative indices
- ✅ Metadata tracks headIndex correctly
- ✅ LRANGE returns items in correct order
- ✅ LLEN uses metadata

---

### Test 3: Mixed LPUSH and RPUSH

**Objective**: Verify metadata tracks both head and tail correctly.

```bash
> RPUSH testlist3 middle
(integer) 1

> LPUSH testlist3 left
(integer) 2

> RPUSH testlist3 right
(integer) 3

> LRANGE testlist3 0 -1
1) "left"
2) "middle"
3) "right"

> LLEN testlist3
(integer) 3
```

**Expected Metadata State**:
```
headIndex = -1  (left item)
tailIndex = 1   (right item)
count = 3
```

**Validation**:
- ✅ LPUSH and RPUSH update correct indices
- ✅ LLEN reflects accurate count
- ✅ Order is preserved

---

### Test 4: LPOP and RPOP Update Metadata

**Objective**: Verify pop operations update metadata count.

```bash
> RPUSH testlist4 a b c d e
(integer) 5

> LLEN testlist4
(integer) 5

> LPOP testlist4
"a"

> LLEN testlist4
(integer) 4

> RPOP testlist4
"e"

> LLEN testlist4
(integer) 3

> LRANGE testlist4 0 -1
1) "b"
2) "c"
3) "d"
```

**Validation**:
- ✅ LPOP decrements count
- ✅ RPOP decrements count
- ✅ LLEN reflects updated count
- ✅ Remaining items are correct

---

### Test 5: Sorted Set Metadata Creation and ZCARD

**Objective**: Verify ZADD creates metadata and ZCARD uses it.

```bash
> ZADD testzset1 100 alice 200 bob 150 charlie
(integer) 3

> ZCARD testzset1
(integer) 3

> ZADD testzset1 175 diana
(integer) 1

> ZCARD testzset1
(integer) 4
```

**Expected Fluss Data**:
```sql
SELECT redis_key, sub_key FROM default.redis_data WHERE redis_key = 'testzset1';

-- Should show:
-- testzset1, __meta__                    (metadata: count=4)
-- testzset1, 4059000000000000:alice      (score 100)
-- testzset1, 4062C00000000000:charlie    (score 150)
-- testzset1, 4065E00000000000:diana      (score 175)
-- testzset1, 4069000000000000:bob        (score 200)
```

**Validation**:
- ✅ Metadata row exists
- ✅ ZCARD uses metadata (O(1))
- ✅ Count increments correctly
- ✅ Score encoding maintains sort order

---

### Test 6: ZREM Updates Metadata

**Objective**: Verify ZREM decrements metadata count.

```bash
> ZADD testzset2 10 a 20 b 30 c
(integer) 3

> ZCARD testzset2
(integer) 3

> ZREM testzset2 b
(integer) 1

> ZCARD testzset2
(integer) 2

> ZRANGE testzset2 0 -1
1) "a"
2) "c"
```

**Validation**:
- ✅ ZREM decrements count
- ✅ ZCARD reflects updated count
- ✅ Removed member is gone
- ✅ Remaining members are correct

---

### Test 7: ZADD Update (Replace Existing Member)

**Objective**: Verify updating existing member doesn't increment count.

```bash
> ZADD testzset3 100 alice
(integer) 1

> ZCARD testzset3
(integer) 1

> ZADD testzset3 200 alice
(integer) 0

> ZCARD testzset3
(integer) 1

> ZSCORE testzset3 alice
"200.0"
```

**Validation**:
- ✅ Update returns 0 (no new member added)
- ✅ Count remains 1
- ✅ Score is updated
- ✅ ZCARD still uses metadata

---

### Test 8: Backward Compatibility (No Metadata)

**Objective**: Verify operations work if metadata doesn't exist.

```bash
# Manually insert data without metadata using Fluss SQL
# (Simulates legacy data)

# In Fluss SQL client:
INSERT INTO default.redis_data VALUES 
  ('legacy_list', 'list', '+0000000000000000', NULL, CAST('item1' AS BYTES));

INSERT INTO default.redis_data VALUES 
  ('legacy_list', 'list', '+0000000000000001', NULL, CAST('item2' AS BYTES));

# Now test in redis-cli
> LLEN legacy_list
(integer) 2

> LRANGE legacy_list 0 -1
1) "item1"
2) "item2"

# Add new item (should create metadata)
> RPUSH legacy_list item3
(integer) 3

> LLEN legacy_list
(integer) 3
```

**Validation**:
- ✅ Operations work without metadata (fallback)
- ✅ Metadata is created on first write
- ✅ Subsequent operations use metadata

---

### Test 9: Empty List/Set Edge Cases

**Objective**: Verify metadata handles empty collections.

```bash
> LPUSH testlist5 x
(integer) 1

> LPOP testlist5
"x"

> LLEN testlist5
(integer) 0

> LPOP testlist5
(nil)

> LRANGE testlist5 0 -1
(empty array)

# Same for sorted sets
> ZADD testzset5 100 alice
(integer) 1

> ZREM testzset5 alice
(integer) 1

> ZCARD testzset5
(integer) 0

> ZRANGE testzset5 0 -1
(empty array)
```

**Validation**:
- ✅ Empty list returns count 0
- ✅ Operations on empty list return nil/empty
- ✅ Metadata correctly shows count=0
- ✅ No errors or exceptions

---

### Test 10: Metadata Filtering in LRANGE/ZRANGE

**Objective**: Verify metadata row is filtered out from results.

```bash
> RPUSH testlist6 a b c
(integer) 3

> LRANGE testlist6 0 -1
1) "a"
2) "b"
3) "c"

# Should NOT include __meta__ in results

> ZADD testzset6 1 x 2 y 3 z
(integer) 3

> ZRANGE testzset6 0 -1
1) "x"
2) "y"
3) "z"

# Should NOT include __meta__ in results
```

**Validation**:
- ✅ LRANGE excludes metadata row
- ✅ ZRANGE excludes metadata row
- ✅ Results are clean and correct
- ✅ No `__meta__` visible to clients

---

## Performance Testing

### Benchmark: LLEN Performance

**Before Optimization (Phase 2.2)**:
```bash
# Create large list
for i in {1..1000}; do
  redis-cli -p 6379 RPUSH biglist "item$i" > /dev/null
done

# Time LLEN (should scan all items - O(n))
time redis-cli -p 6379 LLEN biglist
# Expected: ~2-5ms for 1000 items
```

**After Optimization (Phase 2.2.1)**:
```bash
# Same test
time redis-cli -p 6379 LLEN biglist
# Expected: ~10-50μs (metadata lookup - O(1))
```

**Expected Improvement**: 50-200x faster

---

### Benchmark: ZCARD Performance

```bash
# Create large sorted set
for i in {1..1000}; do
  redis-cli -p 6379 ZADD bigzset $i "member$i" > /dev/null
done

# Time ZCARD
time redis-cli -p 6379 ZCARD bigzset
# Expected: ~10-50μs (metadata lookup - O(1))
```

**Expected Improvement**: 100-200x faster vs Phase 2.2

---

## Verification Checklist

### Metadata Storage

- [ ] Metadata row exists with `sub_key = "__meta__"`
- [ ] Metadata is binary serialized (not human-readable)
- [ ] Metadata is created on first write operation
- [ ] Metadata is updated atomically with data changes

### List Operations

- [ ] LPUSH creates/updates metadata
- [ ] RPUSH creates/updates metadata
- [ ] LPOP decrements count in metadata
- [ ] RPOP decrements count in metadata
- [ ] LLEN uses metadata (O(1))
- [ ] LRANGE filters out metadata row
- [ ] LINDEX filters out metadata row

### Sorted Set Operations

- [ ] ZADD creates/updates metadata
- [ ] ZADD (update) doesn't increment count
- [ ] ZREM decrements count in metadata
- [ ] ZCARD uses metadata (O(1))
- [ ] ZRANGE filters out metadata row
- [ ] ZRANGEBYSCORE filters out metadata row
- [ ] ZSCORE filters out metadata row

### Error Handling

- [ ] No exceptions when metadata is missing
- [ ] Graceful fallback for legacy data
- [ ] Error logging for serialization failures
- [ ] Operations continue if metadata save fails

### Backward Compatibility

- [ ] Existing data without metadata still works
- [ ] Metadata is created automatically on write
- [ ] No breaking changes to existing APIs
- [ ] Mixed metadata/no-metadata data works

---

## Debugging Tips

### View Metadata in Fluss

```sql
-- In Fluss SQL client
SELECT redis_key, sub_key, LENGTH(value) as metadata_size 
FROM default.redis_data 
WHERE sub_key = '__meta__';
```

### Check Metadata Content (Binary)

Metadata is binary and not human-readable, but you can verify:
- ListMetadata: ~20-30 bytes (int + long + long + string)
- ZSetMetadata: ~10-15 bytes (int + string)

### Enable Debug Logging

```bash
# Add to log4j.properties
log4j.logger.org.apache.fluss.redis.metadata=DEBUG

# Restart server
java -jar target/fluss-hbase-compat-1.0.0-SNAPSHOT.jar ...
```

### Manual Metadata Inspection (Java)

```java
// If needed for debugging
byte[] data = adapter.getByCompositeKey("mylist", "__meta__");
ListMetadata meta = ListMetadata.deserialize(data);
System.out.println(meta); // Shows: ListMetadata{count=5, headIndex=-2, tailIndex=3, encoding='distributed'}
```

---

## Known Limitations

### Not Yet Optimized (Phase 2.2.2 targets):

1. **ZSCORE** - Still O(n) member scan (needs reverse index)
2. **ZREM** - Still O(n) member scan (needs reverse index)
3. **ZADD update** - Still O(n) to check if member exists (needs reverse index)

These will be addressed in Phase 2.2.2 with a separate `redis_zset_members` reverse index table.

---

## Success Criteria

### Phase 2.2.1 is successful if:

1. ✅ All 10 test cases pass
2. ✅ LLEN shows 50-200x performance improvement
3. ✅ ZCARD shows 100-200x performance improvement
4. ✅ No errors in server logs
5. ✅ Backward compatibility maintained
6. ✅ Metadata is transparent to clients
7. ✅ Operations remain functionally correct

---

## Rollback Plan

If issues are found:

```bash
# Stop server
kill $(pgrep -f fluss-hbase-compat)

# Rebuild from Phase 2.2 tag (before metadata)
git checkout phase-2.2
mvn clean package -DskipTests

# Restart server
java -jar target/fluss-hbase-compat-1.0.0-SNAPSHOT.jar ...
```

Metadata rows can be safely ignored by Phase 2.2 code (they'll just be extra rows that get filtered out).

---

## Next Steps After Testing

If all tests pass:

1. **Update README.md** with Phase 2.2.1 completion
2. **Create performance comparison document**
3. **Consider Phase 2.2.2** (reverse index for ZSCORE/ZREM optimization)
4. **Production deployment** with confidence in 100x+ speedup

---

**Document Version**: 1.0  
**Last Updated**: January 9, 2026  
**Author**: Sisyphus (OhMyOpenCode AI Agent)
