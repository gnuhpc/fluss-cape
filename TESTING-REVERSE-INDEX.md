# Redis Sorted Set Reverse Index - Testing & Performance

This directory contains test scripts for validating the Redis Sorted Set reverse index optimization.

## Quick Start

### 1. Prerequisites

```bash
# Install redis-cli (if not already installed)
sudo apt-get install redis-tools

# Ensure Fluss cluster is running
# Ensure reverse index table exists:
#   CREATE TABLE default.redis_zset_members (
#     redis_key STRING,
#     member STRING,
#     score DOUBLE,
#     PRIMARY KEY (redis_key, member)
#   );
```

### 2. Build and Start Server

```bash
# Build JAR
mvn clean package -Dmaven.test.skip=true

# Start server with reverse index enabled
java -jar target/fluss-hbase-compat-1.0.0-SNAPSHOT.jar \
  --fluss.bootstrap.servers=localhost:9123 \
  --zookeeper.quorum=localhost:2181 \
  --redis.enable=true \
  --redis.bind.port=6379 \
  --redis.table.name=default.redis_data
```

### 3. Run Tests

```bash
# Functional correctness tests
./test-reverse-index.sh

# Performance benchmarks (10k dataset)
./benchmark-reverse-index.sh

# Large dataset benchmark (100k)
DATASET_SIZE=100000 ./benchmark-reverse-index.sh
```

## Test Scripts

### test-reverse-index.sh

Validates functional correctness of reverse index:
- ✅ Basic ZADD/ZSCORE operations
- ✅ ZADD updates existing members
- ✅ ZREM removes members correctly
- ✅ ZCARD consistency
- ✅ ZRANGE consistency
- ✅ Non-existent member handling
- ✅ Bulk operations
- ✅ Re-adding removed members

Expected output:
```
==========================================
✅ All tests passed!
==========================================
```

### benchmark-reverse-index.sh

Measures performance improvements:
- ZADD latency (with dual-table writes)
- ZSCORE latency (O(1) reverse index lookup)
- ZREM latency (O(1) reverse index lookup)
- ZRANGE latency (unchanged, main table scan)

Expected results (10k dataset):
```
Operation     | Avg Latency  | Complexity
--------------|--------------|------------
ZADD          | 0.150ms      | O(1) + index write
ZSCORE        | 0.050ms      | O(1) reverse index
ZREM          | 0.100ms      | O(1) reverse index
ZRANGE        | 5.000ms      | O(n+k) main table scan

🎉 EXCELLENT: Reverse index is delivering 30-40x speedup!
```

## Performance Targets

| Operation | Without Index (O(n)) | With Index (O(1)) | Speedup Target |
|-----------|----------------------|-------------------|----------------|
| ZSCORE | 2-5ms (scan 10k) | 50-100μs | 20-50x |
| ZREM | 3ms (scan 10k) | 100μs | 30x |
| ZADD update | 3ms (scan 10k) | 150μs | 20x |

## Troubleshooting

### Test fails: "Redis server not responding"

**Solution**: Start the Redis compatibility server first
```bash
java -jar target/fluss-hbase-compat-1.0.0-SNAPSHOT.jar --redis.enable=true
```

### Test fails: "ZSCORE/ZREM still slow"

**Check**:
1. Reverse index table exists in Fluss
2. Server logs show: "Initialized SortedSetCommandExecutor with reverse index"
3. No errors in server logs during ZADD

**Debug**:
```bash
# Check server logs
grep "ZSetReverseIndex" server.log

# Verify reverse index table
# (Connect to Fluss SQL client)
SHOW TABLES;
SELECT COUNT(*) FROM default.redis_zset_members;
```

### Benchmark shows worse performance than expected

**Possible causes**:
1. **Fluss cluster overloaded**: Check Fluss cluster health
2. **Network latency**: Run benchmark on same machine as Fluss
3. **Small dataset**: Use larger datasets for accurate measurement
   ```bash
   DATASET_SIZE=100000 ./benchmark-reverse-index.sh
   ```

## Manual Testing

```bash
redis-cli -p 6379

# Create large sorted set
> ZADD perftest $(for i in {1..10000}; do echo "$i member$i"; done | tr '\n' ' ')

# Test ZSCORE speed (should be <1ms)
> time ZSCORE perftest member5000

# Test ZREM speed (should be <1ms)
> time ZREM perftest member5000

# Verify consistency
> ZCARD perftest
(integer) 9999

> ZRANGE perftest 0 5 WITHSCORES
1) "member1"
2) "1"
3) "member2"
4) "2"
...
```

## CI/CD Integration

Add to your CI pipeline:

```yaml
test-reverse-index:
  script:
    - mvn clean package -DskipTests
    - java -jar target/fluss-hbase-compat-*.jar &
    - sleep 10
    - ./test-reverse-index.sh
    - ./benchmark-reverse-index.sh
  artifacts:
    reports:
      junit: test-results.xml
```

## See Also

- [Reverse Index Setup Guide](../docs/REDIS-ZSET-REVERSE-INDEX-SETUP.md)
- [Redis Sorted Set Commands](../README.md#sorted-set-commands)
- [Performance Tuning](../docs/PERFORMANCE-TUNING.md)
