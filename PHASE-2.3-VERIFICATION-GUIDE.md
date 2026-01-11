# Phase 2.3 TTL/Expiration - Verification Guide

## ⚠️ CRITICAL: VERIFICATION REQUIRED

**Implementation Status:** ✅ Code Complete | ⚠️ NOT VERIFIED

This phase is **NOT complete** until all verification steps below are executed and pass.

---

## Prerequisites

### 1. Fluss Cluster Running
```bash
# Start Fluss cluster (if not already running)
# Follow Apache Fluss documentation for your environment
# Default: localhost:9123
```

### 2. ZooKeeper Running
```bash
# Ensure ZooKeeper is running
# Default: localhost:2181
```

### 3. Create Redis Table in Fluss
```sql
-- Connect to Fluss SQL client
-- Execute the following DDL:

CREATE TABLE default.redis_data (
  redis_key STRING,
  redis_type STRING,
  sub_key STRING,
  score DOUBLE,
  value BYTES,
  PRIMARY KEY (redis_key, sub_key)
);
```

### 4. Install redis-cli
```bash
# Ubuntu/Debian
sudo apt-get install -y redis-tools

# macOS
brew install redis

# Verify installation
redis-cli --version
```

---

## Verification Steps

### Step 1: Build Verification

```bash
cd /root/fluss-hbase-compat-standalone

# Full build with package
mvn clean package -Dmaven.test.skip=true

# Expected output:
# [INFO] BUILD SUCCESS
# [INFO] Total time: ~20s
```

**Success Criteria:** ✅ Exit code 0, JAR created at `target/fluss-hbase-compat-1.0.0-SNAPSHOT.jar`

---

### Step 2: Start Server

```bash
# Start server in background
java -jar target/fluss-hbase-compat-1.0.0-SNAPSHOT.jar \
  --fluss.bootstrap.servers=localhost:9123 \
  --zookeeper.quorum=localhost:2181 \
  --hbase.compat.bind.port=16020 \
  --redis.enable=true \
  --redis.bind.port=6379 \
  --redis.table.name=default.redis_data \
  --tables=default.test_table > server.log 2>&1 &

# Save PID
echo $! > server.pid

# Wait for startup (10 seconds)
sleep 10

# Check server is running
tail -20 server.log
```

**Expected Output:**
```
========================================
Redis compatibility server started on 0.0.0.0:6379
========================================
```

**Success Criteria:** ✅ Server starts without errors, listens on port 6379

---

### Step 3: Manual Smoke Test

```bash
# Test basic connectivity
redis-cli -p 6379 PING
# Expected: PONG

# Test basic SET/GET
redis-cli -p 6379 SET testkey "Hello, TTL!"
# Expected: OK

redis-cli -p 6379 GET testkey
# Expected: Hello, TTL!

# Test EXPIRE command exists
redis-cli -p 6379 EXPIRE testkey 10
# Expected: (integer) 1

# Test TTL command exists
redis-cli -p 6379 TTL testkey
# Expected: (integer) 9 or 10

# Cleanup
redis-cli -p 6379 DEL testkey
```

**Success Criteria:** ✅ All commands respond correctly, no errors

---

### Step 4: Execute Full Test Suite

```bash
# Make test script executable (if not already)
chmod +x test-ttl.sh

# Run comprehensive test suite
./test-ttl.sh

# Or specify custom Redis host/port
REDIS_HOST=localhost REDIS_PORT=6379 ./test-ttl.sh
```

**Expected Output:**
```
================================================
Redis TTL/Expiration Functional Tests
================================================
Target: localhost:6379

Test 1: Basic EXPIRE + TTL
----------------------------
✓ EXPIRE should return 1 for existing key
✓ TTL should return ~10 seconds (value: 10)
✓ Key should still be readable before expiration

Test 2: Expiration actually deletes key
----------------------------------------
Waiting 3 seconds for key to expire...
✓ Key should be nil after expiration
✓ TTL should return -2 for expired/non-existent key

[... 13 more tests ...]

================================================
Test Results
================================================
Passed: 40
Failed: 0

All tests passed!
```

**Success Criteria:** ✅ **ALL 15 test cases pass** (40+ individual assertions), exit code 0

---

### Step 5: Edge Case Manual Verification

```bash
redis-cli -p 6379

# Test 1: Negative TTL deletes immediately
127.0.0.1:6379> SET key1 "value"
OK
127.0.0.1:6379> EXPIRE key1 -5
(integer) 1
127.0.0.1:6379> GET key1
(nil)
# ✅ Expected: Key deleted immediately

# Test 2: EXPIREAT with past timestamp
127.0.0.1:6379> SET key2 "value"
OK
127.0.0.1:6379> EXPIREAT key2 1000000000
(integer) 1
127.0.0.1:6379> GET key2
(nil)
# ✅ Expected: Key deleted immediately

# Test 3: PERSIST removes expiration
127.0.0.1:6379> SET key3 "value"
OK
127.0.0.1:6379> EXPIRE key3 60
(integer) 1
127.0.0.1:6379> TTL key3
(integer) 59
127.0.0.1:6379> PERSIST key3
(integer) 1
127.0.0.1:6379> TTL key3
(integer) -1
# ✅ Expected: TTL returns -1 (no expiration)

# Test 4: Hash expiration
127.0.0.1:6379> HSET user:1 name Alice age 30
(integer) 2
127.0.0.1:6379> EXPIRE user:1 2
(integer) 1
127.0.0.1:6379> HGET user:1 name
"Alice"
# Wait 3 seconds
127.0.0.1:6379> HGET user:1 name
(nil)
# ✅ Expected: Hash expired, returns nil

# Test 5: INCR on expired key
127.0.0.1:6379> SET counter 100
OK
127.0.0.1:6379> EXPIRE counter 1
(integer) 1
# Wait 2 seconds
127.0.0.1:6379> INCR counter
(integer) 1
# ✅ Expected: Starts from 0, returns 1

# Cleanup
127.0.0.1:6379> FLUSHALL
OK
127.0.0.1:6379> EXIT
```

**Success Criteria:** ✅ All 5 edge cases behave as expected

---

### Step 6: Performance Verification

```bash
redis-cli -p 6379

# Measure read overhead (baseline without TTL)
127.0.0.1:6379> SET perfkey "value"
OK
127.0.0.1:6379> --latency GET perfkey
# Record baseline latency (e.g., 0.50ms)

# Measure read overhead WITH TTL
127.0.0.1:6379> EXPIRE perfkey 3600
(integer) 1
127.0.0.1:6379> --latency GET perfkey
# Record TTL latency (e.g., 1.00ms)

# Verify overhead is ~2x (one extra lookup for expiration check)
# Expected: TTL latency ≈ 2x baseline
```

**Success Criteria:** ✅ Overhead is approximately 2x (expected due to lazy deletion implementation)

---

### Step 7: Regression Testing

```bash
# Test that non-TTL commands still work
redis-cli -p 6379

# String commands
127.0.0.1:6379> SET key1 value1
OK
127.0.0.1:6379> GET key1
"value1"

# Hash commands
127.0.0.1:6379> HSET hash1 field1 value1
(integer) 1
127.0.0.1:6379> HGET hash1 field1
"value1"

# Set commands
127.0.0.1:6379> SADD set1 member1 member2
(integer) 2
127.0.0.1:6379> SMEMBERS set1
1) "member1"
2) "member2"

# List commands
127.0.0.1:6379> RPUSH list1 a b c
(integer) 3
127.0.0.1:6379> LRANGE list1 0 -1
1) "a"
2) "b"
3) "c"

# Sorted Set commands
127.0.0.1:6379> ZADD zset1 100 alice 200 bob
(integer) 2
127.0.0.1:6379> ZRANGE zset1 0 -1 WITHSCORES
1) "alice"
2) "100"
3) "bob"
4) "200"

# Type command
127.0.0.1:6379> TYPE key1
string
127.0.0.1:6379> TYPE hash1
hash
```

**Success Criteria:** ✅ All existing commands work without regression

---

## Cleanup

```bash
# Stop server
kill $(cat server.pid)
rm server.pid

# Verify shutdown
tail -10 server.log

# Optional: Clean test data from Fluss
# (Connect to Fluss SQL and execute)
# DROP TABLE IF EXISTS default.redis_data;
```

---

## Evidence Checklist

Mark each item when completed with evidence:

- [ ] **Build Success**: `mvn clean package` exits with code 0
- [ ] **Server Starts**: Server log shows "Redis compatibility server started"
- [ ] **Smoke Test**: Manual EXPIRE/TTL commands work
- [ ] **Full Test Suite**: `./test-ttl.sh` shows "All tests passed!"
- [ ] **Edge Cases**: All 5 manual edge cases verified
- [ ] **Performance**: Overhead is ~2x (expected)
- [ ] **Regression**: Existing commands work without issues

---

## VERIFICATION STATUS: ⚠️ PENDING

**Phase 2.3 is NOT complete until all items above are checked and evidence provided.**

---

## Troubleshooting

### Issue: Server fails to start

**Symptoms:**
```
ERROR: Could not connect to Fluss cluster
```

**Solution:**
```bash
# Check Fluss is running
nc -zv localhost 9123

# Check ZooKeeper is running
nc -zv localhost 2181

# Verify table exists
# Connect to Fluss SQL and run: SHOW TABLES;
```

---

### Issue: Test suite fails with connection error

**Symptoms:**
```
ERROR: Cannot connect to Redis server
```

**Solution:**
```bash
# Check server is running
ps aux | grep fluss-hbase-compat

# Check port is listening
netstat -tuln | grep 6379

# Check server logs
tail -50 server.log
```

---

### Issue: Keys not expiring

**Symptoms:**
```
GET key returns value after expiration time
```

**Solution:**
```bash
# Check TTL was set
redis-cli TTL key

# Verify ExpirationManager is registered
grep "ExpirationCommandExecutor" server.log

# Check for errors in server log
grep ERROR server.log
```

---

### Issue: Performance is worse than 2x

**Symptoms:**
```
Read latency is 5x or higher
```

**Investigation:**
```bash
# Check Fluss cluster performance
# Monitor Fluss metrics

# Verify table is using primary key correctly
# Check table schema in Fluss

# Consider adding Caffeine cache (Phase 2.3.12 - future enhancement)
```

---

## Next Steps After Verification

Once ALL verification steps pass:

1. ✅ Mark Phase 2.3 as **VERIFIED COMPLETE**
2. Document verification results in project documentation
3. Update README.md with TTL command documentation
4. Consider future enhancements:
   - Phase 2.3.11: Background scanner for proactive cleanup
   - Phase 2.3.12: Caffeine cache for expiration timestamps
   - Phase 2.3.13: Per-field expiration (non-standard)

---

## Contact / Issues

If verification fails or issues arise:
1. Check server logs: `tail -100 server.log`
2. Check Fluss cluster status
3. Verify all prerequisites are met
4. Review error messages carefully
5. Consult `docs/PHASE-2.3-TTL-ARCHITECTURE.md` for design details

---

**REMEMBER: Code written ≠ Feature complete. VERIFICATION IS MANDATORY.**
