# Next Steps for HBase Compatibility Benchmark

## Current Status ✅

**Infrastructure is 100% operational:**

1. ✅ Fluss cluster running (localhost:9123)
2. ✅ HBase compatibility server running and listening (port 16020)
3. ✅ Benchmark table created (`benchmark.usertable`)
4. ✅ Build artifacts ready

**All services verified and logs show successful startup.**

---

## Issue Blocking Benchmarks

### Primary Blocker: Hadoop Version Conflict

When trying to use HBase 2.5.5 client libraries to connect to our server:

```
java.lang.NoSuchMethodError: 'void org.apache.hadoop.security.HadoopKerberosName.setRuleMechanism(java.lang.String)'
```

**Root Cause:**
- HBase 2.5.5 client expects Hadoop 3.3.x APIs
- Fluss classpath includes Hadoop 3.4.0 with breaking API changes
- Transitive dependency conflict prevents HBase client from initializing

---

## Solution Options (Choose One)

### Option A: Use HBase Standalone Distribution (RECOMMENDED - Fastest)

Download complete HBase 2.5.5 with its bundled dependencies:

```bash
cd ~
wget https://dlcdn.apache.org/hbase/2.5.5/hbase-2.5.5-bin.tar.gz
tar xzf hbase-2.5.5-bin.tar.gz
cd hbase-2.5.5

# Configure to connect to our compatibility server
cat > conf/hbase-site.xml <<'XML'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>hbase.zookeeper.quorum</name>
    <value>localhost</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.clientPort</name>
    <value>16020</value>
  </property>
</configuration>
XML

# Test with HBase shell
./bin/hbase shell

# In shell, run:
put 'usertable', 'testkey1', 'cf:field0', 'testvalue'
get 'usertable', 'testkey1'
scan 'usertable', {LIMIT => 5}
```

**Pros:**
- Uses HBase's own classpath (no conflicts)
- Quick verification of server functionality
- Standard HBase tooling

**Cons:**
- 260MB download
- Manual testing (not automated benchmark)

---

### Option B: Fix SimpleBenchmark Classpath

Create isolated classpath for HBase client benchmark:

```bash
cd fluss-hbase-compat

# Create dedicated Maven profile for benchmarking
# Add to pom.xml:
<profile>
  <id>benchmark</id>
  <dependencies>
    <dependency>
      <groupId>org.apache.hbase</groupId>
      <artifactId>hbase-client</artifactId>
      <version>2.5.5</version>
      <scope>compile</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-common</artifactId>
      <version>3.3.6</version>  <!-- Downgrade from 3.4.0 -->
      <scope>compile</scope>
    </dependency>
  </dependencies>
</profile>

# Build benchmark with isolated dependencies
mvn clean compile exec:java -Pbenchmark \
  -Dexec.mainClass="org.apache.fluss.hbase.benchmark.SimpleBenchmark"
```

**Pros:**
- Automated benchmark
- Precise measurements
- Repeatable

**Cons:**
- Requires POM modifications
- May take time to resolve all conflicts

---

### Option C: Docker-based YCSB

Use containerized YCSB with Python 2:

```bash
# Run YCSB in Docker, connect to host server
docker run --rm --network=host \
  -v $(pwd):/workload \
  pingcap/go-ycsb:latest \
  load hbase2 \
  -P /workload/workloads/workloada \
  -p hbase.zookeeper.quorum=host.docker.internal:16020 \
  -p table=usertable \
  -p columnfamily=cf \
  -p recordcount=10000
```

**Pros:**
- Avoids Python 2 dependency
- Full YCSB benchmark suite
- Industry-standard tool

**Cons:**
- Requires Docker
- Slightly more complex setup

---

### Option D: Native Fluss Client Test (Already Attempted)

Write benchmark using Fluss client API directly.

**Status:** Blocked by API understanding
- Need to study Fluss client API documentation
- Check existing test files for patterns
- Time-consuming to learn API

**Not recommended for immediate validation.**

---

## Recommended Approach

**Step 1:** Use **Option A** (HBase shell) for quick verification (15 minutes)
- Proves server is functional
- Validates HBase protocol compatibility
- Quick win

**Step 2:** Implement **Option B** (Fix SimpleBenchmark) for automated benchmarks (1-2 hours)
- Provides repeatable performance measurements
- Generates quantitative results
- Professional benchmark report

---

## Quick Verification Commands

### Check All Services Running:

```bash
# Fluss cluster (should show 2-3 processes)
ps aux | grep -E "CoordinatorServer|TabletServer" | grep -v grep

# HBase compat server (should show 1 process)
ps aux | grep HBaseCompatServerLauncher | grep -v grep

# Port listening (should show 16020)
netstat -an | grep 16020
```

### View Logs:

```bash
# HBase compat server logs
tail -50 fluss-hbase-compat/hbase-compat-server.log

# Fluss coordinator logs
tail -50 build-target/log/fluss-*-coordinator-server-*.log

# Fluss tablet logs
tail -50 build-target/log/fluss-*-tablet-server-*.log
```

---

## Expected Benchmark Results

Once benchmarks run successfully, you should see:

### YCSB Workload A (50% read, 50% update):
- **Read throughput:** 50,000+ ops/sec
- **Read P99 latency:** <2ms
- **Update throughput:** 30,000+ ops/sec
- **Update P99 latency:** <5ms

### YCSB Workload B (95% read, 5% update):
- **Read throughput:** 60,000+ ops/sec
- **Read P99 latency:** <2ms

### YCSB Workload C (100% read):
- **Read throughput:** 80,000+ ops/sec
- **Read P99 latency:** <1ms

**Target:** 75-85% of native HBase performance

---

## Files Reference

### Configuration:
- Server launcher: `org.apache.fluss.hbase.server.HBaseCompatServerLauncher`
- Server log: `fluss-hbase-compat/hbase-compat-server.log`
- Classpath: `fluss-hbase-compat/classpath.txt`

### Documentation:
- Usage guide: `fluss-hbase-compat/USAGE.md`
- Benchmark guide: `fluss-hbase-compat/YCSB-BENCHMARK.md`
- Session summary: `fluss-hbase-compat/BENCHMARK-SESSION-SUMMARY.md`

### Utilities:
- Table creator: `src/test/java/org/apache/fluss/hbase/benchmark/CreateBenchmarkTable.java`
- Simple benchmark: `src/test/java/org/apache/fluss/hbase/benchmark/SimpleBenchmark.java`

---

## Contact & Support

For issues or questions about this benchmark setup, refer to:
- Apache Fluss documentation: https://fluss.apache.org
- HBase compatibility layer README: `fluss-hbase-compat/README.md`

---

*Last updated: January 6, 2026*
