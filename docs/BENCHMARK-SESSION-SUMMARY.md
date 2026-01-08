# YCSB Benchmark Session Summary

## Date: January 6, 2026

## Objective
Test the HBase Protocol Compatibility Layer for Apache Fluss using YCSB benchmarks to measure KV read/write performance.

---

## ✅ COMPLETED TASKS

### 1. Fluss Cluster Setup ✅
**Status**: Running successfully

**Components**:
- ZooKeeper: Running
- CoordinatorServer: Running on localhost:9123
- TabletServer: Running
- Distribution: `/Users/gnuhpc/IdeaProjects/fluss/build-target/`

**Verification**:
```bash
$ ps aux | grep -E "fluss|zookeeper" | grep -v grep
# Shows 3 running processes
```

**Build Details**:
- Built with: `./mvnw clean package -DskipTests -pl fluss-dist -am`
- Started with: `./build-target/bin/local-cluster.sh start`
- Build time: ~5 minutes

---

### 2. Benchmark Table Creation ✅
**Status**: Table created and verified

**Table Schema**:
```sql
Database: benchmark
Table: usertable
Primary Key: YCSB_KEY (STRING)
Buckets: 8
Fields: field0, field1, ..., field9 (10 STRING columns)
```

**Verification**:
```bash
$ java -cp "..." org.apache.fluss.hbase.benchmark.CreateBenchmarkTable
✓ Database already exists: benchmark
✓ Table already exists: benchmark.usertable
```

**Utility Class Created**:
- File: `fluss-hbase-compat/src/test/java/org/apache/fluss/hbase/benchmark/CreateBenchmarkTable.java`
- Purpose: Automated table creation for benchmarks
- Status: Compiled and tested successfully

---

### 3. HBase Compatibility Layer Build ✅
**Status**: Built successfully

**Artifact**:
- JAR: `fluss-hbase-compat/target/fluss-hbase-compat-0.9-SNAPSHOT.jar`
- Build command: `mvn package -pl fluss-hbase-compat -Dmaven.test.skip=true`
- Classpath file: `fluss-hbase-compat/classpath.txt` (generated via Maven dependency plugin)

**Build Time**: ~2 seconds (incremental build)

---

### 4. HBase Compatibility Server Startup ✅
**Status**: Running and listening

**Configuration**:
- Bootstrap servers: `localhost:9123`
- Bind address: `0.0.0.0`
- Bind port: `16020`
- Tables: `benchmark.usertable`

**Startup Command**:
```bash
cd /Users/gnuhpc/IdeaProjects/fluss
CLASSPATH=$(cat fluss-hbase-compat/classpath.txt):fluss-hbase-compat/target/fluss-hbase-compat-0.9-SNAPSHOT.jar

java -Dfluss.bootstrap.servers=localhost:9123 \
     -Dhbase.compat.bind.address=0.0.0.0 \
     -Dhbase.compat.bind.port=16020 \
     -Dhbase.compat.tables=benchmark.usertable \
     -cp "$CLASSPATH" \
     org.apache.fluss.hbase.server.HBaseCompatServerLauncher \
     > fluss-hbase-compat/hbase-compat-server.log 2>&1 &
```

**Verification**:
```bash
$ ps aux | grep HBaseCompatServerLauncher | grep -v grep
# PID: 96908 (running)

$ netstat -an | grep 16020
tcp46      0      0  *.16020                *.*                    LISTEN

$ tail fluss-hbase-compat/hbase-compat-server.log
[id: 0x7c8f8b5d, L:/[0:0:0:0:0:0:0:0]:16020] ACTIVE
```

**Key Resolution**: Fixed classpath issues by using Maven's dependency:build-classpath to generate complete runtime classpath including all HBase and Fluss dependencies.

---

## 🔴 BLOCKED TASKS

### 5. YCSB Setup ⏸️
**Status**: Download attempts failed/incomplete

**Issues Encountered**:
1. **GitHub releases download**: Timed out (68MB file, took >3 minutes)
2. **Tar extraction**: Corrupted archive (truncated gzip input errors)
3. **Apache mirror**: URL returned 404 (version not available)
4. **Git clone + Maven build**: YCSB requires Python 2, system has Python 3.13

**Attempts Made**:
- wget from GitHub releases (incomplete)
- curl from GitHub releases (incomplete)
- Apache archive mirror (404)
- Git clone from source (Python 2 dependency)

**Root Cause**: Network issues causing incomplete downloads + YCSB Python 2 incompatibility

---

### 6. Alternative: SimpleBenchmark ⏸️
**Status**: Created but blocked by Hadoop version conflict

**Artifact Created**:
- File: `fluss-hbase-compat/src/test/java/org/apache/fluss/hbase/benchmark/SimpleBenchmark.java`
- Purpose: Direct HBase client benchmark (10K records, 16 threads)
- Status: Compiled successfully

**Blocking Error**:
```
java.lang.NoSuchMethodError: 'void org.apache.hadoop.security.HadoopKerberosName.setRuleMechanism(java.lang.String)'
	at org.apache.hadoop.security.HadoopKerberosName.setConfiguration
	at org.apache.hadoop.security.UserGroupInformation.initialize
	at org.apache.hadoop.hbase.client.ConnectionFactory.createConnection
```

**Root Cause**: 
- HBase 2.5.5 client expects Hadoop 3.3.x with specific security APIs
- Fluss classpath contains Hadoop 3.4.0 which has incompatible API changes
- Conflict between transitive Hadoop dependencies

---

## 📊 CURRENT SYSTEM STATE

### Running Services:
| Service | Status | PID | Port/Location |
|---------|--------|-----|---------------|
| Fluss ZooKeeper | ✅ Running | - | localhost:2181 |
| Fluss CoordinatorServer | ✅ Running | - | localhost:9123 |
| Fluss TabletServer | ✅ Running | - | - |
| HBase Compat Server | ✅ Running | 96908 | 0.0.0.0:16020 |

### Database Objects:
| Type | Name | Status | Details |
|------|------|--------|---------|
| Database | benchmark | ✅ Exists | Created for YCSB |
| Table | benchmark.usertable | ✅ Exists | 8 buckets, 10 fields |

### Files Created:
```
fluss-hbase-compat/
├── classpath.txt                              # Maven runtime classpath
├── hbase-compat-server.log                   # Server logs
├── src/test/java/org/apache/fluss/hbase/benchmark/
│   ├── CreateBenchmarkTable.java            # ✅ Compiled
│   └── SimpleBenchmark.java                  # ✅ Compiled (blocked)
└── target/
    ├── fluss-hbase-compat-0.9-SNAPSHOT.jar  # Main artifact
    └── test-classes/org/apache/fluss/hbase/benchmark/
        ├── CreateBenchmarkTable.class
        ├── SimpleBenchmark.class
        └── SimpleBenchmark$BenchmarkResult.class
```

---

## 🎯 NEXT STEPS TO COMPLETE BENCHMARKING

### Option A: Resolve Hadoop Version Conflict (Recommended)
1. **Isolate HBase client dependencies**: Create a shaded JAR with compatible Hadoop version
2. **Use HBase's bundled Hadoop**: Point classpath to HBase's own Hadoop jars
3. **Downgrade Hadoop in benchmark**: Use Hadoop 3.3.x instead of 3.4.0

### Option B: Use Native HBase Client (Fastest)
1. **Install HBase standalone**: Download HBase 2.5.5 distribution
2. **Use HBase shell**: Test basic operations (put, get, scan)
3. **Use HBase Java API from HBase's own classpath**: Avoids conflicts

### Option C: Direct Fluss Client Benchmark (Alternative)
1. Create benchmark using Fluss client API directly (bypasses HBase compat layer)
2. Measure raw Fluss performance
3. Compare with HBase compat layer results later

### Option D: Docker-based YCSB (Clean Environment)
1. Use YCSB Docker image with Python 2
2. Connect to HBase compat server on host (port 16020)
3. Run benchmark inside container

---

## 💡 RECOMMENDATIONS

### Immediate Action:
**Verify HBase Compatibility Server is functional** by testing with HBase shell:

```bash
# Download HBase 2.5.5 distribution
wget https://archive.apache.org/dist/hbase/2.5.5/hbase-2.5.5-bin.tar.gz
tar xzf hbase-2.5.5-bin.tar.gz

# Configure to connect to our server
cat > hbase-2.5.5/conf/hbase-site.xml <<EOF
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
EOF

# Test with HBase shell
./hbase-2.5.5/bin/hbase shell

# Then run:
put 'usertable', 'testkey', 'cf:field0', 'testvalue'
get 'usertable', 'testkey'
scan 'usertable', {LIMIT => 10}
```

### Long-term Solution:
1. **Fix SimpleBenchmark Hadoop conflict**: Shade or exclude conflicting dependencies
2. **Complete YCSB setup**: Use Docker or resolve Python 2 dependency
3. **Run full benchmark suite**: Load + Workloads A/B/C
4. **Generate performance report**: Compare against native HBase baseline

---

## 📝 SESSION NOTES

### Key Learnings:
1. **Classpath management**: Maven's dependency:build-classpath is essential for complex dependency trees
2. **HBase compatibility**: HBase 2.x has strict Hadoop version requirements
3. **YCSB limitations**: Python 2 dependency makes it difficult on modern systems
4. **Server startup**: Requires complete classpath including Fluss client, RPC, common, and server modules

### Time Spent:
- Fluss cluster setup: ~10 minutes
- HBase compat build: ~5 minutes
- Server startup debugging: ~15 minutes (classpath issues)
- YCSB setup attempts: ~20 minutes (failed)
- SimpleBenchmark creation: ~5 minutes
- Total: ~55 minutes

### Success Rate:
- ✅ Completed: 4/8 tasks (50%)
- ⏸️ Blocked: 2/8 tasks (25%)
- ⏳ Not started: 2/8 tasks (25%)

---

## 🔗 REFERENCES

### Documentation:
- [USAGE.md](USAGE.md) - HBase compatibility layer usage guide
- [YCSB-BENCHMARK.md](YCSB-BENCHMARK.md) - Detailed benchmark instructions
- [run-ycsb-benchmark.sh](run-ycsb-benchmark.sh) - Automated benchmark script

### Key Files:
- Server launcher: `org.apache.fluss.hbase.server.HBaseCompatServerLauncher`
- Table creator: `org.apache.fluss.hbase.benchmark.CreateBenchmarkTable`
- Benchmark: `org.apache.fluss.hbase.benchmark.SimpleBenchmark`

### Configuration:
```properties
fluss.bootstrap.servers=localhost:9123
hbase.compat.bind.address=0.0.0.0
hbase.compat.bind.port=16020
hbase.compat.tables=benchmark.usertable
```

---

## ✅ ACHIEVEMENT SUMMARY

**We successfully completed the infrastructure setup for HBase compatibility benchmarking:**

1. ✅ Fluss cluster running (3 processes)
2. ✅ Benchmark table created (benchmark.usertable with 8 buckets)
3. ✅ HBase compat server running and listening on port 16020
4. ✅ Classpath management resolved
5. ✅ Verification utilities created and tested

**Remaining work is to resolve the HBase client Hadoop version conflict or use an alternative benchmarking approach.**

---

*Generated: January 6, 2026 11:45 AM*
