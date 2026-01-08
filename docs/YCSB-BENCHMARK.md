# YCSB Benchmark Guide - Manual Steps

This document provides manual step-by-step instructions for running YCSB benchmarks against the HBase Compatibility Layer.

## Prerequisites

- Java 11 or higher
- Maven 3.8.6 or higher
- Running Fluss cluster (localhost:9123)
- HBase Compatibility Layer built

## Step-by-Step Instructions

### 1. Build HBase Compatibility Layer

```bash
cd /Users/gnuhpc/IdeaProjects/fluss
mvn clean install -pl fluss-hbase-compat -DskipTests
```

**Expected output**: `fluss-hbase-compat/target/fluss-hbase-compat-0.9-SNAPSHOT.jar`

### 2. Start Fluss Cluster

Since this version doesn't include docker-compose, start Fluss manually:

```bash
# Build distribution
./mvnw clean package -DskipTests

# Extract distribution
cd build-target
tar -xzf fluss-*.tgz
cd fluss-*

# Start coordinator server
./bin/fluss-daemon.sh start coordinatorServer

# Start tablet server
./bin/fluss-daemon.sh start tabletServer

# Verify cluster is running
./bin/fluss-cli.sh cluster list
```

### 3. Create YCSB Benchmark Table

Create SQL file (`/tmp/create_ycsb_table.sql`):

```sql
CREATE DATABASE IF NOT EXISTS benchmark;

DROP TABLE IF EXISTS benchmark.usertable;

CREATE TABLE benchmark.usertable (
    YCSB_KEY STRING,
    field0 STRING,
    field1 STRING,
    field2 STRING,
    field3 STRING,
    field4 STRING,
    field5 STRING,
    field6 STRING,
    field7 STRING,
    field8 STRING,
    field9 STRING,
    PRIMARY KEY (YCSB_KEY) NOT ENFORCED
) WITH (
    'bucket.num' = '8'
);
```

Execute:

```bash
./bin/sql-client.sh -f /tmp/create_ycsb_table.sql
```

**Verify table exists**:

```bash
./bin/sql-client.sh
> SHOW TABLES IN benchmark;
```

### 4. Download and Setup YCSB

```bash
cd ~
wget https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz
tar xfvz ycsb-0.17.0.tar.gz
cd ycsb-0.17.0
```

### 5. Configure YCSB for HBase Compatibility

Create `conf/hbase-site.xml`:

```xml
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
  <property>
    <name>hbase.client.retries.number</name>
    <value>3</value>
  </property>
  <property>
    <name>hbase.client.pause</name>
    <value>100</value>
  </property>
</configuration>
```

### 6. Start HBase Compatibility Server

```bash
cd /Users/gnuhpc/IdeaProjects/fluss

java -Dfluss.bootstrap.servers=localhost:9123 \
     -Dhbase.compat.bind.address=0.0.0.0 \
     -Dhbase.compat.bind.port=16020 \
     -Dhbase.compat.tables=benchmark.usertable \
     -cp fluss-hbase-compat/target/fluss-hbase-compat-0.9-SNAPSHOT.jar \
     org.apache.fluss.hbase.server.HBaseCompatServerLauncher \
     > hbase-compat-server.log 2>&1 &
```

**Verify server started**:

```bash
# Check process
ps aux | grep HBaseCompatServerLauncher

# Check logs
tail -f hbase-compat-server.log

# Expected: "HBase Compatibility Server started!"
```

### 7. Load Data (YCSB Load Phase)

```bash
cd ~/ycsb-0.17.0

./bin/ycsb load hbase2 \
  -P workloads/workloada \
  -p table=usertable \
  -p columnfamily=cf \
  -p recordcount=100000 \
  -p threadcount=16 \
  -s | tee load-results.txt
```

**Expected metrics**:
- `[OVERALL], Throughput(ops/sec)` - Operations per second
- `[INSERT], AverageLatency(us)` - Average insert latency
- `[INSERT], 95thPercentileLatency(us)` - P95 latency
- `[INSERT], 99thPercentileLatency(us)` - P99 latency

**Typical performance**:
- Throughput: 20,000-40,000 ops/sec (depends on hardware)
- P95 latency: < 1000 μs
- P99 latency: < 2000 μs

### 8. Run Workload A (50% Read, 50% Update)

```bash
./bin/ycsb run hbase2 \
  -P workloads/workloada \
  -p table=usertable \
  -p columnfamily=cf \
  -p operationcount=100000 \
  -p threadcount=16 \
  -s | tee workloada-results.txt
```

**Expected metrics**:
- Mixed read/update workload
- `[READ], AverageLatency(us)`
- `[UPDATE], AverageLatency(us)`
- `[OVERALL], Throughput(ops/sec)`

### 9. Run Workload B (95% Read, 5% Update)

```bash
./bin/ycsb run hbase2 \
  -P workloads/workloadb \
  -p table=usertable \
  -p columnfamily=cf \
  -p operationcount=100000 \
  -p threadcount=16 \
  -s | tee workloadb-results.txt
```

**Expected metrics**:
- Read-heavy workload
- Higher throughput than Workload A
- Lower read latency due to caching

### 10. Run Workload C (100% Read)

```bash
./bin/ycsb run hbase2 \
  -P workloads/workloadc \
  -p table=usertable \
  -p columnfamily=cf \
  -p operationcount=100000 \
  -p threadcount=16 \
  -s | tee workloadc-results.txt
```

**Expected metrics**:
- Read-only workload
- Highest throughput
- Lowest latency

### 11. Analyze Results

Extract key metrics:

```bash
# Throughput summary
echo "=== Throughput Summary ===" > summary.txt
grep "Throughput" load-results.txt workloada-results.txt workloadb-results.txt workloadc-results.txt >> summary.txt

# Latency summary
echo "" >> summary.txt
echo "=== Latency Summary ===" >> summary.txt
grep "AverageLatency" load-results.txt workloada-results.txt workloadb-results.txt workloadc-results.txt >> summary.txt

echo "" >> summary.txt
echo "=== P99 Latency ===" >> summary.txt
grep "99thPercentileLatency" load-results.txt workloada-results.txt workloadb-results.txt workloadc-results.txt >> summary.txt

cat summary.txt
```

### 12. Performance Targets

Based on design specifications, expected performance:

| Metric | Target | Notes |
|--------|--------|-------|
| Get QPS | 50,000+ | Single key lookups |
| Put QPS | 30,000+ | Upsert operations |
| Get P99 Latency | < 2ms | Under normal load |
| Put P99 Latency | < 5ms | Under normal load |
| vs Native HBase | 75-85% | Performance ratio |

### 13. Cleanup

```bash
# Stop HBase Compatibility Server
pkill -f HBaseCompatServerLauncher

# Stop Fluss cluster
cd /path/to/fluss
./bin/fluss-daemon.sh stop tabletServer
./bin/fluss-daemon.sh stop coordinatorServer

# Optional: Drop benchmark table
./bin/sql-client.sh
> DROP TABLE benchmark.usertable;
> DROP DATABASE benchmark;
```

## Troubleshooting

### YCSB Connection Errors

**Problem**: `Connection refused` or `UnknownHostException`

**Solutions**:
1. Verify HBase Compatibility Server is running: `ps aux | grep HBaseCompatServerLauncher`
2. Check server logs: `tail -f hbase-compat-server.log`
3. Verify port is listening: `netstat -an | grep 16020`
4. Check firewall: `sudo lsof -i :16020`

### Low Throughput

**Problem**: Throughput much lower than expected

**Possible causes**:
1. **Thread count too low**: Increase `-p threadcount=32`
2. **Network latency**: Run YCSB client on same host as compatibility server
3. **Fluss cluster under-provisioned**: Increase tablet server count
4. **Bucket count too low**: Recreate table with higher `bucket.num`

### High Latency

**Problem**: P99 latency > 10ms

**Possible causes**:
1. **GC pressure**: Increase JVM heap for compatibility server
2. **CPU contention**: Reduce thread count
3. **Fluss overload**: Check Fluss server metrics
4. **Network issues**: Check network latency between components

### Table Not Found

**Problem**: `TableNotFoundException` in YCSB

**Solutions**:
1. Verify table exists in Fluss: `SHOW TABLES IN benchmark;`
2. Check compatibility server registered the table: Look for "Successfully registered table" in logs
3. Restart compatibility server with correct `-Dhbase.compat.tables` parameter

## Advanced Configuration

### Custom Workload

Create custom workload file (`workloads/workloadx`):

```properties
recordcount=1000000
operationcount=1000000
workload=site.ycsb.workloads.CoreWorkload

readallfields=true
readproportion=0.7
updateproportion=0.2
scanproportion=0.05
insertproportion=0.05

requestdistribution=zipfian
```

Run:

```bash
./bin/ycsb run hbase2 -P workloads/workloadx \
  -p table=usertable -p columnfamily=cf -p threadcount=16 -s
```

### Large Dataset Testing

For 10M+ records:

```bash
# Load phase (may take 5-10 minutes)
./bin/ycsb load hbase2 -P workloads/workloada \
  -p table=usertable \
  -p columnfamily=cf \
  -p recordcount=10000000 \
  -p threadcount=32 \
  -s | tee load-10m.txt

# Run phase
./bin/ycsb run hbase2 -P workloads/workloada \
  -p table=usertable \
  -p columnfamily=cf \
  -p operationcount=10000000 \
  -p threadcount=32 \
  -s | tee run-10m.txt
```

### Multi-Client Simulation

Run multiple YCSB clients simultaneously:

```bash
# Terminal 1
./bin/ycsb run hbase2 -P workloads/workloada \
  -p table=usertable -p columnfamily=cf \
  -p threadcount=8 -s > client1.txt &

# Terminal 2
./bin/ycsb run hbase2 -P workloads/workloadb \
  -p table=usertable -p columnfamily=cf \
  -p threadcount=8 -s > client2.txt &

# Terminal 3
./bin/ycsb run hbase2 -P workloads/workloadc \
  -p table=usertable -p columnfamily=cf \
  -p threadcount=8 -s > client3.txt &

# Wait for all to complete
wait

# Aggregate results
cat client*.txt | grep "Throughput"
```

## Performance Tuning

### JVM Settings for Compatibility Server

```bash
java -Xms2g -Xmx4g \
     -XX:+UseG1GC \
     -XX:MaxGCPauseMillis=100 \
     -XX:+PrintGCDetails \
     -Dfluss.bootstrap.servers=localhost:9123 \
     -Dhbase.compat.bind.port=16020 \
     -Dhbase.compat.tables=benchmark.usertable \
     -cp fluss-hbase-compat-0.9-SNAPSHOT.jar \
     org.apache.fluss.hbase.server.HBaseCompatServerLauncher
```

### Fluss Table Tuning

For better performance, adjust bucket count:

```sql
-- For high-throughput workloads
CREATE TABLE benchmark.usertable (
    ...
) WITH (
    'bucket.num' = '16'  -- More buckets = better parallelism
);
```

## Next Steps

After successful benchmarking:

1. **Document results**: Save all result files for comparison
2. **Compare with native HBase**: Run same workloads against real HBase
3. **Profile bottlenecks**: Use JProfiler/VisualVM if performance is below target
4. **Scale testing**: Test with multiple compatibility servers
5. **Stress testing**: Push to failure to find limits

## Support

For issues or questions:
- Check logs: `hbase-compat-server.log`
- Review Fluss logs: `$FLUSS_HOME/logs/`
- Enable debug logging: `-Dlog4j.logger.org.apache.fluss.hbase=DEBUG`
