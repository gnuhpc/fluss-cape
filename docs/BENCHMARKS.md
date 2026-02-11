# Performance Benchmarks

Performance benchmarking guide for Fluss CAPE. This document provides general guidance on benchmarking methodologies and tuning recommendations.

---

## Table of Contents

1. [Benchmarking Overview](#benchmarking-overview)
2. [YCSB Benchmarking](#ycsb-benchmarking)
3. [Tuning Recommendations](#tuning-recommendations)
4. [Hardware Recommendations](#hardware-recommendations)
5. [Running Your Own Benchmarks](#running-your-own-benchmarks)
6. [Performance Monitoring](#performance-monitoring)

---

## Benchmarking Overview

### Important Notes

**Performance varies significantly based on:**
- Hardware specifications (CPU, memory, storage)
- Network configuration and latency
- Workload characteristics (read/write ratio, data size, access patterns)
- Fluss cluster configuration and sizing
- CAPE server configuration

**Specific performance numbers depend heavily on your environment and use case.**

### Key Metrics to Monitor

- **Throughput**: Operations per second
- **Latency**: Response time percentiles (P50, P95, P99)
- **Resource Utilization**: CPU, memory, network, disk I/O
- **Error Rates**: Connection failures, timeouts

---

## YCSB Benchmarking

### What is YCSB?

[Yahoo! Cloud Serving Benchmark](https://github.com/brianfrankcooper/YCSB) is the industry-standard tool for evaluating NoSQL databases.

### Installation

```bash
# Download YCSB
curl -O https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz
tar -xzf ycsb-0.17.0.tar.gz
cd ycsb-0.17.0
```

### Running YCSB Tests

#### HBase Benchmark

```bash
# Load data
./bin/ycsb load hbase2 -P workloads/workloada \
  -p table=usertable \
  -p columnfamily=family \
  -p recordcount=1000000 \
  -p hbase.zookeeper.quorum=localhost:2181 \
  -threads 16

# Run workload
./bin/ycsb run hbase2 -P workloads/workloada \
  -p table=usertable \
  -p columnfamily=family \
  -p operationcount=1000000 \
  -p hbase.zookeeper.quorum=localhost:2181 \
  -threads 16
```

#### Redis Benchmark

```bash
# Load data
./bin/ycsb load redis -P workloads/workloada \
  -p redis.host=localhost \
  -p redis.port=6379 \
  -p recordcount=1000000 \
  -threads 16

# Run workload
./bin/ycsb run redis -P workloads/workloada \
  -p redis.host=localhost \
  -p redis.port=6379 \
  -p operationcount=1000000 \
  -threads 16
```

### YCSB Workloads

| Workload | Description | Read/Write Ratio |
|----------|-------------|------------------|
| **A** | Update heavy | 50% reads, 50% updates |
| **B** | Read mostly | 95% reads, 5% updates |
| **C** | Read only | 100% reads |
| **D** | Read latest | 95% reads, 5% inserts |
| **E** | Short ranges | 95% scans, 5% inserts |
| **F** | Read-modify-write | 50% reads, 50% read-modify-write |

---

## Tuning Recommendations

### 1. Thread Pool Sizing

**Recommendation**: Set worker threads = 2-4x CPU cores

```properties
# For 16-core machine
hbase.compat.worker.threads=64
redis.worker.threads=32
```

### 2. Connection Pool

**Recommendation**: Increase Fluss client pool for high concurrency

```properties
fluss.client.connection.pool.size=50
```

### 3. Batch Operations

**Recommendation**: Use batch operations when possible

```java
// Instead of 1000 individual puts
for (Put put : puts) table.put(put);

// Use batch put
table.put(puts);
```

### 4. Scan Caching

**Recommendation**: Tune caching based on scan size

```properties
hbase.compat.scan.caching=1000  # For large scans
hbase.compat.scan.caching=100   # For small scans
```

### 5. Redis Pipelining

**Recommendation**: Use pipelining for bulk operations

```python
pipe = r.pipeline()
for i in range(1000):
    pipe.set(f'key:{i}', value)
pipe.execute()
```

### 6. JVM Tuning

**Recommendation**: Use G1GC with appropriate heap size

```bash
java -Xmx16g -Xms16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -jar fluss-cape.jar
```

---

## Hardware Recommendations

### Small Deployment (< 1000 concurrent clients)

- **CPU**: 8 cores
- **RAM**: 8 GB
- **Network**: 1 Gbps

### Medium Deployment (1000-5000 concurrent clients)

- **CPU**: 16 cores
- **RAM**: 16 GB
- **Network**: 10 Gbps

### Large Deployment (5000-10000 concurrent clients)

- **CPU**: 32 cores
- **RAM**: 32 GB
- **Network**: 10 Gbps

---

## Running Your Own Benchmarks

### 1. HBase YCSB Full Suite

```bash
#!/bin/bash
# run-hbase-ycsb.sh

WORKLOADS="a b c d e f"
THREADS="1 8 16 32 64"
RECORD_COUNT=10000000
OP_COUNT=10000000

for workload in $WORKLOADS; do
  for threads in $THREADS; do
    echo "Running workload$workload with $threads threads"
    # Load and Run YCSB commands...
  done
done
```

### 2. Redis Benchmark Suite

```bash
#!/bin/bash
# run-redis-benchmark.sh

OPERATIONS="set get incr lpush lpop sadd spop hset hget zadd zrange"
PIPELINES="1 10 100"
COUNT=1000000

for op in $OPERATIONS; do
  for pipeline in $PIPELINES; do
    echo "Running $op with pipeline=$pipeline"
    redis-benchmark -h localhost -p 6379 -n $COUNT -t $op -P $pipeline -q
  done
done
```

---

## Performance Monitoring

### Key Metrics to Monitor

1. **Throughput**: Operations per second
2. **Latency**: P50, P95, P99, P99.9
3. **CPU Usage**: Should be 60-80% under load
4. **Memory Usage**: Monitor heap and GC pauses
5. **Network**: Bandwidth and packet rate
6. **Connection Count**: Active connections

### Tools

- **JMX**: Monitor JVM metrics
- **Prometheus/Grafana**: Monitoring and visualization
- **jstat**: GC monitoring
- **top/htop**: System resource usage
