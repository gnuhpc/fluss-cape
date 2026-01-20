# Kafka Filter Architecture - Performance Benchmark Specification

## Overview

This document specifies the performance benchmarks needed to validate the expected improvements from the Kafka filter architecture implementation.

## Expected Performance Improvements

| Metric | Before | After | Expected Gain | Priority |
|--------|--------|-------|---------------|----------|
| **Fetch Latency** | 15ms | 5ms | 3x faster | HIGH |
| **Produce Latency (100 records)** | 500ms | 10ms | 50x faster | HIGH |
| **Throughput** | ~200 msg/s | 50,000 msg/s | 250x faster | HIGH |
| **Memory Usage** | Unbounded growth | Stable (pooled) | Critical fix | HIGH |
| **Scanner Leaks** | Yes (resource leak) | No (pooled) | Critical fix | HIGH |

---

## Benchmark 1: Producer Performance (HIGH PRIORITY)

### Objective
Measure the impact of async batch writes using WriterPool.

### Setup
```java
// Test configuration
int NUM_RECORDS = 100;
int NUM_ITERATIONS = 1000;
String TOPIC = "perf-test-produce";
int PARTITION_COUNT = 3;
```

### Baseline Test (Before Implementation)
```bash
# Using old synchronous produce logic (before WriterPool)
# Expected: ~500ms for 100 records = 200 records/sec

kafka-producer-perf-test.sh \
  --topic perf-test-produce \
  --num-records 100000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props \
    bootstrap.servers=localhost:9092 \
    batch.size=16384 \
    linger.ms=0
```

### After Implementation Test
```bash
# Using async batch writes with WriterPool
# Expected: ~10ms for 100 records = 10,000 records/sec

kafka-producer-perf-test.sh \
  --topic perf-test-produce \
  --num-records 100000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props \
    bootstrap.servers=localhost:9092 \
    batch.size=16384 \
    linger.ms=0
```

### Metrics to Collect
- **Average latency** (ms per batch)
- **p50, p95, p99 latency** (percentiles)
- **Throughput** (records/sec)
- **CPU usage** (%)
- **Memory usage** (heap size over time)
- **Writer pool statistics** (hit rate, evictions)

### Success Criteria
- ✅ Average latency < 20ms for 100 records (50x improvement from 500ms)
- ✅ Throughput > 5,000 records/sec (25x improvement from 200 rec/sec)
- ✅ p99 latency < 50ms
- ✅ No memory leaks (heap stable after 10 minutes)

---

## Benchmark 2: Consumer Fetch Performance (HIGH PRIORITY)

### Objective
Measure the impact of ScannerPool on fetch latency.

### Setup
```java
// Test configuration
int NUM_RECORDS = 10000;  // Pre-populate topic
int FETCH_ITERATIONS = 1000;
String TOPIC = "perf-test-fetch";
int PARTITION_COUNT = 3;
```

### Baseline Test (Before Implementation)
```bash
# Using old approach (scanner creation per request)
# Expected: 15ms per fetch

kafka-consumer-perf-test.sh \
  --topic perf-test-fetch \
  --messages 100000 \
  --threads 1 \
  --bootstrap-server localhost:9092 \
  --group perf-test-group-baseline
```

### After Implementation Test
```bash
# Using ScannerPool
# Expected: 5ms per fetch

kafka-consumer-perf-test.sh \
  --topic perf-test-fetch \
  --messages 100000 \
  --threads 1 \
  --bootstrap-server localhost:9092 \
  --group perf-test-group-pooled
```

### Metrics to Collect
- **Fetch latency** (ms per fetch request)
- **Scanner creation overhead** (time spent creating vs. reusing)
- **Pool hit rate** (% of fetches using pooled scanners)
- **Pool statistics** (size, evictions, idle timeouts)
- **Memory usage** (scanner objects in heap)

### Success Criteria
- ✅ Average fetch latency < 7ms (3x improvement from 15ms)
- ✅ Pool hit rate > 80%
- ✅ No scanner resource leaks (verify with thread dump)
- ✅ Memory usage stable

---

## Benchmark 3: Consumer Group Operations (MEDIUM PRIORITY)

### Objective
Validate consumer group coordination performance.

### Test Cases

#### 3.1 JoinGroup Latency
```bash
# Test: 10 consumers joining sequentially
# Expected: < 100ms per join

for i in {1..10}; do
  kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic test-topic \
    --group perf-cg-join-$i &
done
```

**Metrics**:
- Time for each consumer to join
- Generation increment latency
- Rebalance duration

**Success Criteria**:
- ✅ JoinGroup response < 100ms
- ✅ Rebalance completes < 500ms for 10 members

#### 3.2 Heartbeat Throughput
```bash
# Test: 100 consumers sending heartbeats every 3 seconds
# Expected: > 1000 heartbeats/sec

# Monitor coordinator logs for heartbeat processing rate
```

**Metrics**:
- Heartbeat processing rate (requests/sec)
- CPU usage during steady-state heartbeat processing
- Memory usage (member metadata)

**Success Criteria**:
- ✅ Handle 1000+ heartbeats/sec
- ✅ CPU usage < 10% for heartbeat processing
- ✅ No memory leaks in member tracking

#### 3.3 Offset Commit/Fetch Performance
```bash
# Test: 100 consumers committing offsets every 5 seconds
# Expected: < 50ms per commit

kafka-consumer-perf-test.sh \
  --topic perf-test-offsets \
  --messages 100000 \
  --threads 10 \
  --bootstrap-server localhost:9092 \
  --group perf-cg-offsets
```

**Metrics**:
- OffsetCommit latency (ms)
- OffsetFetch latency (ms)
- Throughput (commits/sec)
- Fluss table write rate (`__consumer_offsets`)

**Success Criteria**:
- ✅ OffsetCommit < 50ms (p95)
- ✅ OffsetFetch < 20ms (p95)
- ✅ Support 1000+ commits/sec

---

## Benchmark 4: Resource Pool Efficiency (HIGH PRIORITY)

### Objective
Verify pool behavior under load (LRU eviction, idle timeout, health).

### 4.1 ScannerPool Under Load

```java
// JMH Benchmark
@Benchmark
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
public void testScannerPoolHitRate() {
    // Simulate 1000 fetch requests with 100 unique table/partition combos
    // Expected: 90%+ hit rate
}
```

**Metrics**:
- Pool hit rate (%)
- Average borrow time (µs)
- Eviction rate (evictions/sec)
- Idle timeout effectiveness

**Success Criteria**:
- ✅ Hit rate > 80% under typical load
- ✅ Borrow time < 1ms (near-instant)
- ✅ Idle scanners evicted after 5 minutes
- ✅ No memory growth over 1 hour

### 4.2 WriterPool Under Load

```java
@Benchmark
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
public void testWriterPoolHitRate() {
    // Simulate 1000 produce requests to 50 tables
    // Expected: 80%+ hit rate
}
```

**Metrics**:
- Pool hit rate (%)
- Writer creation overhead (ms)
- Eviction behavior (LRU correctness)

**Success Criteria**:
- ✅ Hit rate > 75%
- ✅ Writer creation saved on 75%+ requests
- ✅ LRU eviction working (least-recently-used tables evicted first)

---

## Benchmark 5: Memory Leak Detection (CRITICAL)

### Objective
Verify no resource leaks over extended operation.

### Test Procedure
```bash
# Run for 8 hours with continuous load
# Monitor heap usage every 5 minutes

1. Start Fluss CAPE with -Xmx2G -Xms2G
2. Run continuous producer load (1000 msg/sec)
3. Run continuous consumer load (10 consumers)
4. Monitor:
   - Heap usage (should stabilize)
   - Thread count (should stabilize)
   - File descriptors (should stabilize)
   - Scanner/Writer pool sizes (should stabilize)

5. Take heap dump every hour
6. Analyze for:
   - Growing collections
   - Unreleased Fluss resources
   - Scanner/Writer instances not returned to pool
```

**Success Criteria**:
- ✅ Heap usage stabilizes within 1 hour
- ✅ No continuous heap growth over 8 hours
- ✅ Thread count remains constant
- ✅ File descriptor count stable
- ✅ Pool sizes stable (no unbounded growth)

---

## Benchmark 6: Comparative Load Test (HIGH PRIORITY)

### Objective
End-to-end performance comparison before/after implementation.

### Scenario
Simulate realistic Kafka workload:
- 10 producers (1000 msg/sec each)
- 20 consumers (2 consumer groups)
- 100 topics, 3 partitions each
- Run for 1 hour

### Baseline (Before Implementation)
```bash
# Expected behavior:
# - Produce latency: 500ms per batch
# - Fetch latency: 15ms per fetch
# - Scanner leaks causing OOM after 30 minutes
# - Throughput: ~5,000 msg/sec total
```

### After Implementation
```bash
# Expected behavior:
# - Produce latency: 10ms per batch
# - Fetch latency: 5ms per fetch
# - No resource leaks, stable for 8+ hours
# - Throughput: 50,000+ msg/sec total
```

### Metrics Dashboard
Monitor via Prometheus/Grafana:
- Request rate (req/sec)
- Latency percentiles (p50, p95, p99)
- Error rate (%)
- CPU usage (%)
- Memory usage (MB)
- Pool hit rates (%)
- Fluss table write rate

### Success Criteria
- ✅ Throughput > 40,000 msg/sec (8x improvement)
- ✅ p99 produce latency < 50ms (10x improvement)
- ✅ p99 fetch latency < 10ms (3x improvement)
- ✅ Zero resource leaks over 8 hours
- ✅ CPU usage < 50%
- ✅ Memory usage stable

---

## Test Execution Guide

### Prerequisites
```bash
# 1. Deploy Fluss CAPE with monitoring
docker-compose up -d fluss-cape prometheus grafana

# 2. Pre-create test topics
for i in {1..100}; do
  kafka-topics.sh --create \
    --topic perf-test-$i \
    --partitions 3 \
    --replication-factor 1 \
    --bootstrap-server localhost:9092
done

# 3. Enable JMX metrics
export KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote ..."
```

### Running Benchmarks

#### Quick Validation (30 minutes)
```bash
# Run essential benchmarks
./benchmark.sh --suite quick
# Runs: Benchmark 1, 2, 3 with reduced iterations
```

#### Full Benchmark Suite (8 hours)
```bash
# Run all benchmarks including soak test
./benchmark.sh --suite full
# Runs: All benchmarks including 8-hour leak detection
```

#### Single Benchmark
```bash
# Run specific benchmark
./benchmark.sh --benchmark produce-performance
./benchmark.sh --benchmark fetch-performance
./benchmark.sh --benchmark consumer-groups
```

### Collecting Results

```bash
# Generate performance report
./generate-report.sh \
  --baseline results/baseline.json \
  --improved results/improved.json \
  --output report.html

# Report includes:
# - Side-by-side comparison tables
# - Latency percentile charts
# - Throughput comparison graphs
# - Resource usage trends
# - Pool statistics
```

---

## JMH Benchmark Template

For micro-benchmarks, use JMH:

```java
package org.gnuhpc.fluss.cape.kafka.benchmark;

import org.openjdk.jmh.annotations.*;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(1)
public class ProduceBenchmark {

    private ProduceHandler handler;
    
    @Setup
    public void setup() {
        // Initialize handler with WriterPool dependencies
    }
    
    @Benchmark
    public void testProduceAsync() {
        // Benchmark async produce with 100 records
    }
    
    @TearDown
    public void teardown() {
        // Cleanup
    }
}
```

Run with:
```bash
mvn clean install
java -jar target/benchmarks.jar ProduceBenchmark
```

---

## Monitoring & Observability

### Key Metrics to Track

1. **Application Metrics** (via Micrometer/Prometheus)
   - `kafka.produce.latency` - Produce request latency
   - `kafka.fetch.latency` - Fetch request latency
   - `kafka.consumer.group.join.latency` - JoinGroup latency
   - `scanner.pool.hit.rate` - Scanner pool hit rate
   - `writer.pool.hit.rate` - Writer pool hit rate
   - `scanner.pool.size` - Current scanner pool size
   - `writer.pool.size` - Current writer pool size

2. **JVM Metrics**
   - Heap usage (used/committed/max)
   - GC pause time
   - Thread count
   - CPU usage

3. **Fluss Metrics**
   - Table write rate
   - Table read rate
   - Storage size

### Grafana Dashboard

Create dashboard with panels for:
- Request rate timeline
- Latency heatmap (p50/p95/p99)
- Pool hit rate timeline
- Memory usage timeline
- GC activity
- Error rate

---

## Regression Testing

### Continuous Performance Testing

Add to CI/CD pipeline:

```yaml
# .github/workflows/performance-test.yml
name: Performance Test
on:
  pull_request:
    branches: [main]
    
jobs:
  perf-test:
    runs-on: ubuntu-latest
    steps:
      - name: Run Quick Benchmark
        run: ./benchmark.sh --suite quick --compare baseline.json
      
      - name: Check Regression
        run: |
          # Fail if performance degrades > 10%
          ./check-regression.sh --threshold 0.1
```

### Performance Budget

Define acceptable thresholds:
- Produce latency: < 20ms (p95)
- Fetch latency: < 10ms (p95)
- Throughput: > 40,000 msg/sec
- Memory growth: < 1% per hour

---

## Appendix: Expected Results Summary

| Benchmark | Baseline | Target | Critical? |
|-----------|----------|--------|-----------|
| Produce (100 records) | 500ms | 10ms | YES |
| Fetch latency | 15ms | 5ms | YES |
| Throughput | 200 msg/s | 50k msg/s | YES |
| JoinGroup latency | N/A | <100ms | NO |
| OffsetCommit latency | N/A | <50ms | NO |
| Scanner pool hit rate | N/A | >80% | YES |
| Writer pool hit rate | N/A | >75% | YES |
| Memory leak | YES | NO | YES |

**Status**: Specification complete. Ready for execution when Fluss cluster is available.
