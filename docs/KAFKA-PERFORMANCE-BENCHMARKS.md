# Kafka Filter Architecture - Performance Benchmark Specification

## Overview

This document specifies the performance benchmarks needed to validate the architecture implementation.

---

## Benchmark 1: Producer Performance

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

### Performance Test Method
```bash
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

---

## Benchmark 2: Consumer Fetch Performance

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

### Performance Test Method
```bash
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

---

## Benchmark 3: Consumer Group Operations

### Objective
Validate consumer group coordination performance.

### Test Cases

#### 3.1 JoinGroup Latency
```bash
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

#### 3.2 Heartbeat Throughput
Monitor coordinator logs for heartbeat processing rate.

**Metrics**:
- Heartbeat processing rate (requests/sec)
- CPU usage during steady-state heartbeat processing
- Memory usage (member metadata)

#### 3.3 Offset Commit/Fetch Performance
```bash
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

---

## Benchmark 4: Resource Pool Efficiency

### Objective
Verify pool behavior under load (LRU eviction, idle timeout, health).

### 4.1 ScannerPool Under Load
**Metrics**:
- Pool hit rate (%)
- Average borrow time (µs)
- Eviction rate (evictions/sec)
- Idle timeout effectiveness

### 4.2 WriterPool Under Load
**Metrics**:
- Pool hit rate (%)
- Writer creation overhead (ms)
- Eviction behavior (LRU correctness)

---

## Benchmark 5: Memory Leak Detection

### Objective
Verify no resource leaks over extended operation.

### Test Procedure
1. Start Fluss CAPE with -Xmx2G -Xms2G
2. Run continuous producer load (1000 msg/sec)
3. Run continuous consumer load (10 consumers)
4. Monitor:
   - Heap usage
   - Thread count
   - File descriptors
   - Scanner/Writer pool sizes

---

## Benchmark 6: Comparative Load Test

### Objective
End-to-end performance comparison.

### Scenario
Simulate realistic Kafka workload:
- 10 producers (1000 msg/sec each)
- 20 consumers (2 consumer groups)
- 100 topics, 3 partitions each
- Run for 1 hour

---

## Test Execution Guide

### Running Benchmarks
```bash
# Run essential benchmarks
./benchmark.sh --suite quick

# Run all benchmarks including soak test
./benchmark.sh --suite full

# Run specific benchmark
./benchmark.sh --benchmark produce-performance
```

---

## JMH Benchmark Template

For micro-benchmarks, use JMH:

```java
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ProduceBenchmark {
    @Benchmark
    public void testProduceAsync() {
        // Benchmark async produce
    }
}
```

---

## Monitoring & Observability

### Key Metrics to Track
1. **Application Metrics**
   - `kafka.produce.latency`
   - `kafka.fetch.latency`
   - `kafka.consumer.group.join.latency`
   - `scanner.pool.hit.rate`
   - `writer.pool.hit.rate`
2. **JVM Metrics**
   - Heap usage
   - GC pause time
   - Thread count
3. **Fluss Metrics**
   - Table write/read rate
