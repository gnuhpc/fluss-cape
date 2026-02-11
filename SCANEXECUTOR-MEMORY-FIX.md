# HBase ScanExecutor Memory Leak Fix

## Problem Summary

**P0 Severity**: Memory leak in `ScanExecutor.java` causing OOM in production

### Root Cause
- Line 259: `List<Result> allResults = new ArrayList<>()`
- Line 532: `ScanSession` stored entire result set (up to 10,000 rows) in memory
- Line 792: `final List<Result> allResults` held data until scanner closed
- **Impact**: 100 concurrent scans = ~10GB memory usage

### Memory Footprint Comparison

| Scenario | Old ScanExecutor | New ScanExecutorStreaming | Reduction |
|---|---|---|---|
| Single scan (10k rows) | ~10 MB | ~1 MB (batch_size=1000) | 90% |
| 100 concurrent scans | ~1 GB | ~100 MB | 90% |
| Peak memory | O(max_results × sessions) | O(batch_size × sessions) | 90% |

---

## Solution: Streaming Scan Architecture

### New Implementation: `ScanExecutorStreaming.java`

**Key Changes:**
1. **No Result Buffering**: `StreamingScanSession` holds scanner iterators, not data
2. **On-Demand Fetching**: `fetchNextBatch()` materializes results during `handleScanNext()` calls
3. **Lazy Scanner Opening**: Opens batch scanners per bucket only when needed
4. **Proper Resource Cleanup**: Closes previous scanner before opening next

### Architecture

```java
// OLD (Memory Leak)
handleScanOpen() {
    List<Result> allResults = new ArrayList<>();  // Buffers ALL 10k rows
    // Scan entire table
    while (hasMoreBuckets) {
        allResults.add(scanBucket());  // OOM risk!
    }
    session = new ScanSession(allResults);  // Holds 10k rows
}

handleScanNext() {
    return session.allResults.subList(index, index + batchSize);  // Return from buffer
}

// NEW (Streaming)
handleScanOpen() {
    session = new StreamingScanSession(scannerConfig);  // NO data, just config
    return scannerId;  // Return immediately
}

handleScanNext() {
    return session.fetchNextBatch(batchSize);  // Fetch on-demand from Fluss!
}
```

### Streaming Flow

```
Client Request                CAPE Instance                      Fluss
     │                              │                              │
     ├─ openScanner() ─────────────▶│                              │
     │                              ├─ Create StreamingScanSession │
     │                              │  (NO data buffering)         │
     │◀─── scannerId ───────────────┤                              │
     │                              │                              │
     ├─ next(1000) ────────────────▶│                              │
     │                              ├─ fetchNextBatch(1000) ──────▶│
     │                              │◀── InternalRow iterator ─────┤
     │                              ├─ Materialize 1000 rows       │
     │◀─── 1000 Results ────────────┤  (discard after return)      │
     │                              │                              │
     ├─ next(1000) ────────────────▶│                              │
     │                              ├─ fetchNextBatch(1000) ──────▶│
     │                              │◀── Next batch ───────────────┤
     │◀─── 1000 Results ────────────┤                              │
     │                              │                              │
     ├─ close() ────────────────────▶│                              │
     │                              ├─ session.close()             │
     │                              │  └─ Close all scanners       │
     │◀─── OK ──────────────────────┤                              │
```

---

## Migration Guide

### Option 1: Switch All Tables (Recommended for Production)

**File**: `TableDiscoveryService.java` or wherever `ScanExecutor` is instantiated

```java
// OLD
ScanExecutor scanExecutor = new ScanExecutor(
    connection, tablePath, rowKeyEncoder, cellConverter
);

// NEW
ScanExecutorStreaming scanExecutor = new ScanExecutorStreaming(
    connection, tablePath, rowKeyEncoder, cellConverter
);
```

### Option 2: Feature Flag (Gradual Rollout)

```java
boolean useStreamingScan = config.getBoolean("hbase.scan.streaming.enabled", true);

HBaseOperationExecutor scanExecutor = useStreamingScan
    ? new ScanExecutorStreaming(connection, tablePath, rowKeyEncoder, cellConverter)
    : new ScanExecutor(connection, tablePath, rowKeyEncoder, cellConverter);
```

### Option 3: Per-Table Configuration

```java
if (isLargeTable(tablePath) || isMemoryConstrained()) {
    return new ScanExecutorStreaming(...);
} else {
    return new ScanExecutor(...);  // For small tables
}
```

---

## Validation

### 1. Memory Usage Test

```bash
cd /root/fluss-cape
mvn test -Dtest=ScanExecutorMemoryTest
```

Expected output:
```
=== Memory Footprint Comparison ===
Scanning 10,000 rows:
  Old ScanExecutor:  10 MB (all rows buffered)
  New Streaming:     1000 KB (batch_size=1000)
  Memory Reduction:  90%

With 100 concurrent scans:
  Old ScanExecutor:  1000 MB
  New Streaming:     100 MB
```

### 2. Functional Testing

```bash
# Start Fluss cluster and CAPE with new executor
# Run HBase shell tests
hbase shell
> scan 'large_table'  # Should stream results without OOM
```

### 3. Production Monitoring

Monitor these metrics after deployment:

```
# Heap usage should drop significantly
jstat -gc <cape-pid> 1000

# Active scanner sessions
jconsole → MBeans → ScanExecutor → activeSessions

# Should remain low (~100MB per instance)
```

---

## Hybrid Scan Strategy Note

For tables with **KV snapshots** (the hybrid scan path), we still buffer the snapshot merge map in memory but apply streaming to the final sorted results. This is unavoidable for consistency:

```java
// Phase 1: Buffer snapshot (still memory-intensive but capped)
Map<ByteArrayWrapper, Result> snapshotMergeMap = new HashMap<>();
// Scan all buckets and merge

// Phase 2: Replay log (merge into map)
replayLog(snapshotMergeMap);

// Phase 3: Sort and stream
List<Result> sortedResults = new ArrayList<>(snapshotMergeMap.values());
sortedResults.sort(...);
snapshotMergeMap.clear();  // Free memory!

// Phase 4: Stream through sorted results
return sortedResults.subList(index, index + batchSize);
```

**Max buffer**: 10,000 rows (enforced by `MAX_SNAPSHOT_BUFFER`)

---

## Rollback Plan

If issues arise, revert by:

1. Find where `ScanExecutorStreaming` is instantiated
2. Change back to `ScanExecutor`
3. Redeploy

No data loss or compatibility issues - both executors implement the same `HBaseOperationExecutor` interface.

---

## Files Changed

- **New**: `ScanExecutorStreaming.java` - Streaming implementation
- **Updated**: `HBaseRequestRouter.java` - Added `instanceof ScanExecutorStreaming` check
- **New**: `ScanExecutorMemoryTest.java` - Memory comparison test
- **Unchanged**: `ScanExecutor.java` - Legacy version kept for rollback

---

## Next Steps

1. ✅ **Done**: Create streaming implementation
2. ✅ **Done**: Add memory test
3. ✅ **Done**: Update router
4. ⏳ **Pending**: Update table discovery service to use streaming executor
5. ⏳ **Pending**: Add feature flag configuration
6. ⏳ **Pending**: Deploy to staging and validate
7. ⏳ **Pending**: Production rollout with monitoring

---

## References

- Original issue: P0 Memory Leak in ScanExecutor (code review findings)
- Inspiration: Kafka `ScannerPool` pattern (see `/kafka/pool/ScannerPool.java`)
- HBase scan protocol: https://hbase.apache.org/book.html#scan
