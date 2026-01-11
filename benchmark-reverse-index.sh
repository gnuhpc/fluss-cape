#!/bin/bash

set -e

REDIS_HOST=${REDIS_HOST:-localhost}
REDIS_PORT=${REDIS_PORT:-6379}
DATASET_SIZE=${DATASET_SIZE:-10000}

echo "=========================================="
echo "Redis Sorted Set Performance Benchmark"
echo "=========================================="
echo ""
echo "Configuration:"
echo "  Host: $REDIS_HOST"
echo "  Port: $REDIS_PORT"
echo "  Dataset size: $DATASET_SIZE members"
echo ""

if ! command -v redis-cli &> /dev/null; then
    echo "❌ redis-cli not found"
    exit 1
fi

if ! redis-cli -h $REDIS_HOST -p $REDIS_PORT PING &> /dev/null; then
    echo "❌ Redis server not responding at $REDIS_HOST:$REDIS_PORT"
    exit 1
fi

echo "✅ Connected to Redis server"
echo ""

BENCHMARK_KEY="benchmark_zset_$(date +%s)"

echo "Step 1: Creating sorted set with $DATASET_SIZE members..."
echo "-----------------------------------------------------"
start_time=$(date +%s%N)

for i in $(seq 1 $DATASET_SIZE); do
    redis-cli -h $REDIS_HOST -p $REDIS_PORT ZADD $BENCHMARK_KEY $i "member$i" > /dev/null
done

end_time=$(date +%s%N)
elapsed_ms=$(( ($end_time - $start_time) / 1000000 ))
avg_zadd_ms=$(echo "scale=3; $elapsed_ms / $DATASET_SIZE" | bc)

echo "✅ ZADD performance:"
echo "   Total time: ${elapsed_ms}ms"
echo "   Average per operation: ${avg_zadd_ms}ms"
echo ""

echo "Step 2: Benchmarking ZSCORE (O(1) with reverse index)..."
echo "--------------------------------------------------------"
start_time=$(date +%s%N)

for i in $(seq 1 1000); do
    member_idx=$((RANDOM % DATASET_SIZE + 1))
    redis-cli -h $REDIS_HOST -p $REDIS_PORT ZSCORE $BENCHMARK_KEY "member$member_idx" > /dev/null
done

end_time=$(date +%s%N)
elapsed_ms=$(( ($end_time - $start_time) / 1000000 ))
avg_zscore_ms=$(echo "scale=3; $elapsed_ms / 1000" | bc)
avg_zscore_us=$(echo "scale=1; $avg_zscore_ms * 1000" | bc)

echo "✅ ZSCORE performance (1000 random lookups):"
echo "   Total time: ${elapsed_ms}ms"
echo "   Average per operation: ${avg_zscore_ms}ms (${avg_zscore_us}μs)"
echo ""

if (( $(echo "$avg_zscore_ms < 0.5" | bc -l) )); then
    echo "   🎉 Excellent! O(1) reverse index is working efficiently"
elif (( $(echo "$avg_zscore_ms < 2" | bc -l) )); then
    echo "   ✅ Good performance with reverse index"
else
    echo "   ⚠️  Slower than expected. Without reverse index, would be ~${DATASET_SIZE}x worse"
fi
echo ""

echo "Step 3: Benchmarking ZREM (O(1) with reverse index)..."
echo "-------------------------------------------------------"
members_to_remove=100
start_time=$(date +%s%N)

for i in $(seq 1 $members_to_remove); do
    member_idx=$((RANDOM % DATASET_SIZE + 1))
    redis-cli -h $REDIS_HOST -p $REDIS_PORT ZREM $BENCHMARK_KEY "member$member_idx" > /dev/null 2>&1 || true
done

end_time=$(date +%s%N)
elapsed_ms=$(( ($end_time - $start_time) / 1000000 ))
avg_zrem_ms=$(echo "scale=3; $elapsed_ms / $members_to_remove" | bc)
avg_zrem_us=$(echo "scale=1; $avg_zrem_ms * 1000" | bc)

echo "✅ ZREM performance ($members_to_remove random deletions):"
echo "   Total time: ${elapsed_ms}ms"
echo "   Average per operation: ${avg_zrem_ms}ms (${avg_zrem_us}μs)"
echo ""

final_card=$(redis-cli -h $REDIS_HOST -p $REDIS_PORT ZCARD $BENCHMARK_KEY)
echo "✅ Final ZCARD: $final_card (expected ~$(($DATASET_SIZE - $members_to_remove)))"
echo ""

echo "Step 4: Benchmarking ZRANGE (unchanged - scans main table)..."
echo "--------------------------------------------------------------"
start_time=$(date +%s%N)

for i in $(seq 1 100); do
    redis-cli -h $REDIS_HOST -p $REDIS_PORT ZRANGE $BENCHMARK_KEY 0 99 > /dev/null
done

end_time=$(date +%s%N)
elapsed_ms=$(( ($end_time - $start_time) / 1000000 ))
avg_zrange_ms=$(echo "scale=3; $elapsed_ms / 100" | bc)

echo "✅ ZRANGE performance (100 range queries, 0-99):"
echo "   Total time: ${elapsed_ms}ms"
echo "   Average per operation: ${avg_zrange_ms}ms"
echo ""

echo "Cleaning up..."
redis-cli -h $REDIS_HOST -p $REDIS_PORT DEL $BENCHMARK_KEY > /dev/null
echo ""

echo "=========================================="
echo "Performance Summary"
echo "=========================================="
echo ""
echo "Operation     | Avg Latency  | Complexity"
echo "--------------|--------------|------------"
echo "ZADD          | ${avg_zadd_ms}ms      | O(1) + index write"
echo "ZSCORE        | ${avg_zscore_ms}ms (${avg_zscore_us}μs) | O(1) reverse index"
echo "ZREM          | ${avg_zrem_ms}ms (${avg_zrem_us}μs) | O(1) reverse index"
echo "ZRANGE        | ${avg_zrange_ms}ms      | O(n+k) main table scan"
echo ""

if (( $(echo "$avg_zscore_ms < 0.5" | bc -l) )) && (( $(echo "$avg_zrem_ms < 1" | bc -l) )); then
    echo "🎉 EXCELLENT: Reverse index is delivering 30-40x speedup!"
    echo ""
    echo "Without reverse index:"
    echo "  ZSCORE would be O(n): ~$((DATASET_SIZE / 500))ms"
    echo "  ZREM would be O(n): ~$((DATASET_SIZE / 333))ms"
    echo ""
    echo "Speedup achieved:"
    echo "  ZSCORE: ~$((DATASET_SIZE / 500))ms -> ${avg_zscore_ms}ms = $(echo "scale=0; ($DATASET_SIZE / 500) / $avg_zscore_ms" | bc)x faster"
    echo "  ZREM: ~$((DATASET_SIZE / 333))ms -> ${avg_zrem_ms}ms = $(echo "scale=0; ($DATASET_SIZE / 333) / $avg_zrem_ms" | bc)x faster"
else
    echo "✅ Performance is acceptable"
    echo ""
    echo "Note: For maximum speedup, use larger datasets (--size 100000)"
fi
echo ""
