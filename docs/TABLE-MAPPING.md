# Table Mapping Architecture

This document describes how HBase and Redis data structures are mapped to Apache Fluss table schemas.

---

## Overview

Fluss CAPE translates HBase and Redis protocol operations into Fluss table operations. Understanding the table schemas is crucial for:
- Debugging data storage issues
- Optimizing query patterns
- Understanding storage costs
- Planning capacity and sharding strategies

**Key Concepts:**
- **Primary Key Tables**: All CAPE tables are Fluss Primary Key tables with composite keys
- **Dynamic Schema**: HBase tables support dynamic column qualifiers without schema changes
- **Sharding**: Redis supports 16384-slot hash-based sharding for horizontal scaling
- **Encoding**: Complex data structures are serialized into BYTES columns

---

## Table of Contents

1. [HBase Table Mapping](#hbase-table-mapping)
2. [Redis Table Mapping](#redis-table-mapping)
   - [Sharded Mode (Recommended)](#sharded-mode-recommended)
   - [Single Table Mode (Legacy)](#single-table-mode-legacy)
3. [Encoding Details](#encoding-details)
4. [Performance Considerations](#performance-considerations)

---

## HBase Table Mapping

### Overview

HBase tables in CAPE use **dynamic schema** to support arbitrary column families and qualifiers without pre-configuration.

### Schema Design

**User Table Example: `users` with column families `personal` and `contact`**

**Fluss Table Name:** `default.users`

**Schema:**
```sql
CREATE TABLE default.users (
    row_key   BYTES    PRIMARY KEY,
    personal  BYTES,
    contact   BYTES
);
```

**Characteristics:**
- **row_key**: HBase row key (BYTES type for arbitrary binary data)
- **Column Families**: Each HBase column family becomes a BYTES column
- **Dynamic Creation**: Tables are created on-demand via HBase Admin API
- **Primary Key**: Single column `row_key`

### Data Encoding

Each column family BYTES column stores multiple qualifiers using the following format:

**Binary Format:**
```
[num_qualifiers:int32]
[qualifier1_len:int32][qualifier1:bytes][value1_len:int32][value1:bytes]
[qualifier2_len:int32][qualifier2:bytes][value2_len:int32][value2:bytes]
...
```

**Example HBase Data:**
```
Put('user1', 'personal:name', 'Alice')
Put('user1', 'personal:age', '30')
Put('user1', 'contact:email', 'alice@example.com')
```

**Stored in Fluss:**
```
row_key = b"user1"
personal = [2] [4]name[5]Alice [3]age[2]30
contact = [1] [5]email[19]alice@example.com
```

**Implementation:**
- Encoding: `DynamicTableCodec.encodeColumnFamily()`
- Decoding: `DynamicTableCodec.decodeColumnFamily()`
- File: `src/main/java/org/gnuhpc/fluss/cape/hbase/mapping/DynamicTableCodec.java`

### Operations

| HBase Operation | Fluss Operation | Notes |
|-----------------|-----------------|-------|
| Put | UpsertWriter | Merges qualifiers within same CF |
| Get | Lookuper.lookup() | Single row retrieval by row_key |
| Delete | Tombstone (null row) | Deletes entire row |
| Scan | LogScanner | Requires KV snapshots |

### Metadata Tables

CAPE maintains internal metadata tables for HBase protocol support:

#### `hbase:meta` (Emulated)
- **Purpose**: Table discovery and region metadata (in-memory only, not persisted)
- **Schema**: Standard HBase meta table schema
- **Implementation**: `MetaTableEmulator.java`

#### Table State (ZooKeeper)
- **Purpose**: Track enabled/disabled tables
- **Location**: ZooKeeper under `/hbase/table-state`
- **Implementation**: `TableStateManager.java`

---

## Redis Table Mapping

Redis supports two deployment modes: **Sharded Mode** (recommended for production) and **Single Table Mode** (legacy/simple deployments).

### Sharded Mode (Recommended)

Distributes Redis keys across multiple shard tables using CRC16 hash slot routing (16384 slots, like Redis Cluster).

#### Configuration

```bash
# Enable sharding with 16 shards (default)
REDIS_SHARDING_ENABLED=true
REDIS_SHARDING_NUM_SHARDS=16
```

#### Table Structure

**Total Tables Created: 34 tables**
- 16 data shard tables: `redis_shard_00` to `redis_shard_15`
- 16 stream shard tables: `redis_stream_shard_00` to `redis_stream_shard_15`
- 2 shared index tables: `redis_internal_subkey_index`, `redis_internal_zset_index`

#### General Data Shard Tables

**Table Names:** `redis_shard_00` to `redis_shard_15`

**Schema:**
```sql
CREATE TABLE default.redis_shard_XX (
    redis_key    STRING,
    sub_key      STRING,
    redis_type   STRING,
    score        DOUBLE,
    value        BYTES,
    PRIMARY KEY (redis_key, sub_key)
);
```

**Column Descriptions:**

| Column | Type | Purpose | Example Values |
|--------|------|---------|----------------|
| `redis_key` | STRING | Redis key name | `"user:1001"`, `"session:abc123"` |
| `sub_key` | STRING | Secondary key for multi-element types | Hash field, List index, Set member, ZSet member |
| `redis_type` | STRING | Redis data type identifier | `"string"`, `"hash"`, `"set"`, `"list"`, `"zset"`, `"geo"`, `"hll"` |
| `score` | DOUBLE | Score for sorted sets (null for other types) | `100.5`, `0.0`, `null` |
| `value` | BYTES | Serialized value | String bytes, member bytes, field-value pairs |

**Routing Logic:**
```python
# CRC16 hash slot calculation
slot = CRC16(redis_key) % 16384
shard = slot // 1024  # 0-15
table = f"redis_shard_{shard:02d}"
```

**Hash Tag Support:**
Keys with `{tag}` syntax extract the tag for hashing:
```python
# Co-location examples
"order:{123}:data"   → CRC16("{123}") → Same shard
"order:{123}:status" → CRC16("{123}") → Same shard
```

**Supported Data Types:**

| Redis Type | redis_type | sub_key | score | value | Example |
|------------|------------|---------|-------|-------|---------|
| String | `"string"` | `""` (empty) | null | String bytes | `SET mykey "hello"` |
| Hash | `"hash"` | Field name | null | Field value | `HSET user:1 name "Alice"` |
| Set | `"set"` | Member | null | Member bytes | `SADD myset "a" "b"` |
| List | `"list"` | Index (0, 1, 2...) | null | Element bytes | `LPUSH mylist "x"` |
| Sorted Set | `"zset"` | Member | Score | Member bytes | `ZADD leaders 100 "Alice"` |
| Geo | `"zset"` | Location name | Geohash | Location bytes | `GEOADD loc 13.4 38.1 "Rome"` |
| HyperLogLog | `"hll"` | `""` (empty) | null | HLL data | `PFADD hll1 "a" "b"` |

#### Stream Shard Tables

**Table Names:** `redis_stream_shard_00` to `redis_stream_shard_15`

**Schema:**
```sql
CREATE TABLE default.redis_stream_shard_XX (
    stream_key  STRING,
    entry_id    STRING,
    fields      BYTES,
    PRIMARY KEY (stream_key, entry_id)
);
```

**Column Descriptions:**

| Column | Type | Purpose | Example Values |
|--------|------|---------|----------------|
| `stream_key` | STRING | Stream name | `"events:2024"`, `"logs:app1"` |
| `entry_id` | STRING | Unique entry ID (timestamp-sequence) | `"1234567890123-0"`, `"1234567890124-1"` |
| `fields` | BYTES | Serialized field-value map | `{field1: value1, field2: value2}` |

**Routing:** Uses same CRC16 hash slot algorithm as general data types.

**Stream Commands Supported:**
- `XADD` - Add entries to stream
- `XLEN` - Get stream length
- `XRANGE` - Range query entries
- `XREAD` - Read from streams
- `XDEL` - Delete entries (⚠️ has known bug with tombstone table)

#### Index Tables (Shared Across Shards)

##### Sub-Key Index Table

**Table Name:** `redis_internal_subkey_index`

**Purpose:** Optimize KEYS/SCAN pattern matching (tracks all sub-keys for a Redis key)

**Schema:**
```sql
CREATE TABLE default.redis_internal_subkey_index (
    redis_key  STRING PRIMARY KEY,
    sub_keys   STRING
);
```

**Column Descriptions:**

| Column | Type | Purpose | Example Values |
|--------|------|---------|----------------|
| `redis_key` | STRING | Redis key name | `"user:1001"` |
| `sub_keys` | STRING | JSON-encoded list of sub-keys | `'["field1","field2","field3"]'` (for Hash) |

**Example:**
```
HSET user:1001 name "Alice" age "30" city "NYC"
→ redis_key="user:1001", sub_keys='["name","age","city"]'
```

##### ZSet Reverse Index Table

**Table Name:** `redis_internal_zset_index`

**Purpose:** Fast score-to-member lookups for sorted set operations (reverse index)

**Schema:**
```sql
CREATE TABLE default.redis_internal_zset_index (
    redis_key  STRING,
    member     STRING,
    score      DOUBLE,
    PRIMARY KEY (redis_key, member)
);
```

**Column Descriptions:**

| Column | Type | Purpose | Example Values |
|--------|------|---------|----------------|
| `redis_key` | STRING | Sorted set key | `"leaderboard:2024"` |
| `member` | STRING | Member name | `"Alice"`, `"Bob"` |
| `score` | DOUBLE | Member's score | `100.5`, `200.0` |

**Example:**
```
ZADD leaderboard:2024 100 "Alice" 200 "Bob"
→ Two rows: (leaderboard:2024, Alice, 100.0), (leaderboard:2024, Bob, 200.0)
```

#### Stream Metadata Tables (Shared Across Shards)

##### Consumer Groups Table

**Table Name:** `redis_stream_consumer_groups`

**Purpose:** Track consumer groups for stream processing

**Schema:**
```sql
CREATE TABLE default.redis_stream_consumer_groups (
    stream_key          STRING,
    group_name          STRING,
    last_delivered_id   STRING,
    created_timestamp   BIGINT,
    PRIMARY KEY (stream_key, group_name)
);
```

##### Pending Entries Table

**Table Name:** `redis_stream_pending_entries`

**Purpose:** Track pending messages for consumer groups

**Schema:**
```sql
CREATE TABLE default.redis_stream_pending_entries (
    stream_key          STRING,
    group_name          STRING,
    entry_id            STRING,
    consumer_name       STRING,
    delivery_timestamp  BIGINT,
    PRIMARY KEY (stream_key, group_name, entry_id)
);
```

##### Tombstones Table

**Table Name:** `redis_stream_tombstones`

**Purpose:** Track deleted stream entries (for XDEL command)

**Schema:**
```sql
CREATE TABLE default.redis_stream_tombstones (
    stream_key       STRING,
    entry_id         STRING,
    deleted          BOOLEAN,
    delete_timestamp BIGINT,
    PRIMARY KEY (stream_key, entry_id)
);
```

⚠️ **Known Issue:** XDEL currently fails with field count mismatch error.

---

### Single Table Mode (Legacy)

Uses a single table for all Redis keys (not recommended for production).

#### Configuration

```bash
# Disable sharding (single table mode)
REDIS_SHARDING_ENABLED=false
```

#### Table Structure

**Total Tables Created: 3 tables**
- 1 main data table: `redis_internal_data`
- 2 index tables: `redis_internal_subkey_index`, `redis_internal_zset_index`

#### Main Data Table

**Table Name:** `redis_internal_data`

**Schema:**
```sql
CREATE TABLE default.redis_internal_data (
    redis_key    STRING,
    sub_key      STRING,
    redis_type   STRING,
    score        DOUBLE,
    value        BYTES,
    PRIMARY KEY (redis_key, sub_key)
);
```

**Same schema as sharded tables, but ALL keys stored in one table.**

**Limitations:**
- ❌ Poor horizontal scalability (single table bottleneck)
- ❌ Cannot leverage distributed storage effectively
- ❌ Higher latency for large datasets
- ✅ Simpler deployment (fewer tables)
- ✅ Suitable for testing and small deployments

#### Index Tables

Uses the same `redis_internal_subkey_index` and `redis_internal_zset_index` as sharded mode.

---

## Encoding Details

### Redis String Encoding

**Storage:**
```
redis_key = "mykey"
sub_key = ""  (empty string)
redis_type = "string"
value = "hello world".getBytes(UTF-8)
```

### Redis Hash Encoding

**Example:** `HSET user:1001 name "Alice" age "30"`

**Storage (2 rows):**
```
Row 1: redis_key="user:1001", sub_key="name", redis_type="hash", value=b"Alice"
Row 2: redis_key="user:1001", sub_key="age", redis_type="hash", value=b"30"
```

### Redis Set Encoding

**Example:** `SADD myset "a" "b" "c"`

**Storage (3 rows):**
```
Row 1: redis_key="myset", sub_key="a", redis_type="set", value=b"a"
Row 2: redis_key="myset", sub_key="b", redis_type="set", value=b"b"
Row 3: redis_key="myset", sub_key="c", redis_type="set", value=b"c"
```

### Redis List Encoding

**Example:** `LPUSH mylist "x" "y" "z"` (list order: z, y, x)

**Storage (3 rows):**
```
Row 1: redis_key="mylist", sub_key="0", redis_type="list", value=b"z"
Row 2: redis_key="mylist", sub_key="1", redis_type="list", value=b"y"
Row 3: redis_key="mylist", sub_key="2", redis_type="list", value=b"x"
```

**Note:** List indices are stored as strings ("0", "1", "2", ...)

### Redis Sorted Set Encoding

**Example:** `ZADD leaderboard 100.5 "Alice" 200.0 "Bob"`

**Storage (2 rows in main table + 2 rows in index table):**

**Main Table (redis_shard_XX):**
```
Row 1: redis_key="leaderboard", sub_key="Alice", redis_type="zset", score=100.5, value=b"Alice"
Row 2: redis_key="leaderboard", sub_key="Bob", redis_type="zset", score=200.0, value=b"Bob"
```

**Index Table (redis_internal_zset_index):**
```
Row 1: redis_key="leaderboard", member="Alice", score=100.5
Row 2: redis_key="leaderboard", member="Bob", score=200.0
```

### Redis Geo Encoding

**Note:** Geo commands are backed by Sorted Set (geohash as score).

**Example:** `GEOADD locations 13.361 38.115 "Palermo"`

**Storage (same as ZSet):**
```
redis_key="locations"
sub_key="Palermo"
redis_type="zset"
score=<geohash_encoded>  (e.g., 3479099956230698.0)
value=b"Palermo"
```

### Redis HyperLogLog Encoding

**Example:** `PFADD hll1 "a" "b" "c"`

**Storage:**
```
redis_key="hll1"
sub_key=""  (empty string)
redis_type="hll"
value=<HLL_registers_binary>  (HyperLogLog internal state, ~12KB)
```

### Redis Stream Encoding

**Example:** `XADD mystream * field1 value1 field2 value2`

**Storage (in redis_stream_shard_XX):**
```
stream_key="mystream"
entry_id="1234567890123-0"  (timestamp-sequence)
fields=<serialized_map>  (e.g., Java serialized HashMap: {field1: value1, field2: value2})
```

**Note:** Fields are serialized using Java object serialization (may change to more efficient encoding in future).

---

## Performance Considerations

### Sharding Best Practices

**1. Use Hash Tags for Related Keys**
```bash
# Co-locate user data on same shard
SET user:{1001}:profile "..."
SET user:{1001}:settings "..."
HSET user:{1001}:stats visits 100
```

**2. Avoid Hot Shards**
- Use evenly distributed key names (avoid sequential IDs with low entropy)
- Monitor shard distribution: query each `redis_shard_XX` table for row count

**3. Shard Count Selection**
```
Recommended shard count:
- Small deployment (<1M keys):    4-8 shards
- Medium deployment (1M-10M):     16 shards (default)
- Large deployment (>10M):        32-64 shards
```

### HBase Scan

CAPE does not support HBase Scan. Use Get operations or Fluss native log scan APIs instead.

### Storage Costs

**Storage Overhead by Data Type:**

| Redis Type | Storage Overhead | Reason |
|------------|------------------|--------|
| String | Low (1 row) | Single row per key |
| Hash | Medium (N rows) | N rows for N fields |
| Set | Medium (N rows) | N rows for N members |
| List | Medium (N rows) | N rows for N elements |
| Sorted Set | High (2N rows) | N rows + N index rows |
| Geo | High (2N rows) | Same as Sorted Set |
| HyperLogLog | Low (1 row) | Single row with ~12KB binary |
| Stream | Medium (N rows) | N rows for N entries |

**Optimization Tips:**
- Use Strings for single-value keys (lowest overhead)
- Batch ZADD operations to reduce index updates
- Use TTL/expiration to purge old data (requires implementation)
- Monitor table sizes: `SELECT COUNT(*) FROM redis_shard_XX`

### Query Performance

**Fast Operations (O(1) lookup):**
- GET, SET, HGET, HSET
- SADD, SISMEMBER
- LPUSH, RPUSH, LINDEX
- ZADD, ZSCORE

**Slow Operations (table scan):**
- KEYS, SCAN (requires scanning subkey index)
- ZRANGE, ZREVRANGE (requires sorting, uses ZSet index)
- LRANGE (requires scanning multiple rows)
- SMEMBERS (requires scanning all members)

**Recommendation:** Use fast operations in hot paths, slow operations in batch/background jobs.

---

## Migration Notes

### HBase to CAPE

**Compatibility:**
- ✅ Standard HBase Client API (Get, Put, Delete, Scan)
- ✅ Dynamic column families and qualifiers
- ❌ Coprocessors (not supported)
- ❌ Filters (limited support)
- ❌ Increment operations (not yet implemented)

**Data Migration:**
```bash
# Export from native HBase
hbase org.apache.hadoop.hbase.mapreduce.Export users /export/users

# Import to CAPE (connect to CAPE's ZooKeeper)
hbase org.apache.hadoop.hbase.mapreduce.Import users /export/users
```

### Redis to CAPE

**Compatibility:**
- ✅ 131+ Redis commands
- ✅ RESP protocol (all clients compatible)
- ❌ Lua scripts (not supported)
- ✅ Pub/Sub (full support with pattern matching)
- ❌ Transactions (MULTI/EXEC not yet implemented)

**Data Migration:**
```bash
# Export from native Redis (RDB format)
redis-cli --rdb dump.rdb

# Parse RDB and replay to CAPE
# (requires custom script or redis-rdb-tools)
```

---

## Troubleshooting

### HBase Scan Unsupported

CAPE does not support HBase Scan. Use `get` for reads or Fluss native scan APIs outside of CAPE.

### Redis Key Not Found After SET

**Symptom:** `GET mykey` returns `nil` after `SET mykey value`.

**Cause:** Writer flush hasn't completed.

**Solution:**
1. Check logs for write exceptions
2. Verify Fluss cluster is healthy
3. Use synchronous writes (if available)

### XDEL Stream Entry Fails

**Symptom:** `XDEL mystream <entry_id>` returns error.

**Cause:** Known bug with tombstone table field count mismatch.

**Status:** Under investigation (see GitHub Issues #XX).

**Workaround:** Use XTRIM to truncate old entries instead.

---

## See Also

- [Redis Guide](REDIS-GUIDE.md) - Redis command reference and examples
- [HBase Guide](HBASE-GUIDE.md) - HBase usage and client setup
- [Architecture](../ARCHITECTURE.md) - System architecture overview
- [Configuration](CONFIGURATION.md) - All configuration parameters
- [Apache Fluss Documentation](https://fluss.apache.org/docs/) - Underlying storage system

---

**Last Updated:** 2026-01-11  
**CAPE Version:** 1.0.0  
**Fluss Version:** 0.8.0-incubating
