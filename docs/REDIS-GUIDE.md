# Redis Protocol Guide

This comprehensive guide covers using Apache Fluss through the Redis protocol via Fluss CAPE.

---

## Table of Contents

1. [Overview](#overview)
2. [Command Support](#command-support)
3. [Data Type Mapping](#data-type-mapping)
4. [Basic Usage](#basic-usage)
5. [Client Examples](#client-examples)
6. [Data Types Deep Dive](#data-types-deep-dive)
7. [Best Practices](#best-practices)
8. [Limitations](#limitations)

---

## Overview

### What's Supported

Fluss CAPE implements **131 Redis commands** across 9 data types:

| Data Type | Commands |
|-----------|----------|
| **Strings** (33) | APPEND, BITCOUNT, BITOP, BITPOS, DECR, DECRBY, GET, GETBIT, GETRANGE, GETSET, INCR, INCRBY, INCRBYFLOAT, MGET, MSET, MSETNX, PSETEX, SET, SETBIT, SETEX, SETNX, SETRANGE, STRLEN |
| **Hashes** (15) | HDEL, HEXISTS, HGET, HGETALL, HINCRBY, HINCRBYFLOAT, HKEYS, HLEN, HMGET, HMSET, HSCAN, HSET, HSETNX, HVALS |
| **Sets** (16) | SADD, SCARD, SDIFF, SDIFFSTORE, SINTER, SINTERCARD, SINTERSTORE, SISMEMBER, SMEMBERS, SMOVE, SPOP, SRANDMEMBER, SREM, SSCAN, SUNION, SUNIONSTORE |
| **Lists** (15) | LINDEX, LINSERT, LLEN, LMOVE, LPOP, LPUSH, LPUSHX, LRANGE, LREM, LSET, LTRIM, RPOP, RPOPLPUSH, RPUSH, RPUSHX |
| **Sorted Sets** (25) | ZADD, ZCARD, ZCOUNT, ZINCRBY, ZINTERCARD, ZINTERSTORE, ZLEXCOUNT, ZMSCORE, ZPOPMAX, ZPOPMIN, ZRANGE, ZRANGEBYLEX, ZRANGEBYSCORE, ZRANK, ZREM, ZREMRANGEBYLEX, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREVRANGE, ZREVRANGEBYLEX, ZREVRANGEBYSCORE, ZREVRANK, ZSCAN, ZSCORE, ZUNIONSTORE |
| **Streams** (11) | XADD, XCLAIM, XDEL, XGROUP, XINFO, XLEN, XRANGE, XREAD, XREADGROUP, XTRIM |
| **PubSub** (6) | PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB (CHANNELS, NUMSUB, NUMPAT) |
| **Geo** (7) | GEOADD, GEODIST, GEOHASH, GEOPOS, GEORADIUS, GEORADIUSBYMEMBER, GEOSEARCH |
| **HyperLogLog** (3) | PFADD, PFCOUNT, PFMERGE |

### Key Features

✅ **RESP Protocol** - Full Redis Serialization Protocol support  
✅ **All Major Clients** - Compatible with redis-cli, Python, Node.js, Go, Java  
✅ **Horizontal Sharding** - Redis Cluster-style CRC16 hash slot routing (16384 slots)  
✅ **Hash Tag Support** - Co-locate related keys with `{tag}` syntax  
✅ **Pipelining** - Batch commands for better performance  
✅ **Pub/Sub** - Full publish/subscribe support with pattern matching

### Sharding Modes

CAPE supports two storage modes for Redis data:

**Sharded Mode (Recommended):**
- Distributes data across 16 shard tables using CRC16 hash slot algorithm
- Each shard handles 1024 slots (16384 total slots / 16 shards)
- Supports hash tag co-location: `order:{123}:data` and `order:{123}:status` → same shard
- Better scalability for large datasets
- Enable with: `REDIS_SHARDING_ENABLED=true`

**Single Table Mode (Legacy):**
- All data stored in one `redis_internal_data` table
- Simpler deployment but limited scalability
- Backward compatible with older deployments
- Enable with: `REDIS_SHARDING_ENABLED=false`

See [Table Mapping Architecture](TABLE-MAPPING.md) for detailed schema information.

---

## Command Support

### Strings (33/33 - 100%)

| Command | Status | Notes |
|---------|--------|-------|
| SET, GET, MSET, MGET | ✅ | Full support |
| INCR, DECR, INCRBY, DECRBY | ✅ | Atomic operations |
| APPEND, STRLEN | ✅ | String manipulation |
| GETRANGE, SETRANGE | ✅ | Substring operations |
| SETEX, SETNX, PSETEX | ✅ | With expiration |
| GETSET, GETDEL | ✅ | Atomic get-and-modify |

### Hashes (15/15 - 100%)

| Command | Status | Notes |
|---------|--------|-------|
| HSET, HGET, HMSET, HMGET | ✅ | Field operations |
| HGETALL, HKEYS, HVALS | ✅ | Bulk retrieval |
| HDEL, HEXISTS | ✅ | Field management |
| HINCRBY, HINCRBYFLOAT | ✅ | Atomic increment |
| HLEN, HSETNX, HSTRLEN | ✅ | Metadata ops |

### Lists (18/20 - 90%)

| Command | Status | Notes |
|---------|--------|-------|
| LPUSH, RPUSH, LPOP, RPOP | ✅ | Basic operations |
| LRANGE, LLEN, LINDEX | ✅ | Read operations |
| LSET, LINSERT, LREM | ✅ | Modification |
| LTRIM, LPOS | ✅ | List manipulation |
| BLPOP, BRPOP | ❌ | Blocking ops not supported |

### Sets (16/16 - 100%)

| Command | Status | Notes |
|---------|--------|-------|
| SADD, SREM, SMEMBERS | ✅ | Basic operations |
| SISMEMBER, SCARD | ✅ | Membership & size |
| SPOP, SRANDMEMBER | ✅ | Random operations |
| SUNION, SINTER, SDIFF | ✅ | Set operations |
| SUNIONSTORE, SINTERSTORE, SDIFFSTORE | ✅ | Store results |
| SMOVE, SSCAN | ✅ | Advanced ops |

### Sorted Sets (26/25 - 104%)

| Command | Status | Notes |
|---------|--------|-------|
| ZADD, ZREM, ZCARD | ✅ | Basic operations |
| ZRANGE, ZREVRANGE | ✅ | Range queries |
| ZRANGEBYSCORE, ZREVRANGEBYSCORE | ✅ | Score-based range |
| ZRANK, ZREVRANK | ✅ | Rank queries |
| ZSCORE, ZINCRBY | ✅ | Score operations |
| ZCOUNT, ZREMRANGEBYRANK | ✅ | Bulk operations |
| ZUNION, ZINTER, ZDIFF | ✅ | Set operations |

### Streams (11/15 - 73%)

| Command | Status | Notes |
|---------|--------|-------|
| XADD, XREAD | ✅ | Basic stream ops |
| XLEN, XRANGE, XREVRANGE | ✅ | Read operations |
| XTRIM, XDEL | ✅ | Stream management |
| XGROUP, XREADGROUP | ⚠️ | Limited support |
| XACK, XPENDING | ❌ | Consumer groups limited |

### Geo (7/8 - 87%)

| Command | Status | Notes |
|---------|--------|-------|
| GEOADD, GEODIST | ✅ | Add & distance |
| GEOPOS, GEOHASH | ✅ | Position & hash |
| GEORADIUS, GEORADIUSBYMEMBER | ✅ | Radius search |
| GEOSEARCH | ⚠️ | Partial support |

---

## Data Type Mapping

### How Redis Data Maps to Fluss

```
Redis Key → Fluss Table:Primary Key:Column

Example:
  Redis: user:1001:name
  Fluss: Table=user, PK=1001, Column=name
```

### Table Structure

```
Key Format: <table>:<pk>:<field>

STRING:     users:1001:name → Table: users, Row: 1001, Col: name
HASH:       users:1001 → Table: users, Row: 1001 (multiple columns)
LIST:       queue:tasks → Table: queue, Row: tasks, Col: list_data
SET:        tags:user1 → Table: tags, Row: user1, Col: members
ZSET:       leaderboard → Table: leaderboard, Row: <entry>, Score: <score>
STREAM:     events:log → Table: events, Row: log (append-only)
```

---

## Basic Usage

### Connect with redis-cli

```bash
redis-cli -p 6379

127.0.0.1:6379> PING
PONG

127.0.0.1:6379> INFO
# Server
redis_version:7.0.0-compatible (Fluss CAPE)
...
```

### Basic Commands

```bash
# Strings
SET user:1001:name "Alice"
GET user:1001:name

# With expiration
SETEX session:abc123 3600 "user_data"

# Increment
SET counter:visits 100
INCR counter:visits  # 101

# Multiple operations
MSET user:1:name "Alice" user:2:name "Bob"
MGET user:1:name user:2:name
```

---

## Client Examples

### Python (redis-py)

```python
import redis

# Connect
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Strings
r.set('user:1001:name', 'Alice')
print(r.get('user:1001:name'))  # Alice

# Hashes
r.hset('user:1001', mapping={
    'name': 'Alice',
    'age': 30,
    'email': 'alice@example.com'
})
user = r.hgetall('user:1001')
print(user)  # {'name': 'Alice', 'age': '30', 'email': 'alice@example.com'}

# Lists
r.rpush('tasks', 'task1', 'task2', 'task3')
tasks = r.lrange('tasks', 0, -1)
print(tasks)  # ['task1', 'task2', 'task3']

# Sets
r.sadd('tags:post1', 'python', 'redis', 'database')
tags = r.smembers('tags:post1')
print(tags)  # {'python', 'redis', 'database'}

# Sorted Sets
r.zadd('leaderboard', {'Alice': 100, 'Bob': 200, 'Charlie': 150})
top3 = r.zrevrange('leaderboard', 0, 2, withscores=True)
print(top3)  # [('Bob', 200.0), ('Charlie', 150.0), ('Alice', 100.0)]

# Pipelining
pipe = r.pipeline()
pipe.set('key1', 'value1')
pipe.set('key2', 'value2')
pipe.get('key1')
results = pipe.execute()
print(results)  # [True, True, 'value1']
```

### Node.js (ioredis)

```javascript
const Redis = require('ioredis');
const redis = new Redis({ port: 6379 });

// Strings
await redis.set('user:1001:name', 'Alice');
const name = await redis.get('user:1001:name');
console.log(name);  // Alice

// Hashes
await redis.hset('user:1001', 'name', 'Alice', 'age', 30);
const user = await redis.hgetall('user:1001');
console.log(user);  // { name: 'Alice', age: '30' }

// Lists
await redis.rpush('tasks', 'task1', 'task2');
const tasks = await redis.lrange('tasks', 0, -1);
console.log(tasks);  // ['task1', 'task2']

// Sorted Sets
await redis.zadd('leaderboard', 100, 'Alice', 200, 'Bob');
const top = await redis.zrevrange('leaderboard', 0, 1, 'WITHSCORES');
console.log(top);  // ['Bob', '200', 'Alice', '100']

// Pipeline
const pipeline = redis.pipeline();
pipeline.set('key1', 'value1');
pipeline.set('key2', 'value2');
pipeline.get('key1');
const results = await pipeline.exec();
console.log(results);
```

### Go (go-redis)

```go
package main

import (
    "context"
    "fmt"
    "github.com/redis/go-redis/v9"
)

func main() {
    ctx := context.Background()
    
    // Connect
    rdb := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })
    
    // Strings
    rdb.Set(ctx, "user:1001:name", "Alice", 0)
    name, _ := rdb.Get(ctx, "user:1001:name").Result()
    fmt.Println(name)  // Alice
    
    // Hashes
    rdb.HSet(ctx, "user:1001", map[string]interface{}{
        "name": "Alice",
        "age": 30,
    })
    user := rdb.HGetAll(ctx, "user:1001").Val()
    fmt.Println(user)  // map[name:Alice age:30]
    
    // Lists
    rdb.RPush(ctx, "tasks", "task1", "task2")
    tasks := rdb.LRange(ctx, "tasks", 0, -1).Val()
    fmt.Println(tasks)  // [task1 task2]
    
    // Sorted Sets
    rdb.ZAdd(ctx, "leaderboard", redis.Z{Score: 100, Member: "Alice"})
    rdb.ZAdd(ctx, "leaderboard", redis.Z{Score: 200, Member: "Bob"})
    top := rdb.ZRevRangeWithScores(ctx, "leaderboard", 0, 1).Val()
    fmt.Println(top)
    
    // Pipeline
    pipe := rdb.Pipeline()
    pipe.Set(ctx, "key1", "value1", 0)
    pipe.Set(ctx, "key2", "value2", 0)
    pipe.Get(ctx, "key1")
    results, _ := pipe.Exec(ctx)
    fmt.Println(results)
}
```

### Java (Jedis)

```java
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import java.util.*;

public class FlussRedisExample {
    public static void main(String[] args) {
        // Connect
        Jedis jedis = new Jedis("localhost", 6379);
        
        // Strings
        jedis.set("user:1001:name", "Alice");
        String name = jedis.get("user:1001:name");
        System.out.println(name);  // Alice
        
        // Hashes
        Map<String, String> user = new HashMap<>();
        user.put("name", "Alice");
        user.put("age", "30");
        jedis.hset("user:1001", user);
        Map<String, String> userData = jedis.hgetAll("user:1001");
        System.out.println(userData);
        
        // Lists
        jedis.rpush("tasks", "task1", "task2");
        List<String> tasks = jedis.lrange("tasks", 0, -1);
        System.out.println(tasks);
        
        // Sets
        jedis.sadd("tags:post1", "redis", "java");
        Set<String> tags = jedis.smembers("tags:post1");
        System.out.println(tags);
        
        // Sorted Sets
        jedis.zadd("leaderboard", 100, "Alice");
        jedis.zadd("leaderboard", 200, "Bob");
        List<String> top = jedis.zrevrange("leaderboard", 0, 1);
        System.out.println(top);
        
        // Pipeline
        Pipeline pipeline = jedis.pipelined();
        pipeline.set("key1", "value1");
        pipeline.set("key2", "value2");
        pipeline.get("key1");
        List<Object> results = pipeline.syncAndReturnAll();
        System.out.println(results);
        
        jedis.close();
    }
}
```

---

## Data Types Deep Dive

### Strings

**Use Cases**: Caching, counters, flags, serialized objects

```bash
# Basic operations
SET user:1001:name "Alice"
GET user:1001:name

# Atomic counter
INCR page:views
INCRBY page:views 10
DECR active:users

# Multiple keys
MSET user:1:name "Alice" user:2:name "Bob" user:3:name "Charlie"
MGET user:1:name user:2:name user:3:name

# With expiration (seconds)
SETEX session:abc123 3600 "session_data"

# Set if not exists
SETNX lock:resource "locked"

# Append
SET msg "Hello"
APPEND msg " World"  # "Hello World"

# String manipulation
GETRANGE msg 0 4  # "Hello"
SETRANGE msg 6 "Redis"  # "Hello Redis"
STRLEN msg
```

### Hashes

**Use Cases**: User profiles, object storage, structured data

```bash
# Single field
HSET user:1001 name "Alice"
HGET user:1001 name

# Multiple fields
HMSET user:1001 name "Alice" age 30 email "alice@example.com"
HMGET user:1001 name email

# Get all fields
HGETALL user:1001
HKEYS user:1001  # Field names only
HVALS user:1001  # Values only

# Field operations
HEXISTS user:1001 name
HDEL user:1001 email
HLEN user:1001

# Increment field
HINCRBY user:1001 login_count 1
HINCRBYFLOAT user:1001 balance 10.50

# Set if field doesn't exist
HSETNX user:1001 created_at "2026-01-10"
```

**Python Example**:
```python
# Store user profile
r.hset('user:1001', mapping={
    'username': 'alice',
    'email': 'alice@example.com',
    'age': 30,
    'score': 1250,
    'level': 5
})

# Update specific fields
r.hincrby('user:1001', 'score', 100)
r.hincrby('user:1001', 'level', 1)

# Retrieve profile
profile = r.hgetall('user:1001')
```

### Lists

**Use Cases**: Queues, activity feeds, message lists

```bash
# Push elements
LPUSH tasks "task3"  # Add to head
RPUSH tasks "task1" "task2"  # Add to tail

# Pop elements
LPOP tasks  # Remove from head
RPOP tasks  # Remove from tail

# View list
LRANGE tasks 0 -1  # All elements
LRANGE tasks 0 9   # First 10 elements

# List operations
LLEN tasks  # Length
LINDEX tasks 0  # Element at index
LSET tasks 0 "new_value"  # Set element
LINSERT tasks BEFORE "task2" "task1.5"
LREM tasks 1 "task3"  # Remove 1 occurrence

# Trim list
LTRIM tasks 0 99  # Keep only first 100 elements

# Position
LPOS tasks "task2"
```

**Python Example - Task Queue**:
```python
# Producer: Add tasks
r.rpush('task:queue', 'send_email', 'process_image', 'update_db')

# Consumer: Process tasks
while True:
    task = r.lpop('task:queue')
    if task:
        print(f"Processing: {task}")
    else:
        break

# Activity feed (keep last 100)
r.lpush('feed:user1', 'Posted a photo')
r.ltrim('feed:user1', 0, 99)
recent_activity = r.lrange('feed:user1', 0, 9)  # Last 10 activities
```

### Sets

**Use Cases**: Tags, unique items, relationships

```bash
# Add members
SADD tags:post1 "redis" "database" "nosql"

# Remove members
SREM tags:post1 "nosql"

# Check membership
SISMEMBER tags:post1 "redis"

# View set
SMEMBERS tags:post1
SCARD tags:post1  # Size

# Random operations
SPOP tags:post1  # Remove and return random
SRANDMEMBER tags:post1 2  # Get 2 random (without removing)

# Set operations
SADD set1 "a" "b" "c"
SADD set2 "b" "c" "d"

SUNION set1 set2  # Union: a,b,c,d
SINTER set1 set2  # Intersection: b,c
SDIFF set1 set2   # Difference: a

# Store result
SUNIONSTORE set3 set1 set2
```

**Python Example - Social Graph**:
```python
# User follows
r.sadd('following:alice', 'bob', 'charlie', 'dave')
r.sadd('following:bob', 'charlie', 'eve')

# Who does Alice follow?
alice_follows = r.smembers('following:alice')

# Common follows (mutual friends)
common = r.sinter('following:alice', 'following:bob')

# Tag system
r.sadd('post:1:tags', 'python', 'redis', 'tutorial')
r.sadd('post:2:tags', 'python', 'flask', 'web')

# Find posts with 'python' tag
posts_with_python = r.sinter('post:1:tags', 'post:2:tags')
```

### Sorted Sets

**Use Cases**: Leaderboards, priority queues, time-series data

```bash
# Add members with scores
ZADD leaderboard 100 "Alice" 200 "Bob" 150 "Charlie"

# Range queries
ZRANGE leaderboard 0 -1  # All members (ascending)
ZREVRANGE leaderboard 0 2  # Top 3 (descending)
ZRANGE leaderboard 0 -1 WITHSCORES

# Score-based range
ZRANGEBYSCORE leaderboard 100 200
ZREVRANGEBYSCORE leaderboard 200 100

# Rank and score
ZRANK leaderboard "Alice"  # Position (0-indexed)
ZREVRANK leaderboard "Alice"  # Position from top
ZSCORE leaderboard "Alice"  # Get score

# Score operations
ZINCRBY leaderboard 50 "Alice"  # Increment score
ZREM leaderboard "Bob"  # Remove member

# Count
ZCARD leaderboard  # Total members
ZCOUNT leaderboard 100 200  # Members in score range

# Remove by rank/score
ZREMRANGEBYRANK leaderboard 0 0  # Remove bottom
ZREMRANGEBYSCORE leaderboard 0 100  # Remove score range
```

**Python Example - Gaming Leaderboard**:
```python
# Add player scores
r.zadd('leaderboard:global', {
    'Alice': 1500,
    'Bob': 2000,
    'Charlie': 1750,
    'Dave': 1250
})

# Update score
r.zincrby('leaderboard:global', 100, 'Alice')

# Top 10 players
top10 = r.zrevrange('leaderboard:global', 0, 9, withscores=True)
for rank, (player, score) in enumerate(top10, 1):
    print(f"#{rank}: {player} - {score}")

# Player's rank
alice_rank = r.zrevrank('leaderboard:global', 'Alice') + 1
alice_score = r.zscore('leaderboard:global', 'Alice')
print(f"Alice is #{alice_rank} with {alice_score} points")

# Players within score range
mid_tier = r.zrangebyscore('leaderboard:global', 1500, 2000)
```

### Streams

**Use Cases**: Event logs, message queues, audit trails

```bash
# Add entries
XADD events:log * action "login" user "alice" timestamp "2026-01-10"
XADD events:log * action "purchase" user "bob" amount "99.99"

# Read entries
XREAD COUNT 10 STREAMS events:log 0  # From beginning
XREAD COUNT 10 STREAMS events:log $  # Only new

# Range queries
XRANGE events:log - +  # All entries
XRANGE events:log 1673366400000 1673452800000  # Time range

# Stream info
XLEN events:log
XTRIM events:log MAXLEN 1000  # Keep only last 1000

# Delete entry
XDEL events:log <entry-id>
```

**Python Example - Event Logging**:
```python
# Log events
r.xadd('events:system', {
    'level': 'INFO',
    'message': 'User logged in',
    'user_id': '1001'
})

# Read new events
last_id = '0'
while True:
    events = r.xread({'events:system': last_id}, count=10, block=1000)
    if events:
        for stream, messages in events:
            for msg_id, data in messages:
                print(f"{msg_id}: {data}")
                last_id = msg_id
```

### Geo

**Use Cases**: Location-based services, proximity search

```bash
# Add locations
GEOADD locations 13.361389 38.115556 "Palermo"
GEOADD locations 15.087269 37.502669 "Catania"

# Get position
GEOPOS locations "Palermo"

# Get distance
GEODIST locations "Palermo" "Catania" km

# Radius search
GEORADIUS locations 15 37 200 km WITHDIST
GEORADIUSBYMEMBER locations "Palermo" 100 km

# Get geohash
GEOHASH locations "Palermo"
```

**Python Example - Store Locator**:
```python
# Add store locations
r.geoadd('stores', (
    (-122.4194, 37.7749, 'store1'),  # San Francisco
    (-118.2437, 34.0522, 'store2'),  # Los Angeles
    (-73.9352, 40.7306, 'store3')    # New York
))

# Find stores within 500km of user location
nearby = r.georadius('stores', -122.0, 37.5, 500, unit='km', withdist=True)
for store, distance in nearby:
    print(f"{store}: {distance} km away")
```

---

## Best Practices

### 1. Key Naming Convention

```bash
# Use colon separators
user:1001:profile
user:1001:sessions
order:2023:january

# Include type hint
cache:user:1001
list:tasks:pending
zset:leaderboard:daily
```

### 2. Pipelining for Bulk Operations

```python
# BAD: Individual commands (slow)
for i in range(1000):
    r.set(f'key:{i}', f'value{i}')

# GOOD: Pipeline (fast)
pipe = r.pipeline()
for i in range(1000):
    pipe.set(f'key:{i}', f'value{i}')
pipe.execute()
```

### 3. Use Appropriate Data Structures

```python
# BAD: String for structured data
r.set('user:1001', json.dumps({'name': 'Alice', 'age': 30}))

# GOOD: Hash for structured data
r.hset('user:1001', mapping={'name': 'Alice', 'age': 30})
```

### 4. Memory Management

```bash
# Use expiration for temporary data
SETEX session:abc123 3600 "data"

# Trim large lists/streams
LTRIM activity:feed 0 999
XTRIM events:log MAXLEN 10000
```

### 5. Atomic Operations

```python
# BAD: Race condition
count = int(r.get('counter'))
r.set('counter', count + 1)

# GOOD: Atomic increment
r.incr('counter')
```

---

## Limitations

### ⚠️ Known Issues (Apache Fluss 0.8.0)

**CRITICAL: Hash Operations with Many Fields**

**Issue**: Apache Fluss 0.8.0 has a buffer overflow bug that affects Hash operations with multiple fields and long key names.

**Symptoms**:
- `HMSET` with 10+ fields may fail with `ArrayIndexOutOfBoundsException`
- `HGETALL` may return incomplete results (only first few fields)
- YCSB benchmarks fail immediately

**Affected Operations**:
- `HMSET hash field1 value1 field2 value2 ... field10 value10` ❌
- `HGETALL hash` (when hash has 5+ fields) ⚠️
- Keys longer than 50 bytes total ❌

**Root Cause**: Buffer overflow in `org.apache.fluss.row.compacted.CompactedRowWriter` (Fluss bug, not CAPE bug)

**Workarounds**:

1. **Limit Hash Size** (Recommended):
```bash
# ✅ Works: Use fewer fields per hash (< 5 fields)
HSET user:1 name "Alice"
HSET user:1 email "alice@example.com"
HSET user:1 age "30"

# ❌ Fails: Too many fields at once
HMSET user:1 f0 v0 f1 v1 f2 v2 f3 v3 f4 v4 f5 v5 f6 v6 f7 v7 f8 v8 f9 v9
```

2. **Use Shorter Key Names**:
```bash
# ✅ Works: Short key names
HMSET u:1 name "Alice" email "alice@example.com"

# ❌ Fails: Long key names
HMSET "very_long_table_name:user:1234567890" name "Alice" ...
```

3. **Batch Operations**:
```python
# Instead of HMSET with 10 fields, use multiple HSET
for i in range(10):
    r.hset(f'user:{uid}', f'field{i}', f'value{i}')
```

**Status**: Bug report filed with Apache Fluss project. Expected fix in Fluss 0.9.0.

**Details**: See `/tmp/FLUSS-BUG-REPORT-CompactedRowWriter.md` for full bug report.

---

### Not Supported

1. **Blocking Operations**: BLPOP, BRPOP (use polling instead)
2. **Pub/Sub**: Limited support, use Fluss streams
3. **Transactions**: MULTI/EXEC has limitations
4. **Lua Scripts**: EVAL/EVALSHA not supported
5. **Cluster Commands**: No CLUSTER commands
6. **Replication**: Use Fluss replication instead

### Workarounds

**Blocking Operations**:
```python
# Instead of BLPOP
while True:
    item = r.lpop('queue')
    if item:
        process(item)
    else:
        time.sleep(0.1)
```

**Pub/Sub**:
```python
# Use Streams instead
r.xadd('channel', {'message': 'hello'})
r.xread({'channel': last_id}, count=10)
```

---

## Next Steps

- **[Configuration Guide](CONFIGURATION.md)** - Tune Redis protocol settings
- **[Examples](EXAMPLES.md)** - See production use cases
- **[Benchmarks](BENCHMARKS.md)** - Performance tuning

---

## Quick Reference Card

```bash
# Strings
SET key value
GET key
INCR counter

# Hashes
HSET user:1 name "Alice"
HGETALL user:1

# Lists
LPUSH queue "task"
RPOP queue

# Sets
SADD tags "redis"
SMEMBERS tags

# Sorted Sets
ZADD leaderboard 100 "Alice"
ZREVRANGE leaderboard 0 9

# Streams
XADD log * msg "event"
XREAD STREAMS log 0

# Geo
GEOADD loc 13.36 38.11 "city"
GEORADIUS loc 15 37 200 km
```
