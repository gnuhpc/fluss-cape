# Fluss CAPE - Architecture Design

## Overview

Fluss CAPE provides HBase and Redis protocol compatibility for Apache Fluss, enabling applications to interact with Fluss using familiar HBase/Redis APIs without code changes.

**Key Design Principles:**
- **Protocol Translation**: Standalone layer that translates HBase/Redis protocols to Fluss operations
- **Zero Fluss Modifications**: CAPE runs as an external service, no changes to Fluss core
- **Dynamic Table Management**: Create tables on-demand via HBase shell or Redis commands
- **Horizontal Sharding**: Redis Cluster-style sharding for scalability

## High-Level Architecture

```
┌─────────────────────────────────────────────┐
│         Client Applications                 │
├──────────────┬──────────────────────────────┤
│  HBase API   │      Redis Clients           │
│  (Java/Spark)│  (redis-cli/Python/Node.js)  │
└──────┬───────┴──────────────┬───────────────┘
       │                      │
       │ ZK Discovery         │ HAProxy LB
       ▼                      ▼
┌──────────────────────────────────────────────┐
│       Fluss CAPE Servers (RegionServer)      │
│  ┌─────────────────┬─────────────────────┐   │
│  │ HBase Protocol  │  Redis Protocol     │   │
│  │   Decoder       │   RESP Decoder      │   │
│  ├─────────────────┼─────────────────────┤   │
│  │ HBase Executors │ Redis Executors     │   │
│  │  - Get/Put      │  - String/Hash/Set  │   │
│  │  - Scan         │  - List/ZSet        │   │
│  │  - Admin        │  - Stream/Geo/HLL   │   │
│  └────────┬────────┴──────────┬──────────┘   │
│           │                   │              │
│           └──────┬────────────┘              │
│                  ▼                           │
│         Fluss Client Library                 │
└──────────────────┬───────────────────────────┘
                   │
                   ▼
        ┌─────────────────────────┐
        │    Apache Fluss         │
        │  (Distributed Storage)  │
        │   - Coordinator         │
        │   - Tablet Servers      │
        │   - Log + KV Tables     │
        └─────────────────────────┘
```

## CAPE as HBase RegionServer

**Critical Architectural Concept**: CAPE servers act as **HBase RegionServers**, NOT as HBase Masters.

### Why RegionServer Role?

| Aspect | HBase Master | HBase RegionServer | CAPE Role |
|--------|--------------|-------------------|-----------|
| **Purpose** | Cluster coordination, metadata management | Data serving, client operations | ✅ Data serving |
| **Client Operations** | Admin operations only | Get/Put/Scan/Delete | ✅ Full data operations |
| **Default RPC Port** | 16000 | **16020** | ✅ **16020** |
| **Web UI Port** | 16010 | 16030 | N/A (optional) |
| **ZK Registration** | `/hbase/master` | `/hbase/rs/{server-id}` | ✅ `/hbase/rs/cape-server-*` |
| **Discovery** | Single master endpoint | Multiple servers via ZK | ✅ Multiple CAPE instances |

### Service Discovery

**HBase clients discover CAPE instances through ZooKeeper**:

1. CAPE servers register to ZooKeeper as RegionServers:
   ```
   /hbase/rs/cape-server-1 → "localhost,16020,timestamp"
   /hbase/rs/cape-server-2 → "localhost,16021,timestamp"
   ```

2. HBase clients query ZooKeeper to find all available RegionServers:
   ```java
   config.set("hbase.zookeeper.quorum", "localhost:2181");
   // Client automatically discovers all CAPE instances
   ```

3. Client-side load balancing handles requests across CAPE instances

**Key Differences from Real HBase**:
- Real HBase: Clients → ZK → Master → Meta table → RegionServer assignment
- CAPE: Clients → ZK → Direct CAPE instance (simplified - no region assignment)

### Port Configuration

**Standard HBase Ports**:
```
Master RPC:           16000  (CAPE does NOT use)
Master Web UI:        16010  (CAPE does NOT use)
RegionServer RPC:     16020  (CAPE default)
RegionServer Web UI:  16030  (CAPE can optionally expose)
```

**CAPE Default Configuration**:
- Primary instance: `16020` (matches RegionServer default)
- Additional instances: `16021`, `16022`, etc. (sequential)
- Configuration: `hbase.compat.bind.port=16020`

## Component Details

### 1. Protocol Decoders

**HBase RPC Decoder**:
- Decodes HBase RPC protocol (Protobuf-based)
- Handles CellBlock format for bulk data transfer
- Supports SASL/GSSAPI authentication

**Redis RESP Decoder**:
- Uses Netty's built-in RESP codec
- Handles pipelined commands
- Supports RESP2 protocol

### 2. Command Executors

**HBase Executors**:
- Single-row operations (Get, Put)
- Range scan operations
- Batch operations
- Admin operations for table management

**Redis Executors**:
- String operations (GET, SET, INCR, etc.)
- Hash operations (HSET, HGET, HGETALL, etc.)
- Set operations (SADD, SMEMBERS, etc.)
- List operations (LPUSH, RPOP, LRANGE, etc.)
- Sorted Set operations (ZADD, ZRANGE, ZSCORE, etc.)
- Stream operations (XADD, XRANGE, XREAD, etc.)
- Geo operations (GEOADD, GEODIST)
- HyperLogLog operations (PFADD, PFCOUNT)

### 3. Storage Adapters

**Redis Storage Layer**:

```
RedisStorageAdapter (interface)
   ├── RedisSingleTableAdapter (legacy, single table mode)
   └── RedisShardedAdapter (recommended, sharded mode)
```

**Key Features:**
- **Polymorphic Interface**: Executors use `RedisStorageAdapter` interface
- **Transparent Sharding**: Sharding logic encapsulated in adapters
- **Lazy Loading**: Tables/writers created on-demand
- **Slot Routing**: CRC16 hash → slot → shard mapping

### 4. Table Management

**Dynamic Table Creation**:
- HBase tables created via Admin API
- Redis tables created automatically on first use
- Schema-on-write for flexible column families/fields

**Table Naming Convention**:
- HBase: `{database}.{table_name}` (e.g., `default.users`)
- Redis Sharded: `redis_shard_{XX}` (XX = 00-15)
- Redis Single: `redis_internal_data`



## Data Flow

CAPE translates protocol-specific operations into Fluss table operations:

- **HBase operations** are converted to Fluss row operations
- **Redis operations** are routed through storage adapters that handle sharding and table mapping
- **All data** flows through the Fluss Client Library to the distributed storage layer

## References

- [Apache Fluss Documentation](https://fluss.apache.org/)
- [HBase Protocol Specification](https://hbase.apache.org/book.html#rpc)
- [Redis Protocol Specification](https://redis.io/docs/reference/protocol-spec/)
- [Configuration Reference](docs/CONFIGURATION.md)
- [Getting Started Guide](docs/GETTING-STARTED.md)
