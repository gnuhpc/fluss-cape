# HBase Compatibility Layer Usage Guide

## Architecture Overview

The HBase compatibility layer provides HBase RPC protocol support for Fluss, allowing HBase clients to communicate with Fluss storage.

## Current Implementation Status

### ✅ Implemented Components:
1. **HBase RPC Protocol** - Complete wire protocol support
2. **Server Infrastructure** - Netty-based server listening on configurable port
3. **Table Operations** - Get, Put, Delete, Scan executors
4. **Protocol Translation** - HBase requests → Fluss API calls

### ⚠️ Architecture Limitation Discovered:

**HBase Client Discovery Mechanism:**
- HBase clients use ZooKeeper to discover region servers
- Clients don't connect directly to a fixed port
- They query ZooKeeper's `/hbase/rs` node to find available region servers
- Our server listens on port 16020 but doesn't register with ZooKeeper

**Impact:**
- Direct Java API connections fail (timeout during PUT/GET)
- HBase shell cannot discover the compatibility server
- YCSB benchmarks cannot connect

## Required Architecture Changes

To make this fully functional, the server needs:

1. **ZooKeeper Registration:**
   ```java
   // Register as HBase region server in ZooKeeper
   /hbase/rs/hostname:port
   ```

2. **Meta Table Emulation:**
   - Implement `hbase:meta` table responses
   - Map Fluss tables to HBase regions
   - Return region server locations

3. **Region Server Info:**
   - Provide ServerName information
   - Implement region assignment logic
   - Handle region split/merge metadata

## Current Workaround: Direct Connection

For testing, you can bypass ZooKeeper by configuring HBase client to connect directly:

```java
Configuration config = HBaseConfiguration.create();
// Bypass ZooKeeper, connect directly (requires custom HBase client)
config.set("hbase.client.connection.impl", 
          "org.apache.fluss.hbase.client.DirectConnection");
config.set("hbase.regionserver.address", "localhost:16020");
```

**Note:** This requires implementing a custom `Connection` class that skips ZooKeeper lookup.

## Recommended Next Steps

### Option 1: Add ZooKeeper Integration (Recommended)

Modify `HBaseCompatServerLauncher` to:
1. Connect to ZooKeeper
2. Register as region server at `/hbase/rs/`
3. Implement meta table queries
4. Handle client discovery properly

### Option 2: Standalone Mode

Create a minimal "standalone" mode for testing:
1. Accept direct connections without ZooKeeper
2. Document as "testing mode only"
3. Clearly state limitations

### Option 3: Use Native Fluss Client

For benchmarking Fluss performance:
1. Use Fluss client API directly
2. Skip HBase compatibility layer
3. Measure native performance

## Starting the Server

```bash
cd /Users/gnuhpc/IdeaProjects/fluss

# Generate classpath
cd fluss-hbase-compat
mvn dependency:build-classpath -DincludeScope=runtime -Dmdep.outputFile=classpath.txt

# Start server
CLASSPATH=$(cat classpath.txt):target/fluss-hbase-compat-0.9-SNAPSHOT.jar

java -Dfluss.bootstrap.servers=localhost:9123 \
     -Dhbase.compat.bind.address=0.0.0.0 \
     -Dhbase.compat.bind.port=16020 \
     -Dhbase.compat.tables=benchmark.usertable \
     -cp "$CLASSPATH" \
     org.apache.fluss.hbase.server.HBaseCompatServerLauncher \
     > hbase-compat-server.log 2>&1 &
```

## Verification

```bash
# Check server is running
ps aux | grep HBaseCompatServerLauncher

# Check port is listening
netstat -an | grep 16020

# View logs
tail -f fluss-hbase-compat/hbase-compat-server.log
```

## Configuration Properties

| Property | Default | Description |
|----------|---------|-------------|
| `fluss.bootstrap.servers` | `localhost:9123` | Fluss cluster address |
| `hbase.compat.bind.address` | `0.0.0.0` | Server bind address |
| `hbase.compat.bind.port` | `16020` | Server listen port |
| `hbase.compat.tables` | _(empty)_ | Comma-separated table list |

## Known Limitations

1. **No ZooKeeper registration** - Clients cannot discover server
2. **No meta table** - Region metadata queries fail
3. **Direct connections only** - Requires custom client code
4. **Single "region"** - All data appears as one region
5. **No load balancing** - Single server instance only

## Future Enhancements

- [ ] ZooKeeper integration for service discovery
- [ ] Meta table emulation
- [ ] Multiple region support
- [ ] Load balancing across Fluss tablet servers
- [ ] Advanced filtering and coprocessors

## Support

For questions or issues, see:
- Main README: `fluss-hbase-compat/README.md`
- Benchmark guide: `YCSB-BENCHMARK.md`
- Next steps: `NEXT-STEPS.md`

---

*Last updated: January 6, 2026*
