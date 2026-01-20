# Getting Started with Fluss CAPE

This comprehensive guide walks you through installing, configuring, and using Fluss CAPE for the first time.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Setting up Apache Fluss](#setting-up-apache-fluss)
3. [Installing Fluss CAPE](#installing-fluss-cape)
4. [Starting the CAPE Server](#starting-the-cape-server)
5. [Verifying Installation](#verifying-installation)
6. [Your First Operations](#your-first-operations)
7. [Troubleshooting](#troubleshooting)

---

## Prerequisites

Before you begin, ensure your environment meets these requirements:

### Required

- **Java 11 or higher**
  ```bash
  java -version
  # Should show: java version "11" or higher
  ```

- **Apache Fluss cluster** (v0.4.0+)
  - Running coordinator server
  - At least one tablet server
  - Accessible bootstrap servers endpoint

### Optional (for building from source)

- **Maven 3.8.6+**
  ```bash
  mvn -version
  # Should show: Apache Maven 3.8.6 or higher
  ```

- **Git** (for cloning repository)

---

## Setting up Apache Fluss

If you don't have an Apache Fluss cluster running yet, follow these steps:

### Download and Extract Fluss

```bash
# Download Fluss binary
wget https://github.com/alibaba/fluss/releases/download/v0.5.0/fluss-0.5.0-bin.tgz

# Extract
tar -xzf fluss-0.5.0-bin.tgz
cd fluss-0.5.0
```

### Start Fluss Cluster

```bash
# Start coordinator server
./bin/fluss-daemon.sh start coordinatorServer

# Start tablet server
./bin/fluss-daemon.sh start tabletServer
```

### Verify Fluss is Running

```bash
# Check cluster status
./bin/fluss-cli.sh cluster list

# Expected output:
# CoordinatorServer: localhost:9123 (RUNNING)
# TabletServer: localhost:9124 (RUNNING)
```

### Create a Test Table (Optional)

```bash
# Create a primary key table for testing
./bin/fluss-cli.sh table create \
  --table-name users \
  --schema '{"columns":[{"name":"user_id","type":"STRING"},{"name":"name","type":"STRING"},{"name":"age","type":"INT"}],"primaryKey":["user_id"]}' \
  --database default
```

---

## Installing Fluss CAPE

Choose one of the following installation methods:

### Option 1: Download Pre-built JAR (Recommended)

```bash
# Download latest release
wget https://github.com/gnuhpc/fluss-cape/releases/download/v1.0.0/fluss-cape-1.0.0.jar

# Verify download
ls -lh fluss-cape-1.0.0.jar
# Should show a file ~140MB in size
```

### Option 2: Build from Source

```bash
# Clone repository
git clone https://github.com/gnuhpc/fluss-cape.git
cd fluss-cape

# Build (skip tests for faster build)
mvn clean package -DskipTests

# Built JAR location
ls -lh target/fluss-cape-1.0.0-SNAPSHOT.jar
```

**Note**: Building from source takes 3-5 minutes depending on your machine.

---

## Starting the CAPE Server

### Basic Startup (HBase Protocol Only)

```bash
java -jar fluss-cape-1.0.0.jar \
  --fluss.bootstrap.servers=localhost:9123
```

This starts CAPE with:
- HBase protocol enabled on port **16020** (RegionServer)
- Redis protocol **disabled**
- Connects to Fluss at **localhost:9123**

### Enable Both Protocols

```bash
java -jar fluss-cape-1.0.0.jar \
  --fluss.bootstrap.servers=localhost:9123 \
  --hbase.compat.bind.port=16020 \
  --redis.enable=true \
  --redis.bind.port=6379
```

### Common Startup Configurations

#### Production Setup (Custom Ports)

```bash
java -jar fluss-cape-1.0.0.jar \
  --fluss.bootstrap.servers=fluss-cluster-1:9123,fluss-cluster-2:9123 \
  --hbase.compat.bind.host=0.0.0.0 \
  --hbase.compat.bind.port=16020 \
  --redis.enable=true \
  --redis.bind.host=0.0.0.0 \
  --redis.bind.port=6379
```

#### Redis Only Setup

```bash
java -jar fluss-cape-1.0.0.jar \
  --fluss.bootstrap.servers=localhost:9123 \
  --hbase.compat.enable=false \
  --redis.enable=true \
  --redis.bind.port=6379
```

#### With Custom JVM Options

```bash
java -Xmx4g -Xms4g \
  -XX:+UseG1GC \
  -jar fluss-cape-1.0.0.jar \
  --fluss.bootstrap.servers=localhost:9123
```

### Expected Output

When CAPE starts successfully, you should see logs similar to:

```
2026-01-10 19:15:01 INFO  HBaseCompatServer - Starting HBase compatibility server on 0.0.0.0:16020
2026-01-10 19:15:01 INFO  FlussClientFactory - Connecting to Fluss cluster at localhost:9123
2026-01-10 19:15:02 INFO  RedisServer - Starting Redis server on 0.0.0.0:6379
2026-01-10 19:15:02 INFO  CAPEServerLauncher - Fluss CAPE started successfully
```

---

## Verifying Installation

### Test HBase Protocol

#### 1. Check HBase Port is Listening

```bash
netstat -tuln | grep 16020
# Expected: tcp        0      0 0.0.0.0:16020           0.0.0.0:*               LISTEN
```

#### 2. Use HBase Shell (if installed)

```bash
hbase shell

# In HBase shell:
> list
# Should show tables from Fluss
```

#### 3. Use Java Client

Create a test file `TestHBase.java`:

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class TestHBase {
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost:2181");
        
        Connection conn = ConnectionFactory.createConnection(config);
        Admin admin = conn.getAdmin();
        
        System.out.println("Connected to Fluss via HBase protocol!");
        System.out.println("Available tables: " + admin.listTableNames().length);
        
        conn.close();
    }
}
```

### Test Redis Protocol

#### 1. Check Redis Port is Listening

```bash
netstat -tuln | grep 6379
# Expected: tcp        0      0 0.0.0.0:6379            0.0.0.0:*               LISTEN
```

#### 2. Use redis-cli

```bash
redis-cli -p 6379

# Test basic commands
127.0.0.1:6379> PING
PONG

127.0.0.1:6379> SET test:key "Hello CAPE"
OK

127.0.0.1:6379> GET test:key
"Hello CAPE"
```

#### 3. Use Python Client

```python
import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)
r.set('test:key', 'Hello from Python')
print(r.get('test:key'))  # Output: Hello from Python
```

---

## Your First Operations

### HBase Example: User Management

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class UserManagement {
    public static void main(String[] args) throws Exception {
        // Connect
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost:2181");
        Connection conn = ConnectionFactory.createConnection(config);
        
        // Get table
        Table table = conn.getTable(TableName.valueOf("users"));
        
        // Insert user
        Put put = new Put(Bytes.toBytes("user001"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("Alice"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(30));
        table.put(put);
        System.out.println("User inserted!");
        
        // Read user
        Get get = new Get(Bytes.toBytes("user001"));
        Result result = table.get(get);
        String name = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("name")));
        System.out.println("User name: " + name);
        
        // Cleanup
        table.close();
        conn.close();
    }
}
```

### Redis Example: Session Storage

```bash
redis-cli -p 6379

# Store session data
HSET session:abc123 user_id 1001
HSET session:abc123 username "alice"
HSET session:abc123 login_time "2026-01-10T19:00:00"
EXPIRE session:abc123 3600

# Retrieve session
HGETALL session:abc123

# Check if session exists
EXISTS session:abc123
```

---

## Troubleshooting

### Issue: CAPE fails to start with "Address already in use"

**Cause**: Port 16020 or 6379 is already in use.

**Solution**:
```bash
# Find what's using the port
lsof -i :16020
lsof -i :6379

# Use different ports
java -jar fluss-cape-1.0.0.jar \
  --fluss.bootstrap.servers=localhost:9123 \
  --hbase.compat.bind.port=16021 \
  --redis.bind.port=6380
```

### Issue: "Cannot connect to Fluss cluster"

**Cause**: Fluss bootstrap servers are unreachable.

**Solution**:
```bash
# Verify Fluss is running
curl http://localhost:9123/health

# Check network connectivity
telnet localhost 9123

# Verify bootstrap servers address
java -jar fluss-cape-1.0.0.jar \
  --fluss.bootstrap.servers=<correct-address>:9123
```

### Issue: HBase client can't connect

**Cause**: ZooKeeper connection issue or incorrect configuration.

**Solution**:
```java
// Ensure correct ZooKeeper address
Configuration config = HBaseConfiguration.create();
config.set("hbase.zookeeper.quorum", "localhost:2181");  // Change to your ZK address
config.set("zookeeper.znode.parent", "/fluss");
```

### Issue: Redis commands return errors

**Cause**: Table doesn't exist in Fluss or incorrect key format.

**Solution**:
```bash
# Redis keys map to Fluss tables
# Format: <table_name>:<primary_key>:<column>

# Ensure table exists in Fluss first
# Then use correct key format
SET users:1001:name "Alice"
```

### Issue: OutOfMemoryError

**Cause**: Insufficient heap memory.

**Solution**:
```bash
# Increase JVM heap size
java -Xmx8g -Xms8g -jar fluss-cape-1.0.0.jar \
  --fluss.bootstrap.servers=localhost:9123
```

### Getting Help

If issues persist:

1. Check logs for detailed error messages
2. Verify Fluss cluster health: `./bin/fluss-cli.sh cluster list`
3. Review [Configuration Guide](CONFIGURATION.md) for advanced options
4. Open an issue: https://github.com/gnuhpc/fluss-cape/issues

---

## Next Steps

Now that you have CAPE running:

- **[HBase Guide](HBASE-GUIDE.md)** - Learn HBase API usage with Spark and Phoenix
- **[Redis Guide](REDIS-GUIDE.md)** - Explore all 131 supported Redis commands
- **[PostgreSQL Guide](PGSQL-GUIDE.md)** - Query Fluss via standard SQL and PG clients
- **[Configuration Reference](CONFIGURATION.md)** - Fine-tune CAPE for production

- **[Examples](EXAMPLES.md)** - See real-world use cases

---

## Quick Reference

### Start CAPE (Both Protocols)
```bash
java -jar fluss-cape-1.0.0.jar \
  --fluss.bootstrap.servers=localhost:9123 \
  --redis.enable=true
```

### Test HBase
```bash
echo "list" | hbase shell
```

### Test Redis
```bash
redis-cli -p 6379 PING
```

### Stop CAPE
```bash
# Find PID
ps aux | grep fluss-cape

# Kill process
kill <PID>
```

---

**Ready to build?** Check out the [Examples Guide](EXAMPLES.md) for production use cases!
