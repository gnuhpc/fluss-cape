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

## Production Deployment - Multi-Instance (Distributed Transactions)

**✅ NEW**: Multi-instance Redis deployments now support distributed transactions without sticky sessions!

**What Changed**:
- **Transactions (MULTI/EXEC)**: Now stored in Fluss, work across any instance
- **Blocking Operations (BLPOP/BRPOP)**: Still require sticky sessions OR use Redis Streams instead

### Recommended Architecture

```
                          ┌─────────────┐
                          │Load Balancer│
                          │(Any Strategy)│
                          └──────┬──────┘
                                 │
              ┌──────────────────┼──────────────────┐
              │                  │                  │
         ┌────▼────┐        ┌────▼────┐       ┌────▼────┐
         │ CAPE 1  │        │ CAPE 2  │       │ CAPE 3  │
         │ :6379   │        │ :6379   │       │ :6379   │
         └────┬────┘        └────┬────┘       └────┬────┘
              │                  │                  │
              └──────────────────┼──────────────────┘
                                 │
                          ┌──────▼──────┐
                          │    Fluss    │
                          │   Cluster   │
                          │  (Stores TX)│
                          └─────────────┘
```

### Docker Compose Example with HAProxy

**1. Create `haproxy.cfg`** (no sticky sessions needed for transactions):

```haproxy
global
    log stdout format raw local0
    maxconn 4096

defaults
    log     global
    mode    tcp
    option  tcplog
    timeout connect 5000ms
    timeout client  30000ms
    timeout server  30000ms

frontend redis_frontend
    bind *:6379
    default_backend redis_backend

backend redis_backend
    balance roundrobin
    
    # Health checks
    option tcp-check
    tcp-check connect
    tcp-check send PING\r\n
    tcp-check expect string +PONG
    
    server cape1 cape1:6379 check inter 5s fall 3 rise 2
    server cape2 cape2:6379 check inter 5s fall 3 rise 2
    server cape3 cape3:6379 check inter 5s fall 3 rise 2
```

**2. Create `docker-compose.yml`**:

```yaml
version: '3.8'

services:
  # Load Balancer (no sticky sessions needed for transactions!)
  haproxy:
    image: haproxy:2.8-alpine
    container_name: fluss-haproxy
    ports:
      - "6379:6379"     # Redis protocol
      - "8404:8404"     # HAProxy stats
    volumes:
      - ./haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro
    depends_on:
      - cape1
      - cape2
      - cape3
    networks:
      - fluss-network

  # CAPE Instance 1
  cape1:
    image: fluss-cape:1.0.0
    container_name: fluss-cape-1
    environment:
      - FLUSS_BOOTSTRAP=fluss:9123
      - REDIS_PORT=6379
      - REDIS_ENABLE=true
      - HEALTH_PORT=8080
      - INSTANCE_ID=cape-1
    networks:
      - fluss-network
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 10s
      timeout: 5s
      retries: 3

  # CAPE Instance 2
  cape2:
    image: fluss-cape:1.0.0
    container_name: fluss-cape-2
    environment:
      - FLUSS_BOOTSTRAP=fluss:9123
      - REDIS_PORT=6379
      - REDIS_ENABLE=true
      - HEALTH_PORT=8080
      - INSTANCE_ID=cape-2
    networks:
      - fluss-network
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 10s
      timeout: 5s
      retries: 3

  # CAPE Instance 3
  cape3:
    image: fluss-cape:1.0.0
    container_name: fluss-cape-3
    environment:
      - FLUSS_BOOTSTRAP=fluss:9123
      - REDIS_PORT=6379
      - REDIS_ENABLE=true
      - HEALTH_PORT=8080
      - INSTANCE_ID=cape-3
    networks:
      - fluss-network
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 10s
      timeout: 5s
      retries: 3

networks:
  fluss-network:
    external: true  # Assumes Fluss runs in this network
```

**3. Deploy**:

```bash
# Start all instances
docker-compose up -d

# Check HAProxy stats
curl http://localhost:8404/stats

# Test sticky sessions
redis-cli -p 6379
```

**4. Verify Sticky Sessions**:

```bash
# Connect to load balancer
redis-cli -p 6379

# Test distributed transaction (works across ANY instances!)
> MULTI
OK
> SET key1 "value1"
QUEUED
> SET key2 "value2"
QUEUED
> EXEC
1) OK
2) OK

# Verify keys exist
> MGET key1 key2
1) "value1"
2) "value2"

# Note: Blocking operations (BLPOP/BRPOP) still require sticky sessions
# Or better: Use Redis Streams instead!
> XADD mystream * message "hello"
> XREAD COUNT 1 STREAMS mystream 0
```

### Alternative: Use Streams Instead of Blocking Lists

Redis Streams work perfectly in multi-instance deployments:

```bash
# Producer
> XADD workqueue * task "process_video" priority "high"

# Consumer (can connect to any instance)
> XREADGROUP GROUP workers consumer1 COUNT 1 STREAMS workqueue >
```

### Kubernetes Deployment

For Kubernetes, no special session affinity needed for transactions:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: fluss-cape-redis
spec:
  type: LoadBalancer
  ports:
    - port: 6379
      targetPort: 6379
      protocol: TCP
  selector:
    app: fluss-cape
```

### Production Checklist

Before deploying to production:

- [ ] **Transactions tested** across load balancer (MULTI/EXEC)
- [ ] **Health checks enabled** for all instances
- [ ] **Monitoring configured** (metrics, logs)
- [ ] **Failover tested** (kill instance, verify recovery)
- [ ] **Connection pool limits** set appropriately
- [ ] **Backup/restore** strategy documented
- [ ] **Using Streams instead of BLPOP/BRPOP** (recommended)

For detailed explanation, see [Redis Guide - Multi-Instance Deployment](REDIS-GUIDE.md#-multi-instance-deployment---distributed-transaction-support).

---

## Next Steps

Now that you have CAPE running:

- **[HBase Guide](HBASE-GUIDE.md)** - Learn HBase API usage with Spark and Phoenix
- **[Redis Guide](REDIS-GUIDE.md)** - Explore all 131 supported Redis commands with distributed transactions
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
