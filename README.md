# Fluss CAPE

**C**ompatibility **A**nd **P**rotocol **E**xtensions for Apache Fluss

Transform Apache Fluss into a multi-model database by adding HBase and Redis protocol compatibility.

---

## 🎯 What is Fluss CAPE?

Fluss CAPE is an external compatibility layer that enables applications to interact with [Apache Fluss](https://github.com/alibaba/fluss) using familiar HBase and Redis protocols—without modifying Fluss itself.

**Key Benefits:**
- 🔌 **Zero Fluss Modifications** - Runs as a standalone translation layer
- 🚀 **Instant Migration** - Use existing HBase/Redis applications with Fluss
- 🔄 **Unified Storage** - Access the same data through multiple protocols
- ⚡ **Scalable** - Horizontal scaling with multiple CAPE instances
- 📊 **Production-Ready** - Multiple deployment modes (standalone, clustered, co-located)

---

## ✨ Features

### HBase Compatibility
- **Full Protocol Support**: Get, Put, Scan, Delete, Multi-row operations
- **Dynamic Tables**: Create/drop tables on-the-fly via HBase shell or client API
- **Service Discovery**: Automatic RegionServer discovery via ZooKeeper
- **Admin Operations**: Table management, enable/disable tables
- **Authentication**: SASL/GSSAPI support
- **Standard API**: Works with HBase Client 2.x, 3.x and tools like Spark

### Redis Compatibility
- **150+ Commands**: Full RESP protocol support across 14 data types
- **Automatic Sharding**: Redis Cluster-style CRC16 hashing (16384 slots)
- **Data Types**: Strings, Hashes, Sets, Lists, Sorted Sets, Streams, Geo, HyperLogLog, Pub/Sub
- **Client Compatible**: Works with redis-cli, Python, Node.js, Java, and all standard Redis clients
- **Persistent Storage**: All data durably stored in Fluss

### PostgreSQL Compatibility
- **Wire Protocol Support**: Connect using any standard PostgreSQL client (psql, DBeaver, etc.)
- **SQL Interface**: Query Fluss tables using standard SQL syntax
- **Hybrid Scan Strategy**: Reliable data retrieval combining snapshots and changelogs
- **Information Schema**: Full support for metadata discovery and database introspection

---

## ⚠️ Important Notes

### Fluss Version Compatibility
- **Fluss 0.8.0**: Hash operations limited to 4 fields (buffer overflow bug) - use with caution
- **Fluss 0.9**: Fixed buffer overflow but has Arrow WAL bug (#2346) - use with caution
- **Recommendation**: Both versions have limitations - choose based on your specific needs

For details, see [ARCHITECTURE.md](ARCHITECTURE.md) for design overview and [docs/](docs/) for comprehensive guides.

---

## 🚀 Quick Start

### Prerequisites
- Java 11+
- Apache Fluss cluster running
- ZooKeeper (shared with or separate from Fluss)

### Build and Run

```bash
# Clone repository
git clone https://github.com/gnuhpc/fluss-cape.git
cd fluss-cape

# Build JAR (required for Docker)
mvn clean package -DskipTests

# Build Docker image
docker build -t fluss-cape:1.0.0 .

# Run single instance
docker run -d \
  --name fluss-cape \
  --network host \
  -p 16020:16020 \
  -p 8080:8080 \
  -e FLUSS_BOOTSTRAP=localhost:9123 \
  -e ZK_QUORUM=localhost:2181 \
  -e BIND_PORT=16020 \
  -e HEALTH_PORT=8080 \
  fluss-cape:1.0.0

# Check status
curl http://localhost:8080/health
```

### Use HBase Protocol

```bash
# Connect with HBase shell
hbase shell

# Create table
hbase> create 'users', 'cf'

# Insert data
hbase> put 'users', 'user1', 'cf:name', 'Alice'
hbase> put 'users', 'user1', 'cf:age', '30'

# Retrieve data
hbase> get 'users', 'user1'

# Scan data
hbase> scan 'users'
```

### Use Redis Protocol

```bash
# Connect with redis-cli
redis-cli -p 6379

# String operations
> SET user:1:name "Alice"
OK
> GET user:1:name
"Alice"

# Hash operations
> HSET user:1 name "Alice" age 30
(integer) 2
> HGETALL user:1
1) "name"
2) "Alice"
3) "age"
4) "30"

# Sorted sets
> ZADD leaderboard 100 "Alice" 200 "Bob"
(integer) 2
> ZRANGE leaderboard 0 -1 WITHSCORES
1) "Alice"
2) "100"
3) "Bob"
4) "200"
```

---

## 📖 Documentation

- **[Architecture](ARCHITECTURE.md)** - System design and component architecture
- **[Getting Started](docs/GETTING-STARTED.md)** - Detailed installation and setup guide
- **[HBase Guide](docs/HBASE-GUIDE.md)** - Complete HBase usage with examples
- **[Redis Guide](docs/REDIS-GUIDE.md)** - Redis commands and client examples
- **[PostgreSQL Guide](docs/PGSQL-GUIDE.md)** - PostgreSQL wire protocol usage and SQL examples
- **[Configuration](docs/CONFIGURATION.md)** - All configuration parameters
- **[Performance Benchmarks](docs/BENCHMARKS.md)** - YCSB benchmarks and tuning
- **[Functional Tests](tests/README.md)** - Automated testing suite for single and multi-instance deployments

---

## 🧪 Testing

Fluss CAPE includes a comprehensive test suite for validating Redis/Valkey and HBase protocol compatibility:

```bash
# Run all tests (both single and multi-instance)
cd tests
./run-tests.sh

# Run specific tests
./run-tests.sh -m single          # Single instance only
./run-tests.sh -t redis           # Redis protocol only
./run-tests.sh -m multi -t hbase  # Multi-instance HBase only
./run-tests.sh -v                 # Verbose output

# Generate HTML report
./generate-html-report.sh test-reports/test_report_*.log
```

**Test Coverage:**
- ✅ Redis: String, Hash, List, Set, Sorted Set operations
- ✅ HBase: Table management, Put/Get, Scan, Delete operations
- ✅ Single-instance and multi-instance deployment modes
- ✅ Load balancing and service discovery validation

See [tests/README.md](tests/README.md) for detailed documentation.

---

## 🎯 Use Cases

### 1. HBase Application Migration
Migrate existing HBase applications to Fluss without code changes:
- Spring Data HBase applications
- Apache Phoenix SQL queries
- Spark HBase Connector jobs

### 2. Dynamic Schema Evolution
Create and modify tables on-the-fly:
- Development and testing environments
- Rapid prototyping and experimentation
- Ad-hoc data analysis workflows

### 3. Multi-Protocol Data Access
Access the same data through different interfaces:
- Write via Redis, read via HBase
- Batch processing (HBase) + Real-time access (Redis)
- SQL queries (Phoenix) + KV operations (Redis)

### 4. Redis with Durability
Use Redis protocol with Fluss's durable storage:
- Session storage with replay capability
- Real-time leaderboards with historical data
- Message queues with persistence

---

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## 📄 License

Apache License 2.0. See [LICENSE](LICENSE) for details.

---

## 🔗 Links

- **Apache Fluss**: https://github.com/alibaba/fluss
- **HBase Documentation**: https://hbase.apache.org/
- **Redis Documentation**: https://redis.io/
- **GitHub Issues**: [Report Issues](https://github.com/gnuhpc/fluss-cape/issues)

---

<div align="center">

**Transform Apache Fluss into a multi-model database!**

⭐ Star us on GitHub • 📖 [Read the Docs](docs/GETTING-STARTED.md) • 🚀 [Architecture](ARCHITECTURE.md)

</div>
