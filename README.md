# Fluss CAPE

**C**ompatibility **A**nd **P**rotocol **E**xtensions for Apache Fluss

Transform Apache Fluss into a multi-model database by adding HBase, Redis, Kafka, and PostgreSQL protocol compatibility.

---

## 🎯 What is Fluss CAPE?

Fluss CAPE is an external compatibility layer that enables applications to interact with [Apache Fluss](https://github.com/alibaba/fluss) using familiar HBase, Redis, Kafka, and PostgreSQL protocols—without modifying Fluss itself.

---

## 🏗️ Core Concepts & Principles

### Protocol Translation Architecture
Fluss CAPE operates as a stateless proxy layer that performs **real-time protocol translation**:
1. **Decode**: Receives requests via standard protocol handlers (HBase RPC, Redis RESP, Kafka Wire, PG Wire).
2. **Translate**: Maps protocol-specific operations (e.g., `HSET`, `Put`, `Produce`) to Fluss native table operations.
3. **Execute**: Uses the high-performance Fluss Client to interact with the underlying distributed storage.

### Log vs. KV Tables in CAPE
Fluss provides two primary table types, both of which are leveraged by CAPE to provide multi-model capabilities:
- **KV Tables (Primary Key Tables)**: Used primarily for **HBase** and **Redis**. These tables are optimized for low-latency point lookups and range scans. In HBase, the `row_key` maps to the Fluss primary key; in Redis, a composite key `(redis_key, sub_key)` is used to support complex data structures like Hashes and Sets.
- **Log Tables**: Used primarily for the **Kafka protocol**. These are append-only tables optimized for high-throughput ingestion and sequential consumption.
- **Hybrid Support**: The **PostgreSQL protocol** primarily operates on **Primary Key Tables**, but leverages both their KV component (for snapshots/lookups) and Log component (for real-time changelog replay) to provide consistent SQL query results. Every write to a Primary Key table also generates a changelog in a Log table, enabling unified streaming access.

### 🌊 Lake-Stream Integration (湖流一体)
Fluss CAPE fully inherits Fluss's "Lake-Stream Integration" architecture, providing a unified view of data:
- **Unified Interface**: Write data via a "streaming" protocol (Kafka) and immediately query it via a "database" protocol (PostgreSQL/HBase).
- **Changelog as Stream**: All mutations in HBase or Redis tables are automatically captured as Fluss changelogs, which can be consumed via the Kafka protocol for downstream real-time processing.
- **Snapshot + Incremental**: The PostgreSQL protocol uses a hybrid scan strategy that combines KV snapshots with recent logs to ensure data consistency and freshness.

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

### Kafka Compatibility
- **Wire Protocol Support**: Connect using any standard Kafka client (kafka-console-producer, kafka-python, librdkafka, etc.)
- **Unified Messaging**: Seamlessly bridge Fluss changelogs and Kafka topics
- **High Performance**: Low-latency message production and consumption
- **Ecosystem Ready**: Integration with Kafka Connect, Schema Registry, and stream processing engines (Flink, Spark Streaming)

### PostgreSQL Compatibility
- **Wire Protocol Support**: Connect using any standard PostgreSQL client (psql, DBeaver, etc.)
- **SQL Interface**: Query Fluss tables using standard SQL syntax
- **Hybrid Scan Strategy**: Reliable data retrieval combining snapshots and changelogs
- **Information Schema**: Full support for metadata discovery and database introspection

---

For details, see [ARCHITECTURE.md](ARCHITECTURE.md) for design overview and [docs/](docs/) for comprehensive guides.

---

## 🚀 Quick Start

### Prerequisites
- Java 11+
- Apache Fluss cluster running
- ZooKeeper (shared with or separate from Fluss)

### Build and Run

> **Note**: Fluss CAPE currently depends on **Apache Fluss 0.9-SNAPSHOT**. Please ensure you have built and installed Fluss 0.9-SNAPSHOT locally before building CAPE.

**Steps to install Fluss locally:**
```bash
git clone https://github.com/apache/fluss.git
cd fluss
./mvnw clean install -DskipTests
```

**Build CAPE:**

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
  -p 6379:6379 \
  -p 5432:5432 \
  -p 9092:9092 \
  -p 8080:8080 \
  -e FLUSS_BOOTSTRAP=localhost:9123 \
  -e ZK_QUORUM=localhost:2181 \
  -e BIND_PORT=16020 \
  -e HEALTH_PORT=8080 \
  fluss-cape:1.0.0

# Kafka and other protocols are all served from the same container; Kafka clients can connect to `localhost:9092` (the default `KAFKA_BIND_PORT`, which is enabled unless you disable Kafka explicitly).

# Check status
curl http://localhost:8080/health

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

### Use Kafka Protocol

```bash
# Produce messages
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic my_topic
> Hello Fluss!
> This is a Kafka message.

# Consume messages
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my_topic --from-beginning
Hello Fluss!
This is a Kafka message.
```

### Use PostgreSQL Protocol

```bash
# Connect with psql
psql -h localhost -p 5432 -U fluss -d default

# Create table (note: table name must include database prefix)
CREATE TABLE default.employees (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    age INTEGER,
    email VARCHAR(100)
);

# Insert data
INSERT INTO default.employees (id, name, age, email) VALUES (1, 'Alice', 30, 'alice@example.com');
INSERT INTO default.employees (id, name, age, email) VALUES (2, 'Bob', 25, 'bob@example.com');

# Query all data
SELECT * FROM default.employees;

# Query with WHERE clause (only equality predicates supported)
SELECT * FROM default.employees WHERE id = 1;

# Update data
UPDATE default.employees SET age = 31 WHERE id = 1;

# Delete data
DELETE FROM default.employees WHERE id = 2;

# Drop table
DROP TABLE default.employees;
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
- SQL queries (PostgreSQL) + KV operations (Redis)
- Analytics (PostgreSQL) + Low-latency reads (HBase)

### 4. Redis with Durability
Use Redis protocol with Fluss's durable storage:
- Session storage with replay capability
- Real-time leaderboards with historical data
- Message queues with persistence

### 5. SQL Interface for Streaming Data
Query Fluss tables using standard SQL:
- Business intelligence tools (DBeaver, pgAdmin, Tableau)
- PostgreSQL-compatible ORMs (SQLAlchemy, Hibernate)
- Ad-hoc analysis with familiar SQL syntax
- Integration with PostgreSQL ecosystem

### 6. Streaming Integration with Kafka
Bridge your event-driven architecture with Fluss:
- Ingest real-time events from existing Kafka producers
- Consume Fluss changelogs using standard Kafka consumers
- Seamless integration with the Kafka ecosystem (Connect, KSQL, etc.)
- Unified storage for both streaming events and relational data

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
