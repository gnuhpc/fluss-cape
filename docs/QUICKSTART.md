# Fluss CAPE Quickstart

Get up and running with Fluss CAPE in minutes. This guide covers building, deploying, and verifying multi-protocol compatibility with Apache Fluss.

---

## 1. Prerequisites

| Requirement | Version/Details |
|-------------|-----------------|
| **Java** | 11+ (check: `java -version`) |
| **Apache Fluss** | Running cluster (default: `localhost:9123`) |
| **ZooKeeper** | Ensemble for HBase discovery (default: `localhost:2181`) |
| **Maven** | 3.8+ (for building from source) |
| **Docker** | Optional, for containerized deployment |

---

## 2. Build

### Clone and build JAR

```bash
git clone https://github.com/gnuhpc/fluss-cape.git
cd fluss-cape
mvn clean package -DskipTests
```

**Output**: `target/fluss-cape-1.0.0-SNAPSHOT.jar`

### Build Docker image (optional)

```bash
docker build -t fluss-cape:1.0.0 .
docker images | grep fluss-cape
```

**Image includes**: Java 11 runtime, fluss-cape.jar, health check endpoint, all protocol ports exposed (16020, 6379, 5432, 9092, 8080)

---

## 3. Deploy

Choose your deployment method:

### Option A: Docker (Production)

**Full stack** (all protocols enabled):

```bash
docker run -d \
  --name fluss-cape \
  --network host \
  -e FLUSS_BOOTSTRAP=localhost:9123 \
  -e ZK_QUORUM=localhost:2181 \
  fluss-cape:1.0.0

# Verify
docker logs -f fluss-cape
curl http://localhost:8080/health
```

**Selective protocols**:

```bash
# HBase + Redis only
docker run -d --name fluss-cape --network host \
  -e FLUSS_BOOTSTRAP=localhost:9123 \
  -e ZK_QUORUM=localhost:2181 \
  -e KAFKA_ENABLE=false \
  -e PG_ENABLE=false \
  fluss-cape:1.0.0
```

**Custom ports** (avoid conflicts):

```bash
docker run -d --name fluss-cape \
  -p 16021:16021 -p 6380:6380 -p 5433:5433 -p 9093:9093 -p 8081:8081 \
  -e FLUSS_BOOTSTRAP=localhost:9123 \
  -e ZK_QUORUM=localhost:2181 \
  -e BIND_PORT=16021 \
  -e REDIS_BIND_PORT=6380 \
  -e KAFKA_BIND_PORT=9093 \
  -e PG_BIND_PORT=5433 \
  -e HEALTH_PORT=8081 \
  fluss-cape:1.0.0
```

### Option B: JAR (Development)

**Full stack**:

```bash
java -jar target/fluss-cape-1.0.0-SNAPSHOT.jar \
  --fluss.bootstrap.servers=localhost:9123 \
  --hbase.compat.bind.address=0.0.0.0 \
  --hbase.compat.bind.port=16020 \
  --redis.enable=true \
  --redis.bind.port=6379 \
  --kafka.enable=true \
  --kafka.bind.port=9092 \
  --pg.enabled=true \
  --pg.port=5432 \
  --health.check.port=8080
```

**HBase only** (minimal setup):

```bash
java -jar target/fluss-cape-1.0.0-SNAPSHOT.jar \
  --fluss.bootstrap.servers=localhost:9123 \
  --redis.enable=false \
  --kafka.enable=false \
  --pg.enabled=false
```

---

## 4. Verify Protocols

### HBase

```bash
hbase shell
```

```ruby
create 'users', 'cf'
put 'users', 'user:1', 'cf:name', 'Alice'
put 'users', 'user:1', 'cf:age', '30'
get 'users', 'user:1'
scan 'users'
```

### Redis

```bash
redis-cli -p 6379
```

```
PING
SET user:1:name "Alice"
GET user:1:name
HSET session:abc user_id 1001 username "alice"
HGETALL session:abc
ZADD leaderboard 100 "Alice" 200 "Bob"
ZRANGE leaderboard 0 -1 WITHSCORES
```

### PostgreSQL

```bash
psql "host=localhost port=5432 dbname=default user=postgres"
```

```sql
SELECT version();
SELECT * FROM information_schema.tables LIMIT 5;
```

### Kafka

```bash
# Create topic
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic demo --partitions 1 --replication-factor 1

# Produce
echo "hello cape" | kafka-console-producer.sh \
  --broker-list localhost:9092 --topic demo

# Consume
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic demo --from-beginning --max-messages 1
```

---

## 5. Operations

### Health Checks

```bash
# CAPE health
curl http://localhost:8080/health

# Ports listening
ss -tuln | grep -E "16020|6379|5432|9092|8080"

# Fluss cluster health
curl http://localhost:9123/health
```

### Docker Management

```bash
# View logs
docker logs -f fluss-cape                  # Live tail
docker logs --tail 100 fluss-cape          # Last 100 lines

# Control container
docker stop fluss-cape                     # Stop
docker start fluss-cape                    # Start
docker restart fluss-cape                  # Restart

# Inspect
docker ps -a | grep fluss-cape             # Status
docker stats fluss-cape --no-stream        # Resource usage
docker inspect fluss-cape                  # Full config

# Shell access
docker exec -it fluss-cape sh
```

### Update Deployment

**Docker** (rebuild after code changes):

```bash
# Quick update
docker rm -f fluss-cape && \
  mvn clean package -DskipTests && \
  docker build -t fluss-cape:1.0.0 . && \
  docker run -d --name fluss-cape --network host \
    -e FLUSS_BOOTSTRAP=localhost:9123 \
    -e ZK_QUORUM=localhost:2181 \
    fluss-cape:1.0.0

# Docker Compose (if using compose file)
mvn clean package -DskipTests
docker build -t fluss-cape:1.0.0 .
docker-compose down && docker-compose up -d
```

**JAR** (just rebuild):

```bash
mvn clean package -DskipTests
# Then restart with java -jar command
```

---

## 6. Troubleshooting

| Issue | Solution |
|-------|----------|
| **Port conflicts** | Use custom ports (see section 3) or set env vars: `BIND_PORT`, `REDIS_BIND_PORT`, etc. |
| **Redis errors** | Ensure Fluss tables exist for your key patterns |
| **HBase client fails** | Verify ZooKeeper is reachable (`localhost:2181`) and CAPE registered as RegionServer |
| **Kafka client errors** | Check logs for `KafkaCompatServer` startup messages |
| **Connection refused** | Confirm CAPE is running: `docker ps` or check process with `jps` |

**Log locations**:
- Docker: `docker logs fluss-cape`
- JAR: stdout or `target/fluss-cape.log`

---

## 7. Multi-Instance Deployment

For production high-availability, deploy multiple CAPE instances.

### Docker Compose Example

Create `docker-compose.yml`:

```yaml
version: '3.8'
services:
  fluss-cape-1:
    image: fluss-cape:1.0.0
    container_name: fluss-cape-1
    network_mode: host
    environment:
      FLUSS_BOOTSTRAP: localhost:9123
      ZK_QUORUM: localhost:2181
      SERVER_ID: cape-1
      BIND_PORT: 16020
      REDIS_BIND_PORT: 6379
      KAFKA_BIND_PORT: 9092
      PG_BIND_PORT: 5432
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
    restart: unless-stopped

  fluss-cape-2:
    image: fluss-cape:1.0.0
    container_name: fluss-cape-2
    network_mode: host
    environment:
      FLUSS_BOOTSTRAP: localhost:9123
      ZK_QUORUM: localhost:2181
      SERVER_ID: cape-2
      BIND_PORT: 16021
      REDIS_BIND_PORT: 6380
      KAFKA_BIND_PORT: 9093
      PG_BIND_PORT: 5433
      HEALTH_PORT: 8081
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/health"]
      interval: 30s
      timeout: 10s
      retries: 3
    restart: unless-stopped
```

**Deploy**:

```bash
docker-compose up -d
docker-compose ps
docker-compose logs -f
```

---

## 8. Configuration Reference

### Docker Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FLUSS_BOOTSTRAP` | `localhost:9123` | Fluss bootstrap servers |
| `ZK_QUORUM` | `localhost:2181` | ZooKeeper quorum (HBase discovery) |
| `BIND_ADDRESS` | `0.0.0.0` | HBase bind address |
| `BIND_PORT` | `16020` | HBase RPC port |
| `HEALTH_PORT` | `8080` | Health check HTTP port |
| `REDIS_ENABLE` | `true` | Enable Redis protocol |
| `REDIS_BIND_ADDRESS` | `0.0.0.0` | Redis bind address |
| `REDIS_BIND_PORT` | `6379` | Redis port |
| `REDIS_SHARDING_ENABLED` | `true` | Enable Redis Cluster sharding |
| `REDIS_SHARDING_NUM_SHARDS` | `16` | Number of shards |
| `PG_ENABLE` | `true` | Enable PostgreSQL protocol |
| `PG_BIND_ADDRESS` | `0.0.0.0` | PostgreSQL bind address |
| `PG_BIND_PORT` | `5432` | PostgreSQL port |
| `PG_DATABASE` | `default` | Default database |
| `PG_AUTH_MODE` | `trust` | Auth mode (trust/password) |
| `KAFKA_ENABLE` | `true` | Enable Kafka protocol |
| `KAFKA_BIND_ADDRESS` | `0.0.0.0` | Kafka bind address |
| `KAFKA_BIND_PORT` | `9092` | Kafka port |
| `KAFKA_DEFAULT_DATABASE` | `default` | Default database |
| `SERVER_ID` | _(empty)_ | Unique server ID (multi-instance) |
| `TABLES` | _(empty)_ | Pre-configured HBase tables (comma-separated) |

### JAR Command-Line Arguments

Prefix each variable with `--` and use dots:
- `--fluss.bootstrap.servers=localhost:9123`
- `--hbase.compat.bind.port=16020`
- `--redis.enable=true`
- `--kafka.bind.port=9092`
- `--pg.enabled=true`

---

## 9. Next Steps

| Resource | Purpose |
|----------|---------|
| [GETTING-STARTED.md](GETTING-STARTED.md) | Detailed setup and architecture |
| [HBASE-GUIDE.md](HBASE-GUIDE.md) | HBase protocol usage and examples |
| [REDIS-GUIDE.md](REDIS-GUIDE.md) | Redis commands and data types |
| [PGSQL-GUIDE.md](PGSQL-GUIDE.md) | PostgreSQL SQL interface |
| [CONFIGURATION.md](CONFIGURATION.md) | Advanced configuration options |
| [BENCHMARKS.md](BENCHMARKS.md) | Performance tuning and YCSB results |
| [tests/README.md](../tests/README.md) | Automated test suite |

**Quick experiments**:
- Run automated tests: `cd tests && ./run-tests.sh`
- Multi-instance load balancing: See `tests/run-tests.sh -m multi`
- Performance benchmarking: Follow examples in `BENCHMARKS.md`
