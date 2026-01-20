# Fluss CAPE Quickstart

A concise path to building Fluss CAPE, running the compatibility server, and trying the HBase/Redis/PostgreSQL/Kafka clients you already know.

## 1. Prerequisites

- **Java 11+** installed and on your `PATH` (`java -version` should report 11 or newer).
- **Apache Fluss cluster** reachable via a bootstrap endpoint (default `localhost:9123`).
- **ZooKeeper ensemble** that Fluss uses (default `localhost:2181`).
- **Maven 3.8+** if you build from source.

## 2. Build the project

### 2.1 Build JAR from source

```bash
git clone https://github.com/gnuhpc/fluss-cape.git
cd fluss-cape
mvn clean package -DskipTests
# Result: target/fluss-cape-1.0.0-SNAPSHOT.jar
```

> Tip: reuse the same Maven settings and local Fluss version as your target environment so that the generated `fluss-cape` jar and native config align with your cluster.

### 2.2 Build Docker image

```bash
# Build the JAR first (required)
mvn clean package -DskipTests

# Build Docker image
docker build -t fluss-cape:1.0.0 .

# Verify the image
docker images | grep fluss-cape
```

The Docker image includes:
- Java 11 runtime (eclipse-temurin:11-jre-jammy)
- fluss-cape.jar at `/app/fluss-cape.jar`
- Pre-configured environment variables for all protocols
- Health check endpoint at port 8080
- Exposed ports: 16020 (HBase), 6379 (Redis), 5432 (PostgreSQL), 9092 (Kafka), 8080 (Health)

## 3. Start Fluss CAPE

You can run Fluss CAPE either via **Docker** (recommended for production) or directly via **JAR** (recommended for development).

### 3.1 Run with Docker (Recommended)

#### 3.1.1 Full stack (HBase + Redis + Kafka + PostgreSQL)

```bash
docker run -d \
  --name fluss-cape \
  --network host \
  -e FLUSS_BOOTSTRAP=localhost:9123 \
  -e ZK_QUORUM=localhost:2181 \
  -e BIND_ADDRESS=0.0.0.0 \
  -e BIND_PORT=16020 \
  -e REDIS_ENABLE=true \
  -e REDIS_BIND_PORT=6379 \
  -e KAFKA_ENABLE=true \
  -e KAFKA_BIND_PORT=9092 \
  -e PG_ENABLE=true \
  -e PG_BIND_PORT=5432 \
  -e HEALTH_PORT=8080 \
  fluss-cape:1.0.0

# Check container logs
docker logs -f fluss-cape

# Verify health
curl http://localhost:8080/health
```

#### 3.1.2 HBase + Redis only

```bash
docker run -d \
  --name fluss-cape \
  --network host \
  -e FLUSS_BOOTSTRAP=localhost:9123 \
  -e ZK_QUORUM=localhost:2181 \
  -e REDIS_ENABLE=true \
  -e KAFKA_ENABLE=false \
  -e PG_ENABLE=false \
  fluss-cape:1.0.0
```

#### 3.1.3 Custom ports (avoid conflicts)

```bash
docker run -d \
  --name fluss-cape \
  -p 16021:16021 \
  -p 6380:6380 \
  -p 5433:5433 \
  -p 9093:9093 \
  -p 8081:8081 \
  -e FLUSS_BOOTSTRAP=localhost:9123 \
  -e ZK_QUORUM=localhost:2181 \
  -e BIND_PORT=16021 \
  -e REDIS_BIND_PORT=6380 \
  -e KAFKA_BIND_PORT=9093 \
  -e PG_BIND_PORT=5433 \
  -e HEALTH_PORT=8081 \
  fluss-cape:1.0.0
```

#### 3.1.4 Update running Docker container

When you rebuild the JAR and Docker image, update the running container:

```bash
# Method 1: Stop, remove, and run new container
docker stop fluss-cape
docker rm fluss-cape

# Rebuild image with new code
mvn clean package -DskipTests
docker build -t fluss-cape:1.0.0 .

# Run new container with same configuration
docker run -d \
  --name fluss-cape \
  --network host \
  -e FLUSS_BOOTSTRAP=localhost:9123 \
  -e ZK_QUORUM=localhost:2181 \
  fluss-cape:1.0.0

# Method 2: One-liner update (force recreate)
docker rm -f fluss-cape && \
mvn clean package -DskipTests && \
docker build -t fluss-cape:1.0.0 . && \
docker run -d --name fluss-cape --network host \
  -e FLUSS_BOOTSTRAP=localhost:9123 \
  -e ZK_QUORUM=localhost:2181 \
  fluss-cape:1.0.0

# Method 3: Update with Docker Compose (if using docker-compose.yml)
mvn clean package -DskipTests
docker build -t fluss-cape:1.0.0 .
docker-compose down
docker-compose up -d
```

#### 3.1.5 Docker container management

```bash
# Stop container
docker stop fluss-cape

# Start container
docker start fluss-cape

# Restart container (reload configuration)
docker restart fluss-cape

# Remove container
docker rm -f fluss-cape

# View logs (live tail)
docker logs -f fluss-cape

# View recent logs (last 100 lines)
docker logs --tail 100 fluss-cape

# Execute command inside container
docker exec -it fluss-cape sh

# Check container status
docker ps -a | grep fluss-cape

# Inspect container configuration
docker inspect fluss-cape

# Check resource usage
docker stats fluss-cape --no-stream
```

### 3.2 Run with JAR (Development)

#### 3.2.1 HBase + Redis + Kafka + PostgreSQL (full stack)

```bash
java -jar target/fluss-cape-1.0.0-SNAPSHOT.jar \
  --fluss.bootstrap.servers=localhost:9123 \
  --hbase.compat.bind.address=0.0.0.0 \
  --hbase.compat.bind.port=16020 \
  --redis.enable=true \
  --redis.bind.address=0.0.0.0 \
  --redis.bind.port=6379 \
  --kafka.enable=true \
  --kafka.bind.address=0.0.0.0 \
  --kafka.bind.port=9092 \
  --pg.enabled=true \
  --pg.bind.address=0.0.0.0 \
  --pg.port=5432 \
  --health.check.port=8080
```

#### 3.2.2 HBase-only (fast validation)

```bash
java -jar target/fluss-cape-1.0.0-SNAPSHOT.jar \
  --fluss.bootstrap.servers=localhost:9123 \
  --redis.enable=false \
  --kafka.enable=false \
  --pg.enabled=false
```

> Logs will confirm which ports are bound. The health endpoint (`http://localhost:8080/health`) turns green once CAPE connects to Fluss.

## 4. Verify with open-source clients

### 4.1 HBase shell (or client)

```bash
export HBASE_CONF_DIR=/etc/hbase
hbase shell <<< "list"
```

- Create a table via the standard HBase shell:

```ruby
create 'users', 'cf'
put 'users', 'user:1', 'cf:name', 'Alice'
put 'users', 'user:1', 'cf:age', '30'
get 'users', 'user:1'
scan 'users'
```

- Or use a Java client pointing at ZooKeeper `localhost:2181` (Fluss cluster) and the CAPE port `16020`.

### 4.2 redis-cli

```bash
redis-cli -h localhost -p 6379
> PING
PONG
> SET user:1:name "Alice"
OK
> HSET session:abc123 user_id 1001 username "alice"
(integer) 2
> HGETALL session:abc123
```

Redis keys map to Fluss table rows. Every key you write ends up persisted inside the Fluss storage layer.

### 4.3 psql (PostgreSQL client)

```bash
psql "host=localhost port=5432 dbname=default user=postgres"
SELECT version();
SELECT * FROM information_schema.tables LIMIT 5;
```

By default CAPE uses **trust** auth. Override `--pg.auth.mode` if you need password protection.

### 4.4 Kafka console producer + consumer

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic cape-demo --partitions 1 --replication-factor 1
kafka-console-producer.sh --broker-list localhost:9092 --topic cape-demo <<< "hello cape"
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cape-demo --from-beginning --max-messages 10
```

Kafka topics are backed by Fluss tables in the `default` database, so all produced records are durable and queryable via CAPE PostgreSQL/HBase clients as well.

## 5. Health checks & debugging

- Health endpoint: `curl http://localhost:8080/health`
- Port check: `ss -tuln | grep -E "16020|6379|5432|9092|8080"`
- Fluss bootstrap health: `curl http://localhost:9123/health`
- Logs: `tail -n 100 target/fluss-cape.log` (or stdout if running interactively).

## 6. Troubleshooting tips

- **Ports already in use**: add `--hbase.compat.bind.port`, `--redis.bind.port`, `--kafka.bind.port`, `--pg.port` with unused values.
- **Redis keys failing**: ensure the Fluss tables exist; a missing table manifests as command errors.
- **HBase client failing**: verify ZooKeeper (`localhost:2181`) is reachable and CAPE registered as a RegionServer.
- **Kafka client errors**: CAPE emits Kafka metrics to logs; look for `KafkaCompatServer` startup lines.

## 7. Next steps

- Dive deeper with `docs/GETTING-STARTED.md` and the protocol-specific guides (`HBASE-GUIDE.md`, `REDIS-GUIDE.md`, `PGSQL-GUIDE.md`).
- Run multi-instance deployment with Docker Compose: see `docker-compose.yml` for clustered setup examples.
- Experiment with the `tests/run-tests.sh` suite once you are comfortable with manual flows.
- Check out production deployment patterns in `docs/CONFIGURATION.md`.

## 8. Docker configuration reference

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FLUSS_BOOTSTRAP` | `localhost:9123` | Fluss bootstrap servers |
| `ZK_QUORUM` | `localhost:2181` | ZooKeeper quorum for HBase discovery |
| `BIND_ADDRESS` | `0.0.0.0` | HBase bind address |
| `BIND_PORT` | `16020` | HBase RPC port |
| `HEALTH_PORT` | `8080` | Health check HTTP port |
| `REDIS_ENABLE` | `true` | Enable Redis protocol |
| `REDIS_BIND_ADDRESS` | `0.0.0.0` | Redis bind address |
| `REDIS_BIND_PORT` | `6379` | Redis protocol port |
| `REDIS_SHARDING_ENABLED` | `true` | Enable Redis Cluster sharding |
| `REDIS_SHARDING_NUM_SHARDS` | `16` | Number of shards for Redis |
| `PG_ENABLE` | `true` | Enable PostgreSQL protocol |
| `PG_BIND_ADDRESS` | `0.0.0.0` | PostgreSQL bind address |
| `PG_BIND_PORT` | `5432` | PostgreSQL protocol port |
| `PG_DATABASE` | `default` | Default PostgreSQL database |
| `PG_AUTH_MODE` | `trust` | PostgreSQL auth mode (trust/password) |
| `KAFKA_ENABLE` | `true` | Enable Kafka protocol |
| `KAFKA_BIND_ADDRESS` | `0.0.0.0` | Kafka bind address |
| `KAFKA_BIND_PORT` | `9092` | Kafka protocol port |
| `KAFKA_DEFAULT_DATABASE` | `default` | Default Kafka database |
| `SERVER_ID` | _(empty)_ | Unique server ID for multi-instance |
| `TABLES` | _(empty)_ | Pre-configured HBase tables (comma-separated) |

### Docker Compose Example

For production multi-instance deployment:

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

Run with: `docker-compose up -d`
