# Fluss CAPE Quickstart

A concise path to building Fluss CAPE, running the compatibility server, and trying the HBase/Redis/PostgreSQL/Kafka clients you already know.

## 1. Prerequisites

- **Java 11+** installed and on your `PATH` (`java -version` should report 11 or newer).
- **Apache Fluss cluster** reachable via a bootstrap endpoint (default `localhost:9123`).
- **ZooKeeper ensemble** that Fluss uses (default `localhost:2181`).
- **Maven 3.8+** if you build from source.

## 2. Build the project

```bash
git clone https://github.com/gnuhpc/fluss-cape.git
cd fluss-cape
mvn clean package -DskipTests
# Result: target/fluss-cape-1.0.0-SNAPSHOT.jar
```

> Tip: reuse the same Maven settings and local Fluss version as your target environment so that the generated `fluss-cape` jar and native config align with your cluster.

## 3. Start Fluss CAPE

### 3.1 HBase + Redis + Kafka + PostgreSQL (full stack)

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

### 3.2 HBase-only (fast validation)

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
- Use `docker` scripts in `/scripts` for containerized runs.
- Experiment with the `tests/run-tests.sh` suite once you are comfortable with manual flows.
