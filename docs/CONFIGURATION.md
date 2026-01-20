# Configuration Reference

Complete configuration guide for Fluss CAPE.

---

## Table of Contents

1. [Overview](#overview)
2. [Command-Line Options](#command-line-options)
3. [Configuration File](#configuration-file)
4. [Fluss Client Options](#fluss-client-options)
5. [HBase Protocol Options](#hbase-protocol-options)
6. [Redis Protocol Options](#redis-protocol-options)
7. [Performance Tuning](#performance-tuning)
8. [Security Configuration](#security-configuration)
9. [Environment Variables](#environment-variables)
10. [Examples](#examples)

---

## Overview

Fluss CAPE can be configured through:

1. **Command-line arguments** (highest priority)
2. **Configuration file** (`cape.properties`)
3. **Environment variables** (lowest priority)

---

## Command-Line Options

### Basic Syntax

```bash
java -jar fluss-cape-1.0.0.jar [OPTIONS]
```

### General Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--help` | - | - | Show help message |
| `--version` | - | - | Show version information |
| `--config` | String | - | Path to configuration file |

### Fluss Connection

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--fluss.bootstrap.servers` | String | `localhost:9123` | Fluss coordinator addresses (comma-separated) |
| `--fluss.client.timeout.ms` | Integer | `30000` | Client operation timeout (milliseconds) |
| `--fluss.client.retry.count` | Integer | `3` | Number of retry attempts |

### HBase Protocol

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--hbase.compat.enable` | Boolean | `true` | Enable HBase protocol server |
| `--hbase.compat.bind.host` | String | `0.0.0.0` | HBase server bind address |
| `--hbase.compat.bind.port` | Integer | `16020` | HBase server port |
| `--hbase.compat.worker.threads` | Integer | `16` | Worker thread pool size |
| `--hbase.compat.max.connections` | Integer | `1000` | Maximum concurrent connections |

### Redis Protocol

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--redis.enable` | Boolean | `false` | Enable Redis protocol server |
| `--redis.bind.host` | String | `0.0.0.0` | Redis server bind address |
| `--redis.bind.port` | Integer | `6379` | Redis server port |
| `--redis.worker.threads` | Integer | `8` | Worker thread pool size |
| `--redis.max.connections` | Integer | `10000` | Maximum concurrent connections |
| `--redis.database.count` | Integer | `16` | Number of logical databases (0-15) |

### PostgreSQL Protocol

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--pg.enabled` | Boolean | `false` | Enable PostgreSQL protocol server |
| `--pg.bind.address` | String | `0.0.0.0` | PostgreSQL server bind address |
| `--pg.port` | Integer | `5432` | PostgreSQL server port |
| `--pg.auth.mode` | String | `trust` | Auth mode: trust, password |
| `--pg.auth.user` | String | - | Auth username |
| `--pg.auth.password` | String | - | Auth password |
| `--pg.database` | String | `default` | Default database name |

### Logging

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--log.level` | String | `INFO` | Log level: TRACE, DEBUG, INFO, WARN, ERROR |
| `--log.file` | String | `logs/cape.log` | Log file path |
| `--log.max.size` | String | `100MB` | Maximum log file size |
| `--log.max.files` | Integer | `10` | Maximum number of log files |

---

## Configuration File

### File Format

Create `cape.properties`:

```properties
# Fluss connection
fluss.bootstrap.servers=localhost:9123
fluss.client.timeout.ms=30000

# HBase protocol
hbase.compat.enable=true
hbase.compat.bind.host=0.0.0.0
hbase.compat.bind.port=16020
hbase.compat.worker.threads=16

# Redis protocol
redis.enable=true
redis.bind.host=0.0.0.0
redis.bind.port=6379
redis.worker.threads=8

# PostgreSQL protocol
pg.enabled=true
pg.bind.address=0.0.0.0
pg.port=15432
pg.auth.mode=trust
pg.database=default

# Logging
log.level=INFO
log.file=logs/cape.log
```

### Load Configuration File

```bash
java -jar fluss-cape-1.0.0.jar --config=cape.properties
```

**Note**: Command-line options override file settings.

---

## Fluss Client Options

### Connection Pool

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `fluss.client.connection.pool.size` | Integer | `10` | Connection pool size |
| `fluss.client.connection.max.idle.ms` | Long | `300000` | Max idle time before closing |
| `fluss.client.connection.retry.backoff.ms` | Long | `100` | Retry backoff time |

### Timeout Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `fluss.client.request.timeout.ms` | Integer | `30000` | Request timeout |
| `fluss.client.metadata.refresh.interval.ms` | Long | `60000` | Metadata refresh interval |
| `fluss.client.socket.timeout.ms` | Integer | `30000` | Socket timeout |

### Example

```properties
fluss.bootstrap.servers=node1:9123,node2:9123,node3:9123
fluss.client.connection.pool.size=20
fluss.client.request.timeout.ms=60000
fluss.client.retry.count=5
fluss.client.connection.retry.backoff.ms=200
```

---

## HBase Protocol Options

### Server Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `hbase.compat.bind.host` | String | `0.0.0.0` | Bind address (0.0.0.0 = all interfaces) |
| `hbase.compat.bind.port` | Integer | `16020` | Listen port |
| `hbase.compat.backlog` | Integer | `128` | TCP connection backlog |

### Thread Pool

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `hbase.compat.worker.threads` | Integer | `16` | Worker threads (recommend: 2x CPU cores) |
| `hbase.compat.boss.threads` | Integer | `1` | Boss threads for accepting connections |
| `hbase.compat.max.connections` | Integer | `1000` | Maximum concurrent connections |

### Buffer Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `hbase.compat.receive.buffer.size` | Integer | `65536` | TCP receive buffer (bytes) |
| `hbase.compat.send.buffer.size` | Integer | `65536` | TCP send buffer (bytes) |
| `hbase.compat.max.frame.size` | Integer | `10485760` | Max frame size (10MB) |

### Scan Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `hbase.compat.scan.caching` | Integer | `100` | Default scan caching |
| `hbase.compat.scan.max.result.size` | Long | `2097152` | Max result size (2MB) |
| `hbase.compat.scan.timeout.ms` | Long | `60000` | Scanner timeout |

### Security

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `hbase.compat.security.enabled` | Boolean | `false` | Enable SASL authentication |
| `hbase.compat.sasl.mechanism` | String | `GSSAPI` | SASL mechanism (GSSAPI/PLAIN) |
| `hbase.compat.kerberos.principal` | String | - | Kerberos principal |
| `hbase.compat.kerberos.keytab` | String | - | Kerberos keytab file |

### Example

```properties
hbase.compat.enable=true
hbase.compat.bind.host=0.0.0.0
hbase.compat.bind.port=16020
hbase.compat.worker.threads=32
hbase.compat.max.connections=2000
hbase.compat.scan.caching=500
hbase.compat.max.frame.size=20971520
```

---

## Redis Protocol Options

### Server Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `redis.bind.host` | String | `0.0.0.0` | Bind address |
| `redis.bind.port` | Integer | `6379` | Listen port |
| `redis.backlog` | Integer | `511` | TCP connection backlog |

### Thread Pool

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `redis.worker.threads` | Integer | `8` | Worker threads |
| `redis.boss.threads` | Integer | `1` | Boss threads |
| `redis.max.connections` | Integer | `10000` | Maximum connections |

### Protocol Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `redis.database.count` | Integer | `16` | Number of databases (0-15) |
| `redis.max.bulk.length` | Long | `536870912` | Max bulk string (512MB) |
| `redis.max.multi.bulk.length` | Long | `1024` | Max array elements |
| `redis.timeout.ms` | Long | `0` | Client timeout (0=no timeout) |

### Memory & Limits

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `redis.max.memory` | String | - | Max memory (e.g., "1gb") |
| `redis.max.memory.policy` | String | `noeviction` | Eviction policy |
| `redis.max.clients` | Integer | `10000` | Max clients |

### Pipeline

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `redis.pipeline.enabled` | Boolean | `true` | Enable pipelining |
| `redis.pipeline.batch.size` | Integer | `100` | Max pipeline batch size |

### Example

```properties
redis.enable=true
redis.bind.host=0.0.0.0
redis.bind.port=6379
redis.worker.threads=16
redis.max.connections=20000
redis.database.count=16
redis.timeout.ms=30000
redis.pipeline.enabled=true
redis.pipeline.batch.size=200
```

---

## Performance Tuning

### High Throughput Configuration

```properties
# Fluss client
fluss.bootstrap.servers=node1:9123,node2:9123,node3:9123
fluss.client.connection.pool.size=50

# HBase protocol
hbase.compat.worker.threads=64
hbase.compat.max.connections=5000
hbase.compat.receive.buffer.size=131072
hbase.compat.send.buffer.size=131072
hbase.compat.scan.caching=1000

# Redis protocol
redis.worker.threads=32
redis.max.connections=50000
redis.pipeline.enabled=true
redis.pipeline.batch.size=500
```

### Low Latency Configuration

```properties
# Fluss client
fluss.client.request.timeout.ms=10000
fluss.client.retry.count=1

# HBase protocol
hbase.compat.worker.threads=16
hbase.compat.scan.caching=100

# Redis protocol
redis.worker.threads=8
redis.pipeline.batch.size=50
```

### Memory-Constrained Environment

```properties
# HBase protocol
hbase.compat.worker.threads=4
hbase.compat.max.connections=500
hbase.compat.scan.caching=50
hbase.compat.max.frame.size=5242880

# Redis protocol
redis.worker.threads=4
redis.max.connections=1000
redis.max.bulk.length=10485760
```

---

## Security Configuration

### SASL/Kerberos for HBase

```properties
# Enable security
hbase.compat.security.enabled=true
hbase.compat.sasl.mechanism=GSSAPI

# Kerberos settings
hbase.compat.kerberos.principal=hbase/hostname@REALM
hbase.compat.kerberos.keytab=/etc/security/keytabs/hbase.keytab

# JAAS configuration
java.security.auth.login.config=/path/to/jaas.conf
```

**JAAS Configuration** (`jaas.conf`):

```
Client {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  keyTab="/etc/security/keytabs/hbase.keytab"
  principal="hbase/hostname@REALM"
  useTicketCache=false
  storeKey=true;
};
```

### TLS/SSL (Future Support)

```properties
# Not yet implemented
hbase.compat.ssl.enabled=false
redis.ssl.enabled=false
```

---

## Environment Variables

### Supported Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `CAPE_OPTS` | JVM options | `-Xmx8g -Xms8g` |
| `FLUSS_BOOTSTRAP_SERVERS` | Fluss servers | `node1:9123,node2:9123` |
| `HBASE_PORT` | HBase port | `16020` |
| `REDIS_PORT` | Redis port | `6379` |
| `LOG_LEVEL` | Logging level | `DEBUG` |

### Example Usage

```bash
export CAPE_OPTS="-Xmx8g -Xms8g -XX:+UseG1GC"
export FLUSS_BOOTSTRAP_SERVERS="node1:9123,node2:9123"
export HBASE_PORT=16020
export REDIS_PORT=6379

java $CAPE_OPTS -jar fluss-cape-1.0.0.jar \
  --fluss.bootstrap.servers=$FLUSS_BOOTSTRAP_SERVERS \
  --hbase.compat.bind.port=$HBASE_PORT \
  --redis.enable=true \
  --redis.bind.port=$REDIS_PORT
```

---

## Examples

### Production Deployment

```properties
# cape-production.properties

# Fluss cluster
fluss.bootstrap.servers=fluss-1:9123,fluss-2:9123,fluss-3:9123
fluss.client.connection.pool.size=30
fluss.client.request.timeout.ms=60000
fluss.client.retry.count=5

# HBase protocol
hbase.compat.enable=true
hbase.compat.bind.host=0.0.0.0
hbase.compat.bind.port=16020
hbase.compat.worker.threads=64
hbase.compat.max.connections=5000
hbase.compat.scan.caching=1000
hbase.compat.receive.buffer.size=131072
hbase.compat.send.buffer.size=131072

# Redis protocol
redis.enable=true
redis.bind.host=0.0.0.0
redis.bind.port=6379
redis.worker.threads=32
redis.max.connections=50000
redis.pipeline.enabled=true
redis.pipeline.batch.size=500

# Logging
log.level=INFO
log.file=/var/log/cape/cape.log
log.max.size=200MB
log.max.files=20
```

**Start Command**:

```bash
java -Xmx16g -Xms16g \
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=200 \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=/var/log/cape/heap_dump.hprof \
  -jar fluss-cape-1.0.0.jar \
  --config=/etc/cape/cape-production.properties
```

### Development Environment

```properties
# cape-dev.properties

# Local Fluss
fluss.bootstrap.servers=localhost:9123
fluss.client.timeout.ms=10000

# HBase protocol
hbase.compat.enable=true
hbase.compat.bind.port=16020
hbase.compat.worker.threads=4

# Redis protocol
redis.enable=true
redis.bind.port=6379
redis.worker.threads=2

# Debug logging
log.level=DEBUG
log.file=logs/cape-dev.log
```

### Redis-Only Deployment

```properties
# cape-redis-only.properties

fluss.bootstrap.servers=localhost:9123

# Disable HBase
hbase.compat.enable=false

# Enable Redis
redis.enable=true
redis.bind.host=0.0.0.0
redis.bind.port=6379
redis.worker.threads=16
redis.max.connections=10000
redis.database.count=16

log.level=INFO
```

### HBase-Only Deployment

```properties
# cape-hbase-only.properties

fluss.bootstrap.servers=localhost:9123

# Enable HBase
hbase.compat.enable=true
hbase.compat.bind.host=0.0.0.0
hbase.compat.bind.port=16020
hbase.compat.worker.threads=32
hbase.compat.max.connections=2000

# Disable Redis
redis.enable=false

log.level=INFO
```

### High-Availability Setup

```properties
# cape-ha.properties

# Multiple Fluss coordinators
fluss.bootstrap.servers=fluss-1:9123,fluss-2:9123,fluss-3:9123
fluss.client.connection.pool.size=50
fluss.client.retry.count=10
fluss.client.connection.retry.backoff.ms=500

# Both protocols
hbase.compat.enable=true
hbase.compat.bind.host=0.0.0.0
hbase.compat.bind.port=16020
hbase.compat.worker.threads=64

redis.enable=true
redis.bind.host=0.0.0.0
redis.bind.port=6379
redis.worker.threads=32

log.level=INFO
log.file=/var/log/cape/cape.log
```

**Deploy with systemd**:

```ini
# /etc/systemd/system/cape.service

[Unit]
Description=Fluss CAPE Server
After=network.target

[Service]
Type=simple
User=cape
WorkingDirectory=/opt/cape
ExecStart=/usr/bin/java -Xmx16g -Xms16g -jar /opt/cape/fluss-cape-1.0.0.jar --config=/etc/cape/cape-ha.properties
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
```

---

## JVM Tuning

### Recommended JVM Options

```bash
# Heap size (adjust based on load)
-Xmx8g -Xms8g

# Garbage collector (G1GC recommended)
-XX:+UseG1GC
-XX:MaxGCPauseMillis=200
-XX:G1HeapRegionSize=16M

# GC logging
-Xlog:gc*:file=/var/log/cape/gc.log:time,uptime:filecount=10,filesize=100M

# Heap dump on OOM
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/var/log/cape/heap_dump.hprof

# Performance
-XX:+AlwaysPreTouch
-XX:+UseStringDeduplication
```

### Memory Sizing Guidelines

| Concurrent Connections | Recommended Heap | Worker Threads |
|------------------------|------------------|----------------|
| < 1,000 | 2-4 GB | 8-16 |
| 1,000 - 5,000 | 4-8 GB | 16-32 |
| 5,000 - 10,000 | 8-16 GB | 32-64 |
| > 10,000 | 16-32 GB | 64-128 |

---

## Monitoring Configuration

### Metrics Export (Future Feature)

```properties
# Not yet implemented
metrics.enabled=true
metrics.export.interval.ms=60000
metrics.reporters=prometheus,jmx
```

---

## Troubleshooting

### Check Current Configuration

```bash
# View effective configuration
java -jar fluss-cape-1.0.0.jar --help

# Dry-run mode (prints config and exits)
java -jar fluss-cape-1.0.0.jar --config=cape.properties --dry-run
```

### Common Issues

**Issue**: Out of Memory

**Solution**: Increase heap size:
```bash
java -Xmx16g -Xms16g -jar fluss-cape-1.0.0.jar
```

**Issue**: Too many open files

**Solution**: Increase system limits:
```bash
ulimit -n 65536
```

**Issue**: Port already in use

**Solution**: Change port or kill process:
```bash
lsof -i :16020
kill <PID>
# Or use different port
--hbase.compat.bind.port=16021
```

---

## Next Steps

- **[HBase Guide](HBASE-GUIDE.md)** - Configure HBase protocol
- **[Redis Guide](REDIS-GUIDE.md)** - Configure Redis protocol
- **[Benchmarks](BENCHMARKS.md)** - Performance testing and tuning

---

## Quick Reference

### Minimal Configuration

```bash
java -jar fluss-cape-1.0.0.jar --fluss.bootstrap.servers=localhost:9123
```

### Both Protocols

```bash
java -jar fluss-cape-1.0.0.jar \
  --fluss.bootstrap.servers=localhost:9123 \
  --redis.enable=true
```

### Production Setup

```bash
java -Xmx16g -Xms16g -XX:+UseG1GC \
  -jar fluss-cape-1.0.0.jar \
  --config=/etc/cape/production.properties
```
