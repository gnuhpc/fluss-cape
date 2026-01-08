# HBase Compatibility Layer - Distributed Deployment Design

**Version:** 1.0  
**Date:** 2026-01-09  
**Status:** Design Phase  

---

## 1. Current Architecture Analysis

### 1.1 Current Single-Node Architecture

```
┌─────────────────────────────────────────────┐
│        HBase Client (YCSB, Apps)            │
└──────────────────┬──────────────────────────┘
                   │ HBase RPC Protocol
                   ▼
┌─────────────────────────────────────────────┐
│   HBaseCompatServerLauncher (Single)        │
│                                             │
│  ┌─────────────────────────────────────┐  │
│  │  HBaseCompatServer                   │  │
│  │  (Netty Server: port 16020)          │  │
│  │                                       │  │
│  │  ┌───────────────────────────────┐  │  │
│  │  │  HBaseRequestRouter           │  │  │
│  │  │  - IsMasterRunning            │  │  │
│  │  │  - GetTableNames              │  │  │
│  │  │  - Get/Put/Scan/Multi         │  │  │
│  │  │  - MetaTableEmulator          │  │  │
│  │  └───────────────────────────────┘  │  │
│  │                                       │  │
│  │  ┌───────────────────────────────┐  │  │
│  │  │  VirtualRegionManager         │  │  │
│  │  │  (Region-to-bucket mapping)   │  │  │
│  │  └───────────────────────────────┘  │  │
│  └─────────────────────────────────────┘  │
│                                             │
│  ┌─────────────────────────────────────┐  │
│  │  Fluss Connection (Single)          │  │
│  │  - Admin API                         │  │
│  │  - Table instances (per table)       │  │
│  └─────────────────────────────────────┘  │
└──────────────────┬──────────────────────────┘
                   │ Fluss Client Protocol
                   ▼
┌─────────────────────────────────────────────┐
│          Fluss Cluster                      │
│  (Coordinator + Tablet Servers)             │
└─────────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│          ZooKeeper Cluster                  │
│  /hbase/rs/<server>          (RegionServer) │
│  /hbase/meta-region-server   (Meta location)│
│  /hbase/master               (Master node)  │
└─────────────────────────────────────────────┘
```

### 1.2 Current Components

| Component | Type | Statefulness | Scalability |
|-----------|------|--------------|-------------|
| **HBaseCompatServer** | Netty Server | Stateless | ✅ Horizontally scalable |
| **HBaseRequestRouter** | Request Handler | Stateless | ✅ Horizontally scalable |
| **VirtualRegionManager** | Region Metadata | In-memory | ⚠️ Needs shared state |
| **Fluss Connection** | Resource | Stateful (connections) | ✅ Per-instance OK |
| **Executors** | Operation Logic | Stateless | ✅ Horizontally scalable |
| **ZooKeeper Registration** | Service Discovery | Ephemeral nodes | ⚠️ Needs multi-node support |

### 1.3 Identified Limitations

**Hard Blockers for Distribution:**
1. ❌ **Single ZooKeeper registration** - Only one server can be master/meta-region-server
2. ❌ **No load balancing** - Clients connect to single host:port
3. ❌ **No health checks** - Failed server stays in ZK until session expires
4. ❌ **No failover** - Server crash = complete unavailability

**Soft Limitations (can work, not optimal):**
5. ⚠️ Each server maintains separate Fluss connections (OK, but inefficient)
6. ⚠️ Each server has own VirtualRegionManager (OK, read-only metadata)
7. ⚠️ No request affinity or sticky sessions (OK for stateless operations)

---

## 2. Target Distributed Architecture

### 2.1 Multi-Node Architecture

```
                    ┌─────────────────────────┐
                    │   HBase Clients (YCSB)  │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │   ZooKeeper Cluster     │ (Service Discovery)
                    │   /hbase/rs/*           │
                    │   /hbase/meta-rs        │
                    │   /hbase/master         │
                    └────────────┬────────────┘
                                 │ Discover RegionServers
                    ┌────────────┴────────────┐
                    │                         │
            ┌───────▼──────┐         ┌───────▼──────┐         ┌──────────────┐
            │ HBaseCompat  │         │ HBaseCompat  │         │ HBaseCompat  │
            │ Server #1    │         │ Server #2    │         │ Server #N    │
            │ (host1:16020)│         │ (host2:16020)│  ...    │ (hostN:16020)│
            └───────┬──────┘         └───────┬──────┘         └───────┬──────┘
                    │                        │                         │
                    └────────────────────────┼─────────────────────────┘
                                             │
                                             ▼
                                ┌────────────────────────┐
                                │   Fluss Cluster        │
                                │   (Shared by all)      │
                                └────────────────────────┘
```

### 2.2 Design Principles

1. **Stateless Servers** - Each HBaseCompatServer is fully stateless (except TCP connections)
2. **Shared-Nothing Architecture** - No inter-server communication required
3. **ZooKeeper-Based Discovery** - HBase clients discover servers via ZK (native HBase behavior)
4. **Client-Side Load Balancing** - HBase client distributes requests across RegionServers
5. **Independent Failover** - Failed server auto-removed from ZK, clients reconnect to others
6. **Horizontal Scalability** - Add more servers to increase throughput

---

## 3. Required New Capabilities

### 3.1 Service Registration & Discovery

**Current State:**
- Single server registers as:
  - `/hbase/rs/<server>` (RegionServer)
  - `/hbase/meta-region-server` (Meta region location)
  - `/hbase/master` (Master node)

**Required Changes:**
- ✅ **Multiple RegionServer Registration**
  - Each server registers as `/hbase/rs/<unique-server-name>`
  - Ephemeral nodes (auto-cleanup on failure)
  
- ⚠️ **Distributed Meta Region Management**
  - Option A: All servers claim to host meta (clients can connect to any)
  - Option B: Leader election for meta-region-server (one active, others standby)
  
- ⚠️ **Distributed Master Management**
  - Option A: All servers claim to be master (HBase clients accept this)
  - Option B: Leader election for master role (one active, others standby)

**Recommendation:** Option A (All-Master) - Simpler, stateless, works for read-heavy operations

### 3.2 Health Check & Monitoring

**Required:**
1. **Periodic Health Checks**
   - Each server validates Fluss connection health
   - Each server validates ZooKeeper session health
   - Each server monitors resource usage (memory, CPU, connections)

2. **Health Status Reporting**
   - HTTP endpoint: `GET /health` → `{status: "healthy"/"unhealthy", ...}`
   - Report to monitoring system (Prometheus, etc.)

3. **Graceful Shutdown**
   - Deregister from ZooKeeper BEFORE closing connections
   - Wait for in-flight requests to complete (with timeout)
   - Close Fluss connections cleanly

4. **Auto-Recovery**
   - ZooKeeper session lost → attempt reconnection
   - Fluss connection lost → attempt reconnection
   - Repeated failures → log error and exit (let orchestrator restart)

### 3.3 Load Balancing Strategy

**HBase Native Load Balancing (Built-in):**
- HBase clients maintain connection pool to all RegionServers in `/hbase/rs/*`
- Clients use region-to-server mapping for locality-aware routing
- Random selection for meta table operations

**Our Approach:**
- ✅ Register all servers as RegionServers in ZooKeeper
- ✅ Each server reports all virtual regions (client sees all regions on all servers)
- ✅ HBase client's built-in balancer distributes requests

**No custom load balancer needed!** HBase client handles this.

### 3.4 Configuration Management

**Current:** System properties (`-Dproperty=value`)

**Distributed Needs:**
1. **Server Identification**
   - `server.id` - Unique identifier for each server (hostname or UUID)
   - `server.bind.address` - IP to bind to
   - `server.bind.port` - Port to bind (default: 16020, or auto-increment if taken)

2. **Cluster Configuration**
   - `cluster.name` - Cluster identifier (for multi-cluster deployments)
   - `cluster.servers` - List of all server addresses (for monitoring)

3. **ZooKeeper Configuration**
   - `zookeeper.quorum` - ZK ensemble (existing)
   - `zookeeper.session.timeout` - Session timeout (default: 30s)
   - `zookeeper.root.path` - ZK root path (default: `/hbase`)

4. **Fluss Configuration**
   - `fluss.bootstrap.servers` - Fluss cluster (existing)
   - `fluss.connection.timeout` - Connection timeout
   - `fluss.tables` - Tables to expose (existing)

**Configuration File:** Support `hbase-compat-server.conf` in addition to system properties

### 3.5 Failure Scenarios & Handling

| Scenario | Detection | Handling | Recovery Time |
|----------|-----------|----------|---------------|
| **Server Crash** | ZK session expires | Ephemeral nodes removed | ~30s (ZK timeout) |
| **Network Partition** | ZK heartbeat fails | Server deregisters self | ~30s |
| **Fluss Cluster Down** | Connection errors | Return errors to clients | Immediate |
| **ZooKeeper Quorum Lost** | All operations fail | Shutdown server | Immediate |
| **Out of Memory** | JVM crash | Process exits, ZK cleans up | ~30s |
| **Slow Server** | Client-side timeouts | Clients retry on other servers | Client timeout |

**Key Points:**
- ✅ No distributed consensus needed (servers are stateless)
- ✅ ZooKeeper handles failure detection automatically
- ✅ Clients handle retry and failover (HBase client behavior)

---

## 4. Deployment Models

### 4.1 Model A: Kubernetes StatefulSet

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: hbase-compat-server
spec:
  serviceName: hbase-compat-headless
  replicas: 3
  selector:
    matchLabels:
      app: hbase-compat-server
  template:
    metadata:
      labels:
        app: hbase-compat-server
    spec:
      containers:
      - name: hbase-compat
        image: fluss/hbase-compat:0.9
        env:
        - name: FLUSS_BOOTSTRAP_SERVERS
          value: "fluss-cluster:35739"
        - name: ZOOKEEPER_QUORUM
          value: "zk-0:2181,zk-1:2181,zk-2:2181"
        - name: SERVER_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        ports:
        - containerPort: 16020
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: hbase-compat-headless
spec:
  clusterIP: None
  selector:
    app: hbase-compat-server
  ports:
  - port: 16020
    name: hbase-rpc
```

**Pros:**
- ✅ Auto-restart on failure
- ✅ Predictable DNS names
- ✅ Integrated health checks

**Cons:**
- ⚠️ Requires Kubernetes

### 4.2 Model B: Docker Compose (Development)

```yaml
version: '3.8'
services:
  hbase-compat-1:
    image: fluss/hbase-compat:0.9
    hostname: hbase-compat-1
    environment:
      FLUSS_BOOTSTRAP_SERVERS: "fluss:35739"
      ZOOKEEPER_QUORUM: "zookeeper:2181"
      SERVER_BIND_ADDRESS: "hbase-compat-1"
      SERVER_BIND_PORT: "16020"
    ports:
      - "16020:16020"
    depends_on:
      - fluss
      - zookeeper

  hbase-compat-2:
    image: fluss/hbase-compat:0.9
    hostname: hbase-compat-2
    environment:
      FLUSS_BOOTSTRAP_SERVERS: "fluss:35739"
      ZOOKEEPER_QUORUM: "zookeeper:2181"
      SERVER_BIND_ADDRESS: "hbase-compat-2"
      SERVER_BIND_PORT: "16020"
    ports:
      - "16021:16020"
    depends_on:
      - fluss
      - zookeeper

  hbase-compat-3:
    image: fluss/hbase-compat:0.9
    hostname: hbase-compat-3
    environment:
      FLUSS_BOOTSTRAP_SERVERS: "fluss:35739"
      ZOOKEEPER_QUORUM: "zookeeper:2181"
      SERVER_BIND_ADDRESS: "hbase-compat-3"
      SERVER_BIND_PORT: "16020"
    ports:
      - "16022:16020"
    depends_on:
      - fluss
      - zookeeper
```

### 4.3 Model C: Bare Metal / VM Cluster

```bash
# Server 1 (host1.example.com)
java -jar fluss-hbase-compat.jar \
  -Dfluss.bootstrap.servers=fluss-cluster:35739 \
  -Dzookeeper.quorum=zk1:2181,zk2:2181,zk3:2181 \
  -Dserver.bind.address=host1.example.com \
  -Dserver.bind.port=16020

# Server 2 (host2.example.com)
java -jar fluss-hbase-compat.jar \
  -Dfluss.bootstrap.servers=fluss-cluster:35739 \
  -Dzookeeper.quorum=zk1:2181,zk2:2181,zk3:2181 \
  -Dserver.bind.address=host2.example.com \
  -Dserver.bind.port=16020

# Server 3 (host3.example.com)
java -jar fluss-hbase-compat.jar \
  -Dfluss.bootstrap.servers=fluss-cluster:35739 \
  -Dzookeeper.quorum=zk1:2181,zk2:2181,zk3:2181 \
  -Dserver.bind.address=host3.example.com \
  -Dserver.bind.port=16020
```

---

## 5. Implementation Plan

### Phase 1: Core Distributed Support (HIGH PRIORITY)

**Goal:** Enable multiple servers to run simultaneously

1. ✅ **Unique Server Identity**
   - Generate unique server names (hostname + port + timestamp)
   - Each server registers unique `/hbase/rs/<unique-name>` node
   
2. ✅ **Multi-Server Meta Registration**
   - Change meta registration logic to support multiple servers
   - All servers update `/hbase/meta-region-server` with their info
   - Use ZK versioning to handle concurrent updates
   
3. ✅ **Multi-Server Master Registration**
   - All servers attempt to create `/hbase/master` (ephemeral)
   - First one wins, others log warning and continue (non-critical)

4. ✅ **Configuration File Support**
   - Load config from `hbase-compat-server.conf` file
   - Fall back to system properties
   - Validate required parameters

### Phase 2: Health & Monitoring (MEDIUM PRIORITY)

**Goal:** Enable monitoring and graceful degradation

1. ✅ **HTTP Health Endpoint**
   - Add lightweight HTTP server (port 8080)
   - `GET /health` → `{status, uptime, connections, ...}`
   - `GET /ready` → Readiness probe for Kubernetes

2. ✅ **Graceful Shutdown**
   - Deregister from ZooKeeper first
   - Wait for in-flight requests (max 30s)
   - Close connections

3. ✅ **ZooKeeper Session Monitoring**
   - Watch for session expiration
   - Attempt reconnection with exponential backoff
   - Exit after N failed attempts

### Phase 3: Operational Tools (LOW PRIORITY)

**Goal:** Simplify operations

1. ⚠️ **Startup Scripts**
   - `start-cluster.sh` - Start N servers
   - `stop-cluster.sh` - Gracefully stop all
   - `rolling-restart.sh` - Zero-downtime restart

2. ⚠️ **Monitoring Dashboard**
   - Prometheus metrics exporter
   - Grafana dashboard template

3. ⚠️ **Admin CLI**
   - `hbase-compat-admin list-servers` - Show all servers
   - `hbase-compat-admin check-health` - Health check all servers

---

## 6. Testing Plan

### 6.1 Functional Tests

1. **Multi-Server Registration**
   - Start 3 servers
   - Verify all appear in `/hbase/rs/`
   - Verify clients can connect to all

2. **Load Distribution**
   - Run YCSB against cluster
   - Verify requests distributed across servers
   - Measure throughput improvement

3. **Failover**
   - Kill one server
   - Verify clients auto-reconnect
   - Verify operations continue

4. **Rolling Restart**
   - Restart servers one by one
   - Verify zero downtime
   - Verify no request failures

### 6.2 Performance Tests

| Metric | Single Node | 3-Node Cluster | Expected Improvement |
|--------|-------------|----------------|----------------------|
| **Throughput (ops/sec)** | 158 | 400+ | 2.5-3x |
| **P99 Latency (ms)** | 115 | <120 | Similar |
| **Concurrent Clients** | 16 | 48+ | 3x |
| **Failover Time (s)** | N/A | <30s | - |

### 6.3 Chaos Tests

1. **Network Partition**
   - Partition server from ZooKeeper
   - Verify server deregisters
   - Verify clients reconnect

2. **Fluss Cluster Failure**
   - Stop Fluss cluster
   - Verify errors returned to clients
   - Verify recovery when Fluss restarts

3. **Resource Exhaustion**
   - Send burst traffic
   - Verify graceful degradation
   - Verify no cascading failures

---

## 7. Migration Path

### 7.1 For Existing Single-Node Users

1. **Test distributed setup locally**
   ```bash
   # Start 3 servers on different ports
   ./start-distributed-cluster.sh --nodes 3
   ```

2. **Validate with existing workload**
   ```bash
   ycsb run hbase2 -P workloada \
     -p hbase.zookeeper.quorum=localhost:2181
   ```

3. **Deploy to production incrementally**
   - Week 1: Deploy 1 new server (2 total)
   - Week 2: Add 3rd server (3 total)
   - Week 3: Remove original server

### 7.2 Rollback Plan

- Single-node deployment still supported
- No breaking changes to existing configuration
- Can fallback by stopping extra servers

---

## 8. Future Enhancements (Post-V1)

1. **Request Affinity**
   - Route requests for same key to same server
   - Improves cache hit rate

2. **Dynamic Scaling**
   - Auto-scale based on load
   - Integration with K8s HPA

3. **Multi-Region Deployment**
   - Deploy clusters in multiple data centers
   - Geo-aware routing

4. **Advanced Load Balancing**
   - Weighted routing based on server capacity
   - Least-connections routing

5. **Connection Pooling**
   - Share Fluss connections across servers
   - Reduce connection overhead

---

## 9. Summary

### Required Changes (Minimal)

1. ✅ Change ZK registration to support unique server names
2. ✅ Change meta/master registration to support multiple servers
3. ✅ Add HTTP health endpoint
4. ✅ Add configuration file support
5. ✅ Add graceful shutdown logic

### No Changes Needed (Works as-is)

- ✅ HBaseCompatServer (stateless, already scalable)
- ✅ HBaseRequestRouter (stateless)
- ✅ Executors (stateless)
- ✅ VirtualRegionManager (read-only, no synchronization needed)
- ✅ Fluss Connection (per-instance is fine)

### Key Insights

1. **Architecture is already mostly stateless** - Minimal changes needed
2. **ZooKeeper handles service discovery** - No custom LB needed
3. **HBase clients handle load balancing** - Built-in functionality
4. **Servers are independent** - No inter-server communication

### Estimated Effort

- **Phase 1 (Core):** 2-3 days
- **Phase 2 (Health):** 1-2 days
- **Phase 3 (Tools):** 2-3 days
- **Testing & Documentation:** 2-3 days

**Total:** 7-11 days for full distributed support

---

**Next Steps:**
1. Review and approve design
2. Implement Phase 1 (core distributed support)
3. Test with YCSB benchmark
4. Measure performance improvements
5. Deploy to production

