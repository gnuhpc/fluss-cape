#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FLUSS_HOME="${FLUSS_HOME:-${SCRIPT_DIR}/fluss}"
FLUSS_BOOTSTRAP="${FLUSS_BOOTSTRAP:-localhost:35739}"
ZK_QUORUM="${ZK_QUORUM:-localhost:2181}"
TABLES="${HBASE_COMPAT_TABLES:-default.test_table,default.usertable}"
NUM_SERVERS="${1:-3}"
BASE_PORT="${BASE_PORT:-16020}"
HEALTH_CHECK_BASE_PORT="${HEALTH_CHECK_BASE_PORT:-8080}"

echo "========================================"
echo "Starting HBase Compatibility Cluster"
echo "========================================"
echo "Number of servers: ${NUM_SERVERS}"
echo "Base port: ${BASE_PORT}"
echo "Health check base port: ${HEALTH_CHECK_BASE_PORT}"
echo "Fluss cluster: ${FLUSS_BOOTSTRAP}"
echo "ZooKeeper: ${ZK_QUORUM}"
echo "Tables: ${TABLES}"
echo "========================================"

CLASSPATH="${FLUSS_HOME}/fluss-hbase-compat/target/fluss-hbase-compat-0.9-SNAPSHOT.jar"
CLASSPATH="${CLASSPATH}:${FLUSS_HOME}/fluss-client/target/fluss-client-0.9-SNAPSHOT.jar"
CLASSPATH="${CLASSPATH}:${FLUSS_HOME}/fluss-common/target/fluss-common-0.9-SNAPSHOT.jar"
CLASSPATH="${CLASSPATH}:${FLUSS_HOME}/fluss-rpc/target/fluss-rpc-0.9-SNAPSHOT.jar"
CLASSPATH="${CLASSPATH}:${FLUSS_HOME}/fluss-server/target/fluss-server-0.9-SNAPSHOT.jar"
CLASSPATH="${CLASSPATH}:${FLUSS_HOME}/fluss-metrics/fluss-metrics-jmx/target/fluss-metrics-jmx-0.9-SNAPSHOT.jar"
CLASSPATH="${CLASSPATH}:${SCRIPT_DIR}/slf4j-simple-1.7.36.jar"

for jar in ${SCRIPT_DIR}/hbase-2.5.13-client/lib/*.jar; do
    CLASSPATH="${CLASSPATH}:${jar}"
done

PIDS_FILE="/tmp/hbase-compat-cluster.pids"
> "${PIDS_FILE}"

for i in $(seq 1 ${NUM_SERVERS}); do
    PORT=$((BASE_PORT + i - 1))
    HEALTH_PORT=$((HEALTH_CHECK_BASE_PORT + i - 1))
    LOG_FILE="/tmp/hbase-compat-server-${i}.log"
    
    echo "Starting server ${i} on port ${PORT} (health check: ${HEALTH_PORT})..."
    
    nohup java \
        -Dfluss.bootstrap.servers="${FLUSS_BOOTSTRAP}" \
        -Dhbase.compat.bind.address="localhost" \
        -Dhbase.compat.bind.port="${PORT}" \
        -Dhbase.compat.tables="${TABLES}" \
        -Dhbase.zookeeper.quorum="${ZK_QUORUM}" \
        -Dserver.id="server-${i}" \
        -Dhealth.check.port="${HEALTH_PORT}" \
        -cp "${CLASSPATH}" \
        org.apache.fluss.hbase.server.HBaseCompatServerLauncher \
        > "${LOG_FILE}" 2>&1 &
    
    PID=$!
    echo ${PID} >> "${PIDS_FILE}"
    echo "  Server ${i} started (PID: ${PID}, log: ${LOG_FILE})"
    
    sleep 3
done

echo "========================================"
echo "Cluster started successfully!"
echo "========================================"
echo ""
echo "Server status:"
for i in $(seq 1 ${NUM_SERVERS}); do
    PORT=$((BASE_PORT + i - 1))
    HEALTH_PORT=$((HEALTH_CHECK_BASE_PORT + i - 1))
    echo "  Server ${i}: localhost:${PORT} (health: http://localhost:${HEALTH_PORT}/health)"
done

echo ""
echo "To check health status:"
echo "  curl http://localhost:8080/health"
echo "  curl http://localhost:8081/health"
echo "  curl http://localhost:8082/health"
echo ""
echo "To check ZooKeeper registration:"
echo "  echo 'ls /hbase/rs' | zkCli.sh -server ${ZK_QUORUM}"
echo ""
echo "To stop the cluster:"
echo "  ./stop-distributed-cluster.sh"
echo ""
echo "PIDs saved to: ${PIDS_FILE}"
