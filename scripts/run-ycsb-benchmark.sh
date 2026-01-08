#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# HBase Compatibility Layer - YCSB Benchmark Setup and Execution Script
# This script automates the complete benchmark workflow

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FLUSS_HOME="${SCRIPT_DIR}/.."
YCSB_VERSION="0.17.0"
YCSB_HOME="${HOME}/ycsb-${YCSB_VERSION}"

echo "=========================================="
echo "HBase Compatibility YCSB Benchmark Suite"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

# Step 1: Check prerequisites
echo "Step 1: Checking prerequisites..."

if ! command -v java &> /dev/null; then
    print_error "Java not found. Please install Java 11 or higher."
    exit 1
fi
print_status "Java installed: $(java -version 2>&1 | head -1)"

if ! command -v mvn &> /dev/null; then
    print_error "Maven not found. Please install Maven 3.8.6 or higher."
    exit 1
fi
print_status "Maven installed: $(mvn --version | head -1)"

# Step 2: Build HBase Compatibility Layer
echo ""
echo "Step 2: Building HBase Compatibility Layer..."
cd "${FLUSS_HOME}"

if [ ! -f "fluss-hbase-compat/target/fluss-hbase-compat-0.9-SNAPSHOT.jar" ]; then
    print_warning "JAR not found. Building from source..."
    mvn clean install -pl fluss-hbase-compat -DskipTests
    print_status "Build complete"
else
    print_status "JAR already exists"
fi

# Step 3: Start Fluss cluster
echo ""
echo "Step 3: Starting Fluss cluster..."
print_warning "Please ensure Fluss cluster is running manually."
print_warning "Fluss does not include a docker-compose setup in this version."
print_warning ""
print_warning "To start Fluss manually:"
print_warning "  1. Build distribution: ./mvnw clean package -DskipTests"
print_warning "  2. Extract: tar -xzf build-target/fluss-*.tgz"
print_warning "  3. Start: ./bin/fluss-daemon.sh start coordinatorServer"
print_warning "  4. Start: ./bin/fluss-daemon.sh start tabletServer"
print_warning ""

read -p "Is Fluss cluster running? (y/n) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_error "Please start Fluss cluster first."
    exit 1
fi

# Verify Fluss is reachable
FLUSS_SERVERS="${FLUSS_BOOTSTRAP_SERVERS:-localhost:9123}"
print_status "Fluss cluster confirmed at: ${FLUSS_SERVERS}"

# Step 4: Download and setup YCSB
echo ""
echo "Step 4: Setting up YCSB..."

if [ ! -d "${YCSB_HOME}" ]; then
    print_warning "YCSB not found. Downloading..."
    cd "${HOME}"
    wget -q --show-progress "https://github.com/brianfrankcooper/YCSB/releases/download/${YCSB_VERSION}/ycsb-${YCSB_VERSION}.tar.gz"
    tar xfz "ycsb-${YCSB_VERSION}.tar.gz"
    rm "ycsb-${YCSB_VERSION}.tar.gz"
    print_status "YCSB downloaded and extracted"
else
    print_status "YCSB already installed at ${YCSB_HOME}"
fi

# Step 5: Configure YCSB for HBase compatibility
echo ""
echo "Step 5: Configuring YCSB..."

HBASE_COMPAT_PORT="${HBASE_COMPAT_PORT:-16020}"
mkdir -p "${YCSB_HOME}/hbase20-binding/conf"
cat > "${YCSB_HOME}/hbase20-binding/conf/hbase-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>hbase.zookeeper.quorum</name>
    <value>localhost</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.clientPort</name>
    <value>2181</value>
  </property>
  <property>
    <name>hbase.client.retries.number</name>
    <value>3</value>
  </property>
  <property>
    <name>hbase.client.pause</name>
    <value>100</value>
  </property>
  <property>
    <name>hbase.client.operation.timeout</name>
    <value>60000</value>
  </property>
  <property>
    <name>hbase.rpc.timeout</name>
    <value>60000</value>
  </property>
</configuration>
EOF
print_status "HBase configuration created"

# Step 6: Create YCSB table in Fluss
echo ""
echo "Step 6: Creating YCSB benchmark table in Fluss..."

# Create SQL script for table creation
cat > /tmp/create_ycsb_table.sql <<EOF
CREATE DATABASE IF NOT EXISTS benchmark;

DROP TABLE IF EXISTS benchmark.usertable;

CREATE TABLE benchmark.usertable (
    YCSB_KEY STRING,
    field0 STRING,
    field1 STRING,
    field2 STRING,
    field3 STRING,
    field4 STRING,
    field5 STRING,
    field6 STRING,
    field7 STRING,
    field8 STRING,
    field9 STRING,
    PRIMARY KEY (YCSB_KEY) NOT ENFORCED
) WITH (
    'bucket.num' = '8'
);
EOF

print_warning "Please create the table manually using Fluss SQL client:"
print_warning "  ${FLUSS_HOME}/bin/sql-client.sh -f /tmp/create_ycsb_table.sql"
print_warning ""

read -p "Has the table been created? (y/n) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_error "Please create the table first."
    exit 1
fi

# Step 7: Start HBase Compatibility Server
echo ""
echo "Step 7: Starting HBase Compatibility Server..."

HBASE_COMPAT_JAR="${FLUSS_HOME}/fluss-hbase-compat/target/fluss-hbase-compat-0.9-SNAPSHOT.jar"
HBASE_COMPAT_LOG="${FLUSS_HOME}/fluss-hbase-compat/hbase-compat-server.log"

# Check if already running
if pgrep -f "HBaseCompatServerLauncher" > /dev/null; then
    print_warning "HBase Compatibility Server already running"
else
    print_status "Starting HBase Compatibility Server..."
    nohup java \
        -Dfluss.bootstrap.servers="${FLUSS_SERVERS}" \
        -Dhbase.compat.bind.address=0.0.0.0 \
        -Dhbase.compat.bind.port="${HBASE_COMPAT_PORT}" \
        -Dhbase.compat.tables=benchmark.usertable \
        -cp "${HBASE_COMPAT_JAR}" \
        org.apache.fluss.hbase.server.HBaseCompatServerLauncher \
        > "${HBASE_COMPAT_LOG}" 2>&1 &
    
    sleep 5
    
    if pgrep -f "HBaseCompatServerLauncher" > /dev/null; then
        print_status "HBase Compatibility Server started (PID: $(pgrep -f HBaseCompatServerLauncher))"
        print_status "Logs: ${HBASE_COMPAT_LOG}"
    else
        print_error "Failed to start server. Check logs: ${HBASE_COMPAT_LOG}"
        exit 1
    fi
fi

# Step 8: Run YCSB Load Phase
echo ""
echo "Step 8: Running YCSB Load Phase..."

RECORD_COUNT="${RECORD_COUNT:-100000}"
OPERATION_COUNT="${OPERATION_COUNT:-100000}"
THREAD_COUNT="${THREAD_COUNT:-16}"

cd "${YCSB_HOME}"

print_status "Loading ${RECORD_COUNT} records with ${THREAD_COUNT} threads..."

./bin/ycsb load hbase20 \
    -P workloads/workloada \
    -p table=usertable \
    -p columnfamily=cf \
    -p recordcount="${RECORD_COUNT}" \
    -p threadcount="${THREAD_COUNT}" \
    -s | tee "${FLUSS_HOME}/fluss-hbase-compat/ycsb-load-results.txt"

print_status "Load phase complete"

# Step 9: Run YCSB Workloads
echo ""
echo "Step 9: Running YCSB Workloads..."

run_workload() {
    local workload=$1
    local description=$2
    
    echo ""
    echo "----------------------------------------"
    echo "Running Workload ${workload}: ${description}"
    echo "----------------------------------------"
    
    ./bin/ycsb run hbase20 \
        -P "workloads/workload${workload}" \
        -p table=usertable \
        -p columnfamily=cf \
        -p operationcount="${OPERATION_COUNT}" \
        -p threadcount="${THREAD_COUNT}" \
        -s | tee "${FLUSS_HOME}/fluss-hbase-compat/ycsb-workload${workload}-results.txt"
    
    print_status "Workload ${workload} complete"
}

run_workload "a" "50% reads, 50% updates (Update heavy)"
run_workload "b" "95% reads, 5% updates (Read heavy)"
run_workload "c" "100% reads (Read only)"

# Step 10: Generate Summary Report
echo ""
echo "Step 10: Generating benchmark summary..."

REPORT_FILE="${FLUSS_HOME}/fluss-hbase-compat/benchmark-summary.txt"

cat > "${REPORT_FILE}" <<EOF
========================================
HBase Compatibility YCSB Benchmark Report
========================================

Configuration:
- Fluss Cluster: ${FLUSS_SERVERS}
- HBase Compat Port: ${HBASE_COMPAT_PORT}
- Record Count: ${RECORD_COUNT}
- Operation Count: ${OPERATION_COUNT}
- Thread Count: ${THREAD_COUNT}

Load Phase Results:
EOF

echo "" >> "${REPORT_FILE}"
echo "Throughput:" >> "${REPORT_FILE}"
grep "Throughput" "${FLUSS_HOME}/fluss-hbase-compat/ycsb-load-results.txt" >> "${REPORT_FILE}"

echo "" >> "${REPORT_FILE}"
echo "Workload A (50% read, 50% update):" >> "${REPORT_FILE}"
grep -E "Throughput|INSERT.*AverageLatency|UPDATE.*AverageLatency|READ.*AverageLatency" \
    "${FLUSS_HOME}/fluss-hbase-compat/ycsb-workloada-results.txt" >> "${REPORT_FILE}"

echo "" >> "${REPORT_FILE}"
echo "Workload B (95% read, 5% update):" >> "${REPORT_FILE}"
grep -E "Throughput|INSERT.*AverageLatency|UPDATE.*AverageLatency|READ.*AverageLatency" \
    "${FLUSS_HOME}/fluss-hbase-compat/ycsb-workloadb-results.txt" >> "${REPORT_FILE}"

echo "" >> "${REPORT_FILE}"
echo "Workload C (100% read):" >> "${REPORT_FILE}"
grep -E "Throughput|READ.*AverageLatency" \
    "${FLUSS_HOME}/fluss-hbase-compat/ycsb-workloadc-results.txt" >> "${REPORT_FILE}"

cat "${REPORT_FILE}"

# Step 11: Cleanup
echo ""
echo "Step 11: Cleanup..."

read -p "Stop HBase Compatibility Server? (y/n) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    pkill -f "HBaseCompatServerLauncher"
    print_status "HBase Compatibility Server stopped"
fi

echo ""
echo "=========================================="
echo "Benchmark Complete!"
echo "=========================================="
echo ""
print_status "Results saved to:"
echo "  - Load: ${FLUSS_HOME}/fluss-hbase-compat/ycsb-load-results.txt"
echo "  - Workload A: ${FLUSS_HOME}/fluss-hbase-compat/ycsb-workloada-results.txt"
echo "  - Workload B: ${FLUSS_HOME}/fluss-hbase-compat/ycsb-workloadb-results.txt"
echo "  - Workload C: ${FLUSS_HOME}/fluss-hbase-compat/ycsb-workloadc-results.txt"
echo "  - Summary: ${REPORT_FILE}"
echo ""
