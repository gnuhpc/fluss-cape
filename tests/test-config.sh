#!/bin/bash
# Test Configuration File for Fluss CAPE
# This file contains configurations for single and multi-instance testing

# ==============================================================================
# Client Paths Configuration
# ==============================================================================
VALKEY_CLI="${VALKEY_CLI:-/root/valkey/src/valkey-cli}"
HBASE_CLI="${HBASE_CLI:-/root/hbase-2.5.13-client/bin/hbase}"

# ==============================================================================
# Single Instance Configuration
# ==============================================================================
SINGLE_INSTANCE_MODE="single"
SINGLE_REDIS_HOST="localhost"
SINGLE_REDIS_PORT="6379"
SINGLE_HBASE_ZK_QUORUM="192.168.50.101:2181,192.168.50.102:2181,192.168.50.103:2181"
SINGLE_HBASE_PORT="16020"

# ==============================================================================
# Multi Instance Configuration
# ==============================================================================
MULTI_INSTANCE_MODE="multi"

# Redis/Valkey multi-instance configuration (load balanced via HAProxy)
MULTI_REDIS_HOST="localhost"
MULTI_REDIS_LB_PORT="6379"  # HAProxy load balancer port
MULTI_REDIS_BACKEND_PORTS=("6390" "6391")  # Backend CAPE instance ports

# HBase multi-instance configuration (direct connection to each instance)
MULTI_HBASE_ZK_QUORUM="192.168.50.101:2181,192.168.50.102:2181,192.168.50.103:2181"
MULTI_HBASE_PORTS=("16020" "16021" "16022")  # Each CAPE instance port

# ==============================================================================
# Test Configuration
# ==============================================================================
# Test timeout in seconds
TEST_TIMEOUT=30
HBASE_SCAN_MAX_WAIT_SECONDS=${HBASE_SCAN_MAX_WAIT_SECONDS:-60}
HBASE_SCAN_RETRY_INTERVAL=${HBASE_SCAN_RETRY_INTERVAL:-5}

# Fluss snapshot configuration
# HBase scan operations require KV snapshots to exist. Fluss generates snapshots
# at intervals configured by 'kv.snapshot.interval' (default: 5 minutes).
# For testing, this should match the Fluss cluster configuration (typically 3s in docker-compose).
# The buffer adds extra time to ensure snapshot generation completes.
FLUSS_SNAPSHOT_INTERVAL_SECONDS=${FLUSS_SNAPSHOT_INTERVAL_SECONDS:-3}
SNAPSHOT_WAIT_BUFFER_SECONDS=${SNAPSHOT_WAIT_BUFFER_SECONDS:-5}

# Number of test iterations
TEST_ITERATIONS=10

# Test data size
TEST_DATA_SIZE=1000

# Test verbosity (0=quiet, 1=normal, 2=verbose)
TEST_VERBOSITY="${TEST_VERBOSITY:-1}"

# Test report directory
TEST_REPORT_DIR="./test-reports"

# ==============================================================================
# Color Codes for Output
# ==============================================================================
export COLOR_RED='\033[0;31m'
export COLOR_GREEN='\033[0;32m'
export COLOR_YELLOW='\033[1;33m'
export COLOR_BLUE='\033[0;34m'
export COLOR_MAGENTA='\033[0;35m'
export COLOR_CYAN='\033[0;36m'
export COLOR_RESET='\033[0m'

# ==============================================================================
# Helper Functions
# ==============================================================================

# Print colored message
print_msg() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${COLOR_RESET}"
}

# Print test header
print_test_header() {
    local test_name=$1
    echo ""
    echo "========================================================================"
    print_msg "$COLOR_CYAN" "$test_name"
    echo "========================================================================"
}

# Print test result
print_test_result() {
    local test_name=$1
    local status=$2
    local message=$3
    
    if [ "$status" = "PASS" ]; then
        print_msg "$COLOR_GREEN" "[PASS] $test_name: $message"
    elif [ "$status" = "FAIL" ]; then
        print_msg "$COLOR_RED" "[FAIL] $test_name: $message"
    elif [ "$status" = "WARN" ]; then
        print_msg "$COLOR_YELLOW" "[WARN] $test_name: $message"
    elif [ "$status" = "SKIP" ]; then
        print_msg "$COLOR_MAGENTA" "[SKIP] $test_name: $message"
    else
        print_msg "$COLOR_BLUE" "[INFO] $test_name: $message"
    fi
}

# Check and setup JAVA_HOME
setup_java_home() {
    if [ -n "$JAVA_HOME" ] && [ -d "$JAVA_HOME" ]; then
        return 0
    fi
    
    local java_path=$(command -v java 2>/dev/null)
    if [ -z "$java_path" ]; then
        print_msg "$COLOR_RED" "Error: Java not found in PATH"
        return 1
    fi
    
    local java_home=$(readlink -f "$java_path" | sed 's|/bin/java||')
    if [ -d "$java_home" ]; then
        export JAVA_HOME="$java_home"
        print_msg "$COLOR_GREEN" "✓ Auto-detected JAVA_HOME: $JAVA_HOME"
        return 0
    fi
    
    print_msg "$COLOR_RED" "Error: Could not determine JAVA_HOME"
    return 1
}

# Check if command exists
check_command() {
    local cmd=$1
    if [ ! -f "$cmd" ] && ! command -v "$cmd" &> /dev/null; then
        print_msg "$COLOR_RED" "Error: Command not found: $cmd"
        return 1
    fi
    return 0
}

# Wait for service to be ready
wait_for_service() {
    local host=$1
    local port=$2
    local timeout=${3:-30}
    local elapsed=0
    
    print_msg "$COLOR_YELLOW" "Waiting for service at $host:$port (timeout: ${timeout}s)..."
    
    while [ $elapsed -lt $timeout ]; do
        if timeout 1 bash -c "echo > /dev/tcp/$host/$port" 2>/dev/null; then
            print_msg "$COLOR_GREEN" "Service is ready at $host:$port"
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    
    print_msg "$COLOR_RED" "Timeout waiting for service at $host:$port"
    return 1
}

# Initialize test report directory
init_test_report() {
    mkdir -p "$TEST_REPORT_DIR"
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    export TEST_REPORT_FILE="${TEST_REPORT_DIR}/test_report_${timestamp}.log"
    echo "Test Report - $(date)" > "$TEST_REPORT_FILE"
    echo "======================================================================" >> "$TEST_REPORT_FILE"
}

# Log test result to file
log_test_result() {
    local test_name=$1
    local status=$2
    local message=$3
    
    if [ -n "$TEST_REPORT_FILE" ]; then
        echo "[$status] $test_name: $message" >> "$TEST_REPORT_FILE"
    fi
}

# Generate test summary
generate_test_summary() {
    if [ -n "$TEST_REPORT_FILE" ]; then
        echo "" >> "$TEST_REPORT_FILE"
        echo "======================================================================" >> "$TEST_REPORT_FILE"
        echo "Test Summary:" >> "$TEST_REPORT_FILE"
        echo "  Total: $(grep -c '\[PASS\]\|\[FAIL\]' "$TEST_REPORT_FILE")" >> "$TEST_REPORT_FILE"
        echo "  Passed: $(grep -c '\[PASS\]' "$TEST_REPORT_FILE")" >> "$TEST_REPORT_FILE"
        echo "  Failed: $(grep -c '\[FAIL\]' "$TEST_REPORT_FILE")" >> "$TEST_REPORT_FILE"
        echo "======================================================================" >> "$TEST_REPORT_FILE"
        
        print_msg "$COLOR_CYAN" "\nTest report saved to: $TEST_REPORT_FILE"
    fi
}

wait_for_snapshot_generation() {
    local table_name=$1
    local wait_seconds=$((FLUSS_SNAPSHOT_INTERVAL_SECONDS + SNAPSHOT_WAIT_BUFFER_SECONDS))
    
    print_msg "$COLOR_YELLOW" "Waiting ${wait_seconds}s for Fluss snapshot generation (interval: ${FLUSS_SNAPSHOT_INTERVAL_SECONDS}s)..."
    sleep "$wait_seconds"
}

# Export functions for use in test scripts
export -f print_msg
export -f print_test_header
export -f print_test_result
export -f check_command
export -f setup_java_home
export -f wait_for_service
export -f wait_for_snapshot_generation
export -f log_test_result
