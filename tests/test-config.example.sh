# Example Test Configuration
# Copy this file to test-config.sh and customize as needed

# ==============================================================================
# Client Paths
# ==============================================================================
# Path to Valkey CLI (or Redis CLI)
export VALKEY_CLI="/root/valkey/src/valkey-cli"

# Path to HBase client binary
export HBASE_CLI="/root/hbase-2.5.13-client/bin/hbase"

# ==============================================================================
# Single Instance Configuration
# ==============================================================================
# Use these settings when testing a single CAPE instance

# Redis/Valkey endpoint
SINGLE_REDIS_HOST="localhost"
SINGLE_REDIS_PORT="6379"

# HBase endpoint (via ZooKeeper)
SINGLE_HBASE_ZK_QUORUM="localhost:2181"
SINGLE_HBASE_PORT="16020"

# ==============================================================================
# Multi-Instance Configuration
# ==============================================================================
# Use these settings when testing multiple CAPE instances

# Redis/Valkey Load Balancer
MULTI_REDIS_HOST="localhost"
MULTI_REDIS_LB_PORT="6379"  # HAProxy frontend port

# Redis/Valkey Backend Instances (for direct testing)
MULTI_REDIS_BACKEND_PORTS=("6390" "6391")

# HBase ZooKeeper Ensemble (for service discovery)
MULTI_HBASE_ZK_QUORUM="localhost:2181,localhost:2182,localhost:2183"

# HBase RegionServer Ports (each CAPE instance)
MULTI_HBASE_PORTS=("16020" "16021" "16022")

# ==============================================================================
# Test Parameters
# ==============================================================================
# Timeout for individual test commands (seconds)
TEST_TIMEOUT=30

# Number of iterations for load tests (if applicable)
TEST_ITERATIONS=10

# Test data size for bulk operations
TEST_DATA_SIZE=1000

# Verbosity: 0=quiet, 1=normal, 2=verbose
TEST_VERBOSITY=1

# ==============================================================================
# Notes
# ==============================================================================
# 
# Single Instance Deployment:
# - Simplest setup for development and testing
# - Direct connection to CAPE instance
# - Ports: 6379 (Redis), 16020 (HBase)
#
# Multi-Instance Deployment:
# - Production-like setup with load balancing
# - Redis: HAProxy load balancer on port 6379, backends on 6390, 6391, etc.
# - HBase: Service discovery via ZooKeeper, direct connections to RegionServers
# - Requires docker-compose-cape-multi-instance.yml
#
# Customization:
# - Change ports if your deployment uses different ports
# - Adjust timeout if you have slow network or large datasets
# - Set TEST_VERBOSITY=2 for detailed debugging output
#
