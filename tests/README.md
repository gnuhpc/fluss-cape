# Fluss CAPE Functional Tests

This directory contains comprehensive functional tests for Fluss CAPE, covering both Redis/Valkey and HBase protocol compatibility in single-instance and multi-instance deployment modes.

## 📋 Overview

The test suite validates:
- ✅ **Redis/Valkey Protocol**: String, Hash, List, Set, Sorted Set operations
- ✅ **HBase Protocol**: Table management, Put/Get, Scan, Delete operations
- ✅ **Single Instance**: Direct connection testing
- ✅ **Multi-Instance**: Load balancing and service discovery testing

## 🚀 Quick Start

### Prerequisites

1. **Valkey CLI** (or Redis CLI)
   ```bash
   export VALKEY_CLI=/root/valkey/src/valkey-cli
   ```

2. **HBase Client**
   ```bash
   export HBASE_CLI=/root/hbase-2.5.13-client/bin/hbase
   ```

3. **Running Fluss CAPE Instance(s)**
   - For single-instance tests: One CAPE instance running
   - For multi-instance tests: Multiple CAPE instances with load balancer

### Run All Tests

```bash
cd tests
./run-tests.sh
```

### Run Specific Tests

```bash
# Test single instance only
./run-tests.sh -m single

# Test multi-instance only
./run-tests.sh -m multi

# Test Redis protocol only
./run-tests.sh -t redis

# Test HBase protocol only
./run-tests.sh -t hbase

# Test single-instance Redis only
./run-tests.sh -m single -t redis

# Run with verbose output
./run-tests.sh -v
```

## 📁 Test Files

### Core Test Scripts

| File | Description |
|------|-------------|
| `run-tests.sh` | Main test runner - orchestrates all tests |
| `test-redis.sh` | Redis/Valkey protocol functional tests |
| `test-hbase.sh` | HBase protocol functional tests |
| `test-config.sh` | Shared configuration and helper functions |
| `generate-html-report.sh` | Generate HTML test reports |

### Configuration

Edit `test-config.sh` to customize:

```bash
# Client paths
VALKEY_CLI="/root/valkey/src/valkey-cli"
HBASE_CLI="/root/hbase-2.5.13-client/bin/hbase"

# Single instance configuration
SINGLE_REDIS_HOST="localhost"
SINGLE_REDIS_PORT="6379"
SINGLE_HBASE_ZK_QUORUM="localhost:2181"

# Multi-instance configuration
MULTI_REDIS_LB_PORT="6379"  # Load balancer port
MULTI_REDIS_BACKEND_PORTS=("6390" "6391")  # Backend ports
MULTI_HBASE_PORTS=("16020" "16021" "16022")

# Test parameters
TEST_TIMEOUT=30
TEST_ITERATIONS=10
```

## 🧪 Test Coverage

### Redis/Valkey Tests

#### String Operations
- `SET` / `GET` / `DEL`
- Key existence validation
- Value correctness verification

#### Hash Operations
- `HSET` / `HGET` / `HGETALL`
- Multiple field storage
- Field retrieval accuracy

#### List Operations
- `RPUSH` / `LLEN` / `LRANGE`
- List length verification
- Item ordering validation

#### Set Operations
- `SADD` / `SCARD` / `SISMEMBER`
- Member uniqueness
- Cardinality checks

#### Sorted Set Operations
- `ZADD` / `ZCARD` / `ZRANGE`
- Score-based ordering
- Range queries

### HBase Tests

#### Table Operations
- `CREATE` / `LIST` / `EXISTS` / `DROP`
- Table lifecycle management
- Service discovery validation

#### Put/Get Operations
- Single row insertion
- Column family operations
- Data retrieval accuracy

#### Scan Operations
- Full table scans
- Limited scans
- Row counting

> **⚠️ Scan not supported:** CAPE does not support HBase Scan. Scan tests are skipped/expected to fail; rely on Get/Put for verification.

#### Delete Operations
- Column deletion
- Row deletion (`deleteall`)
- Deletion verification

#### Multi Column Family
- Multiple CF creation
- Cross-CF data operations
- CF isolation validation

## 📊 Test Reports

### Console Output

Tests print colored output to the console:
- 🟢 **Green**: Passed tests
- 🔴 **Red**: Failed tests
- 🟡 **Yellow**: Warnings
- 🔵 **Blue**: Informational messages

### Log Files

Text-based test reports are saved to:
```
test-reports/test_report_YYYYMMDD_HHMMSS.log
```

### HTML Reports

Generate a visual HTML report:
```bash
./generate-html-report.sh test-reports/test_report_YYYYMMDD_HHMMSS.log
```

The HTML report includes:
- 📈 Test statistics and pass rate
- 📊 Visual progress bar
- 📋 Detailed test results with color coding
- 🎨 Responsive design for viewing on any device

## 🔧 Integration with CI/CD

### GitHub Actions Example

```yaml
name: Functional Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Start Fluss CAPE
        run: docker-compose up -d
      
      - name: Run Tests
        run: |
          cd tests
          ./run-tests.sh
      
      - name: Generate HTML Report
        if: always()
        run: |
          cd tests
          LATEST_REPORT=$(ls -t test-reports/*.log | head -1)
          ./generate-html-report.sh "$LATEST_REPORT"
      
      - name: Upload Test Report
        if: always()
        uses: actions/upload-artifact@v2
        with:
          name: test-report
          path: tests/test-reports/*.html
```

### Docker Integration

Run tests inside Docker:
```bash
docker run --rm \
  --network host \
  -v $(pwd):/workspace \
  -e VALKEY_CLI=/usr/local/bin/redis-cli \
  -e HBASE_CLI=/usr/local/hbase/bin/hbase \
  fluss-cape-test:latest \
  /workspace/tests/run-tests.sh
```

## 🐛 Troubleshooting

### Connection Issues

If tests fail to connect:

1. **Check service status**
   ```bash
   # For Redis
   telnet localhost 6379
   
   # For HBase (via ZooKeeper)
   echo stat | nc localhost 2181
   ```

2. **Verify CAPE is running**
   ```bash
   curl http://localhost:8080/health
   ```

3. **Check client paths**
   ```bash
   $VALKEY_CLI --version
   $HBASE_CLI version
   ```

### Test Failures

If specific tests fail:

1. **Run with verbose output**
   ```bash
   ./run-tests.sh -v
   ```

2. **Run individual test suites**
   ```bash
   # Test Redis only
   ./test-redis.sh single
   
   # Test HBase only
   ./test-hbase.sh single
   ```

3. **Check test logs**
   ```bash
   cat test-reports/test_report_*.log
   ```

### HBase Scan Unsupported

CAPE currently does **not** support HBase Scan. Scan-related tests are disabled; use Get/Put only.

### Multi-Instance Issues

For multi-instance test failures:

1. **Verify all instances are running**
   ```bash
   docker ps | grep fluss-cape
   ```

2. **Check load balancer configuration**
   ```bash
   cat ../haproxy.cfg
   ```

3. **Test backend instances directly**
   ```bash
   # Test each backend
   for port in 6390 6391; do
     $VALKEY_CLI -p $port PING
   done
   ```

## 📝 Writing Custom Tests

### Add a New Redis Test

1. Edit `test-redis.sh`
2. Add your test function:
   ```bash
   test_redis_my_feature() {
       local host=$1
       local port=$2
       local test_prefix="Redis-MyFeature-${host}:${port}"
       
       print_test_header "Testing My Feature"
       
       # Your test logic here
       local result=$(run_redis_command "$host" "$port" MYCOMMAND)
       
       if [[ "$result" == "expected_value" ]]; then
           test_result "PASS" "$test_prefix" "Test passed"
       else
           test_result "FAIL" "$test_prefix" "Test failed: $result"
           return 1
       fi
   }
   ```

3. Call it from `test_redis_single_instance()` or `test_redis_multi_instance()`

### Add a New HBase Test

Similar process for `test-hbase.sh`:
```bash
test_hbase_my_feature() {
    local config_dir=$1
    local test_prefix="HBase-MyFeature-${TEST_MODE}"
    
    print_test_header "Testing My HBase Feature"
    
    # Your test logic using run_hbase_shell
}
```

## 🤝 Contributing

When adding new tests:
1. Follow existing naming conventions
2. Use helper functions from `test-config.sh`
3. Log results with `test_result` function
4. Clean up resources after tests
5. Update this README with new test coverage

## 📄 License

Apache License 2.0 - See [LICENSE](../LICENSE) for details.

---

<div align="center">

**Automated Testing for Fluss CAPE**

🧪 [Run Tests](run-tests.sh) • 📊 [Generate Reports](generate-html-report.sh) • 🔧 [Configuration](test-config.sh)

</div>
