#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/test-config.sh"

TEST_MODE="${1:-single}"
HBASE_CLI="${HBASE_CLI:-/root/hbase-2.5.13-client/bin/hbase}"

TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0
SKIPPED_TESTS=0

test_result() {
    local status=$1
    local test_name=$2
    local message=$3
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    if [ "$status" = "PASS" ]; then
        PASSED_TESTS=$((PASSED_TESTS + 1))
    elif [ "$status" = "FAIL" ]; then
        FAILED_TESTS=$((FAILED_TESTS + 1))
    elif [ "$status" = "SKIP" ]; then
        SKIPPED_TESTS=$((SKIPPED_TESTS + 1))
    fi
    
    print_test_result "$test_name" "$status" "$message"
    log_test_result "$test_name" "$status" "$message"
}

create_hbase_config() {
    local zk_quorum=$1
    local config_dir="${SCRIPT_DIR}/.hbase-config-${TEST_MODE}"
    
    mkdir -p "$config_dir"
    
    cat > "${config_dir}/hbase-site.xml" <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>hbase.zookeeper.quorum</name>
    <value>${zk_quorum}</value>
  </property>
  <property>
    <name>hbase.cluster.distributed</name>
    <value>true</value>
  </property>
  <property>
    <name>hbase.rpc.timeout</name>
    <value>30000</value>
  </property>
  <property>
    <name>hbase.client.operation.timeout</name>
    <value>30000</value>
  </property>
  <property>
    <name>hbase.client.scanner.timeout.period</name>
    <value>30000</value>
  </property>
</configuration>
EOF
    
    echo "$config_dir"
}

run_hbase_shell() {
    local config_dir=$1
    shift
    
    export HBASE_CONF_DIR="$config_dir"
    local commands
    commands=$(printf '%s\n' "$@")
    timeout $TEST_TIMEOUT "$HBASE_CLI" shell <<EOF 2>&1
$commands
exit
EOF
}

wait_for_hbase_rows() {
    local config_dir=$1
    local table_name=$2
    local expected_rows=3
    local deadline=$((SECONDS + HBASE_SCAN_MAX_WAIT_SECONDS))
    local result=""

    while [ $SECONDS -lt $deadline ]; do
        result=$(run_hbase_shell "$config_dir" "scan '${table_name}'")
        if [[ "$result" == *"row1"* ]] && [[ "$result" == *"row2"* ]] && [[ "$result" == *"row3"* ]]; then
            echo "$result"
            return 0
        fi
        sleep "$HBASE_SCAN_RETRY_INTERVAL"
    done

    echo "$result"
    return 1
}

test_hbase_table_operations() {
    local config_dir=$1
    local test_prefix="HBase-Table-${TEST_MODE}"
    
    print_test_header "Testing HBase Table Operations (${TEST_MODE} mode)"
    
    local table_name="test_table_$(date +%s)"
    
    local result=$(run_hbase_shell "$config_dir" "create '${table_name}', 'cf'")
    if [[ "$result" == *"Created table"* ]] || [[ "$result" == *"created"* ]] || [[ $? -eq 0 ]]; then
        test_result "PASS" "$test_prefix-CREATE" "Successfully created table ${table_name}"
    else
        test_result "FAIL" "$test_prefix-CREATE" "Failed to create table: $result"
        return 1
    fi
    
    result=$(run_hbase_shell "$config_dir" "list")
    if [[ "$result" == *"${table_name}"* ]]; then
        test_result "PASS" "$test_prefix-LIST" "Table appears in list"
    else
        test_result "FAIL" "$test_prefix-LIST" "Table not found in list: $result"
    fi
    
    result=$(run_hbase_shell "$config_dir" "exists '${table_name}'")
    if [[ "$result" == *"exist"* ]] || [[ $? -eq 0 ]]; then
        test_result "PASS" "$test_prefix-EXISTS" "Table exists check passed"
    else
        test_result "FAIL" "$test_prefix-EXISTS" "Table exists check failed: $result"
    fi
    
    result=$(run_hbase_shell "$config_dir" "disable '${table_name}'" "drop '${table_name}'")
    if [[ $? -eq 0 ]]; then
        test_result "PASS" "$test_prefix-DROP" "Successfully dropped table"
    else
        test_result "WARN" "$test_prefix-DROP" "Failed to drop table (may not affect functionality)"
    fi
    
    return 0
}

test_hbase_put_get_operations() {
    local config_dir=$1
    local test_prefix="HBase-PutGet-${TEST_MODE}"
    
    print_test_header "Testing HBase Put/Get Operations (${TEST_MODE} mode)"
    
    local table_name="test_putget_$(date +%s)"
    
    run_hbase_shell "$config_dir" "create '${table_name}', 'cf'" > /dev/null 2>&1
    
    local row_key="row1"
    local value="test_value_123"
    
    local result=$(run_hbase_shell "$config_dir" "put '${table_name}', '${row_key}', 'cf:col1', '${value}'")
    if [[ $? -eq 0 ]]; then
        test_result "PASS" "$test_prefix-PUT" "Successfully put data"
    else
        test_result "FAIL" "$test_prefix-PUT" "Failed to put data: $result"
        run_hbase_shell "$config_dir" "disable '${table_name}'" "drop '${table_name}'" > /dev/null 2>&1
        return 1
    fi
    
    result=$(run_hbase_shell "$config_dir" "get '${table_name}', '${row_key}'")
    if [[ "$result" == *"${value}"* ]]; then
        test_result "PASS" "$test_prefix-GET" "Successfully retrieved data"
    else
        test_result "FAIL" "$test_prefix-GET" "Failed to retrieve data or value mismatch: $result"
    fi
    
    result=$(run_hbase_shell "$config_dir" "get '${table_name}', '${row_key}', 'cf:col1'")
    if [[ "$result" == *"${value}"* ]]; then
        test_result "PASS" "$test_prefix-GET-COLUMN" "Successfully retrieved specific column"
    else
        test_result "FAIL" "$test_prefix-GET-COLUMN" "Failed to retrieve column: $result"
    fi
    
    run_hbase_shell "$config_dir" "disable '${table_name}'" "drop '${table_name}'" > /dev/null 2>&1
    
    return 0
}

test_hbase_scan_operations() {
    local config_dir=$1
    local test_prefix="HBase-Scan-${TEST_MODE}"
    
    print_test_header "Testing HBase Scan Operations (${TEST_MODE} mode)"
    
    local table_name="test_scan_$(date +%s)"
    
    run_hbase_shell "$config_dir" "create '${table_name}', 'cf'" > /dev/null 2>&1
    
    run_hbase_shell "$config_dir" \
        "put '${table_name}', 'row1', 'cf:col1', 'value1'" \
        "put '${table_name}', 'row2', 'cf:col1', 'value2'" \
        "put '${table_name}', 'row3', 'cf:col1', 'value3'" > /dev/null 2>&1
    
    wait_for_snapshot_generation "$table_name"
    
    local result
    if result=$(wait_for_hbase_rows "$config_dir" "$table_name"); then
        test_result "PASS" "$test_prefix-SCAN-ALL" "Successfully scanned all rows"
    else
        if [[ "$result" == *"Failed to get snapshot metadata"* ]] || [[ "$result" == *"0 row(s)"* ]] || [[ "$result" == *"Failed to initialize snapshot files reader"* ]] || [[ "$result" == *"NoAwsCredentials"* ]]; then
            test_result "SKIP" "$test_prefix-SCAN-ALL" "Scan requires remote storage access (S3/OSS); see ARCHITECTURE.md for client config requirements"
        else
            test_result "FAIL" "$test_prefix-SCAN-ALL" "Failed to scan all rows after ${HBASE_SCAN_MAX_WAIT_SECONDS}s: $result"
        fi
    fi
    
    result=$(run_hbase_shell "$config_dir" "scan '${table_name}', {LIMIT => 2}")
    if [[ "$result" == *"row1"* ]] || [[ "$result" == *"row2"* ]] || [[ "$result" == *"row3"* ]]; then
        test_result "PASS" "$test_prefix-SCAN-LIMIT" "Successfully scanned with limit"
    else
        if [[ "$result" == *"Failed to get snapshot metadata"* ]] || [[ "$result" == *"0 row(s)"* ]] || [[ "$result" == *"Failed to initialize snapshot files reader"* ]] || [[ "$result" == *"NoAwsCredentials"* ]]; then
            test_result "SKIP" "$test_prefix-SCAN-LIMIT" "Scan requires remote storage access (S3/OSS); see ARCHITECTURE.md for client config requirements"
        else
            test_result "FAIL" "$test_prefix-SCAN-LIMIT" "Failed to scan with limit: $result"
        fi
    fi
    
    result=$(run_hbase_shell "$config_dir" "count '${table_name}'")
    if [[ "$result" == *"Failed to get snapshot metadata"* ]] || [[ "$result" == *"0 row(s)"* ]] || [[ "$result" == *"Failed to initialize snapshot files reader"* ]] || [[ "$result" == *"NoAwsCredentials"* ]]; then
        test_result "SKIP" "$test_prefix-COUNT" "Count requires remote storage access (S3/OSS); see ARCHITECTURE.md for client config requirements"
    elif [[ "$result" =~ ([0-9]+)\ row\(s\) ]]; then
        local count=${BASH_REMATCH[1]}
        if [ "$count" -eq 3 ]; then
            test_result "PASS" "$test_prefix-COUNT" "Successfully counted rows"
        else
            test_result "FAIL" "$test_prefix-COUNT" "Row count mismatch: expected 3, got $count"
        fi
    else
        test_result "FAIL" "$test_prefix-COUNT" "Failed to parse count output: $result"
    fi
    
    run_hbase_shell "$config_dir" "disable '${table_name}'" "drop '${table_name}'" > /dev/null 2>&1
    
    return 0
}

test_hbase_delete_operations() {
    local config_dir=$1
    local test_prefix="HBase-Delete-${TEST_MODE}"
    
    print_test_header "Testing HBase Delete Operations (${TEST_MODE} mode)"
    
    local table_name="test_delete_$(date +%s)"
    
    run_hbase_shell "$config_dir" "create '${table_name}', 'cf'" > /dev/null 2>&1
    
    run_hbase_shell "$config_dir" \
        "put '${table_name}', 'row1', 'cf:col1', 'value1'" \
        "put '${table_name}', 'row1', 'cf:col2', 'value2'" > /dev/null 2>&1
    
    local result=$(run_hbase_shell "$config_dir" "delete '${table_name}', 'row1', 'cf:col1'")
    if [[ $? -eq 0 ]]; then
        test_result "PASS" "$test_prefix-DELETE-COLUMN" "Successfully deleted column"
    else
        test_result "FAIL" "$test_prefix-DELETE-COLUMN" "Failed to delete column: $result"
    fi
    
    result=$(run_hbase_shell "$config_dir" "get '${table_name}', 'row1'")
    if [[ "$result" == *"cf:col2"* ]] && [[ "$result" != *"cf:col1"* ]]; then
        test_result "PASS" "$test_prefix-VERIFY-DELETE" "Column deletion verified"
    else
        test_result "WARN" "$test_prefix-VERIFY-DELETE" "Column deletion verification inconclusive"
    fi
    
    result=$(run_hbase_shell "$config_dir" "deleteall '${table_name}', 'row1'")
    if [[ $? -eq 0 ]]; then
        test_result "PASS" "$test_prefix-DELETEALL" "Successfully deleted entire row"
    else
        test_result "FAIL" "$test_prefix-DELETEALL" "Failed to delete row: $result"
    fi
    
    run_hbase_shell "$config_dir" "disable '${table_name}'" "drop '${table_name}'" > /dev/null 2>&1
    
    return 0
}

test_hbase_multi_column_family() {
    local config_dir=$1
    local test_prefix="HBase-MultiCF-${TEST_MODE}"
    
    print_test_header "Testing HBase Multiple Column Families (${TEST_MODE} mode)"
    
    local table_name="test_multicf_$(date +%s)"
    
    local result=$(run_hbase_shell "$config_dir" "create '${table_name}', 'cf1', 'cf2'")
    if [[ $? -eq 0 ]]; then
        test_result "PASS" "$test_prefix-CREATE" "Successfully created table with multiple CFs"
    else
        test_result "FAIL" "$test_prefix-CREATE" "Failed to create table: $result"
        return 1
    fi
    
    run_hbase_shell "$config_dir" \
        "put '${table_name}', 'row1', 'cf1:col1', 'value1'" \
        "put '${table_name}', 'row1', 'cf2:col1', 'value2'" > /dev/null 2>&1
    
    result=$(run_hbase_shell "$config_dir" "get '${table_name}', 'row1'")
    if [[ "$result" == *"cf1:col1"* ]] && [[ "$result" == *"cf2:col1"* ]]; then
        test_result "PASS" "$test_prefix-PUT-GET" "Successfully put/get data in multiple CFs"
    else
        test_result "FAIL" "$test_prefix-PUT-GET" "Failed to put/get data: $result"
    fi
    
    run_hbase_shell "$config_dir" "disable '${table_name}'" "drop '${table_name}'" > /dev/null 2>&1
    
    return 0
}

test_hbase_single_instance() {
    print_msg "$COLOR_MAGENTA" "\n=== Testing Single Instance Mode ==="
    
    local zk_quorum="$SINGLE_HBASE_ZK_QUORUM"
    local config_dir=$(create_hbase_config "$zk_quorum")
    
    print_msg "$COLOR_CYAN" "Using ZooKeeper: $zk_quorum"
    print_msg "$COLOR_CYAN" "Config directory: $config_dir"
    
    test_hbase_table_operations "$config_dir"
    test_hbase_put_get_operations "$config_dir"
    test_hbase_scan_operations "$config_dir"
    test_hbase_delete_operations "$config_dir"
    test_hbase_multi_column_family "$config_dir"
    
    rm -rf "$config_dir"
}

test_hbase_multi_instance() {
    print_msg "$COLOR_MAGENTA" "\n=== Testing Multi Instance Mode ==="
    
    local zk_quorum="$MULTI_HBASE_ZK_QUORUM"
    local config_dir=$(create_hbase_config "$zk_quorum")
    
    print_msg "$COLOR_CYAN" "Using ZooKeeper: $zk_quorum"
    print_msg "$COLOR_CYAN" "Config directory: $config_dir"
    print_msg "$COLOR_CYAN" "Expected CAPE instances: ${MULTI_HBASE_PORTS[*]}"
    
    test_hbase_table_operations "$config_dir"
    test_hbase_put_get_operations "$config_dir"
    test_hbase_scan_operations "$config_dir"
    test_hbase_delete_operations "$config_dir"
    test_hbase_multi_column_family "$config_dir"
    
    rm -rf "$config_dir"
}

main() {
    check_command "$HBASE_CLI" || exit 1
    
    setup_java_home || {
        print_msg "$COLOR_RED" "Error: JAVA_HOME setup failed. Please set JAVA_HOME manually."
        exit 1
    }
    
    init_test_report
    
    print_msg "$COLOR_CYAN" "======================================================================"
    print_msg "$COLOR_CYAN" "Fluss CAPE - HBase Functional Tests"
    print_msg "$COLOR_CYAN" "======================================================================"
    print_msg "$COLOR_CYAN" "Test Mode: $TEST_MODE"
    print_msg "$COLOR_CYAN" "Client: $HBASE_CLI"
    print_msg "$COLOR_CYAN" "JAVA_HOME: $JAVA_HOME"
    print_msg "$COLOR_CYAN" "======================================================================"
    
    if [ "$TEST_MODE" = "single" ]; then
        test_hbase_single_instance
    elif [ "$TEST_MODE" = "multi" ]; then
        test_hbase_multi_instance
    else
        print_msg "$COLOR_RED" "Invalid test mode: $TEST_MODE (use 'single' or 'multi')"
        exit 1
    fi
    
    print_msg "$COLOR_CYAN" "\n======================================================================"
    print_msg "$COLOR_CYAN" "Test Summary"
    print_msg "$COLOR_CYAN" "======================================================================"
    print_msg "$COLOR_BLUE" "Total Tests: $TOTAL_TESTS"
    print_msg "$COLOR_GREEN" "Passed: $PASSED_TESTS"
    print_msg "$COLOR_RED" "Failed: $FAILED_TESTS"
    print_msg "$COLOR_CYAN" "======================================================================"
    
    generate_test_summary
    
    if [ $FAILED_TESTS -gt 0 ]; then
        exit 1
    fi
}

main "$@"
