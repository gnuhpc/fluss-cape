#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/test-config.sh"

TEST_MODE="${1:-single}"
VALKEY_CLI="${VALKEY_CLI:-/root/valkey/src/valkey-cli}"

TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

test_result() {
    local status=$1
    local test_name=$2
    local message=$3
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    if [ "$status" = "PASS" ]; then
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
    
    print_test_result "$test_name" "$status" "$message"
    log_test_result "$test_name" "$status" "$message"
}

run_redis_command() {
    local host=$1
    local port=$2
    shift 2
    timeout $TEST_TIMEOUT "$VALKEY_CLI" -h "$host" -p "$port" "$@" 2>&1
}

test_redis_string_operations() {
    local host=$1
    local port=$2
    local test_prefix="Redis-String-${host}:${port}"
    
    print_test_header "Testing Redis String Operations on ${host}:${port}"
    
    local key="test:string:$(date +%s)"
    local value="test_value_123"
    
    local result=$(run_redis_command "$host" "$port" SET "$key" "$value")
    if [[ "$result" == "OK" ]]; then
        test_result "PASS" "$test_prefix-SET" "Successfully set key"
    else
        test_result "FAIL" "$test_prefix-SET" "Failed to set key: $result"
        return 1
    fi
    
    result=$(run_redis_command "$host" "$port" GET "$key")
    if [[ "$result" == "$value" ]]; then
        test_result "PASS" "$test_prefix-GET" "Successfully retrieved value"
    else
        test_result "FAIL" "$test_prefix-GET" "Value mismatch: expected '$value', got '$result'"
        return 1
    fi
    
    result=$(run_redis_command "$host" "$port" DEL "$key")
    if [[ "$result" == "1" ]]; then
        test_result "PASS" "$test_prefix-DEL" "Successfully deleted key"
    else
        test_result "FAIL" "$test_prefix-DEL" "Failed to delete key: $result"
        return 1
    fi
    
    return 0
}

test_redis_hash_operations() {
    local host=$1
    local port=$2
    local test_prefix="Redis-Hash-${host}:${port}"
    
    print_test_header "Testing Redis Hash Operations on ${host}:${port}"
    
    local key="test:hash:$(date +%s)"
    
    local result=$(run_redis_command "$host" "$port" HSET "$key" field1 "value1" field2 "value2")
    if [[ "$result" == "2" ]]; then
        test_result "PASS" "$test_prefix-HSET" "Successfully set hash fields"
    else
        test_result "FAIL" "$test_prefix-HSET" "Failed to set hash fields: $result"
        return 1
    fi
    
    result=$(run_redis_command "$host" "$port" HGET "$key" field1)
    if [[ "$result" == "value1" ]]; then
        test_result "PASS" "$test_prefix-HGET" "Successfully retrieved hash field"
    else
        test_result "FAIL" "$test_prefix-HGET" "Value mismatch: expected 'value1', got '$result'"
        return 1
    fi
    
    result=$(run_redis_command "$host" "$port" HGETALL "$key")
    if [[ "$result" == *"field1"* ]] && [[ "$result" == *"value1"* ]]; then
        test_result "PASS" "$test_prefix-HGETALL" "Successfully retrieved all hash fields"
    else
        test_result "FAIL" "$test_prefix-HGETALL" "Failed to retrieve all fields: $result"
        return 1
    fi
    
    run_redis_command "$host" "$port" DEL "$key" > /dev/null
    
    return 0
}

test_redis_list_operations() {
    local host=$1
    local port=$2
    local test_prefix="Redis-List-${host}:${port}"
    
    print_test_header "Testing Redis List Operations on ${host}:${port}"
    
    local key="test:list:$(date +%s)"
    
    local result=$(run_redis_command "$host" "$port" RPUSH "$key" "item1" "item2" "item3")
    if [[ "$result" == "3" ]]; then
        test_result "PASS" "$test_prefix-RPUSH" "Successfully pushed items to list"
    else
        test_result "FAIL" "$test_prefix-RPUSH" "Failed to push items: $result"
        return 1
    fi
    
    result=$(run_redis_command "$host" "$port" LLEN "$key")
    if [[ "$result" == "3" ]]; then
        test_result "PASS" "$test_prefix-LLEN" "List length is correct"
    else
        test_result "FAIL" "$test_prefix-LLEN" "Length mismatch: expected 3, got $result"
        return 1
    fi
    
    result=$(run_redis_command "$host" "$port" LRANGE "$key" 0 -1)
    if [[ "$result" == *"item1"* ]] && [[ "$result" == *"item2"* ]] && [[ "$result" == *"item3"* ]]; then
        test_result "PASS" "$test_prefix-LRANGE" "Successfully retrieved list items"
    else
        test_result "FAIL" "$test_prefix-LRANGE" "Failed to retrieve items: $result"
        return 1
    fi
    
    run_redis_command "$host" "$port" DEL "$key" > /dev/null
    
    return 0
}

test_redis_set_operations() {
    local host=$1
    local port=$2
    local test_prefix="Redis-Set-${host}:${port}"
    
    print_test_header "Testing Redis Set Operations on ${host}:${port}"
    
    local key="test:set:$(date +%s)"
    
    local result=$(run_redis_command "$host" "$port" SADD "$key" "member1" "member2" "member3")
    if [[ "$result" == "3" ]]; then
        test_result "PASS" "$test_prefix-SADD" "Successfully added members to set"
    else
        test_result "FAIL" "$test_prefix-SADD" "Failed to add members: $result"
        return 1
    fi
    
    result=$(run_redis_command "$host" "$port" SCARD "$key")
    if [[ "$result" == "3" ]]; then
        test_result "PASS" "$test_prefix-SCARD" "Set cardinality is correct"
    else
        test_result "FAIL" "$test_prefix-SCARD" "Cardinality mismatch: expected 3, got $result"
        return 1
    fi
    
    result=$(run_redis_command "$host" "$port" SISMEMBER "$key" "member1")
    if [[ "$result" == "1" ]]; then
        test_result "PASS" "$test_prefix-SISMEMBER" "Member exists in set"
    else
        test_result "FAIL" "$test_prefix-SISMEMBER" "Member check failed: $result"
        return 1
    fi
    
    run_redis_command "$host" "$port" DEL "$key" > /dev/null
    
    return 0
}

test_redis_zset_operations() {
    local host=$1
    local port=$2
    local test_prefix="Redis-ZSet-${host}:${port}"
    
    print_test_header "Testing Redis Sorted Set Operations on ${host}:${port}"
    
    local key="test:zset:$(date +%s)"
    
    local result=$(run_redis_command "$host" "$port" ZADD "$key" 100 "member1" 200 "member2" 150 "member3")
    if [[ "$result" == "3" ]]; then
        test_result "PASS" "$test_prefix-ZADD" "Successfully added members to sorted set"
    else
        test_result "FAIL" "$test_prefix-ZADD" "Failed to add members: $result"
        return 1
    fi
    
    result=$(run_redis_command "$host" "$port" ZCARD "$key")
    if [[ "$result" == "3" ]]; then
        test_result "PASS" "$test_prefix-ZCARD" "Sorted set cardinality is correct"
    else
        test_result "FAIL" "$test_prefix-ZCARD" "Cardinality mismatch: expected 3, got $result"
        return 1
    fi
    
    result=$(run_redis_command "$host" "$port" ZRANGE "$key" 0 -1)
    if [[ "$result" == *"member1"* ]] && [[ "$result" == *"member2"* ]]; then
        test_result "PASS" "$test_prefix-ZRANGE" "Successfully retrieved sorted set members"
    else
        test_result "FAIL" "$test_prefix-ZRANGE" "Failed to retrieve members: $result"
        return 1
    fi
    
    run_redis_command "$host" "$port" DEL "$key" > /dev/null
    
    return 0
}

test_redis_single_instance() {
    print_msg "$COLOR_MAGENTA" "\n=== Testing Single Instance Mode ==="
    
    local host="$SINGLE_REDIS_HOST"
    local port="$SINGLE_REDIS_PORT"
    
    if ! wait_for_service "$host" "$port" 30; then
        print_msg "$COLOR_RED" "Cannot connect to Redis at ${host}:${port}"
        return 1
    fi
    
    test_redis_string_operations "$host" "$port"
    test_redis_hash_operations "$host" "$port"
    test_redis_list_operations "$host" "$port"
    test_redis_set_operations "$host" "$port"
    test_redis_zset_operations "$host" "$port"
}

test_redis_multi_instance() {
    print_msg "$COLOR_MAGENTA" "\n=== Testing Multi Instance Mode (Load Balanced) ==="
    
    local lb_host="$MULTI_REDIS_HOST"
    local lb_port="$MULTI_REDIS_LB_PORT"
    
    if ! wait_for_service "$lb_host" "$lb_port" 30; then
        print_msg "$COLOR_RED" "Cannot connect to Redis load balancer at ${lb_host}:${lb_port}"
        return 1
    fi
    
    print_msg "$COLOR_CYAN" "Testing via Load Balancer (${lb_host}:${lb_port})"
    test_redis_string_operations "$lb_host" "$lb_port"
    test_redis_hash_operations "$lb_host" "$lb_port"
    test_redis_list_operations "$lb_host" "$lb_port"
    test_redis_set_operations "$lb_host" "$lb_port"
    test_redis_zset_operations "$lb_host" "$lb_port"
    
    print_msg "$COLOR_CYAN" "\nTesting Individual Backend Instances"
    for port in "${MULTI_REDIS_BACKEND_PORTS[@]}"; do
        if wait_for_service "$lb_host" "$port" 5; then
            print_msg "$COLOR_YELLOW" "Testing backend instance at ${lb_host}:${port}"
            test_redis_string_operations "$lb_host" "$port"
        else
            print_msg "$COLOR_YELLOW" "Backend instance ${lb_host}:${port} not available, skipping"
        fi
    done
}

main() {
    check_command "$VALKEY_CLI" || exit 1
    
    init_test_report
    
    print_msg "$COLOR_CYAN" "======================================================================"
    print_msg "$COLOR_CYAN" "Fluss CAPE - Redis/Valkey Functional Tests"
    print_msg "$COLOR_CYAN" "======================================================================"
    print_msg "$COLOR_CYAN" "Test Mode: $TEST_MODE"
    print_msg "$COLOR_CYAN" "Client: $VALKEY_CLI"
    print_msg "$COLOR_CYAN" "======================================================================"
    
    if [ "$TEST_MODE" = "single" ]; then
        test_redis_single_instance
    elif [ "$TEST_MODE" = "multi" ]; then
        test_redis_multi_instance
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
