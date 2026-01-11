#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/test-config.sh"

show_usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Fluss CAPE Functional Test Suite

OPTIONS:
    -m, --mode MODE          Test mode: single, multi, or all (default: all)
    -t, --type TYPE          Test type: redis, hbase, or all (default: all)
    -v, --verbose            Enable verbose output
    -h, --help              Show this help message

MODES:
    single                   Test single instance deployment
    multi                    Test multi-instance deployment
    all                      Test both single and multi-instance

TYPES:
    redis                    Test Redis/Valkey protocol compatibility
    hbase                    Test HBase protocol compatibility
    all                      Test both Redis and HBase protocols

EXAMPLES:
    $(basename "$0")                          Run all tests
    $(basename "$0") -m single                Test single instance only
    $(basename "$0") -t redis                 Test Redis protocol only
    $(basename "$0") -m multi -t hbase        Test multi-instance HBase only
    $(basename "$0") -v                       Run all tests with verbose output

ENVIRONMENT VARIABLES:
    VALKEY_CLI              Path to valkey-cli (default: /root/valkey/src/valkey-cli)
    HBASE_CLI               Path to hbase client (default: /root/hbase-2.5.13-client/bin/hbase)
    TEST_VERBOSITY          Verbosity level: 0=quiet, 1=normal, 2=verbose

EOF
}

TEST_MODE="all"
TEST_TYPE="all"

while [[ $# -gt 0 ]]; do
    case $1 in
        -m|--mode)
            TEST_MODE="$2"
            shift 2
            ;;
        -t|--type)
            TEST_TYPE="$2"
            shift 2
            ;;
        -v|--verbose)
            export TEST_VERBOSITY=2
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_msg "$COLOR_RED" "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

if [[ ! "$TEST_MODE" =~ ^(single|multi|all)$ ]]; then
    print_msg "$COLOR_RED" "Invalid mode: $TEST_MODE (must be single, multi, or all)"
    exit 1
fi

if [[ ! "$TEST_TYPE" =~ ^(redis|hbase|all)$ ]]; then
    print_msg "$COLOR_RED" "Invalid type: $TEST_TYPE (must be redis, hbase, or all)"
    exit 1
fi

run_test_suite() {
    local mode=$1
    local type=$2
    local exit_code=0
    
    print_msg "$COLOR_MAGENTA" "\n========================================================================"
    print_msg "$COLOR_MAGENTA" "Starting Test Suite: Mode=$mode, Type=$type"
    print_msg "$COLOR_MAGENTA" "========================================================================"
    
    if [ "$type" = "redis" ] || [ "$type" = "all" ]; then
        print_msg "$COLOR_CYAN" "\n>>> Running Redis/Valkey Tests (mode: $mode)"
        if [ -f "${SCRIPT_DIR}/test-redis.sh" ]; then
            if "${SCRIPT_DIR}/test-redis.sh" "$mode"; then
                print_msg "$COLOR_GREEN" "Redis tests completed successfully"
            else
                print_msg "$COLOR_RED" "Redis tests failed"
                exit_code=1
            fi
        else
            print_msg "$COLOR_RED" "Redis test script not found: ${SCRIPT_DIR}/test-redis.sh"
            exit_code=1
        fi
    fi
    
    if [ "$type" = "hbase" ] || [ "$type" = "all" ]; then
        print_msg "$COLOR_CYAN" "\n>>> Running HBase Tests (mode: $mode)"
        if [ -f "${SCRIPT_DIR}/test-hbase.sh" ]; then
            if "${SCRIPT_DIR}/test-hbase.sh" "$mode"; then
                print_msg "$COLOR_GREEN" "HBase tests completed successfully"
            else
                print_msg "$COLOR_RED" "HBase tests failed"
                exit_code=1
            fi
        else
            print_msg "$COLOR_RED" "HBase test script not found: ${SCRIPT_DIR}/test-hbase.sh"
            exit_code=1
        fi
    fi
    
    return $exit_code
}

main() {
    local overall_exit_code=0
    
    init_test_report
    
    print_msg "$COLOR_CYAN" "======================================================================"
    print_msg "$COLOR_CYAN" "Fluss CAPE - Functional Test Suite"
    print_msg "$COLOR_CYAN" "======================================================================"
    print_msg "$COLOR_CYAN" "Test Mode: $TEST_MODE"
    print_msg "$COLOR_CYAN" "Test Type: $TEST_TYPE"
    print_msg "$COLOR_CYAN" "Verbosity: $TEST_VERBOSITY"
    print_msg "$COLOR_CYAN" "======================================================================"
    
    if [ "$TEST_MODE" = "all" ]; then
        for mode in single multi; do
            if ! run_test_suite "$mode" "$TEST_TYPE"; then
                overall_exit_code=1
            fi
        done
    else
        if ! run_test_suite "$TEST_MODE" "$TEST_TYPE"; then
            overall_exit_code=1
        fi
    fi
    
    print_msg "$COLOR_CYAN" "\n======================================================================"
    print_msg "$COLOR_CYAN" "Overall Test Results"
    print_msg "$COLOR_CYAN" "======================================================================"
    
    if [ $overall_exit_code -eq 0 ]; then
        print_msg "$COLOR_GREEN" "✓ All tests passed successfully!"
    else
        print_msg "$COLOR_RED" "✗ Some tests failed. Please check the logs above."
    fi
    
    print_msg "$COLOR_CYAN" "======================================================================"
    
    generate_test_summary
    
    exit $overall_exit_code
}

main "$@"
