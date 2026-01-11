#!/bin/bash

echo "Phase 2.3 TTL Test Results"
echo "=========================="
echo ""

PASS=0
FAIL=0

run_test() {
    local name="$1"
    local command="$2"
    local expected="$3"
    
    result=$(eval "$command" 2>&1)
    if [ "$result" = "$expected" ]; then
        echo "✓ $name"
        ((PASS++))
    else
        echo "✗ $name (expected: $expected, got: $result)"
        ((FAIL++))
    fi
}

redis-cli -p 6379 DEL test1 test2 test3 test4 test5 > /dev/null 2>&1

echo "String + TTL Tests:"
redis-cli -p 6379 SET test1 val1 > /dev/null
run_test "EXPIRE returns 1" "redis-cli -p 6379 EXPIRE test1 60" "1"
run_test "TTL returns positive" "redis-cli -p 6379 TTL test1 | awk '{print (\$1 > 50 && \$1 <= 60) ? \"1\" : \"0\"}'" "1"
run_test "PERSIST returns 1" "redis-cli -p 6379 PERSIST test1" "1"
run_test "TTL after PERSIST is -1" "redis-cli -p 6379 TTL test1" "-1"

echo ""
echo "PEXPIRE + PTTL Tests:"
redis-cli -p 6379 SET test2 val2 > /dev/null
run_test "PEXPIRE returns 1" "redis-cli -p 6379 PEXPIRE test2 5000" "1"
run_test "PTTL returns positive ms" "redis-cli -p 6379 PTTL test2 | awk '{print (\$1 > 4000 && \$1 <= 5000) ? \"1\" : \"0\"}'" "1"

echo ""
echo "EXPIREAT Tests:"
FUTURE=$(($(date +%s) + 30))
redis-cli -p 6379 SET test3 val3 > /dev/null
run_test "EXPIREAT returns 1" "redis-cli -p 6379 EXPIREAT test3 $FUTURE" "1"
run_test "TTL after EXPIREAT" "redis-cli -p 6379 TTL test3 | awk '{print (\$1 > 25 && \$1 <= 30) ? \"1\" : \"0\"}'" "1"

echo ""
echo "Edge Case Tests:"
run_test "TTL on non-existent key" "redis-cli -p 6379 TTL nonexistent" "-2"
redis-cli -p 6379 SET test4 val4 > /dev/null
run_test "TTL without expiration" "redis-cli -p 6379 TTL test4" "-1"
run_test "EXPIRE non-existent returns 0" "redis-cli -p 6379 EXPIRE nonexistent 60" "0"

echo ""
echo "Expiration Test (3 second wait):"
redis-cli -p 6379 SET test5 val5 > /dev/null
redis-cli -p 6379 EXPIRE test5 2 > /dev/null
sleep 3
run_test "Key expires automatically" "redis-cli -p 6379 GET test5" ""

redis-cli -p 6379 DEL test1 test2 test3 test4 test5 > /dev/null 2>&1

echo ""
echo "=========================="
echo "Total: $((PASS + FAIL)) tests"
echo "Passed: $PASS"
echo "Failed: $FAIL"
echo "=========================="

[ $FAIL -eq 0 ] && echo "✓ ALL TESTS PASSED" || echo "✗ SOME TESTS FAILED"
exit $FAIL
