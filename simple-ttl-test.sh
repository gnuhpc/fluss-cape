#!/bin/bash
set -e

echo "=== Simple TTL Test Suite ==="
echo ""

PASS=0
FAIL=0

test_result() {
    if [ $1 -eq 0 ]; then
        echo "✓ $2"
        ((PASS++))
    else
        echo "✗ $2"
        ((FAIL++))
    fi
}

echo "Test 1: EXPIRE returns 1 for existing key"
redis-cli -p 6379 SET ttl_test_1 "value" > /dev/null
RESULT=$(redis-cli -p 6379 EXPIRE ttl_test_1 60)
if [ "$RESULT" = "1" ]; then
    test_result 0 "EXPIRE existing key"
else
    test_result 1 "EXPIRE existing key (got: $RESULT)"
fi

echo ""
echo "Test 2: TTL returns remaining seconds"
TTL=$(redis-cli -p 6379 TTL ttl_test_1)
if [ "$TTL" -gt 55 ] && [ "$TTL" -le 60 ]; then
    test_result 0 "TTL check (${TTL}s)"
else
    test_result 1 "TTL check (got: ${TTL}s)"
fi

echo ""
echo "Test 3: PERSIST removes expiration"
redis-cli -p 6379 PERSIST ttl_test_1 > /dev/null
TTL_AFTER=$(redis-cli -p 6379 TTL ttl_test_1)
if [ "$TTL_AFTER" = "-1" ]; then
    test_result 0 "PERSIST removes TTL"
else
    test_result 1 "PERSIST failed (TTL: $TTL_AFTER)"
fi

echo ""
echo "Test 4: PEXPIRE sets millisecond expiration"
redis-cli -p 6379 SET ttl_test_2 "value" > /dev/null
RESULT=$(redis-cli -p 6379 PEXPIRE ttl_test_2 5000)
if [ "$RESULT" = "1" ]; then
    test_result 0 "PEXPIRE existing key"
else
    test_result 1 "PEXPIRE failed"
fi

echo ""
echo "Test 5: PTTL returns remaining milliseconds"
PTTL=$(redis-cli -p 6379 PTTL ttl_test_2)
if [ "$PTTL" -gt 4000 ] && [ "$PTTL" -le 5000 ]; then
    test_result 0 "PTTL check (${PTTL}ms)"
else
    test_result 1 "PTTL check (got: ${PTTL}ms)"
fi

echo ""
echo "Test 6: EXPIREAT with future timestamp"
FUTURE_TS=$(($(date +%s) + 30))
redis-cli -p 6379 SET ttl_test_3 "value" > /dev/null
RESULT=$(redis-cli -p 6379 EXPIREAT ttl_test_3 $FUTURE_TS)
TTL=$(redis-cli -p 6379 TTL ttl_test_3)
if [ "$RESULT" = "1" ] && [ "$TTL" -gt 25 ]; then
    test_result 0 "EXPIREAT future timestamp"
else
    test_result 1 "EXPIREAT failed"
fi

echo ""
echo "Test 7: Auto-expiration (2 second test)"
redis-cli -p 6379 SET ttl_test_4 "value" > /dev/null
redis-cli -p 6379 EXPIRE ttl_test_4 2 > /dev/null
sleep 3
EXPIRED=$(redis-cli -p 6379 GET ttl_test_4)
if [ -z "$EXPIRED" ]; then
    test_result 0 "Key auto-expires"
else
    test_result 1 "Key still exists after TTL"
fi

echo ""
echo "Test 8: TTL returns -2 for non-existent key"
TTL=$(redis-cli -p 6379 TTL nonexistent_key)
if [ "$TTL" = "-2" ]; then
    test_result 0 "TTL -2 for missing key"
else
    test_result 1 "TTL wrong for missing key (got: $TTL)"
fi

echo ""
echo "Test 9: TTL returns -1 for key without expiration"
redis-cli -p 6379 SET ttl_test_5 "value" > /dev/null
TTL=$(redis-cli -p 6379 TTL ttl_test_5)
if [ "$TTL" = "-1" ]; then
    test_result 0 "TTL -1 for no expiration"
else
    test_result 1 "TTL wrong (got: $TTL)"
fi

echo ""
echo "Test 10: EXPIRE on non-existent key returns 0"
RESULT=$(redis-cli -p 6379 EXPIRE nonexistent_key 60)
if [ "$RESULT" = "0" ]; then
    test_result 0 "EXPIRE non-existent returns 0"
else
    test_result 1 "EXPIRE non-existent failed"
fi

redis-cli -p 6379 DEL ttl_test_1 ttl_test_2 ttl_test_3 ttl_test_4 ttl_test_5 > /dev/null 2>&1

echo ""
echo "========================================"
echo "Results: $PASS passed, $FAIL failed"
echo "========================================"

[ $FAIL -eq 0 ] && echo "✓ All tests passed!" || echo "✗ Some tests failed"
exit $FAIL
