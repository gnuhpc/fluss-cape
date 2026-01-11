#!/bin/bash

set -e

REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

TESTS_PASSED=0
TESTS_FAILED=0

redis_cmd() {
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" "$@"
}

assert_equals() {
    local expected="$1"
    local actual="$2"
    local test_name="$3"
    
    if [ "$actual" = "$expected" ]; then
        echo -e "${GREEN}✓${NC} $test_name"
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "${RED}✗${NC} $test_name"
        echo -e "  Expected: ${YELLOW}$expected${NC}"
        echo -e "  Got:      ${YELLOW}$actual${NC}"
        ((TESTS_FAILED++))
        return 1
    fi
}

assert_range() {
    local min="$1"
    local max="$2"
    local actual="$3"
    local test_name="$4"
    
    if [ "$actual" -ge "$min" ] && [ "$actual" -le "$max" ]; then
        echo -e "${GREEN}✓${NC} $test_name (value: $actual)"
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "${RED}✗${NC} $test_name"
        echo -e "  Expected: ${YELLOW}$min-$max${NC}"
        echo -e "  Got:      ${YELLOW}$actual${NC}"
        ((TESTS_FAILED++))
        return 1
    fi
}

echo "================================================"
echo "Redis TTL/Expiration Functional Tests"
echo "================================================"
echo "Target: $REDIS_HOST:$REDIS_PORT"
echo ""

redis_cmd PING > /dev/null 2>&1 || {
    echo -e "${RED}ERROR:${NC} Cannot connect to Redis server at $REDIS_HOST:$REDIS_PORT"
    exit 1
}

redis_cmd FLUSHALL > /dev/null 2>&1 || true

echo "Test 1: Basic EXPIRE + TTL"
echo "----------------------------"
redis_cmd SET key1 value1 > /dev/null
result=$(redis_cmd EXPIRE key1 10)
assert_equals "1" "$result" "EXPIRE should return 1 for existing key"

ttl=$(redis_cmd TTL key1)
assert_range 8 10 "$ttl" "TTL should return ~10 seconds"

value=$(redis_cmd GET key1)
assert_equals "value1" "$value" "Key should still be readable before expiration"
echo ""

echo "Test 2: Expiration actually deletes key"
echo "----------------------------------------"
redis_cmd SET key2 value2 > /dev/null
redis_cmd EXPIRE key2 2 > /dev/null
echo "Waiting 3 seconds for key to expire..."
sleep 3
result=$(redis_cmd GET key2)
assert_equals "" "$result" "Key should be nil after expiration"

ttl=$(redis_cmd TTL key2)
assert_equals "-2" "$ttl" "TTL should return -2 for expired/non-existent key"
echo ""

echo "Test 3: PERSIST removes expiration"
echo "-----------------------------------"
redis_cmd SET key3 value3 > /dev/null
redis_cmd EXPIRE key3 5 > /dev/null
result=$(redis_cmd PERSIST key3)
assert_equals "1" "$result" "PERSIST should return 1 for key with expiration"

ttl=$(redis_cmd TTL key3)
assert_equals "-1" "$ttl" "TTL should return -1 after PERSIST"

sleep 6
value=$(redis_cmd GET key3)
assert_equals "value3" "$value" "Key should still exist after PERSIST"
echo ""

echo "Test 4: EXPIREAT with past timestamp"
echo "-------------------------------------"
redis_cmd SET key4 value4 > /dev/null
result=$(redis_cmd EXPIREAT key4 1000000000)
assert_equals "1" "$result" "EXPIREAT should return 1"

value=$(redis_cmd GET key4)
assert_equals "" "$value" "Key should be deleted with past timestamp"
echo ""

echo "Test 5: PEXPIRE millisecond precision"
echo "--------------------------------------"
redis_cmd SET key5 value5 > /dev/null
redis_cmd PEXPIRE key5 500 > /dev/null
sleep 1
result=$(redis_cmd GET key5)
assert_equals "" "$result" "Key should expire after 500ms"
echo ""

echo "Test 6: PTTL returns milliseconds"
echo "----------------------------------"
redis_cmd SET key6 value6 > /dev/null
redis_cmd PEXPIRE key6 5000 > /dev/null
pttl=$(redis_cmd PTTL key6)
assert_range 4000 5000 "$pttl" "PTTL should return ~5000 milliseconds"
echo ""

echo "Test 7: EXPIRE on non-existent key"
echo "-----------------------------------"
result=$(redis_cmd EXPIRE nonexistent 10)
assert_equals "0" "$result" "EXPIRE should return 0 for non-existent key"

ttl=$(redis_cmd TTL nonexistent)
assert_equals "-2" "$ttl" "TTL should return -2 for non-existent key"
echo ""

echo "Test 8: EXPIRE with negative value deletes key"
echo "-----------------------------------------------"
redis_cmd SET key7 value7 > /dev/null
result=$(redis_cmd EXPIRE key7 -1)
assert_equals "1" "$result" "EXPIRE with negative value should return 1"

value=$(redis_cmd GET key7)
assert_equals "" "$value" "Key should be deleted immediately"
echo ""

echo "Test 9: TTL on different data types (Hash)"
echo "-------------------------------------------"
redis_cmd HSET myhash field1 value1 > /dev/null
redis_cmd EXPIRE myhash 10 > /dev/null
ttl=$(redis_cmd TTL myhash)
assert_range 8 10 "$ttl" "TTL works on hash"

value=$(redis_cmd HGET myhash field1)
assert_equals "value1" "$value" "Hash should be readable before expiration"

redis_cmd EXPIRE myhash 1 > /dev/null
sleep 2
value=$(redis_cmd HGET myhash field1)
assert_equals "" "$value" "Hash should be nil after expiration"
echo ""

echo "Test 10: TTL on Sets"
echo "---------------------"
redis_cmd SADD myset member1 member2 > /dev/null
redis_cmd EXPIRE myset 10 > /dev/null
ttl=$(redis_cmd TTL myset)
assert_range 8 10 "$ttl" "TTL works on set"

count=$(redis_cmd SCARD myset)
assert_equals "2" "$count" "Set should be readable before expiration"

redis_cmd EXPIRE myset 1 > /dev/null
sleep 2
count=$(redis_cmd SCARD myset)
assert_equals "0" "$count" "Set should be empty after expiration"
echo ""

echo "Test 11: TTL on Lists"
echo "---------------------"
redis_cmd RPUSH mylist a b c > /dev/null
redis_cmd EXPIRE mylist 10 > /dev/null
ttl=$(redis_cmd TTL mylist)
assert_range 8 10 "$ttl" "TTL works on list"

len=$(redis_cmd LLEN mylist)
assert_equals "3" "$len" "List should be readable before expiration"

redis_cmd EXPIRE mylist 1 > /dev/null
sleep 2
len=$(redis_cmd LLEN mylist)
assert_equals "0" "$len" "List should be empty after expiration"
echo ""

echo "Test 12: TTL on Sorted Sets"
echo "----------------------------"
redis_cmd ZADD myzset 100 alice 200 bob > /dev/null
redis_cmd EXPIRE myzset 10 > /dev/null
ttl=$(redis_cmd TTL myzset)
assert_range 8 10 "$ttl" "TTL works on sorted set"

count=$(redis_cmd ZCARD myzset)
assert_equals "2" "$count" "Sorted set should be readable before expiration"

redis_cmd EXPIRE myzset 1 > /dev/null
sleep 2
count=$(redis_cmd ZCARD myzset)
assert_equals "0" "$count" "Sorted set should be empty after expiration"
echo ""

echo "Test 13: INCR on expired key starts from 0"
echo "-------------------------------------------"
redis_cmd SET counter 5 > /dev/null
redis_cmd EXPIRE counter 1 > /dev/null
sleep 2
result=$(redis_cmd INCR counter)
assert_equals "1" "$result" "INCR on expired key should start from 0"
echo ""

echo "Test 14: EXISTS respects expiration"
echo "------------------------------------"
redis_cmd SET key14 value14 > /dev/null
redis_cmd EXPIRE key14 1 > /dev/null
result=$(redis_cmd EXISTS key14)
assert_equals "1" "$result" "EXISTS should return 1 before expiration"

sleep 2
result=$(redis_cmd EXISTS key14)
assert_equals "0" "$result" "EXISTS should return 0 after expiration"
echo ""

echo "Test 15: TYPE respects expiration"
echo "----------------------------------"
redis_cmd SET key15 value15 > /dev/null
redis_cmd EXPIRE key15 1 > /dev/null
result=$(redis_cmd TYPE key15)
assert_equals "string" "$result" "TYPE should return 'string' before expiration"

sleep 2
result=$(redis_cmd TYPE key15)
assert_equals "none" "$result" "TYPE should return 'none' after expiration"
echo ""

echo "================================================"
echo "Test Results"
echo "================================================"
echo -e "${GREEN}Passed:${NC} $TESTS_PASSED"
echo -e "${RED}Failed:${NC} $TESTS_FAILED"
echo ""

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed!${NC}"
    exit 1
fi
