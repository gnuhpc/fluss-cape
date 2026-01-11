#!/bin/bash

# Phase 3.1 - String Batch Operations Test Suite
# Tests all 11 new commands: MGET, MSET, SETNX, SETEX, PSETEX, MSETNX, GETSET, APPEND, STRLEN, DECR, DECRBY

set -e

REDIS_CLI="redis-cli -p 6379"
PASSED=0
FAILED=0

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Phase 3.1 String Batch Operations Tests"
echo "=========================================="
echo ""

test_command() {
    local test_name="$1"
    local test_cmd="$2"
    
    echo -n "Testing: $test_name ... "
    if eval "$test_cmd"; then
        echo -e "${GREEN}✓ PASSED${NC}"
        PASSED=$((PASSED + 1))
    else
        echo -e "${RED}✗ FAILED${NC}"
        FAILED=$((FAILED + 1))
    fi
}

# ==========================
# Test 1: MGET - Batch get
# ==========================
echo -e "${YELLOW}=== Test Group 1: MGET (Batch Get) ===${NC}"

test_command "MGET - Basic batch retrieval" '
    $REDIS_CLI DEL mg1 mg2 mg3 >/dev/null 2>&1
    $REDIS_CLI SET mg1 val1 >/dev/null
    $REDIS_CLI SET mg2 val2 >/dev/null
    $REDIS_CLI SET mg3 val3 >/dev/null
    sleep 1
    result=$($REDIS_CLI MGET mg1 mg2 mg3)
    [[ "$result" == *"val1"* ]] && [[ "$result" == *"val2"* ]] && [[ "$result" == *"val3"* ]]
'

test_command "MGET - Mix existing and non-existing keys" '
    $REDIS_CLI DEL exist1 exist2 noexist >/dev/null 2>&1
    $REDIS_CLI SET exist1 v1 >/dev/null
    $REDIS_CLI SET exist2 v2 >/dev/null
    sleep 1
    result=$($REDIS_CLI MGET exist1 noexist exist2)
    [[ "$result" == *"v1"* ]] && [[ "$result" == *"v2"* ]]
'

echo ""

# ==========================
# Test 2: MSET - Batch set
# ==========================
echo -e "${YELLOW}=== Test Group 2: MSET (Batch Set) ===${NC}"

test_command "MSET - Basic batch set" '
    $REDIS_CLI DEL ms1 ms2 ms3 >/dev/null 2>&1
    $REDIS_CLI MSET ms1 val1 ms2 val2 ms3 val3
    sleep 1
    result=$($REDIS_CLI MGET ms1 ms2 ms3)
    [[ "$result" == *"val1"* ]] && [[ "$result" == *"val2"* ]] && [[ "$result" == *"val3"* ]]
'

test_command "MSET - Overwrite existing keys" '
    $REDIS_CLI SET over1 old1 >/dev/null
    $REDIS_CLI SET over2 old2 >/dev/null
    $REDIS_CLI MSET over1 new1 over2 new2
    sleep 1
    result=$($REDIS_CLI MGET over1 over2)
    [[ "$result" == *"new1"* ]] && [[ "$result" == *"new2"* ]]
'

test_command "MSET - Large batch (10 keys)" '
    $REDIS_CLI DEL k1 k2 k3 k4 k5 k6 k7 k8 k9 k10 >/dev/null 2>&1
    $REDIS_CLI MSET k1 v1 k2 v2 k3 v3 k4 v4 k5 v5 k6 v6 k7 v7 k8 v8 k9 v9 k10 v10
    sleep 2
    result=$($REDIS_CLI EXISTS k1 k2 k3 k4 k5 k6 k7 k8 k9 k10)
    [[ "$result" == "10" ]]
'

echo ""

# ==========================
# Test 3: SETNX - Set if not exists
# ==========================
echo -e "${YELLOW}=== Test Group 3: SETNX (Set If Not Exists) ===${NC}"

test_command "SETNX - Set non-existing key" '
    $REDIS_CLI DEL setnx1 >/dev/null 2>&1
    result=$($REDIS_CLI SETNX setnx1 value1)
    [[ "$result" == "1" ]]
'

test_command "SETNX - Fail on existing key" '
    $REDIS_CLI SET setnx2 existing >/dev/null
    sleep 1
    result=$($REDIS_CLI SETNX setnx2 newvalue)
    [[ "$result" == "0" ]]
'

test_command "SETNX - Verify value unchanged" '
    $REDIS_CLI SET setnx3 original >/dev/null
    sleep 1
    $REDIS_CLI SETNX setnx3 attempt >/dev/null
    sleep 1
    result=$($REDIS_CLI GET setnx3)
    [[ "$result" == "original" ]]
'

echo ""

# ==========================
# Test 4: SETEX - Set with TTL (seconds)
# ==========================
echo -e "${YELLOW}=== Test Group 4: SETEX (Set With TTL Seconds) ===${NC}"

test_command "SETEX - Set with 60 second TTL" '
    $REDIS_CLI DEL setex1 >/dev/null 2>&1
    $REDIS_CLI SETEX setex1 60 myvalue
    sleep 1
    result=$($REDIS_CLI GET setex1)
    ttl=$($REDIS_CLI TTL setex1)
    [[ "$result" == "myvalue" ]] && [[ $ttl -gt 50 ]] && [[ $ttl -le 60 ]]
'

test_command "SETEX - Overwrite existing key with TTL" '
    $REDIS_CLI SET setex2 old >/dev/null
    sleep 1
    $REDIS_CLI SETEX setex2 30 new
    sleep 1
    result=$($REDIS_CLI GET setex2)
    [[ "$result" == "new" ]]
'

echo ""

# ==========================
# Test 5: PSETEX - Set with TTL (milliseconds)
# ==========================
echo -e "${YELLOW}=== Test Group 5: PSETEX (Set With TTL Milliseconds) ===${NC}"

test_command "PSETEX - Set with 5000ms TTL" '
    $REDIS_CLI DEL psetex1 >/dev/null 2>&1
    $REDIS_CLI PSETEX psetex1 5000 myvalue
    sleep 1
    result=$($REDIS_CLI GET psetex1)
    pttl=$($REDIS_CLI PTTL psetex1)
    [[ "$result" == "myvalue" ]] && [[ $pttl -gt 3000 ]] && [[ $pttl -le 5000 ]]
'

echo ""

# ==========================
# Test 6: MSETNX - Batch SETNX (atomic)
# ==========================
echo -e "${YELLOW}=== Test Group 6: MSETNX (Batch Set If Not Exists - Atomic) ===${NC}"

test_command "MSETNX - All keys non-existing (success)" '
    $REDIS_CLI DEL msetnx1 msetnx2 msetnx3 >/dev/null 2>&1
    sleep 1
    result=$($REDIS_CLI MSETNX msetnx1 v1 msetnx2 v2 msetnx3 v3)
    [[ "$result" == "1" ]]
'

test_command "MSETNX - Verify all keys set" '
    sleep 1
    result=$($REDIS_CLI EXISTS msetnx1 msetnx2 msetnx3)
    [[ "$result" == "3" ]]
'

test_command "MSETNX - One key exists (atomic failure)" '
    $REDIS_CLI SET msetnx_existing value >/dev/null
    sleep 1
    result=$($REDIS_CLI MSETNX msetnx_new1 v1 msetnx_existing v2 msetnx_new2 v3)
    [[ "$result" == "0" ]]
'

test_command "MSETNX - Verify no new keys created on failure" '
    sleep 1
    exists_new1=$($REDIS_CLI EXISTS msetnx_new1)
    exists_new2=$($REDIS_CLI EXISTS msetnx_new2)
    [[ "$exists_new1" == "0" ]] && [[ "$exists_new2" == "0" ]]
'

echo ""

# ==========================
# Test 7: GETSET - Get old, set new (atomic)
# ==========================
echo -e "${YELLOW}=== Test Group 7: GETSET (Get Old Value, Set New - Atomic) ===${NC}"

test_command "GETSET - Get old value and set new" '
    $REDIS_CLI SET getset1 oldvalue >/dev/null
    sleep 1
    result=$($REDIS_CLI GETSET getset1 newvalue)
    [[ "$result" == "oldvalue" ]]
'

test_command "GETSET - Verify new value set" '
    sleep 1
    result=$($REDIS_CLI GET getset1)
    [[ "$result" == "newvalue" ]]
'

test_command "GETSET - Non-existing key returns nil" '
    $REDIS_CLI DEL getset_new >/dev/null 2>&1
    sleep 1
    result=$($REDIS_CLI GETSET getset_new firstvalue)
    [[ -z "$result" ]]
'

test_command "GETSET - Verify value set after nil return" '
    sleep 1
    result=$($REDIS_CLI GET getset_new)
    [[ "$result" == "firstvalue" ]]
'

echo ""

# ==========================
# Test 8: APPEND - Append to string
# ==========================
echo -e "${YELLOW}=== Test Group 8: APPEND (Append String) ===${NC}"

test_command "APPEND - Append to existing string" '
    $REDIS_CLI SET append1 "Hello" >/dev/null
    sleep 1
    length=$($REDIS_CLI APPEND append1 " World")
    [[ "$length" == "11" ]]
'

test_command "APPEND - Verify appended value" '
    sleep 1
    result=$($REDIS_CLI GET append1)
    [[ "$result" == "Hello World" ]]
'

test_command "APPEND - Append to non-existing key (creates key)" '
    $REDIS_CLI DEL append_new >/dev/null 2>&1
    sleep 1
    length=$($REDIS_CLI APPEND append_new "FirstValue")
    [[ "$length" == "10" ]]
'

test_command "APPEND - Multiple appends" '
    $REDIS_CLI SET append2 "A" >/dev/null
    sleep 1
    $REDIS_CLI APPEND append2 "B" >/dev/null
    $REDIS_CLI APPEND append2 "C" >/dev/null
    sleep 1
    result=$($REDIS_CLI GET append2)
    [[ "$result" == "ABC" ]]
'

echo ""

# ==========================
# Test 9: STRLEN - String length
# ==========================
echo -e "${YELLOW}=== Test Group 9: STRLEN (String Length) ===${NC}"

test_command "STRLEN - Get length of existing string" '
    $REDIS_CLI SET strlen1 "HelloWorld" >/dev/null
    sleep 1
    result=$($REDIS_CLI STRLEN strlen1)
    [[ "$result" == "10" ]]
'

test_command "STRLEN - Empty string length" '
    $REDIS_CLI SET strlen2 "" >/dev/null
    sleep 1
    result=$($REDIS_CLI STRLEN strlen2)
    [[ "$result" == "0" ]]
'

test_command "STRLEN - Non-existing key returns 0" '
    $REDIS_CLI DEL strlen_noexist >/dev/null 2>&1
    result=$($REDIS_CLI STRLEN strlen_noexist)
    [[ "$result" == "0" ]]
'

echo ""

# ==========================
# Test 10: DECR - Decrement by 1
# ==========================
echo -e "${YELLOW}=== Test Group 10: DECR (Decrement By 1) ===${NC}"

test_command "DECR - Decrement existing integer" '
    $REDIS_CLI SET decr1 10 >/dev/null
    sleep 1
    result=$($REDIS_CLI DECR decr1)
    [[ "$result" == "9" ]]
'

test_command "DECR - Decrement non-existing key (starts at 0)" '
    $REDIS_CLI DEL decr_new >/dev/null 2>&1
    result=$($REDIS_CLI DECR decr_new)
    [[ "$result" == "-1" ]]
'

test_command "DECR - Multiple decrements" '
    $REDIS_CLI SET decr2 5 >/dev/null
    sleep 1
    $REDIS_CLI DECR decr2 >/dev/null
    $REDIS_CLI DECR decr2 >/dev/null
    result=$($REDIS_CLI DECR decr2)
    [[ "$result" == "2" ]]
'

echo ""

# ==========================
# Test 11: DECRBY - Decrement by value
# ==========================
echo -e "${YELLOW}=== Test Group 11: DECRBY (Decrement By Value) ===${NC}"

test_command "DECRBY - Decrement by 5" '
    $REDIS_CLI SET decrby1 20 >/dev/null
    sleep 1
    result=$($REDIS_CLI DECRBY decrby1 5)
    [[ "$result" == "15" ]]
'

test_command "DECRBY - Decrement by 100" '
    $REDIS_CLI SET decrby2 50 >/dev/null
    sleep 1
    result=$($REDIS_CLI DECRBY decrby2 100)
    [[ "$result" == "-50" ]]
'

test_command "DECRBY - Non-existing key (starts at 0)" '
    $REDIS_CLI DEL decrby_new >/dev/null 2>&1
    result=$($REDIS_CLI DECRBY decrby_new 10)
    [[ "$result" == "-10" ]]
'

echo ""

# ==========================
# Summary
# ==========================
echo "=========================================="
echo "Test Summary"
echo "=========================================="
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${RED}Failed: $FAILED${NC}"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    echo ""
    echo "Phase 3.1 String Batch Operations implementation is complete and verified."
    exit 0
else
    echo -e "${RED}✗ Some tests failed!${NC}"
    exit 1
fi
