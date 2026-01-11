#!/bin/bash
set -e
echo "=== Phase 2.3 Smoke Test ==="
echo ""

# Test 1: Basic connectivity
echo -n "1. PING: "
redis-cli -p 6379 PING

# Test 2: Basic SET/GET
echo -n "2. SET/GET: "
redis-cli -p 6379 SET smoke_key "smoke_value" > /dev/null
RESULT=$(redis-cli -p 6379 GET smoke_key)
[ "$RESULT" = "smoke_value" ] && echo "PASS" || echo "FAIL"

# Test 3: EXPIRE
echo -n "3. EXPIRE: "
redis-cli -p 6379 SET exp_key "value" > /dev/null
EXPIRE_RESULT=$(redis-cli -p 6379 EXPIRE exp_key 60)
[ "$EXPIRE_RESULT" = "1" ] && echo "PASS" || echo "FAIL"

# Test 4: TTL
echo -n "4. TTL: "
TTL_RESULT=$(redis-cli -p 6379 TTL exp_key)
[ "$TTL_RESULT" -gt 55 ] && [ "$TTL_RESULT" -le 60 ] && echo "PASS (${TTL_RESULT}s)" || echo "FAIL ($TTL_RESULT)"

# Test 5: PERSIST
echo -n "5. PERSIST: "
PERSIST_RESULT=$(redis-cli -p 6379 PERSIST exp_key)
TTL_AFTER=$(redis-cli -p 6379 TTL exp_key)
[ "$PERSIST_RESULT" = "1" ] && [ "$TTL_AFTER" = "-1" ] && echo "PASS" || echo "FAIL"

# Test 6: Expiration works
echo -n "6. Auto-expiration: "
redis-cli -p 6379 SET auto_exp "value" > /dev/null
redis-cli -p 6379 EXPIRE auto_exp 2 > /dev/null
sleep 3
EXPIRED=$(redis-cli -p 6379 GET auto_exp)
[ -z "$EXPIRED" ] && echo "PASS" || echo "FAIL (key still exists)"

# Cleanup
redis-cli -p 6379 DEL smoke_key exp_key auto_exp > /dev/null 2>&1

echo ""
echo "✓ Smoke test complete"
