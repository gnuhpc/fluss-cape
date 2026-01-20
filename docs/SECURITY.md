# Security Guidelines - Fluss CAPE

This document outlines security best practices for Fluss CAPE development, with emphasis on preventing information disclosure vulnerabilities.

---

## Error Handling Security

### The Problem: Information Disclosure via Error Messages

Detailed error messages can reveal sensitive internal information to potential attackers:

- **Java stack traces** - Expose class names, method signatures, line numbers
- **File paths** - Reveal system architecture (`/var/lib/fluss/data/shard_42/segment.log`)
- **Database schema** - Leak table names, field names, bucket indices
- **Implementation details** - Framework versions, internal APIs, data structures
- **User enumeration** - Distinguish "user not found" from "wrong password"

**Attack Vector**: Malicious clients can probe system internals by triggering exceptions and analyzing error responses.

---

## Redis Error Handling

### DO NOT Leak Internal Details

❌ **Bad - Leaks internals**:
```java
catch (Exception e) {
    return RedisResponse.error("ERR " + e.getMessage());
    // Returns: "ERR Cannot invoke 'org.gnuhpc.fluss.cape.redis.adapter.get' 
    //           at StringCommandExecutor.java:42"
}
```

✅ **Good - Generic error**:
```java
catch (Exception e) {
    return RedisErrorSanitizer.sanitizeError(e, "GET");
    // Returns: "ERR invalid key or value"
    // Logs full exception with stack trace server-side
}
```

### Using RedisErrorSanitizer

**Standard error sanitization**:
```java
try {
    byte[] value = adapter.get(keyBytes);
    return RedisResponse.bulkString(value);
} catch (Exception e) {
    return RedisErrorSanitizer.sanitizeError(e, "GET");
}
```

**With key context (for logging)**:
```java
try {
    adapter.set(keyBytes, valueBytes);
    return RedisResponse.ok();
} catch (Exception e) {
    return RedisErrorSanitizer.sanitizeError(e, "SET", key);
}
```

**Authentication errors** (prevents user enumeration):
```java
try {
    validatePassword(username, password);
    return RedisResponse.ok();
} catch (Exception e) {
    return RedisErrorSanitizer.sanitizeAuthError(e);
    // Always returns "ERR invalid password" regardless of failure reason
}
```

**Rate limiting**:
```java
if (rateLimiter.isExceeded()) {
    return RedisErrorSanitizer.createRateLimitError("AUTH");
}
```

### Error Mapping Table

| Exception Type | Client Message | Logged Server-Side |
|----------------|----------------|-------------------|
| `NullPointerException` | `ERR invalid key or value` | Full stack trace |
| `IllegalArgumentException` | `ERR invalid argument` | Full stack trace |
| `NumberFormatException` | `ERR value is not an integer or out of range` | Full stack trace |
| `ClassCastException` | `WRONGTYPE Operation against a key holding the wrong kind of value` | Full stack trace |
| `IOException` | `ERR I/O error` | Full stack trace |
| `TimeoutException` | `ERR operation timeout` | Full stack trace |
| `UnsupportedOperationException` | `ERR operation not supported` | Full stack trace |
| `IndexOutOfBoundsException` | `ERR index out of range` | Full stack trace |
| Message contains "not found" | `ERR key not found` | Full stack trace |
| Message contains "already exists" | `ERR key already exists` | Full stack trace |
| Authentication failure | `ERR invalid password` | Full stack trace |
| Unknown exception | `ERR internal server error` | Full stack trace |

---

## Kafka Error Handling

### Current State

Kafka protocol handlers currently use direct exception messages in some places. This needs similar sanitization.

**TODO**: Implement `KafkaErrorSanitizer` utility following the same pattern as Redis.

---

## HBase Error Handling

### Current State

HBase protocol handlers need review for information disclosure vulnerabilities.

**TODO**: Audit HBase executors for error message leakage.

---

## PostgreSQL Error Handling

### Current State

PostgreSQL wire protocol handlers need review for information disclosure vulnerabilities.

**TODO**: Audit PG executors for error message leakage.

---

## Testing Requirements

### Security Test Coverage

All protocol implementations MUST have security tests verifying:

1. **No stack traces in client responses**
2. **No Java class/package names exposed**
3. **No file paths or line numbers leaked**
4. **No internal schema details revealed**
5. **Authentication errors don't enable user enumeration**

### Example Test Pattern

```java
@Test
@DisplayName("Exception should not leak internal details")
void testExceptionSanitized() {
    Exception exception = new NullPointerException(
        "Cannot invoke 'get' on org.gnuhpc.fluss.cape.redis.adapter at line 42"
    );
    
    RedisMessage response = RedisErrorSanitizer.sanitizeError(exception, "GET");
    String errorMsg = ((ErrorRedisMessage) response).content();
    
    // Verify no leakage
    assertThat(errorMsg).doesNotContain("org.gnuhpc");
    assertThat(errorMsg).doesNotContain(".java:");
    assertThat(errorMsg).doesNotContain("line 42");
    assertThat(errorMsg).doesNotContain("NullPointerException");
    
    // Verify generic error returned
    assertThat(errorMsg).startsWith("ERR");
    assertThat(errorMsg).contains("invalid");
}
```

See `src/test/java/org/gnuhpc/fluss/cape/redis/security/RedisErrorSanitizationTest.java` for complete examples.

---

## Security Review Checklist

Before merging code, verify:

- [ ] All `catch` blocks use error sanitization utilities (not `e.getMessage()`)
- [ ] No raw exception messages exposed to clients
- [ ] All executor files import appropriate sanitizer
- [ ] Security tests verify no information leakage
- [ ] Server logs contain full exception details for debugging
- [ ] Authentication errors use special sanitization (no user enumeration)

---

## Compliance

Proper error handling helps meet security compliance requirements:

- **OWASP Top 10** - A05:2021 Security Misconfiguration
- **PCI-DSS 6.5.5** - Improper error handling
- **CWE-209** - Information Exposure Through Error Messages
- **CWE-209** - Generation of Error Message Containing Sensitive Information

---

## Server-Side Logging

Full exception details are always logged server-side for debugging:

```
2026-01-19 14:20:15 ERROR RedisErrorSanitizer - Redis operation failed: GET key=user:12345
java.lang.NullPointerException: Cannot invoke "String.getBytes" because "value" is null
    at org.gnuhpc.fluss.cape.redis.executor.StringCommandExecutor.executeGet(StringCommandExecutor.java:42)
    at org.gnuhpc.fluss.cape.redis.executor.StringCommandExecutor.execute(StringCommandExecutor.java:28)
    ...
```

Client receives only: `ERR invalid key or value`

---

## Future Work

### Redis Module ✅
- [x] RedisErrorSanitizer utility created
- [x] 41 instances fixed across 19 files
- [x] 17 security tests added
- [x] All tests passing

### Kafka Module ⏳
- [ ] Create KafkaErrorSanitizer utility
- [ ] Audit all Kafka handlers for error leakage
- [ ] Add security tests

### HBase Module ⏳
- [ ] Create HBaseErrorSanitizer utility
- [ ] Audit all HBase executors for error leakage
- [ ] Add security tests

### PostgreSQL Module ⏳
- [ ] Create PgErrorSanitizer utility
- [ ] Audit all PG executors for error leakage
- [ ] Add security tests

---

## References

- [OWASP - Improper Error Handling](https://owasp.org/www-community/Improper_Error_Handling)
- [CWE-209: Information Exposure Through Error Messages](https://cwe.mitre.org/data/definitions/209.html)
- [Redis Protocol Specification](https://redis.io/docs/reference/protocol-spec/)
- `src/main/java/org/gnuhpc/fluss/cape/redis/util/RedisErrorSanitizer.java` - Reference implementation

---

**Last Updated**: 2026-01-19  
**Status**: Redis module complete, other modules pending
