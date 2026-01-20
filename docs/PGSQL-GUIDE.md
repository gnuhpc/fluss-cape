# PostgreSQL Protocol Guide

This guide covers using Apache Fluss through the PostgreSQL wire protocol via Fluss CAPE.

---

## Table of Contents

1. [Overview](#overview)
2. [Supported Features](#supported-features)
3. [Connection Details](#connection-details)
4. [Client Usage Examples](#client-usage-examples)
    - [psql Command Line](#psql-command-line)
    - [Python (psycopg2)](#python-psycopg2)
    - [Java (JDBC)](#java-jdbc)
5. [Technical Architecture](#technical-architecture)
6. [Current Limitations](#current-limitations)
7. [Best Practices & Troubleshooting](#best-practices--troubleshooting)

---

## Overview

Fluss CAPE provides PostgreSQL wire protocol compatibility for Apache Fluss, enabling standard PostgreSQL clients and tools to interact with Fluss tables. This allows developers to use familiar SQL syntax and mature ecosystems (like JDBC, SQLAlchemy, or DBeaver) to query and manage data stored in Fluss.

The compatibility layer translates PostgreSQL frontend/backend messages into Fluss client operations, leveraging **Apache Calcite** for SQL parsing and **ShardingSphere** for protocol handling.

---

## Supported Features

Fluss CAPE supports the following PostgreSQL-compatible features:

*   **Standard SQL Queries**: `SELECT` queries with filtering, sorting, and basic aggregations.
*   **Data Modification**: `INSERT`, `UPDATE`, and `DELETE` operations (mapped to Fluss UpsertWriter).
*   **Information Schema**: Emulated `pg_catalog` views allowing clients to discover tables and schemas.
*   **Metadata Exploration**: Support for `SHOW TABLES`, `SHOW DATABASES`, and `DESCRIBE` commands.
*   **Hybrid Scan Strategy**: Ensures data consistency by combining KV snapshots with real-time changelog replay.
*   **Authentication**: Supports `TRUST` and `PASSWORD` (cleartext) authentication modes.

---

## Connection Details

To connect to the PostgreSQL compatibility layer in Fluss CAPE, use the following default settings:

| Parameter | Default Value | Notes |
|-----------|---------------|-------|
| **Host** | `localhost` | Or the IP where CAPE is running |
| **Port** | `15432` | Configurable via `pg.port` |
| **Database** | `default` | Maps to Fluss database name |
| **User** | `fluss` | Configurable in authentication settings |
| **Password** | (any) | Required if `auth_mode` is set to `password` |
| **Auth Mode** | `TRUST` | Can be set to `password` for credential verification |

---

## Client Usage Examples

### psql Command Line

You can use the standard `psql` client to interact with Fluss:

```bash
# Connect to Fluss CAPE
psql -h localhost -p 15432 -U fluss -d default

# List tables
default=> \dt

# Query a table
default=> SELECT * FROM users LIMIT 10;

# Insert data
default=> INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
```

### Python (psycopg2)

Connect to Fluss using the popular `psycopg2` library:

```python
import psycopg2

# Connection parameters
conn = psycopg2.connect(
    host="localhost",
    port=15432,
    database="default",
    user="fluss",
    password="password"
)

# Create a cursor
cur = conn.cursor()

# Execute a query
cur.execute("SELECT name, age FROM users WHERE age > %s", (25,))

# Fetch results
for record in cur.fetchall():
    print(f"Name: {record[0]}, Age: {record[1]}")

# Close connection
cur.close()
conn.close()
```

### Java (JDBC)

Add the PostgreSQL JDBC driver to your project and connect:

```java
import java.sql.*;
import java.util.Properties;

public class FlussPgExample {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:postgresql://localhost:15432/default";
        Properties props = new Properties();
        props.setProperty("user", "fluss");
        props.setProperty("password", "password");
        props.setProperty("ssl", "false");

        try (Connection conn = DriverManager.getConnection(url, props)) {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM users");
            while (rs.next()) {
                System.out.println("User: " + rs.getString("name"));
            }
            rs.close();
            st.close();
        }
    }
}
```

---

## Technical Architecture

### Table Mapping
Fluss databases and tables map directly to PostgreSQL schemas and tables.
*   **Databases**: Fluss databases are exposed as PostgreSQL databases.
*   **Tables**: Fluss tables are exposed as PostgreSQL tables within the `public` or named schema.
*   **Types**: Fluss types are mapped to standard PostgreSQL OIDs:
    *   `INT` → `int4` (23)
    *   `BIGINT` → `int8` (20)
    *   `STRING` → `text/varchar` (1043)
    *   `BOOLEAN` → `bool` (16)
    *   `BYTES` → `bytea` (17)

### Hybrid Scan Strategy
To provide up-to-the-minute data consistency for `SELECT` queries, Fluss CAPE employs a **Hybrid Scan Strategy**:
1.  **Snapshot Phase**: Uses `BatchScanner` to read the latest consistent KV snapshot from storage.
2.  **Log Replay Phase**: Identifies the log offset at the time of the snapshot and uses `LogScanner` to replay all subsequent changelog events (Insert/Update/Delete).
3.  **Merge Phase**: Combines the snapshot data with replayed events in-memory to produce the final, accurate result set.

This strategy ensures that queries do not miss data even if snapshot compaction occurred recently.

---

## Current Limitations

While the PostgreSQL protocol provides a powerful interface, there are some current limitations:

1.  **Read-Only Focus**: While `INSERT/UPDATE/DELETE` are supported, the primary optimization and testing focus is on `SELECT` queries.
2.  **No Transactions**: `BEGIN`, `COMMIT`, and `ROLLBACK` are not supported. Statements are executed in auto-commit mode.
3.  **Limited SQL Syntax**: Complex joins, window functions, and advanced PostgreSQL-specific extensions may not be fully supported by the underlying execution engine.
4.  **Authentication**: Only `TRUST` and `cleartext password` modes are implemented. `MD5` and `SCRAM` are not supported.
5.  **Binary Parameters**: Currently, only text-format parameters are supported in extended query protocols.

---

## Best Practices & Troubleshooting

### Best Practices
*   **Use Primary Keys**: Queries filtering on primary keys are significantly faster as they use the `Lookuper` API instead of a full scan.
*   **Limit Result Sets**: Always use `LIMIT` when querying large tables to reduce memory pressure on the CAPE server during the merge phase.
*   **Monitor Memory**: Since the hybrid scan merges data in-memory, ensure the CAPE server has sufficient heap space for large result sets.

### Troubleshooting
*   **Connection Refused**: Verify that Fluss CAPE is running and the `pg.port` (default 15432) is open and not blocked by a firewall.
*   **"Table Not Found"**: Ensure you are connected to the correct database and that the table exists in Fluss. Use `\dt` in `psql` to verify visibility.
*   **Data Latency**: If data appears missing, verify that the `LogScanner` is correctly reaching the latest offset. Check CAPE logs for any "Log scan timeout" or "Connection lost" errors.
*   **Transaction Errors**: If your client library automatically starts transactions, disable them or set the connection to auto-commit mode.

---

## Next Steps

*   **[HBase Guide](HBASE-GUIDE.md)** - Learn about HBase protocol support.
*   **[Redis Guide](REDIS-GUIDE.md)** - Learn about Redis protocol support.
*   **[Configuration](CONFIGURATION.md)** - Detailed configuration parameters for PG support.
