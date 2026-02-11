# HBase Protocol Guide

This guide covers using Apache Fluss through the HBase protocol via Fluss CAPE.

---

## Table of Contents

1. [Overview](#overview)
2. [Setup](#setup)
3. [Java Client Usage](#java-client-usage)
4. [Spark Integration](#spark-integration)
5. [Apache Phoenix Integration](#apache-phoenix-integration)
6. [API Reference](#api-reference)
7. [Best Practices](#best-practices)
8. [Troubleshooting](#troubleshooting)

---

## Overview

### What's Supported

Fluss CAPE provides HBase 2.x/3.x protocol compatibility for:

| Operation | Status | Notes |
|-----------|--------|-------|
| **Get** | ✅ Full | Single-row retrieval |
| **Put** | ✅ Full | Single-row insert/update |
| **Delete** | ✅ Full | Single-row deletion |
| **Scan** | ✅ Full | Range scans with filters |
| **Multi** | ✅ Full | Batch operations (Get/Put/Delete) |
| **CheckAndMutate** | ⚠️ Non-Atomic | See [Atomicity Limitations](#atomicity-limitations) |
| **Increment/Append** | ⚠️ Non-Atomic | See [Atomicity Limitations](#atomicity-limitations) |
| **Meta Table** | ✅ Emulated | Table discovery and region info |
| **Admin Operations** | ✅ Full | CreateTable, DeleteTable, DisableTable, EnableTable, GetTableDescriptors |
| **Security** | ✅ Full | SASL/GSSAPI authentication |
| **Coprocessors** | ❌ Not Supported | - |
| **Filters** | ⚠️ Partial | Basic filters only |

### Key Differences from Native HBase

1. **No WAL Guarantee** - Writes are ack'd when persisted to Fluss's log
2. **Primary Key Required** - All Fluss tables must have primary keys
3. **Column Families** - Mapped to Fluss column groups (optional)
4. **No Compaction Control** - Compaction managed by Fluss
5. **⚠️ No Atomic Conditional Operations** - See [Atomicity Limitations](#atomicity-limitations) below

### Atomicity Limitations

**CRITICAL**: CheckAndMutate, Increment, and Append operations are **NOT fully atomic** in CAPE due to Fluss storage limitations.

#### The Problem

HBase's CheckAndMutate guarantees atomicity through a single atomic operation:
```
ATOMIC { check_condition() AND mutate() }
```

CAPE implements this as **three separate steps**:
1. READ - lookup current value
2. CHECK - evaluate condition  
3. WRITE - upsert new value if condition met

**Race condition window** exists between steps 1 and 3, where another client can modify the row.

#### Impact

| Operation | Risk | Example |
|-----------|------|---------|
| **CheckAndMutate** | Lost updates | Two clients check-and-update same counter simultaneously → one update lost |
| **Increment** | Incorrect final value | Concurrent increments may produce wrong sum |
| **Append** | Data loss | Concurrent appends may overwrite each other |

#### Why This Happens

Fluss KV storage does **not** support:
- Transactions (BEGIN/COMMIT/ROLLBACK)
- Compare-and-swap (CAS) operations
- Conditional atomic updates

This is an architectural limitation of the underlying storage layer.

#### Recommendations

For **non-critical** use cases (e.g., caching, approximate counters):
- ✅ Safe to use CheckAndMutate/Increment/Append
- ✅ Accept eventual consistency

For **critical** use cases (e.g., financial transactions, strict counters):
- ❌ Do NOT use CheckAndMutate/Increment/Append
- ✅ Use external locking (e.g., ZooKeeper, Redis)
- ✅ Implement optimistic versioning at application layer
- ✅ Use idempotent operations with retry logic

#### Example Workaround

```java
// Instead of CheckAndMutate (non-atomic)
table.checkAndMutate(row, family)
    .qualifier(qual)
    .ifEquals(oldValue)
    .thenPut(newPut);

// Use external locking (atomic)
Lock lock = lockService.acquire("row:" + rowKey);
try {
    Get get = new Get(row);
    Result result = table.get(get);
    if (matches(result, condition)) {
        Put put = new Put(row);
        // ... build put ...
        table.put(put);
    }
} finally {
    lock.release();
}
```

For more details, see inline documentation in `CheckAndMutateExecutor.java`.

---

## Setup

### 1. Start CAPE Server

```bash
java -jar fluss-cape-1.0.0.jar \
  --fluss.bootstrap.servers=localhost:9123 \
  --hbase.compat.bind.port=16020
```

### 2. Add Dependencies

#### Maven

```xml
<dependencies>
    <!-- HBase Client 2.x -->
    <dependency>
        <groupId>org.apache.hbase</groupId>
        <artifactId>hbase-client</artifactId>
        <version>2.5.5</version>
    </dependency>
    
    <!-- Or HBase Client 3.x -->
    <dependency>
        <groupId>org.apache.hbase</groupId>
        <artifactId>hbase-client</artifactId>
        <version>3.0.0-alpha-4</version>
    </dependency>
</dependencies>
```

#### Gradle

```gradle
dependencies {
    implementation 'org.apache.hbase:hbase-client:2.5.5'
}
```

### 3. Configure Connection

Create `hbase-site.xml`:

```xml
<configuration>
    <property>
        <name>hbase.zookeeper.quorum</name>
        <value>localhost:2181</value>
    </property>
    <property>
        <name>zookeeper.znode.parent</name>
        <value>/fluss</value>
    </property>
</configuration>
```

Or programmatically:

```java
Configuration config = HBaseConfiguration.create();
config.set("hbase.zookeeper.quorum", "localhost:2181");
config.set("zookeeper.znode.parent", "/fluss");
```

---

## HBase Shell Commands

CAPE supports standard HBase Shell operations for interactive table management.

### Installation

If you don't have HBase Shell installed:

```bash
# Download HBase
wget https://archive.apache.org/dist/hbase/2.5.13/hbase-2.5.13-bin.tar.gz
tar xzf hbase-2.5.13-bin.tar.gz
export HBASE_HOME=/path/to/hbase-2.5.13
```

Configure connection in `$HBASE_HOME/conf/hbase-site.xml`:

```xml
<configuration>
    <property>
        <name>hbase.zookeeper.quorum</name>
        <value>localhost:2181</value>
    </property>
</configuration>
```

### Supported Shell Commands

| Command | Description | Example |
|---------|-------------|---------|
| **create** | Create table with column families | `create 'users', 'cf'` |
| **list** | List all tables | `list` |
| **describe** | Show table schema and metadata | `describe 'users'` |
| **put** | Insert/update cell value | `put 'users', 'row1', 'cf:name', 'Alice'` |
| **get** | Retrieve row data | `get 'users', 'row1'` |
| **scan** | Scan table rows | `scan 'users'` |
| **delete** | Delete cell or row | `delete 'users', 'row1', 'cf:name'` |
| **deleteall** | Delete entire row | `deleteall 'users', 'row1'` |
| **disable** | Disable table | `disable 'users'` |
| **enable** | Enable table | `enable 'users'` |
| **drop** | Delete table (must disable first) | `drop 'users'` |
| **exists** | Check if table exists | `exists 'users'` |

### Example Session

```bash
# Start HBase Shell
$HBASE_HOME/bin/hbase shell

# Create a table with multiple column families
hbase> create 'products', 'info', 'pricing'
Created table products

# Insert data
hbase> put 'products', 'prod001', 'info:name', 'Widget'
hbase> put 'products', 'prod001', 'info:category', 'Hardware'
hbase> put 'products', 'prod001', 'pricing:cost', '10.50'
hbase> put 'products', 'prod001', 'pricing:retail', '19.99'

# View table schema
hbase> describe 'products'
Table products is ENABLED
products, {TABLE_ATTRIBUTES => {METADATA => {'FLUSS_TABLE' => 'true'}}}
COLUMN FAMILIES DESCRIPTION
{NAME => 'cf', ...}

# Retrieve data
hbase> get 'products', 'prod001'
COLUMN                    CELL
 info:name                timestamp=..., value=Widget
 info:category            timestamp=..., value=Hardware
 pricing:cost             timestamp=..., value=10.50
 pricing:retail           timestamp=..., value=19.99

# Scan table
hbase> scan 'products'
ROW                       COLUMN+CELL
 prod001                  column=info:name, value=Widget
 prod001                  column=info:category, value=Hardware
 prod001                  column=pricing:cost, value=10.50
 prod001                  column=pricing:retail, value=19.99
1 row(s)

# Delete table
hbase> disable 'products'
hbase> drop 'products'

# Exit
hbase> exit
```

### Notes on describe Command

The `describe` command shows:
- **Table state**: ENABLED/DISABLED
- **Table attributes**: Custom metadata (e.g., `FLUSS_TABLE => 'true'`)
- **Column families**: All column families with default HBase properties

**Important**: CAPE extracts column families from Fluss table schema. Since Fluss stores all columns in a flat structure, CAPE uses a default `cf` column family and maps HBase's `family:qualifier` format to Fluss column names (e.g., `info:name` → `info_name`).

---

## Java Client Usage

### Basic Connection

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class FlussHBaseExample {
    public static void main(String[] args) throws Exception {
        // Create connection
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost:2181");
        
        Connection connection = ConnectionFactory.createConnection(config);
        
        // Use connection...
        
        connection.close();
    }
}
```

### Put Operation (Insert/Update)

#### Single Put

```java
Table table = connection.getTable(TableName.valueOf("users"));

Put put = new Put(Bytes.toBytes("user001"));
put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("Alice"));
put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(30));
put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("email"), Bytes.toBytes("alice@example.com"));

table.put(put);
```

#### Batch Put

```java
List<Put> puts = new ArrayList<>();

for (int i = 0; i < 1000; i++) {
    Put put = new Put(Bytes.toBytes("user" + String.format("%06d", i)));
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("User" + i));
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(20 + (i % 50)));
    puts.add(put);
}

table.put(puts);
```

### Get Operation (Retrieve)

#### Single Get

```java
Get get = new Get(Bytes.toBytes("user001"));
Result result = table.get(get);

// Extract values
byte[] name = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("name"));
byte[] age = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("age"));

System.out.println("Name: " + Bytes.toString(name));
System.out.println("Age: " + Bytes.toInt(age));
```

#### Get Specific Columns

```java
Get get = new Get(Bytes.toBytes("user001"));
get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"));
get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("email"));

Result result = table.get(get);
```

#### Batch Get

```java
List<Get> gets = new ArrayList<>();
for (int i = 0; i < 100; i++) {
    gets.add(new Get(Bytes.toBytes("user" + String.format("%06d", i))));
}

Result[] results = table.get(gets);
for (Result result : results) {
    if (!result.isEmpty()) {
        String name = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("name")));
        System.out.println("Name: " + name);
    }
}
```

### Delete Operation

#### Single Delete

```java
Delete delete = new Delete(Bytes.toBytes("user001"));
table.delete(delete);
```

#### Delete Specific Columns

```java
Delete delete = new Delete(Bytes.toBytes("user001"));
delete.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("email"));
table.delete(delete);
```

#### Batch Delete

```java
List<Delete> deletes = new ArrayList<>();
for (int i = 0; i < 100; i++) {
    deletes.add(new Delete(Bytes.toBytes("user" + String.format("%06d", i))));
}
table.delete(deletes);
```

### Scan Operation

#### Full Table Scan

```java
Scan scan = new Scan();
ResultScanner scanner = table.getScanner(scan);

for (Result result : scanner) {
    String rowKey = Bytes.toString(result.getRow());
    String name = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("name")));
    System.out.println(rowKey + ": " + name);
}

scanner.close();
```

#### Range Scan

```java
Scan scan = new Scan();
scan.withStartRow(Bytes.toBytes("user000000"));
scan.withStopRow(Bytes.toBytes("user001000"));

ResultScanner scanner = table.getScanner(scan);
for (Result result : scanner) {
    // Process results...
}
scanner.close();
```

#### Scan with Column Filter

```java
Scan scan = new Scan();
scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"));
scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"));

ResultScanner scanner = table.getScanner(scan);
for (Result result : scanner) {
    String name = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("name")));
    int age = Bytes.toInt(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("age")));
    System.out.println(name + " - " + age);
}
scanner.close();
```

#### Scan with Limit

```java
Scan scan = new Scan();
scan.setLimit(100);  // Only return first 100 rows

ResultScanner scanner = table.getScanner(scan);
```

---

## Spark Integration

### Add Spark HBase Connector

```xml
<dependency>
    <groupId>org.apache.hbase.connectors.spark</groupId>
    <artifactId>hbase-spark</artifactId>
    <version>1.0.1</version>
</dependency>
```

### Read from Fluss via HBase

```scala
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration

val spark = SparkSession.builder()
  .appName("Fluss HBase Reader")
  .master("local[*]")
  .getOrCreate()

val hbaseConf = HBaseConfiguration.create()
hbaseConf.set("hbase.zookeeper.quorum", "localhost:2181")

val hbaseContext = new HBaseContext(spark.sparkContext, hbaseConf)

// Read data
val rdd = hbaseContext.hbaseRDD(
  TableName.valueOf("users"),
  new Scan()
)

rdd.foreach { case (key, result) =>
  val name = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("name")))
  println(s"${Bytes.toString(key)}: $name")
}
```

### Write to Fluss via HBase

```scala
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

val data = spark.sparkContext.parallelize(1 to 10000)

val putsRDD = data.map { i =>
  val put = new Put(Bytes.toBytes(s"user$i"))
  put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(s"User$i"))
  put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(20 + (i % 50)))
  put
}

hbaseContext.bulkPut(putsRDD, TableName.valueOf("users"))
```

### Spark SQL with HBase Catalog

```scala
spark.sql("""
  CREATE TABLE users
  USING org.apache.hadoop.hbase.spark
  OPTIONS(
    'catalog'='{
      "table":{"namespace":"default", "name":"users"},
      "rowkey":"user_id",
      "columns":{
        "user_id":{"cf":"rowkey", "col":"user_id", "type":"string"},
        "name":{"cf":"cf", "col":"name", "type":"string"},
        "age":{"cf":"cf", "col":"age", "type":"int"}
      }
    }',
    'hbase.zookeeper.quorum'='localhost:2181'
  )
""")

spark.sql("SELECT * FROM users WHERE age > 25").show()
```

---

## Apache Phoenix Integration

### Setup Phoenix

1. Download Phoenix binary matching your HBase version
2. Copy `phoenix-client.jar` to your classpath

### Start Phoenix Query Server

```bash
# Configure Phoenix to point to CAPE
export HBASE_CONF_DIR=/path/to/hbase-conf

# Start query server
bin/queryserver.py start
```

### Connect via sqlline

```bash
bin/sqlline.py localhost:8765
```

### Create Phoenix View

```sql
-- Map to existing Fluss table
CREATE VIEW "users" (
    pk VARCHAR PRIMARY KEY,
    "cf"."name" VARCHAR,
    "cf"."age" INTEGER,
    "cf"."email" VARCHAR
);
```

### Query Data

```sql
-- Select
SELECT * FROM "users" LIMIT 10;

SELECT pk, "cf"."name", "cf"."age" 
FROM "users" 
WHERE "cf"."age" > 25;

-- Aggregation
SELECT "cf"."age", COUNT(*) as count
FROM "users"
GROUP BY "cf"."age"
ORDER BY count DESC;

-- Insert (maps to HBase Put)
UPSERT INTO "users" VALUES ('user999', 'Bob', 35, 'bob@example.com');

-- Delete
DELETE FROM "users" WHERE pk = 'user999';
```

### Phoenix JDBC

```java
import java.sql.*;

public class PhoenixExample {
    public static void main(String[] args) throws Exception {
        Connection conn = DriverManager.getConnection(
            "jdbc:phoenix:localhost:2181:/fluss"
        );
        
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(
            "SELECT pk, \"cf\".\"name\" FROM \"users\" LIMIT 10"
        );
        
        while (rs.next()) {
            System.out.println(rs.getString(1) + ": " + rs.getString(2));
        }
        
        rs.close();
        stmt.close();
        conn.close();
    }
}
```

---

## API Reference

### Connection Management

```java
// Create connection
Configuration config = HBaseConfiguration.create();
Connection conn = ConnectionFactory.createConnection(config);

// Connection pooling (recommended)
ConnectionFactory.createAsyncConnection(config).get();

// Close connection
conn.close();
```

### Table Operations

```java
// Get table reference
Table table = conn.getTable(TableName.valueOf("tableName"));

// Check table exists
Admin admin = conn.getAdmin();
boolean exists = admin.tableExists(TableName.valueOf("tableName"));

// List tables
TableName[] tables = admin.listTableNames();

// Get table descriptor
TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf("tableName"));
System.out.println("Table: " + descriptor.getTableName());
for (ColumnFamilyDescriptor cf : descriptor.getColumnFamilies()) {
    System.out.println("  Column Family: " + cf.getNameAsString());
}
```

### Admin Operations

CAPE supports full table lifecycle management through the Admin API:

```java
Admin admin = conn.getAdmin();

// Create table with single column family
TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf("users"));
ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf"));
tableBuilder.setColumnFamily(cfBuilder.build());
admin.createTable(tableBuilder.build());

// Create table with multiple column families
TableDescriptorBuilder productTable = TableDescriptorBuilder.newBuilder(TableName.valueOf("products"));
productTable.setColumnFamily(ColumnFamilyDescriptorBuilder.of("info"));
productTable.setColumnFamily(ColumnFamilyDescriptorBuilder.of("pricing"));
admin.createTable(productTable.build());

// Get table descriptor
TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf("users"));
System.out.println("Table: " + descriptor.getTableName());
System.out.println("Families: " + descriptor.getColumnFamilyCount());

// Disable table (required before deletion)
admin.disableTable(TableName.valueOf("users"));

// Enable table
admin.enableTable(TableName.valueOf("users"));

// Check table state
boolean isEnabled = admin.isTableEnabled(TableName.valueOf("users"));
boolean isDisabled = admin.isTableDisabled(TableName.valueOf("users"));

// Delete table (must be disabled first)
admin.disableTable(TableName.valueOf("users"));
admin.deleteTable(TableName.valueOf("users"));

// List all tables
TableName[] tables = admin.listTableNames();
for (TableName table : tables) {
    System.out.println(table.getNameAsString());
}

admin.close();
```

**Supported Admin Methods:**

| Method | Description | Status |
|--------|-------------|--------|
| `createTable(TableDescriptor)` | Create new table | ✅ Full |
| `deleteTable(TableName)` | Delete table | ✅ Full |
| `disableTable(TableName)` | Disable table | ✅ Full |
| `enableTable(TableName)` | Enable table | ✅ Full |
| `getDescriptor(TableName)` | Get table schema | ✅ Full |
| `listTableNames()` | List all tables | ✅ Full |
| `tableExists(TableName)` | Check table existence | ✅ Full |
| `isTableEnabled(TableName)` | Check if enabled | ✅ Full |
| `isTableDisabled(TableName)` | Check if disabled | ✅ Full |
| `modifyTable(TableDescriptor)` | Modify table schema | ⚠️ Limited |

### Row Operations

| Method | Description | Example |
|--------|-------------|---------|
| `table.put(Put)` | Insert/update row | `table.put(new Put(key))` |
| `table.get(Get)` | Retrieve row | `table.get(new Get(key))` |
| `table.delete(Delete)` | Delete row | `table.delete(new Delete(key))` |
| `table.exists(Get)` | Check if row exists | `table.exists(new Get(key))` |

### Batch Operations

| Method | Description | Throughput |
|--------|-------------|------------|
| `table.put(List<Put>)` | Batch insert | ~10x faster |
| `table.get(List<Get>)` | Batch retrieve | ~8x faster |
| `table.delete(List<Delete>)` | Batch delete | ~10x faster |
| `table.batch(List<Row>)` | Mixed operations | Varies |

### Scan Options

```java
Scan scan = new Scan();

// Range
scan.withStartRow(startKey);
scan.withStopRow(stopKey);

// Columns
scan.addFamily(family);
scan.addColumn(family, qualifier);

// Limit
scan.setLimit(1000);

// Caching
scan.setCaching(500);  // Fetch 500 rows per RPC

// Batch size
scan.setBatch(100);    // Return 100 columns per Result
```

---

## Best Practices

### 1. Connection Pooling

```java
// BAD: Creating connection per operation
for (int i = 0; i < 1000; i++) {
    Connection conn = ConnectionFactory.createConnection(config);
    Table table = conn.getTable(TableName.valueOf("users"));
    // ... operation
    conn.close();
}

// GOOD: Reuse connection
Connection conn = ConnectionFactory.createConnection(config);
Table table = conn.getTable(TableName.valueOf("users"));
for (int i = 0; i < 1000; i++) {
    // ... operation
}
conn.close();
```

### 2. Batch Operations

```java
// BAD: Individual puts
for (int i = 0; i < 1000; i++) {
    Put put = new Put(Bytes.toBytes("key" + i));
    put.addColumn(cf, col, value);
    table.put(put);  // 1000 RPCs
}

// GOOD: Batch put
List<Put> puts = new ArrayList<>();
for (int i = 0; i < 1000; i++) {
    Put put = new Put(Bytes.toBytes("key" + i));
    put.addColumn(cf, col, value);
    puts.add(put);
}
table.put(puts);  // 1 RPC
```

### 3. Scan Optimization

```java
Scan scan = new Scan();

// Set appropriate caching
scan.setCaching(500);  // Reduce RPCs

// Limit columns
scan.addColumn(cf, qualifier);  // Don't fetch all columns

// Use limit for pagination
scan.setLimit(1000);

// Set reasonable batch size
scan.setBatch(100);
```

### 4. Resource Cleanup

```java
Table table = null;
ResultScanner scanner = null;
try {
    table = conn.getTable(TableName.valueOf("users"));
    scanner = table.getScanner(new Scan());
    // ... process results
} finally {
    if (scanner != null) scanner.close();
    if (table != null) table.close();
}
```

### 5. Error Handling

```java
try {
    table.put(put);
} catch (RetriesExhaustedWithDetailsException e) {
    // Handle batch operation failures
    for (Throwable t : e.getCauses()) {
        System.err.println("Failed: " + t.getMessage());
    }
} catch (IOException e) {
    // Handle connection/IO errors
    e.printStackTrace();
}
```

---

## Troubleshooting

### Issue: Connection Timeout

```
org.apache.hadoop.hbase.client.ConnectionFactory$Impl: Connection timeout
```

**Solution**:
```java
Configuration config = HBaseConfiguration.create();
config.setInt("hbase.client.operation.timeout", 60000);
config.setInt("hbase.client.scanner.timeout.period", 60000);
```

### Issue: No Region Server Found

```
java.io.IOException: No live region servers found
```

**Solution**: Ensure CAPE server is running and ZooKeeper path is correct:
```bash
# Check CAPE is running
netstat -tuln | grep 16020

# Verify ZooKeeper path
zkCli.sh
ls /fluss
```

### Issue: Scan Returns No Results

**Cause**: Row key encoding mismatch.

**Solution**: Ensure consistent byte encoding:
```java
// Use Bytes utility consistently
Put put = new Put(Bytes.toBytes("key"));
Get get = new Get(Bytes.toBytes("key"));  // Same encoding
```

### Issue: Performance Issues

**Symptoms**: Slow batch operations, high latency.

**Solutions**:
1. Increase batch size: `scan.setCaching(1000)`
2. Use batch operations instead of individual puts/gets
3. Add connection pooling
4. Check network latency between client and CAPE server

---

## Next Steps

- **[Redis Guide](REDIS-GUIDE.md)** - Learn Redis protocol usage
- **[Configuration](CONFIGURATION.md)** - Tune HBase protocol settings
- **[Examples](EXAMPLES.md)** - See production use cases

---

## Quick Reference Card

```java
// Connection
Configuration config = HBaseConfiguration.create();
Connection conn = ConnectionFactory.createConnection(config);
Table table = conn.getTable(TableName.valueOf("users"));

// Insert
Put put = new Put(Bytes.toBytes("key"));
put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("col"), Bytes.toBytes("value"));
table.put(put);

// Read
Get get = new Get(Bytes.toBytes("key"));
Result result = table.get(get);
byte[] value = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("col"));

// Scan
Scan scan = new Scan();
ResultScanner scanner = table.getScanner(scan);
for (Result r : scanner) { /* process */ }
scanner.close();

// Delete
Delete delete = new Delete(Bytes.toBytes("key"));
table.delete(delete);

// Cleanup
table.close();
conn.close();
```
