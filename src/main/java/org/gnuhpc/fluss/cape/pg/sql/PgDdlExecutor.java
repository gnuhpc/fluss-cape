/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gnuhpc.fluss.cape.pg.sql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.CreateAclsResult;
import org.apache.fluss.client.admin.DropAclsResult;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.metadata.KvSnapshotMetadata;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.config.cluster.ConfigEntry;
import org.apache.fluss.exception.KvSnapshotNotExistException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AccessControlEntryFilter;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.AclBindingFilter;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.security.acl.ResourceFilter;
import org.apache.fluss.security.acl.ResourceType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.gnuhpc.fluss.cape.pg.executor.PgCompatStubResults;
import org.gnuhpc.fluss.cape.pg.protocol.PgRowDescriptionBuilder;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.RowDescription;
import org.gnuhpc.fluss.cape.pg.session.PgSession;

public final class PgDdlExecutor {

    private PgDdlExecutor() {
    }

    public static PgExecutionResult executeShowDatabases(PgSession session) throws Exception {
        Admin admin = session.getFlussAdmin();
        List<String> databases = admin.listDatabases().get();

        List<Object[]> rows = new ArrayList<>();
        for (String db : databases) {
            rows.add(new Object[]{db});
        }

        RowDescription description = PgCompatStubResults.singleColumn("database_name", "").getRowDescription();
        return PgExecutionResult.query(new PgQueryResult(description, rows), "SHOW");
    }

    public static PgExecutionResult executeShowDatabaseExists(PgSession session, String sql) throws Exception {
        String normalized = normalizeSql(sql);
        String remainder = stripLeadingKeyword(normalized, "SHOW DATABASE EXISTS").trim();
        String dbName = remainder.replace("\"", "").replace("'", "").split("\\s+")[0].toLowerCase();
        Admin admin = session.getFlussAdmin();
        boolean exists = admin.databaseExists(dbName).get();

        RowDescription description = PgCompatStubResults.singleColumn("exists", "").getRowDescription();
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{String.valueOf(exists)});
        return PgExecutionResult.query(new PgQueryResult(description, rows), "SHOW");
    }

    public static PgExecutionResult executeShowDatabaseInfo(PgSession session, String sql) throws Exception {
        String normalized = normalizeSql(sql);
        String remainder = stripLeadingKeyword(normalized, "SHOW DATABASE INFO").trim();
        String dbName = remainder.replace("\"", "").replace("'", "").split("\\s+")[0].toLowerCase();
        Admin admin = session.getFlussAdmin();
        org.apache.fluss.metadata.DatabaseInfo info = admin.getDatabaseInfo(dbName).get();

        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{"name", info.getDatabaseName()});
        info.getDatabaseDescriptor().getComment().ifPresent(comment -> rows.add(new Object[]{"comment", comment}));
        if (info.getDatabaseDescriptor().getCustomProperties().isEmpty()) {
            rows.add(new Object[]{"properties", ""});
        } else {
            for (Map.Entry<String, String> entry : info.getDatabaseDescriptor().getCustomProperties().entrySet()) {
                rows.add(new Object[]{"property." + entry.getKey(), entry.getValue()});
            }
        }
        rows.add(new Object[]{"created", String.valueOf(info.getCreatedTime())});
        rows.add(new Object[]{"modified", String.valueOf(info.getModifiedTime())});

        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType rowType = typeFactory.createStructType(
                List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR), typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                List.of("field", "value"));
        return PgExecutionResult.query(new PgQueryResult(PgRowDescriptionBuilder.fromRelType(rowType), rows), "SHOW");
    }

    public static PgExecutionResult executeUseDatabase(PgSession session, String sql) throws Exception {
        String normalized = normalizeSql(sql);
        String remainder = stripLeadingKeyword(normalized, "USE DATABASE").trim();
        String dbName = remainder.replace("\"", "").replace("'", "").split("\\s+")[0].toLowerCase();
        Admin admin = session.getFlussAdmin();
        if (!admin.databaseExists(dbName).get()) {
            throw new IllegalArgumentException("Database does not exist: " + dbName);
        }
        session.setDatabase(dbName);
        return PgExecutionResult.update("USE DATABASE");
    }

    public static PgExecutionResult executeShowTables(PgSession session, String databaseName) throws Exception {
        Admin admin = session.getFlussAdmin();
        String targetDb = databaseName != null ? databaseName.toLowerCase() : session.getDatabase();

        if (targetDb == null) {
            throw new IllegalArgumentException("No database specified. Use SHOW TABLES FROM database");
        }

        List<String> tables = admin.listTables(targetDb).get();
        List<Object[]> rows = new ArrayList<>();
        for (String table : tables) {
            rows.add(new Object[]{table});
        }

        RowDescription description = PgCompatStubResults.singleColumn("table_name", "").getRowDescription();
        return PgExecutionResult.query(new PgQueryResult(description, rows), "SHOW");
    }

    public static PgExecutionResult executeShowTableExists(PgSession session, String sql) throws Exception {
        String normalized = normalizeSql(sql);
        String upper = normalized.toUpperCase();
        String remainder = normalized.substring(upper.indexOf("SHOW TABLE EXISTS") + "SHOW TABLE EXISTS".length()).trim();
        String tableName = remainder.replace("\"", "").replace("'", "").split("\\s+")[0].toLowerCase();
        TablePath tablePath = resolveTablePath(session, tableName);
        Admin admin = session.getFlussAdmin();
        boolean exists = admin.tableExists(tablePath).get();

        RowDescription description = PgCompatStubResults.singleColumn("exists", "").getRowDescription();
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{String.valueOf(exists)});
        return PgExecutionResult.query(new PgQueryResult(description, rows), "SHOW");
    }

    public static PgExecutionResult executeShowTableSchema(PgSession session, String sql) throws Exception {
        String normalized = normalizeSql(sql);
        String upper = normalized.toUpperCase();
        String remainder = normalized.substring(upper.indexOf("SHOW TABLE SCHEMA") + "SHOW TABLE SCHEMA".length()).trim();
        String tableName = remainder.replace("\"", "").replace("'", "").split("\\s+")[0].toLowerCase();
        TablePath tablePath = resolveTablePath(session, tableName);
        Admin admin = session.getFlussAdmin();
        org.apache.fluss.metadata.SchemaInfo schemaInfo = admin.getTableSchema(tablePath).get();
        org.apache.fluss.metadata.Schema schema = schemaInfo.getSchema();

        RowType rowType = schema.getRowType();
        List<Object[]> rows = new ArrayList<>();
        for (DataField field : rowType.getFields()) {
            String columnName = field.getName();
            String dataType = field.getType().toString();
            String nullable = field.getType().isNullable() ? "YES" : "NO";
            rows.add(new Object[]{columnName, dataType, nullable, ""});
        }

        RowDescription description = PgCompatStubResults.describeTableColumns().getRowDescription();
        return PgExecutionResult.query(new PgQueryResult(description, rows), "SHOW");
    }

    public static PgExecutionResult executeShowCreateTable(PgSession session, String sql) throws Exception {
        String normalized = normalizeSql(sql);
        String upper = normalized.toUpperCase();
        String remainder = normalized.substring(upper.indexOf("SHOW CREATE TABLE") + "SHOW CREATE TABLE".length()).trim();
        String tableName = remainder.replace("\"", "").replace("'", "").split("\\s+")[0].toLowerCase();
        TablePath tablePath = resolveTablePath(session, tableName);
        Admin admin = session.getFlussAdmin();
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        org.apache.fluss.metadata.Schema schema = tableInfo.getSchema();

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(tablePath).append(" (\n");

        RowType rowType = schema.getRowType();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (i > 0) {
                ddl.append(",\n");
            }
            String columnName = rowType.getFieldNames().get(i);
            org.apache.fluss.types.DataType dataType = rowType.getTypeAt(i);
            ddl.append("  ").append(columnName).append(" ").append(dataType.getTypeRoot());
        }

        if (schema.getPrimaryKey().isPresent()) {
            List<String> pkColumns = schema.getPrimaryKeyColumnNames();
            ddl.append(",\n  PRIMARY KEY (").append(String.join(", ", pkColumns)).append(")");
        }

        ddl.append("\n);");

        RowDescription description = PgCompatStubResults.singleColumn("create_table", "").getRowDescription();
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{ddl.toString()});
        return PgExecutionResult.query(new PgQueryResult(description, rows), "SHOW");
    }

    public static PgExecutionResult executeDescribeTable(PgSession session, String tableName) throws Exception {
        tableName = tableName.toLowerCase();
        TablePath tablePath = resolveTablePath(session, tableName);
        Admin admin = session.getFlussAdmin();
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();

        if (tableInfo == null) {
            throw new IllegalArgumentException("Table not found: " + tablePath);
        }

        RowType rowType = tableInfo.getRowType();
        List<String> primaryKeys = tableInfo.getPrimaryKeys();
        List<Object[]> rows = new ArrayList<>();

        for (DataField field : rowType.getFields()) {
            String columnName = field.getName();
            String dataType = field.getType().toString();
            String nullable = field.getType().isNullable() ? "YES" : "NO";
            String key = primaryKeys.contains(columnName) ? "PRI" : "";

            rows.add(new Object[]{columnName, dataType, nullable, key});
        }

        RowDescription description = PgCompatStubResults.describeTableColumns().getRowDescription();
        return PgExecutionResult.query(new PgQueryResult(description, rows), "DESCRIBE");
    }

    public static PgExecutionResult executeShowPartitions(PgSession session, String sql) throws Exception {
        String normalized = normalizeSql(sql);
        String remainder = stripLeadingKeyword(normalized, "SHOW PARTITIONS").trim();
        String[] parts = splitFirstToken(remainder);
        if (parts[0].isEmpty()) {
            throw new IllegalArgumentException("SHOW PARTITIONS requires table name");
        }

        TablePath tablePath = resolveTablePath(session, parts[0]);
        PartitionSpec partitionSpec = null;
        if (!parts[1].isEmpty()) {
            partitionSpec = parsePartitionSpec(parts[1]);
        }

        Admin admin = session.getFlussAdmin();
        if (partitionSpec != null) {
            validatePartitionSpec(admin, tablePath, partitionSpec, false);
        }

        List<PartitionInfo> partitions = partitionSpec == null
                ? admin.listPartitionInfos(tablePath).get()
                : admin.listPartitionInfos(tablePath, partitionSpec).get();

        List<Object[]> rows = new ArrayList<>();
        for (PartitionInfo partition : partitions) {
            rows.add(new Object[]{partition.getPartitionName(), String.valueOf(partition.getPartitionId())});
        }

        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType rowType = typeFactory.createStructType(
                List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR), typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                List.of("partition", "partition_id"));
        return PgExecutionResult.query(new PgQueryResult(PgRowDescriptionBuilder.fromRelType(rowType), rows), "SHOW");
    }

    public static PgExecutionResult executeShowOffsets(PgSession session, String sql) throws Exception {
        String normalized = normalizeSql(sql);
        String remainder = stripLeadingKeyword(normalized, "SHOW OFFSETS").trim();
        String[] parts = splitFirstToken(remainder);
        if (parts[0].isEmpty()) {
            throw new IllegalArgumentException("SHOW OFFSETS requires table name");
        }

        TablePath tablePath = resolveTablePath(session, parts[0]);
        String tail = parts[1];
        PartitionSpec partitionSpec = null;
        List<Integer> buckets = new ArrayList<>();
        OffsetSpec offsetSpec = parseOffsetSpec(tail);

        Matcher partitionMatcher = Pattern.compile("PARTITION\\s*\\(([^)]*)\\)", Pattern.CASE_INSENSITIVE).matcher(tail);
        if (partitionMatcher.find()) {
            partitionSpec = parsePartitionSpec(partitionMatcher.group(1));
            validatePartitionSpec(session.getFlussAdmin(), tablePath, partitionSpec, true);
        }

        Matcher bucketsMatcher = Pattern.compile("BUCKETS\\s*\\(([^)]*)\\)", Pattern.CASE_INSENSITIVE).matcher(tail);
        if (bucketsMatcher.find()) {
            for (String bucket : bucketsMatcher.group(1).split(",")) {
                String trimmed = bucket.trim();
                if (!trimmed.isEmpty()) {
                    buckets.add(Integer.parseInt(trimmed));
                }
            }
        }

        if (buckets.isEmpty()) {
            Admin admin = session.getFlussAdmin();
            TableInfo tableInfo = admin.getTableInfo(tablePath).get();
            int bucketCount = tableInfo.getNumBuckets();
            for (int i = 0; i < bucketCount; i++) {
                buckets.add(i);
            }
        }

        Admin admin = session.getFlussAdmin();
        ListOffsetsResult offsetsResult;
        if (partitionSpec == null) {
            offsetsResult = admin.listOffsets(tablePath, buckets, offsetSpec);
        } else {
            String partitionName = resolvePartitionName(admin, tablePath, partitionSpec);
            offsetsResult = admin.listOffsets(tablePath, partitionName, buckets, offsetSpec);
        }

        Map<Integer, Long> offsets = offsetsResult.all().get();
        List<Object[]> rows = new ArrayList<>();
        for (Map.Entry<Integer, Long> entry : offsets.entrySet()) {
            rows.add(new Object[]{String.valueOf(entry.getKey()), String.valueOf(entry.getValue())});
        }

        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType rowType = typeFactory.createStructType(
                List.of(typeFactory.createSqlType(SqlTypeName.INTEGER), typeFactory.createSqlType(SqlTypeName.BIGINT)),
                List.of("bucket", "offset"));
        return PgExecutionResult.query(new PgQueryResult(PgRowDescriptionBuilder.fromRelType(rowType), rows), "SHOW");
    }

    public static PgExecutionResult executeCreateDatabase(PgSession session, String databaseName, boolean ignoreIfExists) throws Exception {
        Admin admin = session.getFlussAdmin();
        DatabaseDescriptor descriptor = DatabaseDescriptor.builder().build();
        admin.createDatabase(databaseName, descriptor, ignoreIfExists).get();
        return PgExecutionResult.update("CREATE DATABASE");
    }

    public static PgExecutionResult executeDropDatabase(PgSession session, String databaseName, boolean ignoreIfNotExists, boolean cascade) throws Exception {
        Admin admin = session.getFlussAdmin();
        admin.dropDatabase(databaseName, ignoreIfNotExists, cascade).get();
        return PgExecutionResult.update("DROP DATABASE");
    }

    public static PgExecutionResult executeCreateTable(PgSession session, String fullTableName, List<ColumnDef> columns, List<String> primaryKeys) throws Exception {
        TablePath tablePath = parseTablePath(session, fullTableName);
        Admin admin = session.getFlussAdmin();
        
        Schema.Builder schemaBuilder = Schema.newBuilder();
        for (ColumnDef col : columns) {
            schemaBuilder.column(col.name, col.dataType);
        }
        
        if (!primaryKeys.isEmpty()) {
            schemaBuilder.primaryKey(primaryKeys);
        }
        
        Schema schema = schemaBuilder.build();
        TableDescriptor.Builder tableDescriptorBuilder = TableDescriptor.builder().schema(schema);
        
        int bucketCount = 3;
        if (schema.getPrimaryKey().isPresent()) {
            List<String> pkColumns = schema.getPrimaryKeyColumnNames();
            tableDescriptorBuilder.distributedBy(bucketCount, new ArrayList<>(pkColumns));
        } else {
            tableDescriptorBuilder.distributedBy(bucketCount, new ArrayList<>());
        }
        
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        admin.createTable(tablePath, tableDescriptor, false).get();
        
        return PgExecutionResult.update("CREATE TABLE");
    }

    public static PgExecutionResult executeDropTable(PgSession session, String tableName, boolean ignoreIfNotExists) throws Exception {
        TablePath tablePath = resolveTablePath(session, tableName);
        Admin admin = session.getFlussAdmin();
        admin.dropTable(tablePath, ignoreIfNotExists).get();
        return PgExecutionResult.update("DROP TABLE");
    }

    public static PgExecutionResult executeAlterTable(PgSession session, String sql) throws Exception {
        String normalized = normalizeSql(sql);
        String upper = normalized.toUpperCase();
        int keywordIndex = upper.indexOf("ALTER TABLE");
        String remaining = normalized.substring(keywordIndex + "ALTER TABLE".length()).trim();
        if (remaining.toUpperCase().startsWith("IF EXISTS ")) {
            remaining = remaining.substring("IF EXISTS".length()).trim();
        }
        String[] parts = splitFirstToken(remaining);
        if (parts[0].isEmpty() || parts[1].isEmpty()) {
            throw new IllegalArgumentException("ALTER TABLE requires table name and action: " + normalized);
        }

        TablePath tablePath = parseTablePath(session, parts[0]);
        String action = parts[1].trim();
        String actionUpper = action.toUpperCase();
        Admin admin = session.getFlussAdmin();

        if (actionUpper.startsWith("SET")) {
            String content = extractParenthesizedContent(stripLeadingKeyword(action, "SET"));
            Map<String, String> properties = parseKeyValueMap(content);
            List<TableChange> changes = new ArrayList<>();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                changes.add(TableChange.set(entry.getKey(), entry.getValue()));
            }
            admin.alterTable(tablePath, changes, false).get();
            return PgExecutionResult.update("ALTER TABLE");
        }

        if (actionUpper.startsWith("RESET")) {
            String content = extractParenthesizedContent(stripLeadingKeyword(action, "RESET"));
            List<String> keys = parseKeyList(content);
            List<TableChange> changes = new ArrayList<>();
            for (String key : keys) {
                changes.add(TableChange.reset(key));
            }
            admin.alterTable(tablePath, changes, false).get();
            return PgExecutionResult.update("ALTER TABLE");
        }

        throw new UnsupportedOperationException("Unsupported ALTER TABLE action: " + action);
    }

    public static PgExecutionResult executeShowKvSnapshots(PgSession session, String sql) throws Exception {
        String normalized = normalizeSql(sql);
        String remainder = stripLeadingKeyword(normalized, "SHOW KV SNAPSHOTS").trim();
        String[] parts = splitFirstToken(remainder);
        if (parts[0].isEmpty()) {
            throw new IllegalArgumentException("SHOW KV SNAPSHOTS requires table name");
        }
        TablePath tablePath = resolveTablePath(session, parts[0]);
        PartitionSpec partitionSpec = null;
        if (!parts[1].isEmpty()) {
            partitionSpec = parsePartitionSpec(parts[1]);
        }

        Admin admin = session.getFlussAdmin();
        KvSnapshots snapshots;
        if (partitionSpec == null) {
            snapshots = admin.getLatestKvSnapshots(tablePath).get();
        } else {
            validatePartitionSpec(admin, tablePath, partitionSpec, true);
            String partitionName = resolvePartitionName(admin, tablePath, partitionSpec);
            snapshots = admin.getLatestKvSnapshots(tablePath, partitionName).get();
        }

        List<Integer> bucketIds = new ArrayList<>(snapshots.getBucketIds());
        Collections.sort(bucketIds);
        List<Object[]> rows = new ArrayList<>();
        for (Integer bucketId : bucketIds) {
            String snapshotText = snapshots.getSnapshotId(bucketId).isPresent()
                    ? String.valueOf(snapshots.getSnapshotId(bucketId).getAsLong())
                    : "NONE";
            String logOffsetText = snapshots.getLogOffset(bucketId).isPresent()
                    ? String.valueOf(snapshots.getLogOffset(bucketId).getAsLong())
                    : "EARLIEST";
            rows.add(new Object[]{bucketId, snapshotText, logOffsetText});
        }

        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType rowType = typeFactory.createStructType(
                List.of(typeFactory.createSqlType(SqlTypeName.INTEGER),
                        typeFactory.createSqlType(SqlTypeName.VARCHAR),
                        typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                List.of("bucket", "snapshot_id", "log_offset"));
        return PgExecutionResult.query(new PgQueryResult(PgRowDescriptionBuilder.fromRelType(rowType), rows), "SHOW");
    }

    public static PgExecutionResult executeShowKvSnapshotMetadata(PgSession session, String sql) throws Exception {
        String normalized = normalizeSql(sql);
        String remainder = stripLeadingKeyword(normalized, "SHOW KV SNAPSHOT METADATA").trim();
        String[] parts = remainder.split("\\s+");
        if (parts.length < 3) {
            throw new IllegalArgumentException("SHOW KV SNAPSHOT METADATA requires table, bucket, snapshot id");
        }
        TablePath tablePath = resolveTablePath(session, parts[0]);
        int bucketId = Integer.parseInt(parts[1]);
        long snapshotId = Long.parseLong(parts[2]);

        Admin admin = session.getFlussAdmin();
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), bucketId);
        KvSnapshotMetadata metadata;
        try {
            metadata = admin.getKvSnapshotMetadata(tableBucket, snapshotId).get();
        } catch (ExecutionException exception) {
            if (exception.getCause() instanceof KvSnapshotNotExistException) {
                JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
                RelDataType rowType = typeFactory.createStructType(
                        List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                        List.of("metadata"));
                return PgExecutionResult.query(
                        new PgQueryResult(PgRowDescriptionBuilder.fromRelType(rowType), List.of()), "SHOW");
            }
            throw exception;
        }

        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType rowType = typeFactory.createStructType(
                List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                List.of("metadata"));
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{metadata.toString()});
        return PgExecutionResult.query(new PgQueryResult(PgRowDescriptionBuilder.fromRelType(rowType), rows), "SHOW");
    }

    public static PgExecutionResult executeShowLakeSnapshot(PgSession session, String sql) throws Exception {
        String normalized = normalizeSql(sql);
        String remainder = stripLeadingKeyword(normalized, "SHOW LAKE SNAPSHOT").trim();
        if (remainder.isEmpty()) {
            throw new IllegalArgumentException("SHOW LAKE SNAPSHOT requires table name");
        }
        TablePath tablePath = resolveTablePath(session, remainder.split("\\s+")[0]);
        Admin admin = session.getFlussAdmin();
        LakeSnapshot snapshot;
        try {
            snapshot = admin.getLatestLakeSnapshot(tablePath).get();
        } catch (ExecutionException exception) {
            if (exception.getCause() instanceof LakeTableSnapshotNotExistException) {
                JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
                RelDataType rowType = typeFactory.createStructType(
                        List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                        List.of("snapshot_id"));
                return PgExecutionResult.query(
                        new PgQueryResult(PgRowDescriptionBuilder.fromRelType(rowType), List.of()), "SHOW");
            }
            throw exception;
        }

        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType rowType = typeFactory.createStructType(
                List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR),
                        typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                List.of("bucket", "offset"));
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{"snapshot_id", String.valueOf(snapshot.getSnapshotId())});
        for (Map.Entry<TableBucket, Long> entry : snapshot.getTableBucketsOffset().entrySet()) {
            rows.add(new Object[]{entry.getKey().toString(), String.valueOf(entry.getValue())});
        }
        return PgExecutionResult.query(new PgQueryResult(PgRowDescriptionBuilder.fromRelType(rowType), rows), "SHOW");
    }

    public static PgExecutionResult executeShowServers(PgSession session) throws Exception {
        Admin admin = session.getFlussAdmin();
        List<ServerNode> servers = admin.getServerNodes().get();
        List<Object[]> rows = new ArrayList<>();
        for (ServerNode server : servers) {
            rows.add(new Object[]{server.id(), server.host(), server.port(), server.serverType().toString()});
        }
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType rowType = typeFactory.createStructType(
                List.of(typeFactory.createSqlType(SqlTypeName.INTEGER),
                        typeFactory.createSqlType(SqlTypeName.VARCHAR),
                        typeFactory.createSqlType(SqlTypeName.INTEGER),
                        typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                List.of("id", "host", "port", "type"));
        return PgExecutionResult.query(new PgQueryResult(PgRowDescriptionBuilder.fromRelType(rowType), rows), "SHOW");
    }

    public static PgExecutionResult executeShowClusterConfigs(PgSession session) throws Exception {
        Admin admin = session.getFlussAdmin();
        Collection<ConfigEntry> configs = admin.describeClusterConfigs().get();
        List<Object[]> rows = new ArrayList<>();
        for (ConfigEntry entry : configs) {
            rows.add(new Object[]{entry.key(), entry.value() == null ? "<unset>" : entry.value(), entry.source().toString()});
        }
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType rowType = typeFactory.createStructType(
                List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR),
                        typeFactory.createSqlType(SqlTypeName.VARCHAR),
                        typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                List.of("key", "value", "source"));
        return PgExecutionResult.query(new PgQueryResult(PgRowDescriptionBuilder.fromRelType(rowType), rows), "SHOW");
    }

    public static PgExecutionResult executeAlterClusterConfigs(PgSession session, String sql) throws Exception {
        String normalized = normalizeSql(sql);
        Matcher setMatcher = Pattern.compile("ALTER\\s+CLUSTER\\s+SET\\s+(?:WITH\\s+)?\\((.*)\\)", Pattern.CASE_INSENSITIVE).matcher(normalized);
        Matcher resetMatcher = Pattern.compile("ALTER\\s+CLUSTER\\s+RESET\\s+(?:WITH\\s+)?\\((.*)\\)", Pattern.CASE_INSENSITIVE).matcher(normalized);
        Matcher appendMatcher = Pattern.compile("ALTER\\s+CLUSTER\\s+APPEND\\s+(?:WITH\\s+)?\\((.*)\\)", Pattern.CASE_INSENSITIVE).matcher(normalized);
        Matcher subtractMatcher = Pattern.compile("ALTER\\s+CLUSTER\\s+SUBTRACT\\s+(?:WITH\\s+)?\\((.*)\\)", Pattern.CASE_INSENSITIVE).matcher(normalized);

        List<AlterConfig> configs = new ArrayList<>();
        AlterConfigOpType opType;
        String content;

        if (setMatcher.find()) {
            opType = AlterConfigOpType.SET;
            content = setMatcher.group(1);
        } else if (resetMatcher.find()) {
            opType = AlterConfigOpType.DELETE;
            content = resetMatcher.group(1);
        } else if (appendMatcher.find()) {
            opType = AlterConfigOpType.APPEND;
            content = appendMatcher.group(1);
        } else if (subtractMatcher.find()) {
            opType = AlterConfigOpType.SUBTRACT;
            content = subtractMatcher.group(1);
        } else {
            throw new IllegalArgumentException("ALTER CLUSTER supports SET/RESET/APPEND/SUBTRACT with property list: " + sql);
        }

        if (opType == AlterConfigOpType.SET || opType == AlterConfigOpType.APPEND || opType == AlterConfigOpType.SUBTRACT) {
            Map<String, String> properties = parseKeyValueMap(content);
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                configs.add(new AlterConfig(entry.getKey(), entry.getValue(), opType));
            }
        } else {
            List<String> keys = parseKeyList(content);
            for (String key : keys) {
                configs.add(new AlterConfig(key, null, opType));
            }
        }

        Admin admin = session.getFlussAdmin();
        admin.alterClusterConfigs(configs).get();
        return PgExecutionResult.update("ALTER CLUSTER");
    }

    public static PgExecutionResult executeRebalanceCluster(PgSession session, String sql) throws Exception {
        String normalized = normalizeSql(sql);
        Matcher matcher = Pattern.compile("REBALANCE\\s+CLUSTER\\s+WITH\\s+GOALS\\s*\\((.*)\\)", Pattern.CASE_INSENSITIVE).matcher(normalized);
        if (!matcher.find()) {
            throw new IllegalArgumentException("REBALANCE CLUSTER requires GOALS list: " + sql);
        }

        List<String> goals = parseKeyList(matcher.group(1));
        Admin admin = session.getFlussAdmin();
        String rebalanceId = invokeRebalance(admin, goals);

        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType rowType = typeFactory.createStructType(
                List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                List.of("rebalance_id"));
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{rebalanceId});
        return PgExecutionResult.query(new PgQueryResult(PgRowDescriptionBuilder.fromRelType(rowType), rows), "REBALANCE");
    }

    public static PgExecutionResult executeShowRebalance(PgSession session) throws Exception {
        Admin admin = session.getFlussAdmin();
        Optional<?> progress = invokeListRebalanceProgress(admin, null);
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType rowType = typeFactory.createStructType(
                List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                List.of("progress"));
        List<Object[]> rows = new ArrayList<>();
        if (progress.isPresent()) {
            rows.add(new Object[]{progress.get().toString()});
        }
        return PgExecutionResult.query(new PgQueryResult(PgRowDescriptionBuilder.fromRelType(rowType), rows), "SHOW");
    }

    public static PgExecutionResult executeCancelRebalance(PgSession session, String sql) throws Exception {
        String normalized = normalizeSql(sql);
        Matcher matcher = Pattern.compile("CANCEL\\s+REBALANCE(\\s+ID\\s+(\\S+))?", Pattern.CASE_INSENSITIVE).matcher(normalized);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Invalid CANCEL REBALANCE statement: " + sql);
        }

        String rebalanceId = matcher.group(2);
        Admin admin = session.getFlussAdmin();
        invokeCancelRebalance(admin, rebalanceId);
        return PgExecutionResult.update("CANCEL REBALANCE");
    }

    public static PgExecutionResult executeShowAcls(PgSession session, String sql) throws Exception {
        AclBindingFilter filter = parseAclBindingFilter(sql, false);
        Admin admin = session.getFlussAdmin();
        Collection<AclBinding> acls = admin.listAcls(filter).get();
        List<Object[]> rows = new ArrayList<>();
        for (AclBinding acl : acls) {
            rows.add(new Object[]{acl.toString()});
        }
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType rowType = typeFactory.createStructType(
                List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                List.of("acl"));
        return PgExecutionResult.query(new PgQueryResult(PgRowDescriptionBuilder.fromRelType(rowType), rows), "SHOW");
    }

    public static PgExecutionResult executeCreateAcl(PgSession session, String sql) throws Exception {
        AclBinding aclBinding = parseAclBinding(sql);
        Admin admin = session.getFlussAdmin();
        CreateAclsResult result = admin.createAcls(Collections.singletonList(aclBinding));
        result.all().get();
        return PgExecutionResult.update("CREATE ACL");
    }

    public static PgExecutionResult executeDropAcl(PgSession session, String sql) throws Exception {
        AclBindingFilter filter = parseAclBindingFilter(sql, true);
        Admin admin = session.getFlussAdmin();
        DropAclsResult result = admin.dropAcls(Collections.singletonList(filter));
        result.all().get();
        return PgExecutionResult.update("DROP ACL");
    }

    private static TablePath resolveTablePath(PgSession session, String tableReference) {
        String database = session.getDatabase() == null ? "default" : session.getDatabase();
        String[] parts = tableReference.split("\\.");
        if (parts.length == 2) {
            return TablePath.of(parts[0], parts[1]);
        }
        if (parts.length == 1) {
            return TablePath.of(database, parts[0]);
        }
        throw new IllegalArgumentException("Invalid table reference: " + tableReference);
    }

    private static TablePath parseTablePath(PgSession session, String fullTableName) {
        String[] parts = fullTableName.split("\\.");
        if (parts.length == 2) {
            return TablePath.of(parts[0], parts[1]);
        }
        throw new IllegalArgumentException("Table name must be in format 'database.table': " + fullTableName);
    }

    private static String normalizeSql(String sql) {
        String normalized = sql == null ? "" : sql.trim();
        if (normalized.endsWith(";")) {
            normalized = normalized.substring(0, normalized.length() - 1).trim();
        }
        return normalized;
    }

    private static String[] splitFirstToken(String input) {
        String trimmed = input.trim();
        int spaceIndex = trimmed.indexOf(' ');
        if (spaceIndex < 0) {
            return new String[]{trimmed, ""};
        }
        String first = trimmed.substring(0, spaceIndex).trim();
        String rest = trimmed.substring(spaceIndex + 1).trim();
        return new String[]{first, rest};
    }

    private static String stripLeadingKeyword(String text, String keyword) {
        String upper = text.toUpperCase();
        if (!upper.startsWith(keyword)) {
            return text;
        }
        return text.substring(keyword.length()).trim();
    }

    private static String extractParenthesizedContent(String text) {
        int start = text.indexOf('(');
        int end = text.lastIndexOf(')');
        if (start < 0 || end <= start) {
            throw new IllegalArgumentException("Expected parentheses content: " + text);
        }
        return text.substring(start + 1, end).trim();
    }

    private static Map<String, String> parseKeyValueMap(String content) {
        Map<String, String> result = new java.util.LinkedHashMap<>();
        for (String entry : splitCommaSeparated(content)) {
            String[] parts = entry.split("=", 2);
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid key/value: " + entry);
            }
            String key = unquote(parts[0].trim());
            String value = unquote(parts[1].trim());
            result.put(key, value);
        }
        return result;
    }

    private static List<String> parseKeyList(String content) {
        List<String> keys = new ArrayList<>();
        for (String entry : splitCommaSeparated(content)) {
            keys.add(unquote(entry.trim()));
        }
        return keys;
    }

    private static List<String> splitCommaSeparated(String content) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        char quoteChar = 0;
        for (int i = 0; i < content.length(); i++) {
            char ch = content.charAt(i);
            if ((ch == '\'' || ch == '"')) {
                if (inQuotes && ch == quoteChar) {
                    inQuotes = false;
                    quoteChar = 0;
                } else if (!inQuotes) {
                    inQuotes = true;
                    quoteChar = ch;
                }
                current.append(ch);
                continue;
            }
            if (ch == ',' && !inQuotes) {
                tokens.add(current.toString().trim());
                current.setLength(0);
                continue;
            }
            current.append(ch);
        }
        if (current.length() > 0) {
            tokens.add(current.toString().trim());
        }
        return tokens;
    }

    private static PartitionSpec parsePartitionSpec(String input) {
        String content = input.trim();
        String upper = content.toUpperCase();
        if (upper.startsWith("PARTITION")) {
            content = content.substring("PARTITION".length()).trim();
        }
        if (content.startsWith("(")) {
            content = extractParenthesizedContent(content);
        }
        Map<String, String> spec = parseKeyValueMap(content);
        return new PartitionSpec(spec);
    }

    private static void validatePartitionSpec(Admin admin, TablePath tablePath, PartitionSpec partitionSpec, boolean requireAll) throws Exception {
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        List<String> partitionKeys = tableInfo.getPartitionKeys();
        if (partitionKeys.isEmpty()) {
            throw new IllegalArgumentException("Table " + tablePath + " is not partitioned");
        }
        Map<String, String> specMap = partitionSpec.getSpecMap();
        for (String key : specMap.keySet()) {
            if (!partitionKeys.contains(key)) {
                throw new IllegalArgumentException("Unknown partition key '" + key + "' for table " + tablePath);
            }
        }
        if (requireAll) {
            for (String key : partitionKeys) {
                if (!specMap.containsKey(key)) {
                    throw new IllegalArgumentException("Partition spec must include all partition keys");
                }
            }
        }
    }

    private static String resolvePartitionName(Admin admin, TablePath tablePath, PartitionSpec partitionSpec) throws Exception {
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        ResolvedPartitionSpec resolvedSpec = ResolvedPartitionSpec.fromPartitionSpec(tableInfo.getPartitionKeys(), partitionSpec);
        return resolvedSpec.getPartitionName();
    }

    private static OffsetSpec parseOffsetSpec(String sqlTail) {
        Matcher timestampMatcher = Pattern.compile("AT\\s+TIMESTAMP\\s+(?:'([^']*)'|\"([^\"]*)\"|(\\d+))", Pattern.CASE_INSENSITIVE).matcher(sqlTail);
        if (timestampMatcher.find()) {
            String quoted = timestampMatcher.group(1) != null ? timestampMatcher.group(1) : timestampMatcher.group(2);
            if (quoted != null) {
                return new OffsetSpec.TimestampSpec(parseTimestampLiteral(quoted));
            }
            long timestamp = Long.parseLong(timestampMatcher.group(3));
            return new OffsetSpec.TimestampSpec(timestamp);
        }
        String upper = sqlTail.toUpperCase();
        if (upper.contains("AT EARLIEST")) {
            return new OffsetSpec.EarliestSpec();
        }
        if (upper.contains("AT LATEST")) {
            return new OffsetSpec.LatestSpec();
        }
        return new OffsetSpec.LatestSpec();
    }

    private static long parseTimestampLiteral(String literal) {
        String trimmed = literal.trim();
        try {
            return java.time.Instant.parse(trimmed).toEpochMilli();
        } catch (java.time.format.DateTimeParseException ignored) {
            java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            java.time.LocalDateTime localDateTime = java.time.LocalDateTime.parse(trimmed, formatter);
            return localDateTime.toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
        }
    }

    private static AclBinding parseAclBinding(String sql) {
        String content = extractParenthesizedContent(sql);
        Map<String, String> values = parseKeyValueMap(content);
        String resourceType = values.get("resource_type");
        String resourceName = values.get("resource_name");
        String principalValue = values.get("principal");
        String principalType = values.get("principal_type");
        String host = values.getOrDefault("host", AccessControlEntry.WILD_CARD_HOST);
        String operation = values.get("operation");
        String permission = values.get("permission");

        if (resourceType == null || principalValue == null || principalValue.trim().isEmpty() || operation == null || permission == null) {
            throw new IllegalArgumentException("CREATE ACL requires resource_type, principal, operation, permission");
        }

        Resource resource = parseResource(resourceType, resourceName, false);
        FlussPrincipal principal = parsePrincipal(principalValue, principalType, false);
        OperationType operationType = parseOperationType(operation, false);
        PermissionType permissionType = parsePermissionType(permission, false);

        AccessControlEntry entry = new AccessControlEntry(principal, host, operationType, permissionType);
        return new AclBinding(resource, entry);
    }

    private static AclBindingFilter parseAclBindingFilter(String sql, boolean required) {
        Matcher matcher = Pattern.compile("FILTER\\s*\\(([^)]*)\\)", Pattern.CASE_INSENSITIVE).matcher(sql);
        if (!matcher.find()) {
            if (required) {
                throw new IllegalArgumentException("DROP ACL requires FILTER clause");
            }
            return AclBindingFilter.ANY;
        }

        Map<String, String> values = parseKeyValueMap(matcher.group(1));
        String resourceType = values.get("resource_type");
        String resourceName = values.get("resource_name");
        String principalValue = values.get("principal");
        String principalType = values.get("principal_type");
        String host = values.get("host");
        String operation = values.get("operation");
        String permission = values.get("permission");

        ResourceFilter resourceFilter = ResourceFilter.ANY;
        String resourceToken = values.get("resource");
        if (resourceToken != null && (resourceType == null && resourceName == null)) {
            resourceFilter = parseResourceFilter(resourceToken);
        } else if (resourceType != null || resourceName != null) {
            if (resourceType == null) {
                throw new IllegalArgumentException("resource_type is required when resource_name is set");
            }
            ResourceType type = ResourceType.fromName(resourceType);
            resourceFilter = new ResourceFilter(type, resourceName == null ? null : unquote(resourceName));
        }

        FlussPrincipal principal = parsePrincipal(principalValue, principalType, true);
        OperationType operationType = parseOperationType(operation, true);
        PermissionType permissionType = parsePermissionType(permission, true);

        AccessControlEntryFilter entryFilter = new AccessControlEntryFilter(principal, host, operationType, permissionType);
        return new AclBindingFilter(resourceFilter, entryFilter);
    }

    private static FlussPrincipal parsePrincipal(String principalValue, String principalType, boolean forFilter) {
        if (principalValue == null || principalValue.trim().isEmpty()) {
            return forFilter ? FlussPrincipal.ANY : null;
        }

        String trimmed = unquote(principalValue);
        String type = principalType == null ? "User" : unquote(principalType);

        if (trimmed.contains(":")) {
            String[] parts = trimmed.split(":", 2);
            type = parts[0];
            trimmed = parts[1];
        }

        if ("*".equals(trimmed) && "*".equals(type)) {
            return FlussPrincipal.WILD_CARD_PRINCIPAL;
        }

        if ("*".equals(trimmed)) {
            return new FlussPrincipal(trimmed, type);
        }

        return new FlussPrincipal(trimmed, type);
    }

    private static ResourceFilter parseResourceFilter(String token) {
        String trimmed = unquote(token);
        if ("*".equals(trimmed)) {
            return ResourceFilter.ANY;
        }
        if (!trimmed.contains(":")) {
            ResourceType type = ResourceType.fromName(trimmed);
            return new ResourceFilter(type, null);
        }
        String[] parts = trimmed.split(":", 2);
        ResourceType type = ResourceType.fromName(parts[0]);
        String name = parts[1];
        if (name.isEmpty() || "*".equals(name)) {
            return new ResourceFilter(type, null);
        }
        return new ResourceFilter(type, name);
    }

    private static OperationType parseOperationType(String operation, boolean forFilter) {
        if (operation == null || operation.trim().isEmpty()) {
            return forFilter ? OperationType.ANY : null;
        }
        return OperationType.valueOf(unquote(operation).toUpperCase());
    }

    private static PermissionType parsePermissionType(String permission, boolean forFilter) {
        if (permission == null || permission.trim().isEmpty()) {
            return forFilter ? PermissionType.ANY : null;
        }
        return PermissionType.valueOf(unquote(permission).toUpperCase());
    }

    private static Resource parseResource(String resourceType, String resourceName, boolean forFilter) {
        ResourceType type = ResourceType.fromName(resourceType);
        if (type == ResourceType.ANY) {
            if (forFilter) {
                return Resource.any();
            }
            throw new IllegalArgumentException("resource_type ANY is only allowed in filters");
        }
        if (type == ResourceType.CLUSTER) {
            return Resource.cluster();
        }
        if (resourceName == null || resourceName.trim().isEmpty()) {
            if (forFilter) {
                return Resource.any();
            }
            throw new IllegalArgumentException("resource_name is required for " + resourceType);
        }
        String trimmedName = unquote(resourceName);
        if (Resource.WILDCARD_RESOURCE.equals(trimmedName)) {
            return new Resource(type, Resource.WILDCARD_RESOURCE);
        }
        return new Resource(type, trimmedName);
    }

    private static String invokeRebalance(Admin admin, List<String> goals) throws Exception {
        Class<?> goalTypeClass = Class.forName("org.apache.fluss.cluster.rebalance.GoalType");
        List<Object> goalTypes = new ArrayList<>();
        for (String goal : goals) {
            goalTypes.add(goalTypeClass.getMethod("fromName", String.class).invoke(null, goal));
        }
        Object future = admin.getClass().getMethod("rebalance", List.class).invoke(admin, goalTypes);
        return (String) ((java.util.concurrent.CompletableFuture<?>) future).get();
    }

    private static Optional<?> invokeListRebalanceProgress(Admin admin, String rebalanceId) throws Exception {
        Object future = admin.getClass().getMethod("listRebalanceProgress", String.class).invoke(admin, rebalanceId);
        Object result = ((java.util.concurrent.CompletableFuture<?>) future).get();
        if (result instanceof Optional) {
            return (Optional<?>) result;
        }
        return Optional.empty();
    }

    private static void invokeCancelRebalance(Admin admin, String rebalanceId) throws Exception {
        Object future = admin.getClass().getMethod("cancelRebalance", String.class).invoke(admin, rebalanceId);
        ((java.util.concurrent.CompletableFuture<?>) future).get();
    }

    private static String unquote(String value) {
        if ((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'"))) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    public static class ColumnDef {
        public final String name;
        public final DataType dataType;

        public ColumnDef(String name, DataType dataType) {
            this.name = name;
            this.dataType = dataType;
        }
    }
}
