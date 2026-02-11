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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.Lex;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;
import org.gnuhpc.fluss.cape.pg.calcite.FlussSchema;
import org.gnuhpc.fluss.cape.pg.executor.PgCompatStubResults;
import org.gnuhpc.fluss.cape.pg.protocol.PgRowDescriptionBuilder;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.RowDescription;
import org.gnuhpc.fluss.cape.pg.session.PgCopyState;
import org.gnuhpc.fluss.cape.pg.session.PgSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PgSqlEngine {

    private static final Logger LOG = LoggerFactory.getLogger(PgSqlEngine.class);

    private PgSqlEngine() {
    }

    public static void validate(String sql) {
        String normalized = normalizeSql(sql).toUpperCase(Locale.ROOT);
        if (normalized.startsWith("SHOW ") || normalized.startsWith("DESCRIBE ") || 
            normalized.startsWith("DESC ") || normalized.startsWith("CREATE ") || 
            normalized.startsWith("DROP ") || normalized.startsWith("ALTER ") ||
            normalized.startsWith("REBALANCE ") || normalized.startsWith("CANCEL ")) {
            return;
        }
        if (handleCompatCommands(normalized) != null) {
            return;
        }
        if (PgSqlParser.parseCopyFromStdin(sql) != null) {
            return;
        }
        SqlNode parsed = parse(sql);
        SqlNode queryNode = (parsed instanceof SqlOrderBy) ? ((SqlOrderBy) parsed).query : parsed;
        
        if (!(queryNode instanceof SqlSelect
                || queryNode instanceof SqlInsert
                || queryNode instanceof SqlUpdate
                || queryNode instanceof SqlDelete)) {
        throw new IllegalArgumentException("only SELECT/INSERT/UPDATE/DELETE/CREATE/DROP/ALTER supported");
        }
    }

    public static PgExecutionResult execute(PgSession session, String sql, List<Object> parameters) throws Exception {
        String normalized = normalizeSql(sql).toUpperCase(Locale.ROOT);
        
        PgExecutionResult metaResult = handleMetadataCommands(session, sql);
        if (metaResult != null) {
            return metaResult;
        }
        
        PgExecutionResult compat = handleCompatCommands(normalized);
        if (compat != null) {
            return compat;
        }
        
        if (normalized.startsWith("CREATE TABLE")) {
            return handleCreateTable(session, sql);
        }
        
        String resolvedSql = applyParameters(sql, parameters);
        SqlNode parsed = parse(resolvedSql);
        SqlNode queryNode = (parsed instanceof SqlOrderBy) ? ((SqlOrderBy) parsed).query : parsed;

        if (queryNode instanceof SqlSelect) {
            return executeSelect(session, resolvedSql, (SqlSelect) queryNode);
        }
        if (queryNode instanceof SqlInsert) {
            return executeInsert(session, (SqlInsert) queryNode);
        }
        if (queryNode instanceof SqlUpdate) {
            return executeUpdate(session, (SqlUpdate) queryNode);
        }
        if (queryNode instanceof SqlDelete) {
            return executeDelete(session, (SqlDelete) queryNode);
        }
        throw new IllegalArgumentException("only SELECT/INSERT/UPDATE/DELETE/CREATE/DROP/ALTER supported");
    }

    public static PgQueryResult describe(PgSession session, String sql) throws Exception {
        String normalized = normalizeSql(sql).toUpperCase(Locale.ROOT);
        
        if (normalized.startsWith("SHOW ") || normalized.startsWith("DESCRIBE ") || 
            normalized.startsWith("DESC ") || normalized.startsWith("CREATE ") || 
            normalized.startsWith("DROP ")) {
            try {
                PgExecutionResult result = handleMetadataCommands(session, sql);
                if (result != null && result.getQueryResult() != null) {
                    return result.getQueryResult();
                }
            } catch (Exception e) {
            }
            return new PgQueryResult(null, List.of());
        }
        
        PgQueryResult compat = describeCompat(normalized);
        if (compat != null) {
            return compat;
        }
        SqlNode parsed = parse(sql);
        if (parsed instanceof SqlSelect) {
            return describeSelect(session, (SqlSelect) parsed);
        }
        return new PgQueryResult(null, List.of());
    }

    public static List<Object> decodeParameters(List<byte[]> parameters, short[] formatCodes) {
        List<Object> decoded = new ArrayList<>(parameters.size());
        for (int i = 0; i < parameters.size(); i++) {
            byte[] value = parameters.get(i);
            short format = formatCodes.length == 1 ? formatCodes[0] : formatCodes[i];
            if (format != 0) {
                throw new IllegalArgumentException("binary parameters not supported");
            }
            decoded.add(value == null ? null : new String(value, StandardCharsets.UTF_8));
        }
        return decoded;
    }

    public static List<byte[]> encodeRow(Object[] row, RowDescription description) {
        List<byte[]> values = new ArrayList<>(row.length);
        for (Object value : row) {
            values.add(encodeValue(value));
        }
        return values;
    }

    public static List<byte[]> encodeRow(Object[] row, RowDescription description, short[] formatCodes) {
        if (formatCodes == null || formatCodes.length == 0) {
            return encodeRow(row, description);
        }

        List<byte[]> values = new ArrayList<>(row.length);
        for (int i = 0; i < row.length; i++) {
            short formatCode = i < formatCodes.length ? formatCodes[i] : formatCodes[0];
            if (formatCode == 0) {
                values.add(encodeValue(row[i]));
            } else {
                values.add(encodeBinaryValue(row[i], description, i));
            }
        }
        return values;
    }

    public static long copyFromStdin(PgSession session, PgCopyState copyState) throws Exception {
        PgSqlParser.CopyCommand command = copyState.getCommand();
        TablePath tablePath = resolveTablePath(session, command.getTable());
        TableInfo tableInfo = getTableInfo(session, tablePath);
        Table table = session.getFlussConnection().getTable(tablePath);
        RowType rowType = tableInfo.getRowType();
        List<String> columns = command.getColumns().isEmpty()
                ? rowType.getFields().stream().map(DataField::getName).collect(Collectors.toList())
                : command.getColumns();
        long count = 0;
        String data = copyState.getData();
        
        UpsertWriter writer = table.newUpsert().createWriter();
        try {
            for (String rawLine : data.split("\\n")) {
                String line = rawLine.endsWith("\r") ? rawLine.substring(0, rawLine.length() - 1) : rawLine;
                if (line.isEmpty()) {
                    continue;
                }
                String[] tokens = line.split("\\t", -1);
                GenericRow row = new GenericRow(rowType.getFieldCount());
                for (int i = 0; i < columns.size(); i++) {
                    int index = fieldIndex(rowType, columns.get(i));
                    String token = i < tokens.length ? tokens[i] : "";
                    Object value = "\\N".equals(token) ? null : convertFromText(token, rowType.getFields().get(index).getType());
                    row.setField(index, value);
                }
                writer.upsert(row).join();
                count += 1;
            }
        } finally {
            writer.flush();
        }
        return count;
    }

    private static PgExecutionResult executeSelect(PgSession session, String sql, SqlSelect select) throws Exception {
        if (select.getFrom() == null) {
            return executeLiteralSelect(select);
        }

        Connection connection = session.getFlussConnection();
        Admin admin = session.getFlussAdmin();
        String database = session.getDatabase() == null ? "default" : session.getDatabase();

        if (select.getFrom() instanceof SqlIdentifier && select.getWhere() != null) {
            SqlIdentifier identifier = (SqlIdentifier) select.getFrom();
            TablePath tablePath = resolveTablePath(session, identifier.toString().toLowerCase());
            TableInfo tableInfo = getTableInfo(session, tablePath);
            RowType rowType = tableInfo.getRowType();
            List<String> primaryKeys = tableInfo.getPrimaryKeys();
            Map<String, SqlLiteral> predicates = new HashMap<>();
            
            if (!primaryKeys.isEmpty() && extractPredicates(select.getWhere(), predicates)) {
                if (predicates.keySet().containsAll(primaryKeys)) {
                    LOG.info("Using optimized Lookuper path for PK query on {}", tablePath);
                    List<String> selectedColumns = resolveSelectedColumns(select.getSelectList(), rowType);
                    InternalRow.FieldGetter[] getters = InternalRow.createFieldGetters(rowType);
                    
                    GenericRow keyRow = new GenericRow(rowType.getFieldCount());
                    for (String key : primaryKeys) {
                        int index = fieldIndex(rowType, key);
                        SqlLiteral literal = predicates.get(key);
                        keyRow.setField(index, convertLiteral(literal, rowType.getFields().get(index).getType()));
                    }
                    
                    Table table = connection.getTable(tablePath);
                    Lookuper lookuper = table.newLookup().createLookuper();
                    LookupResult lookupResult = lookuper.lookup(keyRow).join();
                    List<Object[]> rows = new ArrayList<>();
                    if (lookupResult != null && lookupResult.getRowList() != null) {
                        for (InternalRow row : lookupResult.getRowList()) {
                            rows.add(extractRow(rowType, getters, selectedColumns, row));
                        }
                    }
                    
                    RowDescription description = PgRowDescriptionBuilder.fromRowType(rowType, selectedColumns);
                    return PgExecutionResult.query(new PgQueryResult(description, rows), "SELECT " + rows.size());
                }
            }
        }

        LOG.info("Using Calcite execution path for query: {}", sql);
        
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        FlussSchema flussSchema = new FlussSchema(connection, admin, database);
        rootSchema.add(database, flussSchema);

        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema.getSubSchema(database))
                .parserConfig(SqlParser.configBuilder()
                        .setLex(Lex.JAVA)  // Use JAVA lexer for PostgreSQL-compatible identifier handling
                        .setConformance(SqlConformanceEnum.PRAGMATIC_2003)  // PostgreSQL follows SQL:2003 pragmatically
                        .build())
                .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
                .programs(Programs.ofRules(
                        EnumerableRules.ENUMERABLE_PROJECT_RULE,
                        EnumerableRules.ENUMERABLE_FILTER_RULE,
                        EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                        EnumerableRules.ENUMERABLE_JOIN_RULE,
                        EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
                        EnumerableRules.ENUMERABLE_SORT_RULE,
                        EnumerableRules.ENUMERABLE_LIMIT_RULE,
                        EnumerableRules.ENUMERABLE_UNION_RULE,
                        EnumerableRules.ENUMERABLE_VALUES_RULE
                ))
                .build();

        Planner planner = Frameworks.getPlanner(config);
        SqlNode parsed = planner.parse(sql);
        SqlNode validated = planner.validate(parsed);
        RelNode rel = planner.rel(validated).project();

        RelTraitSet traitSet = planner.getEmptyTraitSet().replace(EnumerableConvention.INSTANCE);
        RelNode optimized = planner.transform(0, traitSet, rel);
        
        List<Object[]> rows = new ArrayList<>();
        try (Interpreter interpreter = new Interpreter(new DummyDataContext(), optimized)) {
            for (Object[] row : interpreter) {
                if (optimized.getRowType().getFieldCount() == 1 && !(row instanceof Object[])) {
                    rows.add(new Object[]{row});
                } else {
                    rows.add(row);
                }
            }
        }

        RowDescription description = PgRowDescriptionBuilder.fromRelType(optimized.getRowType());
        return PgExecutionResult.query(new PgQueryResult(description, rows), "SELECT " + rows.size());
    }

    private static PgExecutionResult executeLiteralSelect(SqlSelect select) {
        SelectShape shape = buildSelectShape(select);
        Object[] values = new Object[shape.names.size()];
        for (int i = 0; i < shape.expressions.size(); i++) {
            SqlLiteral literal = shape.expressions.get(i);
            values[i] = literal.getValue();
        }
        List<Object[]> rows = new ArrayList<>();
        rows.add(values);
        RelDataType rowType = shape.typeFactory.createStructType(shape.types, shape.names);
        return PgExecutionResult.query(new PgQueryResult(PgRowDescriptionBuilder.fromRelType(rowType), rows), "SELECT 1");
    }

    private static PgQueryResult describeSelect(PgSession session, SqlSelect select) throws Exception {
        if (select.getFrom() == null) {
            SelectShape shape = buildSelectShape(select);
            RelDataType rowType = shape.typeFactory.createStructType(shape.types, shape.names);
            return new PgQueryResult(PgRowDescriptionBuilder.fromRelType(rowType), List.of());
        }
        if (!(select.getFrom() instanceof SqlIdentifier)) {
            return new PgQueryResult(null, List.of());
        }
        SqlIdentifier identifier = (SqlIdentifier) select.getFrom();
        TablePath tablePath = resolveTablePath(session, identifier.toString().toLowerCase());
        TableInfo tableInfo = getTableInfo(session, tablePath);
        RowType rowType = tableInfo.getRowType();
        List<String> selectedColumns = resolveSelectedColumns(select.getSelectList(), rowType);
        return new PgQueryResult(PgRowDescriptionBuilder.fromRowType(rowType, selectedColumns), List.of());
    }

    private static PgExecutionResult executeInsert(PgSession session, SqlInsert insert) throws Exception {
        if (!(insert.getTargetTable() instanceof SqlIdentifier)) {
            throw new IllegalArgumentException("only simple insert supported");
        }
        SqlIdentifier identifier = (SqlIdentifier) insert.getTargetTable();
        TablePath tablePath = resolveTablePath(session, identifier.toString().toLowerCase());
        TableInfo tableInfo = getTableInfo(session, tablePath);
        RowType rowType = tableInfo.getRowType();
        List<String> columns = resolveInsertColumns(insert.getTargetColumnList(), rowType);
        List<List<SqlLiteral>> allRowValues = extractInsertValuesAllRows(insert.getSource());
        
        Table table = session.getFlussConnection().getTable(tablePath);
        
        int rowsInserted = 0;
        UpsertWriter writer = table.newUpsert().createWriter();
        for (List<SqlLiteral> values : allRowValues) {
                if (values.size() != columns.size()) {
                    throw new IllegalArgumentException("column/value count mismatch");
                }
                GenericRow row = new GenericRow(rowType.getFieldCount());
                for (int i = 0; i < columns.size(); i++) {
                    int index = fieldIndex(rowType, columns.get(i));
                    row.setField(index, convertLiteral(values.get(i), rowType.getFields().get(index).getType()));
                }
            writer.upsert(row).join();
            rowsInserted++;
        }
        
        return PgExecutionResult.update("INSERT 0 " + rowsInserted);
    }

    private static PgExecutionResult executeUpdate(PgSession session, SqlUpdate update) throws Exception {
        if (!(update.getTargetTable() instanceof SqlIdentifier)) {
            throw new IllegalArgumentException("only simple update supported");
        }
        SqlIdentifier identifier = (SqlIdentifier) update.getTargetTable();
        TablePath tablePath = resolveTablePath(session, identifier.toString().toLowerCase());
        TableInfo tableInfo = getTableInfo(session, tablePath);
        RowType rowType = tableInfo.getRowType();
        Map<String, SqlLiteral> predicates = new HashMap<>();
        if (update.getCondition() == null || !extractPredicates(update.getCondition(), predicates)) {
            throw new IllegalArgumentException("update requires primary key equality predicate");
        }
        List<String> primaryKeys = tableInfo.getPrimaryKeys();
        if (primaryKeys.isEmpty()) {
            throw new IllegalArgumentException("table has no primary keys for update");
        }
        if (!predicates.keySet().containsAll(primaryKeys)) {
            throw new IllegalArgumentException("update requires full primary key predicate");
        }
        GenericRow keyRow = new GenericRow(rowType.getFieldCount());
        for (String key : primaryKeys) {
            int index = fieldIndex(rowType, key);
            keyRow.setField(index, convertLiteral(predicates.get(key), rowType.getFields().get(index).getType()));
        }
        Table table = session.getFlussConnection().getTable(tablePath);
        
        Lookuper lookuper = table.newLookup().createLookuper();
        LookupResult lookupResult = lookuper.lookup(keyRow).join();
        if (lookupResult == null || lookupResult.getRowList() == null || lookupResult.getRowList().isEmpty()) {
                return PgExecutionResult.update("UPDATE 0");
            }
            InternalRow existing = lookupResult.getRowList().get(0);
            InternalRow.FieldGetter[] getters = InternalRow.createFieldGetters(rowType);
            GenericRow updated = new GenericRow(rowType.getFieldCount());
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                Object value = existing.isNullAt(i) ? null : getters[i].getFieldOrNull(existing);
                updated.setField(i, value);
            }
            for (int i = 0; i < update.getTargetColumnList().size(); i++) {
                SqlIdentifier column = (SqlIdentifier) update.getTargetColumnList().get(i);
                SqlLiteral literal = (SqlLiteral) update.getSourceExpressionList().get(i);
                int index = fieldIndex(rowType, column.getSimple());
                updated.setField(index, convertLiteral(literal, rowType.getFields().get(index).getType()));
            }
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(updated).join();
        return PgExecutionResult.update("UPDATE 1");
    }

    private static PgExecutionResult executeDelete(PgSession session, SqlDelete delete) throws Exception {
        if (!(delete.getTargetTable() instanceof SqlIdentifier)) {
            throw new IllegalArgumentException("only simple delete supported");
        }
        SqlIdentifier identifier = (SqlIdentifier) delete.getTargetTable();
        TablePath tablePath = resolveTablePath(session, identifier.toString().toLowerCase());
        TableInfo tableInfo = getTableInfo(session, tablePath);
        RowType rowType = tableInfo.getRowType();
        Map<String, SqlLiteral> predicates = new HashMap<>();
        if (delete.getCondition() == null || !extractPredicates(delete.getCondition(), predicates)) {
            throw new IllegalArgumentException("delete requires primary key equality predicate");
        }
        List<String> primaryKeys = tableInfo.getPrimaryKeys();
        if (primaryKeys.isEmpty()) {
            throw new IllegalArgumentException("table has no primary keys for delete");
        }
        if (!predicates.keySet().containsAll(primaryKeys)) {
            throw new IllegalArgumentException("delete requires full primary key predicate");
        }
        GenericRow keyRow = new GenericRow(rowType.getFieldCount());
        for (String key : primaryKeys) {
            int index = fieldIndex(rowType, key);
            keyRow.setField(index, convertLiteral(predicates.get(key), rowType.getFields().get(index).getType()));
        }
        Table table = session.getFlussConnection().getTable(tablePath);
        UpsertWriter writer = table.newUpsert().createWriter();
        writer.delete(keyRow).join();
        return PgExecutionResult.update("DELETE 1");
    }

    private static PgExecutionResult handleMetadataCommands(PgSession session, String sql) throws Exception {
        String normalized = normalizeSql(sql).toUpperCase(Locale.ROOT);
        if (normalized.startsWith("SHOW DATABASES")) {
            return PgDdlExecutor.executeShowDatabases(session);
        }
        if (normalized.startsWith("SHOW DATABASE EXISTS")) {
            return PgDdlExecutor.executeShowDatabaseExists(session, sql);
        }
        if (normalized.startsWith("SHOW DATABASE INFO")) {
            return PgDdlExecutor.executeShowDatabaseInfo(session, sql);
        }
        if (normalized.startsWith("USE DATABASE")) {
            return PgDdlExecutor.executeUseDatabase(session, sql);
        }
        if (normalized.startsWith("SHOW TABLES FROM ") || normalized.startsWith("SHOW TABLES IN ")) {
            String dbName = normalized.substring(normalized.indexOf("FROM ") >= 0 ? normalized.indexOf("FROM ") + 5 : normalized.indexOf("IN ") + 3).trim();
            dbName = dbName.replace("\"", "").replace("'", "").toLowerCase();
            return PgDdlExecutor.executeShowTables(session, dbName);
        }
        if (normalized.startsWith("SHOW TABLES")) {
            return PgDdlExecutor.executeShowTables(session, null);
        }
        if (normalized.startsWith("SHOW TABLE EXISTS")) {
            return PgDdlExecutor.executeShowTableExists(session, sql);
        }
        if (normalized.startsWith("SHOW TABLE SCHEMA")) {
            return PgDdlExecutor.executeShowTableSchema(session, sql);
        }
        if (normalized.startsWith("SHOW CREATE TABLE")) {
            return PgDdlExecutor.executeShowCreateTable(session, sql);
        }
        if (normalized.startsWith("SHOW PARTITIONS")) {
            return PgDdlExecutor.executeShowPartitions(session, sql);
        }
        if (normalized.startsWith("SHOW KV SNAPSHOT METADATA")) {
            return PgDdlExecutor.executeShowKvSnapshotMetadata(session, sql);
        }
        if (normalized.startsWith("SHOW KV SNAPSHOTS")) {
            return PgDdlExecutor.executeShowKvSnapshots(session, sql);
        }
        if (normalized.startsWith("SHOW LAKE SNAPSHOT")) {
            return PgDdlExecutor.executeShowLakeSnapshot(session, sql);
        }
        if (normalized.startsWith("SHOW OFFSETS")) {
            return PgDdlExecutor.executeShowOffsets(session, sql);
        }
        if (normalized.startsWith("SHOW SERVERS")) {
            return PgDdlExecutor.executeShowServers(session);
        }
        if (normalized.startsWith("SHOW CLUSTER CONFIGS")) {
            return PgDdlExecutor.executeShowClusterConfigs(session);
        }
        if (normalized.startsWith("ALTER CLUSTER")) {
            return PgDdlExecutor.executeAlterClusterConfigs(session, sql);
        }
        if (normalized.startsWith("REBALANCE CLUSTER")) {
            return PgDdlExecutor.executeRebalanceCluster(session, sql);
        }
        if (normalized.startsWith("SHOW REBALANCE")) {
            return PgDdlExecutor.executeShowRebalance(session);
        }
        if (normalized.startsWith("CANCEL REBALANCE")) {
            return PgDdlExecutor.executeCancelRebalance(session, sql);
        }
        if (normalized.startsWith("SHOW ACLS")) {
            return PgDdlExecutor.executeShowAcls(session, sql);
        }
        if (normalized.startsWith("CREATE ACL")) {
            return PgDdlExecutor.executeCreateAcl(session, sql);
        }
        if (normalized.startsWith("DROP ACL")) {
            return PgDdlExecutor.executeDropAcl(session, sql);
        }
        if (normalized.startsWith("DESCRIBE ") || normalized.startsWith("DESC ")) {
            String tableName = normalized.startsWith("DESCRIBE ") 
                ? normalized.substring(9).trim() 
                : normalized.substring(5).trim();
            tableName = tableName.replace("\"", "").replace("'", "").toLowerCase();
            return PgDdlExecutor.executeDescribeTable(session, tableName);
        }
        if (normalized.startsWith("CREATE DATABASE ")) {
            String dbName = normalized.substring(16).trim();
            boolean ignoreIfExists = dbName.toUpperCase().contains("IF NOT EXISTS");
            if (ignoreIfExists) {
                dbName = dbName.replaceAll("(?i)IF NOT EXISTS", "").trim();
            }
            dbName = dbName.replace("\"", "").replace("'", "").split("\\s+")[0].toLowerCase();
            return PgDdlExecutor.executeCreateDatabase(session, dbName, ignoreIfExists);
        }
        if (normalized.startsWith("DROP DATABASE ")) {
            String remainder = normalized.substring(14).trim();
            boolean ignoreIfNotExists = remainder.toUpperCase().contains("IF EXISTS");
            boolean cascade = remainder.toUpperCase().contains("CASCADE");
            String dbName = remainder.replaceAll("(?i)IF EXISTS", "")
                                     .replaceAll("(?i)CASCADE", "")
                                     .trim()
                                     .replace("\"", "").replace("'", "")
                                     .split("\\s+")[0].toLowerCase();
            return PgDdlExecutor.executeDropDatabase(session, dbName, ignoreIfNotExists, cascade);
        }
        if (normalized.startsWith("DROP TABLE ")) {
            String remainder = normalized.substring(11).trim();
            boolean ignoreIfNotExists = remainder.toUpperCase().contains("IF EXISTS");
            String tableName = remainder.replaceAll("(?i)IF EXISTS", "")
                                        .trim()
                                        .replace("\"", "").replace("'", "")
                                        .split("\\s+")[0].toLowerCase();
            return PgDdlExecutor.executeDropTable(session, tableName, ignoreIfNotExists);
        }
        if (normalized.startsWith("ALTER TABLE ")) {
            return PgDdlExecutor.executeAlterTable(session, sql);
        }
        return null;
    }

    private static PgExecutionResult handleCompatCommands(String normalized) {
        if (normalized.startsWith("SHOW TIME ZONE")) {
            PgQueryResult result = PgCompatStubResults.singleColumn("time_zone", "UTC");
            return PgExecutionResult.query(result, "SHOW");
        }
        if (normalized.startsWith("SHOW ")) {
            PgQueryResult result = PgCompatStubResults.singleColumn("show", "");
            return PgExecutionResult.query(result, "SHOW");
        }
        if (normalized.contains("PG_CATALOG.SET_CONFIG")) {
            return PgExecutionResult.update("SELECT 0");
        }
        if (normalized.startsWith("SET TIME ZONE") || normalized.startsWith("SET TIMEZONE")) {
            return PgExecutionResult.update("SET");
        }
        if (normalized.startsWith("SET ")) {
            return PgExecutionResult.update("SET");
        }
        if (normalized.contains("UNNEST(CURRENT_SCHEMAS")) {
            PgQueryResult result = PgCompatStubResults.schemas();
            return PgExecutionResult.query(result, "SELECT " + result.getRows().size());
        }
        if (normalized.contains("PG_CATALOG.PG_NAMESPACE")) {
            PgQueryResult result = PgCompatStubResults.namespaces();
            return PgExecutionResult.query(result, "SELECT " + result.getRows().size());
        }
        if (normalized.contains("PG_CATALOG.PG_CLASS")) {
            PgQueryResult result = PgCompatStubResults.tables();
            return PgExecutionResult.query(result, "SELECT " + result.getRows().size());
        }
        if (normalized.contains("PG_CATALOG.PG_TYPE")) {
            PgQueryResult result = PgCompatStubResults.pgType();
            return PgExecutionResult.query(result, "SELECT " + result.getRows().size());
        }
        if (normalized.contains("PG_CATALOG.PG_ATTRIBUTE")) {
            PgQueryResult result = PgCompatStubResults.pgAttribute();
            return PgExecutionResult.query(result, "SELECT " + result.getRows().size());
        }
        return null;
    }

    private static PgQueryResult describeCompat(String normalized) {
        if (normalized.startsWith("SHOW TIME ZONE")) {
            return PgCompatStubResults.singleColumn("time_zone", "UTC");
        }
        if (normalized.startsWith("SHOW ")) {
            return PgCompatStubResults.singleColumn("show", "");
        }
        if (normalized.startsWith("SET ")
                || normalized.startsWith("SET TIME ZONE")
                || normalized.startsWith("SET TIMEZONE")
                || normalized.contains("PG_CATALOG.SET_CONFIG")) {
            return new PgQueryResult(null, List.of());
        }
        if (normalized.contains("UNNEST(CURRENT_SCHEMAS")) {
            return PgCompatStubResults.schemas();
        }
        if (normalized.contains("PG_CATALOG.PG_NAMESPACE")) {
            return PgCompatStubResults.namespaces();
        }
        if (normalized.contains("PG_CATALOG.PG_CLASS")) {
            return PgCompatStubResults.tables();
        }
        if (normalized.contains("PG_CATALOG.PG_TYPE")) {
            return PgCompatStubResults.pgType();
        }
        if (normalized.contains("PG_CATALOG.PG_ATTRIBUTE")) {
            return PgCompatStubResults.pgAttribute();
        }
        return null;
    }

    private static SqlNode parse(String sql) {
        String normalized = normalizeSql(sql);
        SqlParser.Config parserConfig = SqlParser.configBuilder()
                .setConformance(SqlConformanceEnum.DEFAULT)
                .build();
        FrameworkConfig config = Frameworks.newConfigBuilder().parserConfig(parserConfig).build();
        Planner planner = Frameworks.getPlanner(config);
        try {
            return planner.parse(normalized);
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to parse sql", e);
        }
    }

    private static String normalizeSql(String sql) {
        String normalized = sql == null ? "" : sql.trim();
        if (normalized.endsWith(";")) {
            normalized = normalized.substring(0, normalized.length() - 1).trim();
        }
        return normalized;
    }
    
    private static String stripDatabasePrefix(String sql) {
        return sql.replaceAll("\\b(default|[a-zA-Z_][a-zA-Z0-9_]*)\\.([a-zA-Z_][a-zA-Z0-9_]*)\\b", "$2");
    }

    private static String applyParameters(String sql, List<Object> parameters) {
        if (parameters == null || parameters.isEmpty()) {
            return sql;
        }

        StringBuilder result = new StringBuilder();
        List<Integer> parameterOrder = new ArrayList<>();
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;
        String dollarTag = null;
        int pos = 0;

        while (pos < sql.length()) {
            char c = sql.charAt(pos);

            if (inLineComment) {
                result.append(c);
                if (c == '\n') {
                    inLineComment = false;
                }
                pos++;
                continue;
            }
            if (inBlockComment) {
                if (c == '*' && pos + 1 < sql.length() && sql.charAt(pos + 1) == '/') {
                    result.append("*/");
                    pos += 2;
                    inBlockComment = false;
                    continue;
                }
                result.append(c);
                pos++;
                continue;
            }
            if (dollarTag != null) {
                if (sql.startsWith(dollarTag, pos)) {
                    result.append(dollarTag);
                    pos += dollarTag.length();
                    dollarTag = null;
                    continue;
                }
                result.append(c);
                pos++;
                continue;
            }

            if (!inSingleQuote && !inDoubleQuote) {
                if (c == '-' && pos + 1 < sql.length() && sql.charAt(pos + 1) == '-') {
                    result.append("--");
                    pos += 2;
                    inLineComment = true;
                    continue;
                }
                if (c == '/' && pos + 1 < sql.length() && sql.charAt(pos + 1) == '*') {
                    result.append("/*");
                    pos += 2;
                    inBlockComment = true;
                    continue;
                }
                if (c == '$') {
                    String tag = detectDollarTag(sql, pos);
                    if (tag != null) {
                        result.append(tag);
                        pos += tag.length();
                        dollarTag = tag;
                        continue;
                    }
                    int numStart = pos + 1;
                    int numEnd = numStart;
                    while (numEnd < sql.length() && Character.isDigit(sql.charAt(numEnd))) {
                        numEnd++;
                    }
                    if (numEnd > numStart) {
                        int paramIndex = Integer.parseInt(sql.substring(numStart, numEnd));
                        if (paramIndex < 1 || paramIndex > parameters.size()) {
                            throw new IllegalArgumentException("parameter $" + paramIndex + " out of range");
                        }
                        result.append(renderLiteral(parameters.get(paramIndex - 1)));
                        pos = numEnd;
                        continue;
                    }
                }
            }

            if (c == '\'' && !inDoubleQuote) {
                result.append(c);
                if (inSingleQuote) {
                    if (pos + 1 < sql.length() && sql.charAt(pos + 1) == '\'') {
                        result.append('\'');
                        pos += 2;
                        continue;
                    }
                    inSingleQuote = false;
                    pos++;
                    continue;
                }
                inSingleQuote = true;
                pos++;
                continue;
            }
            if (c == '"' && !inSingleQuote) {
                result.append(c);
                if (inDoubleQuote) {
                    if (pos + 1 < sql.length() && sql.charAt(pos + 1) == '"') {
                        result.append('"');
                        pos += 2;
                        continue;
                    }
                    inDoubleQuote = false;
                    pos++;
                    continue;
                }
                inDoubleQuote = true;
                pos++;
                continue;
            }

            result.append(c);
            pos++;
        }

        return result.toString();
    }

    private static String renderLiteral(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        String raw = value.toString().replace("'", "''");
        return "'" + raw + "'";
    }

    private static String detectDollarTag(String sql, int start) {
        int index = start + 1;
        while (index < sql.length() && isDollarTagChar(sql.charAt(index))) {
            index++;
        }
        if (index < sql.length() && sql.charAt(index) == '$') {
            return sql.substring(start, index + 1);
        }
        return null;
    }

    private static boolean isDollarTagChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    private static List<String> resolveSelectedColumns(SqlNodeList selectList, RowType rowType) {
        if (selectList == null || selectList.isEmpty()) {
            return rowType.getFields().stream().map(DataField::getName).collect(Collectors.toList());
        }
        List<String> columns = new ArrayList<>();
        for (SqlNode item : selectList) {
            if (item instanceof SqlIdentifier) {
                SqlIdentifier identifier = (SqlIdentifier) item;
                if (identifier.isStar()) {
                    return rowType.getFields().stream().map(DataField::getName).collect(Collectors.toList());
                }
                columns.add(identifier.getSimple().toLowerCase());
            } else {
                throw new IllegalArgumentException("only column selects supported");
            }
        }
        return columns;
    }

    private static List<String> resolveInsertColumns(SqlNodeList columnList, RowType rowType) {
        if (columnList == null || columnList.isEmpty()) {
            return rowType.getFields().stream().map(DataField::getName).collect(Collectors.toList());
        }
        List<String> columns = new ArrayList<>();
        for (SqlNode node : columnList) {
            columns.add(((SqlIdentifier) node).getSimple().toLowerCase());
        }
        return columns;
    }

    private static List<SqlLiteral> extractInsertValues(SqlNode source) {
        List<List<SqlLiteral>> allRows = extractInsertValuesAllRows(source);
        if (allRows.isEmpty()) {
            throw new IllegalArgumentException("insert requires values");
        }
        return allRows.get(0);
    }

    private static List<List<SqlLiteral>> extractInsertValuesAllRows(SqlNode source) {
        List<List<SqlLiteral>> allRows = new ArrayList<>();
        
        if (source instanceof SqlBasicCall && source.getKind() == SqlKind.VALUES) {
            SqlBasicCall call = (SqlBasicCall) source;
            
            for (SqlNode operand : call.getOperandList()) {
                List<SqlLiteral> rowValues = new ArrayList<>();
                
                if (operand instanceof SqlNodeList) {
                    for (SqlNode node : (SqlNodeList) operand) {
                        rowValues.add((SqlLiteral) node);
                    }
                } else if (operand instanceof SqlBasicCall && operand.getKind() == SqlKind.ROW) {
                    SqlBasicCall rowCall = (SqlBasicCall) operand;
                    for (SqlNode rowOperand : rowCall.getOperandList()) {
                        rowValues.add((SqlLiteral) rowOperand);
                    }
                } else {
                    throw new IllegalArgumentException("unexpected row structure: " + operand.getClass().getName());
                }
                
                allRows.add(rowValues);
            }
        }
        
        if (allRows.isEmpty()) {
            throw new IllegalArgumentException("only VALUES inserts supported");
        }
        return allRows;
    }

    private static boolean extractPredicates(SqlNode node, Map<String, SqlLiteral> predicates) {
        if (node instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall) node;
            if (call.getKind() == SqlKind.AND) {
                return extractPredicates(call.operand(0), predicates)
                        && extractPredicates(call.operand(1), predicates);
            }
            if (call.getKind() == SqlKind.EQUALS) {
                SqlNode left = call.operand(0);
                SqlNode right = call.operand(1);
                if (left instanceof SqlIdentifier && right instanceof SqlLiteral) {
                    predicates.put(((SqlIdentifier) left).getSimple().toLowerCase(), (SqlLiteral) right);
                    return true;
                }
                if (right instanceof SqlIdentifier && left instanceof SqlLiteral) {
                    predicates.put(((SqlIdentifier) right).getSimple().toLowerCase(), (SqlLiteral) left);
                    return true;
                }
            }
        }
        return false;
    }

    private static SelectShape buildSelectShape(SqlSelect select) {
        SqlNodeList selectList = select.getSelectList();
        List<String> names = new ArrayList<>();
        List<RelDataType> types = new ArrayList<>();
        List<SqlLiteral> expressions = new ArrayList<>();

        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

        for (int i = 0; i < selectList.size(); i++) {
            SqlNode item = selectList.get(i);
            String name = "col" + (i + 1);
            SqlNode expression = item;
            if (item instanceof SqlBasicCall && SqlKind.AS == item.getKind()) {
                SqlBasicCall call = (SqlBasicCall) item;
                expression = call.operand(0);
                SqlNode aliasNode = call.operand(1);
                if (aliasNode instanceof SqlIdentifier) {
                    name = ((SqlIdentifier) aliasNode).getSimple();
                }
            }
            if (expression instanceof SqlBasicCall) {
                SqlBasicCall call = (SqlBasicCall) expression;
                if (call.getOperator().getName().equalsIgnoreCase("VERSION")) {
                    expression = SqlLiteral.createCharString("PostgreSQL 13.0 (Cape)", item.getParserPosition());
                }
            }
            if (!(expression instanceof SqlLiteral)) {
                throw new IllegalArgumentException("only literal select items supported");
            }
            SqlLiteral literal = (SqlLiteral) expression;
            SqlTypeName typeName = literal.getTypeName();
            RelDataType relType = typeFactory.createSqlType(typeName == null ? SqlTypeName.ANY : typeName);
            names.add(name);
            types.add(relType);
            expressions.add(literal);
        }
        return new SelectShape(names, types, expressions, typeFactory);
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

    private static TableInfo getTableInfo(PgSession session, TablePath tablePath) throws Exception {
        TableInfo tableInfo = session.getFlussAdmin().getTableInfo(tablePath).get();
        if (tableInfo == null) {
            throw new IllegalArgumentException("Table not found: " + tablePath);
        }
        return tableInfo;
    }

    private static int fieldIndex(RowType rowType, String column) {
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (rowType.getFields().get(i).getName().equalsIgnoreCase(column)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Unknown column: " + column);
    }

    private static Object[] extractRow(RowType rowType, InternalRow.FieldGetter[] getters, List<String> selectedColumns, InternalRow row) {
        Object[] result = new Object[selectedColumns.size()];
        for (int i = 0; i < selectedColumns.size(); i++) {
            int index = fieldIndex(rowType, selectedColumns.get(i));
            result[i] = row.isNullAt(index) ? null : getters[index].getFieldOrNull(row);
        }
        return result;
    }

    private static Object convertLiteral(SqlLiteral literal, DataType dataType) {
        if (literal == null || literal.getValue() == null) {
            return null;
        }
        Object value = literal.getValue();
        return convertValue(value, dataType);
    }

    private static Object convertFromText(String token, DataType dataType) {
        return convertValue(token, dataType);
    }

    private static Object convertValue(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }
        DataTypeRoot root = dataType.getTypeRoot();
        String stringValue = value.toString();
        switch (root) {
            case BOOLEAN:
                if ("t".equalsIgnoreCase(stringValue) || "true".equalsIgnoreCase(stringValue)) {
                    return true;
                }
                if ("f".equalsIgnoreCase(stringValue) || "false".equalsIgnoreCase(stringValue)) {
                    return false;
                }
                throw new IllegalArgumentException("Invalid boolean literal: " + stringValue);
            case TINYINT:
                return Byte.parseByte(stringValue);
            case SMALLINT:
                return Short.parseShort(stringValue);
            case INTEGER:
                return Integer.parseInt(stringValue);
            case BIGINT:
                return Long.parseLong(stringValue);
            case FLOAT:
                return Float.parseFloat(stringValue);
            case DOUBLE:
                return Double.parseDouble(stringValue);
            case DECIMAL:
                return new BigDecimal(stringValue);
            case DATE:
                return LocalDate.parse(stringValue);
            case TIME_WITHOUT_TIME_ZONE:
                return LocalTime.parse(stringValue);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return LocalDateTime.parse(stringValue.replace(' ', 'T'));
            case BYTES:
            case BINARY:
                return parseBytea(stringValue);
            case STRING:
            case CHAR:
                return BinaryString.fromString(stringValue);
            default:
                return stringValue;
        }
    }

    private static byte[] parseBytea(String value) {
        if (value.startsWith("\\x")) {
            String hex = value.substring(2);
            int len = hex.length();
            byte[] data = new byte[len / 2];
            for (int i = 0; i < len; i += 2) {
                data[i / 2] = (byte) Integer.parseInt(hex.substring(i, i + 2), 16);
            }
            return data;
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();
    
    private static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int v = bytes[i] & 0xFF;
            hexChars[i * 2] = HEX_ARRAY[v >>> 4];
            hexChars[i * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }
    
    private static byte[] encodeValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            return ("\\x" + bytesToHex(bytes)).getBytes(StandardCharsets.UTF_8);
        }
        return Objects.toString(value).getBytes(StandardCharsets.UTF_8);
    }

    private static final ThreadLocal<ByteBuffer> BUFFER_CACHE = 
        ThreadLocal.withInitial(() -> ByteBuffer.allocate(8));
    
    private static byte[] encodeBinaryValue(Object value, RowDescription description, int columnIndex) {
        if (value == null) {
            return null;
        }
        int oid = description.getFields().get(columnIndex).getTypeOid();
        ByteBuffer buf = BUFFER_CACHE.get();
        switch (oid) {
            case 16:
                return new byte[]{(byte) (toBoolean(value) ? 1 : 0)};
            case 21:
                buf.clear();
                buf.putShort(((Number) toNumber(value)).shortValue());
                return java.util.Arrays.copyOfRange(buf.array(), 0, 2);
            case 23:
                buf.clear();
                buf.putInt(((Number) toNumber(value)).intValue());
                return java.util.Arrays.copyOfRange(buf.array(), 0, 4);
            case 20:
                buf.clear();
                buf.putLong(((Number) toNumber(value)).longValue());
                return java.util.Arrays.copyOfRange(buf.array(), 0, 8);
            case 700:
                buf.clear();
                buf.putFloat(((Number) toNumber(value)).floatValue());
                return java.util.Arrays.copyOfRange(buf.array(), 0, 4);
            case 701:
                buf.clear();
                buf.putDouble(((Number) toNumber(value)).doubleValue());
                return java.util.Arrays.copyOfRange(buf.array(), 0, 8);
            case 1700:
                return encodeNumeric(value);
            case 1082:
                return encodeDate(value);
            case 1083:
                return encodeTime(value);
            case 1114:
                return encodeTimestamp(value);
            case 17:
                return encodeBytea(value);
            case 1043:
            default:
                return encodeTextBinary(value);
        }
    }

    private static Number toNumber(Object value) {
        if (value instanceof Number) {
            return (Number) value;
        }
        return new BigDecimal(value.toString());
    }

    private static boolean toBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        String normalized = value.toString().trim();
        if ("t".equalsIgnoreCase(normalized) || "true".equalsIgnoreCase(normalized) || "1".equals(normalized)) {
            return true;
        }
        if ("f".equalsIgnoreCase(normalized) || "false".equalsIgnoreCase(normalized) || "0".equals(normalized)) {
            return false;
        }
        throw new IllegalArgumentException("Invalid boolean value: " + value);
    }

    private static byte[] encodeTextBinary(Object value) {
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] encodeBytea(Object value) {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        return parseBytea(value.toString());
    }

    private static byte[] encodeDate(Object value) {
        LocalDate date = value instanceof LocalDate ? (LocalDate) value : LocalDate.parse(value.toString());
        long days = date.toEpochDay() - LocalDate.of(2000, 1, 1).toEpochDay();
        return ByteBuffer.allocate(4).putInt((int) days).array();
    }

    private static byte[] encodeTime(Object value) {
        LocalTime time = value instanceof LocalTime ? (LocalTime) value : LocalTime.parse(value.toString());
        long micros = time.toNanoOfDay() / 1000;
        return ByteBuffer.allocate(8).putLong(micros).array();
    }

    private static byte[] encodeTimestamp(Object value) {
        LocalDateTime dateTime = value instanceof LocalDateTime
                ? (LocalDateTime) value
                : LocalDateTime.parse(value.toString().replace(' ', 'T'));
        long micros = ChronoUnit.MICROS.between(LocalDateTime.of(2000, 1, 1, 0, 0), dateTime);
        return ByteBuffer.allocate(8).putLong(micros).array();
    }

    private static byte[] encodeNumeric(Object value) {
        BigDecimal decimal = value instanceof BigDecimal ? (BigDecimal) value : new BigDecimal(value.toString());
        byte[] bytes = decimal.toPlainString().getBytes(StandardCharsets.UTF_8);
        return bytes;
    }


    private static PgExecutionResult handleCreateTable(PgSession session, String sql) throws Exception {
        Pattern pattern = Pattern.compile(
            "(?i)CREATE\\s+TABLE\\s+([a-zA-Z0-9_\\.]+)\\s*\\((.+)\\)\\s*;?",
            Pattern.DOTALL
        );
        Matcher matcher = pattern.matcher(sql.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid CREATE TABLE syntax");
        }
        
        String tableName = matcher.group(1).toLowerCase();
        String columnsPart = matcher.group(2);
        
        List<PgDdlExecutor.ColumnDef> columns = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();
        
        String[] parts = columnsPart.split(",");
        for (String part : parts) {
            String trimmed = part.trim();
            
            if (trimmed.toUpperCase().startsWith("PRIMARY KEY")) {
                Pattern pkPattern = Pattern.compile("(?i)PRIMARY\\s+KEY\\s*\\(([^)]+)\\)");
                Matcher pkMatcher = pkPattern.matcher(trimmed);
                if (pkMatcher.find()) {
                    String pkCols = pkMatcher.group(1);
                    for (String pk : pkCols.split(",")) {
                        primaryKeys.add(pk.trim().toLowerCase());
                    }
                }
                continue;
            }
            
            String[] tokens = trimmed.split("\\s+", 2);
            if (tokens.length < 2) {
                continue;
            }
            
            String colName = tokens[0].toLowerCase();
            String typeStr = tokens[1].toUpperCase();
            
            boolean isColumnPrimaryKey = typeStr.matches(".*\\bPRIMARY\\s+KEY\\b.*");
            
            typeStr = typeStr.replaceAll("(?i)\\s+PRIMARY\\s+KEY.*", "")
                             .replaceAll("(?i)\\s+NOT\\s+NULL.*", "")
                             .trim();
            
            if (isColumnPrimaryKey && !primaryKeys.contains(colName)) {
                primaryKeys.add(colName);
            }
            
            DataType dataType = parseDataType(typeStr);
            columns.add(new PgDdlExecutor.ColumnDef(colName, dataType));
        }
        
        return PgDdlExecutor.executeCreateTable(session, tableName, columns, primaryKeys);
    }

    private static DataType parseDataType(String typeStr) {
        typeStr = typeStr.toUpperCase().trim();
        
        if (typeStr.startsWith("VARCHAR") || typeStr.startsWith("TEXT") || typeStr.startsWith("CHAR")) {
            return org.apache.fluss.types.DataTypes.STRING();
        }
        if (typeStr.startsWith("INT") || typeStr.equals("INTEGER")) {
            return org.apache.fluss.types.DataTypes.INT();
        }
        if (typeStr.startsWith("BIGINT")) {
            return org.apache.fluss.types.DataTypes.BIGINT();
        }
        if (typeStr.startsWith("SMALLINT")) {
            return org.apache.fluss.types.DataTypes.SMALLINT();
        }
        if (typeStr.startsWith("BOOLEAN") || typeStr.startsWith("BOOL")) {
            return org.apache.fluss.types.DataTypes.BOOLEAN();
        }
        if (typeStr.startsWith("DECIMAL") || typeStr.startsWith("NUMERIC")) {
            return org.apache.fluss.types.DataTypes.DECIMAL(38, 18);
        }
        if (typeStr.startsWith("TIMESTAMP")) {
            return org.apache.fluss.types.DataTypes.TIMESTAMP_LTZ(3);
        }
        if (typeStr.startsWith("DATE")) {
            return org.apache.fluss.types.DataTypes.DATE();
        }
        if (typeStr.startsWith("BYTEA") || typeStr.startsWith("BINARY")) {
            return org.apache.fluss.types.DataTypes.BYTES();
        }
        
        throw new IllegalArgumentException("Unsupported data type: " + typeStr);
    }

    private static final class SelectShape {
        private final List<String> names;
        private final List<RelDataType> types;
        private final List<SqlLiteral> expressions;
        private final RelDataTypeFactory typeFactory;

        private SelectShape(
                List<String> names,
                List<RelDataType> types,
                List<SqlLiteral> expressions,
                RelDataTypeFactory typeFactory) {
            this.names = names;
            this.types = types;
            this.expressions = expressions;
            this.typeFactory = typeFactory;
        }
    }

    private static final class DummyDataContext implements org.apache.calcite.DataContext {
        @Override public SchemaPlus getRootSchema() { return null; }
        @Override public JavaTypeFactory getTypeFactory() { return new JavaTypeFactoryImpl(); }
        @Override public QueryProvider getQueryProvider() { return null; }
        @Override public Object get(String name) { return null; }
    }
}
