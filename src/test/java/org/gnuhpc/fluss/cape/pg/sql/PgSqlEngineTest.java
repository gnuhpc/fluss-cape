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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookup;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.gnuhpc.fluss.cape.pg.session.PgSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;

@ExtendWith(MockitoExtension.class)
class PgSqlEngineTest {

    @Mock private PgSession mockSession;
    @Mock private Admin mockAdmin;
    @Mock private TableInfo mockTableInfo;
    @Mock private Connection mockConnection;
    @Mock private Table mockTable;

    private RowType testRowType;
    private TablePath testTablePath;

    @BeforeEach
    void setUp() {
        testRowType = RowType.builder()
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .build();
        testTablePath = TablePath.of("testdb", "testtable");

        lenient().when(mockSession.getFlussAdmin()).thenReturn(mockAdmin);
        lenient().when(mockSession.getDatabase()).thenReturn("testdb");
        lenient().when(mockSession.getFlussConnection()).thenReturn(mockConnection);
    }

    // ========== VALIDATION TESTS ==========

    @Test
    void testValidate_selectQuery_shouldSucceed() {
        PgSqlEngine.validate("SELECT * FROM users");
    }

    @Test
    void testValidate_insertQuery_shouldSucceed() {
        PgSqlEngine.validate("INSERT INTO users (id, name) VALUES (1, 'Alice')");
    }

    @Test
    void testValidate_updateQuery_shouldSucceed() {
        PgSqlEngine.validate("UPDATE users SET name = 'Bob' WHERE id = 1");
    }

    @Test
    void testValidate_deleteQuery_shouldSucceed() {
        PgSqlEngine.validate("DELETE FROM users WHERE id = 1");
    }

    @Test
    void testValidate_showDatabasesCommand_shouldSucceed() {
        PgSqlEngine.validate("SHOW DATABASES");
    }

    @Test
    void testValidate_showTablesCommand_shouldSucceed() {
        PgSqlEngine.validate("SHOW TABLES");
    }

    @Test
    void testValidate_describeCommand_shouldSucceed() {
        PgSqlEngine.validate("DESCRIBE users");
        PgSqlEngine.validate("DESC users");
    }

    @Test
    void testValidate_createDatabaseCommand_shouldSucceed() {
        PgSqlEngine.validate("CREATE DATABASE testdb");
    }

    @Test
    void testValidate_dropTableCommand_shouldSucceed() {
        PgSqlEngine.validate("DROP TABLE users");
    }

    @Test
    void testValidate_compatibilityCommands_shouldSucceed() {
        PgSqlEngine.validate("SET TIME ZONE 'UTC'");
        PgSqlEngine.validate("SHOW TIME ZONE");
    }

    @Test
    void testValidate_unsupportedQuery_shouldThrowException() {
        assertThatThrownBy(() -> PgSqlEngine.validate("ALTER TABLE users ADD COLUMN age INT"))
                .isInstanceOf(Exception.class);
    }

    // ========== DECODE PARAMETERS TESTS ==========

    @Test
    void testDecodeParameters_textFormat() {
        byte[] param1 = "value1".getBytes(StandardCharsets.UTF_8);
        byte[] param2 = "value2".getBytes(StandardCharsets.UTF_8);
        List<byte[]> parameters = Arrays.asList(param1, param2);
        short[] formatCodes = {0};

        List<Object> decoded = PgSqlEngine.decodeParameters(parameters, formatCodes);

        assertThat(decoded).hasSize(2);
        assertThat(decoded.get(0)).isEqualTo("value1");
        assertThat(decoded.get(1)).isEqualTo("value2");
    }

    @Test
    void testDecodeParameters_withNullValue() {
        byte[] param1 = "value1".getBytes(StandardCharsets.UTF_8);
        List<byte[]> parameters = Arrays.asList(param1, null);
        short[] formatCodes = {0};

        List<Object> decoded = PgSqlEngine.decodeParameters(parameters, formatCodes);

        assertThat(decoded).hasSize(2);
        assertThat(decoded.get(0)).isEqualTo("value1");
        assertThat(decoded.get(1)).isNull();
    }

    @Test
    void testDecodeParameters_binaryFormat_shouldThrowException() {
        byte[] param1 = "value1".getBytes(StandardCharsets.UTF_8);
        List<byte[]> parameters = Collections.singletonList(param1);
        short[] formatCodes = {1};

        assertThatThrownBy(() -> PgSqlEngine.decodeParameters(parameters, formatCodes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("binary parameters not supported");
    }

    @Test
    void testDecodeParameters_multipleFormatCodes() {
        byte[] param1 = "value1".getBytes(StandardCharsets.UTF_8);
        byte[] param2 = "value2".getBytes(StandardCharsets.UTF_8);
        List<byte[]> parameters = Arrays.asList(param1, param2);
        short[] formatCodes = {0, 0};

        List<Object> decoded = PgSqlEngine.decodeParameters(parameters, formatCodes);

        assertThat(decoded).hasSize(2);
        assertThat(decoded.get(0)).isEqualTo("value1");
        assertThat(decoded.get(1)).isEqualTo("value2");
    }

    // ========== LITERAL SELECT TESTS ==========

    @Test
    void testExecute_literalSelect() throws Exception {
        String sql = "SELECT 1 AS num, 'hello' AS text";

        PgExecutionResult result = PgSqlEngine.execute(mockSession, sql, Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.getQueryResult()).isNotNull();
        assertThat(result.getQueryResult().getRows()).hasSize(1);
        assertThat(result.getCommandTag()).startsWith("SELECT");
    }

    @Test
    void testExecute_literalSelectWithNumbers() throws Exception {
        String sql = "SELECT 42";

        PgExecutionResult result = PgSqlEngine.execute(mockSession, sql, Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.getQueryResult()).isNotNull();
        assertThat(result.getQueryResult().getRows()).hasSize(1);
    }

    // ========== COMPATIBILITY COMMAND TESTS ==========

    @Test
    void testExecute_showTimeZone() throws Exception {
        String sql = "SHOW TIME ZONE";

        PgExecutionResult result = PgSqlEngine.execute(mockSession, sql, Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.getQueryResult()).isNotNull();
        assertThat(result.getQueryResult().getRows()).hasSize(1);
        assertThat(result.getQueryResult().getRows().get(0)[0]).isEqualTo("UTC");
    }

    @Test
    void testExecute_setTimeZone() throws Exception {
        String sql = "SET TIME ZONE 'UTC'";

        PgExecutionResult result = PgSqlEngine.execute(mockSession, sql, Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.getCommandTag()).isEqualTo("SET");
    }

    @Test
    void testExecute_setCommand() throws Exception {
        String sql = "SET client_encoding = 'UTF8'";

        PgExecutionResult result = PgSqlEngine.execute(mockSession, sql, Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.getCommandTag()).isEqualTo("SET");
    }

    @Test
    void testExecute_setCommand_caseInsensitive() throws Exception {
        String sql = "set client_encoding = 'UTF8'";

        PgExecutionResult result = PgSqlEngine.execute(mockSession, sql, Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.getCommandTag()).isEqualTo("SET");
    }

    // ========== DESCRIBE TESTS ==========

    @Test
    void testDescribe_literalSelect() throws Exception {
        String sql = "SELECT 1 AS num";

        PgQueryResult result = PgSqlEngine.describe(mockSession, sql);

        assertThat(result).isNotNull();
        assertThat(result.getRowDescription()).isNotNull();
    }

    @Test
    void testDescribe_metadataCommand() throws Exception {
        String sql = "SHOW DATABASES";

        PgQueryResult result = PgSqlEngine.describe(mockSession, sql);

        assertThat(result).isNotNull();
    }

    @Test
    void testDescribe_setCommand_returnsEmptyResult() throws Exception {
        String sql = "SET TIME ZONE 'UTC'";

        PgQueryResult result = PgSqlEngine.describe(mockSession, sql);

        assertThat(result).isNotNull();
        assertThat(result.getRowDescription()).isNull();
        assertThat(result.getRows()).isEmpty();
    }

    // ========== METADATA COMMAND ROUTING TESTS ==========

    @Test
    void testExecute_showDatabases_routesToDdlExecutor() throws Exception {
        when(mockAdmin.listDatabases()).thenReturn(CompletableFuture.completedFuture(Arrays.asList("db1", "db2")));

        PgExecutionResult result = PgSqlEngine.execute(mockSession, "SHOW DATABASES", Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isTrue();
        verify(mockAdmin).listDatabases();
    }

    @Test
    void testExecute_showTables_routesToDdlExecutor() throws Exception {
        when(mockAdmin.listTables("testdb")).thenReturn(CompletableFuture.completedFuture(Collections.singletonList("table1")));

        PgExecutionResult result = PgSqlEngine.execute(mockSession, "SHOW TABLES", Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isTrue();
        verify(mockAdmin).listTables("testdb");
    }

    @Test
    void testExecute_showTablesFrom_routesToDdlExecutor() throws Exception {
        when(mockAdmin.listTables("otherdb")).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));

        PgExecutionResult result = PgSqlEngine.execute(mockSession, "SHOW TABLES FROM otherdb", Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isTrue();
        verify(mockAdmin).listTables("otherdb");
    }

    @Test
    void testExecute_showTablesIn_routesToDdlExecutor() throws Exception {
        when(mockAdmin.listTables("anotherdb")).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));

        PgExecutionResult result = PgSqlEngine.execute(mockSession, "SHOW TABLES IN anotherdb", Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isTrue();
        verify(mockAdmin).listTables("anotherdb");
    }

    @Test
    void testExecute_describeTable_routesToDdlExecutor() throws Exception {
        when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
        when(mockTableInfo.getRowType()).thenReturn(testRowType);
        when(mockTableInfo.getPrimaryKeys()).thenReturn(Collections.singletonList("id"));

        PgExecutionResult result = PgSqlEngine.execute(mockSession, "DESCRIBE testtable", Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isTrue();
        verify(mockAdmin).getTableInfo(testTablePath);
    }

    @Test
    void testExecute_descTable_shortForm_routesToDdlExecutor() throws Exception {
        when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
        when(mockTableInfo.getRowType()).thenReturn(testRowType);
        when(mockTableInfo.getPrimaryKeys()).thenReturn(Collections.singletonList("id"));

        PgExecutionResult result = PgSqlEngine.execute(mockSession, "DESC testtable", Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isTrue();
        verify(mockAdmin).getTableInfo(testTablePath);
    }

    // ========== CASE SENSITIVITY TESTS ==========

    @Test
    void testExecute_mixedCaseCommands_normalized() throws Exception {
        when(mockAdmin.listDatabases()).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));

        PgExecutionResult result = PgSqlEngine.execute(mockSession, "show databases", Collections.emptyList());

        assertThat(result).isNotNull();
        verify(mockAdmin).listDatabases();
    }

    @Test
    void testExecute_mixedCaseTableName_lowercase() throws Exception {
        TablePath mixedCasePath = TablePath.of("testdb", "testtable");
        when(mockAdmin.getTableInfo(mixedCasePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
        when(mockTableInfo.getRowType()).thenReturn(testRowType);
        when(mockTableInfo.getPrimaryKeys()).thenReturn(Collections.emptyList());

        PgExecutionResult result = PgSqlEngine.execute(mockSession, "DESCRIBE TestTable", Collections.emptyList());

        assertThat(result).isNotNull();
        verify(mockAdmin).getTableInfo(mixedCasePath);
    }

    // ========== SELECT WITH LOOKUP TESTS ==========

    @Test
    void testExecute_selectWithFullPrimaryKey_usesLookup() throws Exception {
        // Setup table with primary key
        RowType rowType = RowType.builder()
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .build();
        when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
        when(mockTableInfo.getRowType()).thenReturn(rowType);
        when(mockTableInfo.getPrimaryKeys()).thenReturn(Collections.singletonList("id"));
        when(mockConnection.getTable(testTablePath)).thenReturn(mockTable);

        // Setup lookup to return a row
        Lookup mockLookup = mock(Lookup.class);
        Lookuper mockLookuper = mock(Lookuper.class);
        LookupResult mockLookupResult = mock(LookupResult.class);
        InternalRow mockRow = mock(InternalRow.class);
        
        when(mockTable.newLookup()).thenReturn(mockLookup);
        when(mockLookup.createLookuper()).thenReturn(mockLookuper);
        when(mockLookuper.lookup(any(InternalRow.class)))
                .thenReturn(CompletableFuture.completedFuture(mockLookupResult));
        when(mockLookupResult.getRowList()).thenReturn(Collections.singletonList(mockRow));
        
        // Configure mock row
        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.isNullAt(1)).thenReturn(false);
        when(mockRow.getInt(0)).thenReturn(1);
        when(mockRow.getString(1)).thenReturn(BinaryString.fromString("Alice"));

        PgExecutionResult result = PgSqlEngine.execute(mockSession, 
                "SELECT * FROM testtable WHERE id = 1", Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isTrue();
        assertThat(result.getQueryResult().getRows()).hasSize(1);
        verify(mockLookuper).lookup(any(InternalRow.class));
    }

    @Test
    void testExecute_selectWithFullPrimaryKey_noMatch_returnsEmpty() throws Exception {
        // Setup table with primary key
        RowType rowType = RowType.builder()
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .build();
        when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
        when(mockTableInfo.getRowType()).thenReturn(rowType);
        when(mockTableInfo.getPrimaryKeys()).thenReturn(Collections.singletonList("id"));
        when(mockConnection.getTable(testTablePath)).thenReturn(mockTable);

        // Setup lookup to return null
        Lookup mockLookup = mock(Lookup.class);
        Lookuper mockLookuper = mock(Lookuper.class);
        
        when(mockTable.newLookup()).thenReturn(mockLookup);
        when(mockLookup.createLookuper()).thenReturn(mockLookuper);
        when(mockLookuper.lookup(any(InternalRow.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgSqlEngine.execute(mockSession, 
                "SELECT * FROM testtable WHERE id = 999", Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isTrue();
        assertThat(result.getQueryResult().getRows()).isEmpty();
        verify(mockLookuper).lookup(any(InternalRow.class));
    }

    // ========== INSERT TESTS ==========

    @Test
    void testExecute_insertWithExplicitColumns() throws Exception {
        RowType rowType = RowType.builder()
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .build();
        when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
        when(mockTableInfo.getRowType()).thenReturn(rowType);
        when(mockConnection.getTable(testTablePath)).thenReturn(mockTable);

        Upsert mockUpsert = mock(Upsert.class);
        UpsertWriter mockWriter = mock(UpsertWriter.class);
        when(mockTable.newUpsert()).thenReturn(mockUpsert);
        when(mockUpsert.createWriter()).thenReturn(mockWriter);
        when(mockWriter.upsert(any(GenericRow.class))).thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgSqlEngine.execute(mockSession,
                "INSERT INTO testtable (id, name, age) VALUES (1, 'Alice', 30)", Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isFalse();
        assertThat(result.getCommandTag()).isEqualTo("INSERT 0 1");
        verify(mockWriter).upsert(any(GenericRow.class));
    }

    @Test
    void testExecute_insertWithPartialColumns() throws Exception {
        RowType rowType = RowType.builder()
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .build();
        when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
        when(mockTableInfo.getRowType()).thenReturn(rowType);
        when(mockConnection.getTable(testTablePath)).thenReturn(mockTable);

        Upsert mockUpsert = mock(Upsert.class);
        UpsertWriter mockWriter = mock(UpsertWriter.class);
        when(mockTable.newUpsert()).thenReturn(mockUpsert);
        when(mockUpsert.createWriter()).thenReturn(mockWriter);
        when(mockWriter.upsert(any(GenericRow.class))).thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgSqlEngine.execute(mockSession,
                "INSERT INTO testtable (id, name) VALUES (2, 'Bob')", Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isFalse();
        assertThat(result.getCommandTag()).isEqualTo("INSERT 0 1");
        verify(mockWriter).upsert(any(GenericRow.class));
    }

    @Test
    void testExecute_insertWithNullValue() throws Exception {
        RowType rowType = RowType.builder()
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .build();
        when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
        when(mockTableInfo.getRowType()).thenReturn(rowType);
        when(mockConnection.getTable(testTablePath)).thenReturn(mockTable);

        Upsert mockUpsert = mock(Upsert.class);
        UpsertWriter mockWriter = mock(UpsertWriter.class);
        when(mockTable.newUpsert()).thenReturn(mockUpsert);
        when(mockUpsert.createWriter()).thenReturn(mockWriter);
        when(mockWriter.upsert(any(GenericRow.class))).thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgSqlEngine.execute(mockSession,
                "INSERT INTO testtable (id, name) VALUES (3, NULL)", Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isFalse();
        verify(mockWriter).upsert(any(GenericRow.class));
    }

    @Test
    void testExecute_insertWithStringValue() throws Exception {
        RowType rowType = RowType.builder()
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .build();
        when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
        when(mockTableInfo.getRowType()).thenReturn(rowType);
        when(mockConnection.getTable(testTablePath)).thenReturn(mockTable);

        Upsert mockUpsert = mock(Upsert.class);
        UpsertWriter mockWriter = mock(UpsertWriter.class);
        when(mockTable.newUpsert()).thenReturn(mockUpsert);
        when(mockUpsert.createWriter()).thenReturn(mockWriter);
        when(mockWriter.upsert(any(GenericRow.class))).thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgSqlEngine.execute(mockSession,
                "INSERT INTO testtable VALUES (4, 'Charlie')", Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isFalse();
        verify(mockWriter).upsert(any(GenericRow.class));
    }

    @Test
    void testExecute_insertWithMultipleRows() throws Exception {
        RowType rowType = RowType.builder()
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .build();
        when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
        when(mockTableInfo.getRowType()).thenReturn(rowType);
        when(mockConnection.getTable(testTablePath)).thenReturn(mockTable);

        Upsert mockUpsert = mock(Upsert.class);
        UpsertWriter mockWriter = mock(UpsertWriter.class);
        when(mockTable.newUpsert()).thenReturn(mockUpsert);
        when(mockUpsert.createWriter()).thenReturn(mockWriter);
        when(mockWriter.upsert(any(GenericRow.class))).thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgSqlEngine.execute(mockSession,
                "INSERT INTO testtable VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isFalse();
        assertThat(result.getCommandTag()).isEqualTo("INSERT 0 3");
        verify(mockWriter, times(3)).upsert(any(GenericRow.class));
    }

    // ========== UPDATE TESTS ==========

    @Test
    void testExecute_updateWithPrimaryKey() throws Exception {
        RowType rowType = RowType.builder()
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .build();
        when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
        when(mockTableInfo.getRowType()).thenReturn(rowType);
        when(mockTableInfo.getPrimaryKeys()).thenReturn(Collections.singletonList("id"));
        when(mockConnection.getTable(testTablePath)).thenReturn(mockTable);

        // Setup lookup to find existing row
        Lookup mockLookup = mock(Lookup.class);
        Lookuper mockLookuper = mock(Lookuper.class);
        LookupResult mockLookupResult = mock(LookupResult.class);
        InternalRow mockRow = mock(InternalRow.class);

        when(mockTable.newLookup()).thenReturn(mockLookup);
        when(mockLookup.createLookuper()).thenReturn(mockLookuper);
        when(mockLookuper.lookup(any(InternalRow.class)))
                .thenReturn(CompletableFuture.completedFuture(mockLookupResult));
        when(mockLookupResult.getRowList()).thenReturn(Collections.singletonList(mockRow));

        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.isNullAt(1)).thenReturn(false);
        when(mockRow.isNullAt(2)).thenReturn(false);
        when(mockRow.getInt(0)).thenReturn(1);
        when(mockRow.getString(1)).thenReturn(BinaryString.fromString("Alice"));
        when(mockRow.getInt(2)).thenReturn(30);

        // Setup upsert writer
        Upsert mockUpsert = mock(Upsert.class);
        UpsertWriter mockWriter = mock(UpsertWriter.class);
        when(mockTable.newUpsert()).thenReturn(mockUpsert);
        when(mockUpsert.createWriter()).thenReturn(mockWriter);
        when(mockWriter.upsert(any(GenericRow.class))).thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgSqlEngine.execute(mockSession,
                "UPDATE testtable SET name = 'Bob' WHERE id = 1", Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isFalse();
        assertThat(result.getCommandTag()).isEqualTo("UPDATE 1");
        verify(mockLookuper).lookup(any(InternalRow.class));
        verify(mockWriter).upsert(any(GenericRow.class));
    }

    @Test
    void testExecute_updateNonExistentRow_returnsZero() throws Exception {
        RowType rowType = RowType.builder()
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .build();
        when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
        when(mockTableInfo.getRowType()).thenReturn(rowType);
        when(mockTableInfo.getPrimaryKeys()).thenReturn(Collections.singletonList("id"));
        when(mockConnection.getTable(testTablePath)).thenReturn(mockTable);

        // Setup lookup to return null (no row found)
        Lookup mockLookup = mock(Lookup.class);
        Lookuper mockLookuper = mock(Lookuper.class);

        when(mockTable.newLookup()).thenReturn(mockLookup);
        when(mockLookup.createLookuper()).thenReturn(mockLookuper);
        when(mockLookuper.lookup(any(InternalRow.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgSqlEngine.execute(mockSession,
                "UPDATE testtable SET name = 'Bob' WHERE id = 999", Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isFalse();
        assertThat(result.getCommandTag()).isEqualTo("UPDATE 0");
        verify(mockLookuper).lookup(any(InternalRow.class));
    }

    @Test
    void testExecute_updateWithoutPrimaryKey_throwsException() throws Exception {
        RowType rowType = RowType.builder()
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .build();
        lenient().when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
        lenient().when(mockTableInfo.getRowType()).thenReturn(rowType);
        lenient().when(mockTableInfo.getPrimaryKeys()).thenReturn(Collections.emptyList());

        assertThatThrownBy(() -> PgSqlEngine.execute(mockSession,
                "UPDATE testtable SET name = 'Bob' WHERE id = 1", Collections.emptyList()))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("no primary keys");
    }

    @Test
    void testExecute_updateWithoutWhereClause_throwsException() throws Exception {
        RowType rowType = RowType.builder()
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .build();
        lenient().when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
        lenient().when(mockTableInfo.getRowType()).thenReturn(rowType);
        lenient().when(mockTableInfo.getPrimaryKeys()).thenReturn(Collections.singletonList("id"));

        assertThatThrownBy(() -> PgSqlEngine.execute(mockSession,
                "UPDATE testtable SET name = 'Bob'", Collections.emptyList()))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("primary key equality predicate");
    }

    @Test
    void testExecute_updateWithPartialPrimaryKey_throwsException() throws Exception {
        RowType rowType = RowType.builder()
                .field("id", DataTypes.INT())
                .field("region", DataTypes.STRING())
                .field("name", DataTypes.STRING())
                .build();
        when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
        when(mockTableInfo.getRowType()).thenReturn(rowType);
        when(mockTableInfo.getPrimaryKeys()).thenReturn(Arrays.asList("id", "region"));

        assertThatThrownBy(() -> PgSqlEngine.execute(mockSession,
                "UPDATE testtable SET name = 'Bob' WHERE id = 1", Collections.emptyList()))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("full primary key predicate");
    }

    // ========== DELETE TESTS ==========

    @Test
    void testExecute_deleteWithPrimaryKey() throws Exception {
        RowType rowType = RowType.builder()
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .build();
        when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
        when(mockTableInfo.getRowType()).thenReturn(rowType);
        when(mockTableInfo.getPrimaryKeys()).thenReturn(Collections.singletonList("id"));
        when(mockConnection.getTable(testTablePath)).thenReturn(mockTable);

        Upsert mockUpsert = mock(Upsert.class);
        UpsertWriter mockWriter = mock(UpsertWriter.class);
        when(mockTable.newUpsert()).thenReturn(mockUpsert);
        when(mockUpsert.createWriter()).thenReturn(mockWriter);
        when(mockWriter.delete(any(GenericRow.class))).thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgSqlEngine.execute(mockSession,
                "DELETE FROM testtable WHERE id = 1", Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isFalse();
        assertThat(result.getCommandTag()).isEqualTo("DELETE 1");
        verify(mockWriter).delete(any(GenericRow.class));
    }

    @Test
    void testExecute_deleteWithCompositePrimaryKey() throws Exception {
        RowType rowType = RowType.builder()
                .field("id", DataTypes.INT())
                .field("region", DataTypes.STRING())
                .field("name", DataTypes.STRING())
                .build();
        when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
        when(mockTableInfo.getRowType()).thenReturn(rowType);
        when(mockTableInfo.getPrimaryKeys()).thenReturn(Arrays.asList("id", "region"));
        when(mockConnection.getTable(testTablePath)).thenReturn(mockTable);

        Upsert mockUpsert = mock(Upsert.class);
        UpsertWriter mockWriter = mock(UpsertWriter.class);
        when(mockTable.newUpsert()).thenReturn(mockUpsert);
        when(mockUpsert.createWriter()).thenReturn(mockWriter);
        when(mockWriter.delete(any(GenericRow.class))).thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgSqlEngine.execute(mockSession,
                "DELETE FROM testtable WHERE id = 1 AND region = 'US'", Collections.emptyList());

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isFalse();
        assertThat(result.getCommandTag()).isEqualTo("DELETE 1");
        verify(mockWriter).delete(any(GenericRow.class));
    }

    @Test
    void testExecute_deleteWithoutPrimaryKey_throwsException() throws Exception {
        RowType rowType = RowType.builder()
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .build();
        lenient().when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
        lenient().when(mockTableInfo.getRowType()).thenReturn(rowType);
        lenient().when(mockTableInfo.getPrimaryKeys()).thenReturn(Collections.emptyList());

        assertThatThrownBy(() -> PgSqlEngine.execute(mockSession,
                "DELETE FROM testtable WHERE id = 1", Collections.emptyList()))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("no primary keys");
    }

    @Test
    void testExecute_deleteWithoutWhereClause_throwsException() throws Exception {
        RowType rowType = RowType.builder()
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .build();
        lenient().when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
        lenient().when(mockTableInfo.getRowType()).thenReturn(rowType);
        lenient().when(mockTableInfo.getPrimaryKeys()).thenReturn(Collections.singletonList("id"));

        assertThatThrownBy(() -> PgSqlEngine.execute(mockSession,
                "DELETE FROM testtable", Collections.emptyList()))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("primary key equality predicate");
    }

    @Test
    void testExecute_deleteWithPartialPrimaryKey_throwsException() throws Exception {
        RowType rowType = RowType.builder()
                .field("id", DataTypes.INT())
                .field("region", DataTypes.STRING())
                .field("name", DataTypes.STRING())
                .build();
        lenient().when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
        lenient().when(mockTableInfo.getRowType()).thenReturn(rowType);
        lenient().when(mockTableInfo.getPrimaryKeys()).thenReturn(Arrays.asList("id", "region"));

        assertThatThrownBy(() -> PgSqlEngine.execute(mockSession,
                "DELETE FROM testtable WHERE id = 1", Collections.emptyList()))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("full primary key predicate");
    }


}
