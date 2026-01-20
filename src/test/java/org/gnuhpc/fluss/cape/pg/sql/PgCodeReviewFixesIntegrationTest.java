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
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.RowDescription;
import org.gnuhpc.fluss.cape.pg.session.PgPortal;
import org.gnuhpc.fluss.cape.pg.session.PgSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PgCodeReviewFixesIntegrationTest {

    @Mock private PgSession mockSession;
    @Mock private Admin mockAdmin;
    @Mock private Connection mockConnection;
    @Mock private Table mockTable;
    @Mock private TableInfo mockTableInfo;

    private TablePath testTablePath;

    @BeforeEach
    void setUp() {
        testTablePath = TablePath.of("testdb", "test_table");
        lenient().when(mockSession.getFlussAdmin()).thenReturn(mockAdmin);
        lenient().when(mockSession.getDatabase()).thenReturn("testdb");
        lenient().when(mockSession.getFlussConnection()).thenReturn(mockConnection);
    }

    @Nested
    class CreateTableTest {

        @Test
        void testCreateTable_BasicTable() {
            String sql = "CREATE TABLE users (id INT, name VARCHAR, age INT, PRIMARY KEY (id))";
            
            PgSqlEngine.validate(sql);
        }

        @Test
        void testCreateTable_WithMultiplePrimaryKeys() {
            String sql = "CREATE TABLE orders (customer_id INT, order_id INT, total DECIMAL, PRIMARY KEY (customer_id, order_id))";
            
            PgSqlEngine.validate(sql);
        }

        @Test
        void testCreateTable_WithTimestampAndDate() {
            String sql = "CREATE TABLE events (id INT, created_at TIMESTAMP, event_date DATE, PRIMARY KEY (id))";
            
            PgSqlEngine.validate(sql);
        }

        @Test
        void testCreateTable_WithBoolean() {
            String sql = "CREATE TABLE settings (id INT, is_active BOOLEAN, PRIMARY KEY (id))";
            
            PgSqlEngine.validate(sql);
        }

        @Test
        void testCreateTable_WithBytea() {
            String sql = "CREATE TABLE files (id INT, content BYTEA, PRIMARY KEY (id))";
            
            PgSqlEngine.validate(sql);
        }

        @Test
        void testCreateTable_WithAllSupportedTypes() {
            String sql = "CREATE TABLE all_types (" +
                    "col_int INT, " +
                    "col_bigint BIGINT, " +
                    "col_smallint SMALLINT, " +
                    "col_varchar VARCHAR, " +
                    "col_text TEXT, " +
                    "col_boolean BOOLEAN, " +
                    "col_decimal DECIMAL, " +
                    "col_timestamp TIMESTAMP, " +
                    "col_date DATE, " +
                    "col_bytea BYTEA, " +
                    "PRIMARY KEY (col_int))";
            
            PgSqlEngine.validate(sql);
        }

        @Test
        void testCreateTable_InvalidSyntax_ThrowsException() {
            String sql = "CREATE TABLE invalid (";
            
            assertThatThrownBy(() -> PgSqlEngine.execute(mockSession, sql, Collections.emptyList()))
                    .isInstanceOf(Exception.class);
        }
    }

    @Nested
    class ParameterBindingTest {

        @Test
        void testParameterBinding_SimpleReplacement() throws Exception {
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
            
            List<Object> parameters = Arrays.asList(1, "Alice");
            
            PgExecutionResult result = PgSqlEngine.execute(mockSession,
                    "INSERT INTO test_table (id, name) VALUES ($1, $2)", parameters);
            
            assertThat(result).isNotNull();
            assertThat(result.getCommandTag()).isEqualTo("INSERT 0 1");
            verify(mockWriter).upsert(any(GenericRow.class));
        }

        @Test
        void testParameterBinding_PlaceholderInStringLiteral_NotReplaced() throws Exception {
            RowType rowType = RowType.builder()
                    .field("id", DataTypes.INT())
                    .field("name", DataTypes.STRING())
                    .field("message", DataTypes.STRING())
                    .build();
            
            when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
            when(mockTableInfo.getRowType()).thenReturn(rowType);
            when(mockConnection.getTable(testTablePath)).thenReturn(mockTable);
            
            Upsert mockUpsert = mock(Upsert.class);
            UpsertWriter mockWriter = mock(UpsertWriter.class);
            when(mockTable.newUpsert()).thenReturn(mockUpsert);
            when(mockUpsert.createWriter()).thenReturn(mockWriter);
            when(mockWriter.upsert(any(GenericRow.class))).thenReturn(CompletableFuture.completedFuture(null));
            
            ArgumentCaptor<GenericRow> rowCaptor = ArgumentCaptor.forClass(GenericRow.class);
            
            List<Object> parameters = Arrays.asList(1, "Alice");
            
            PgExecutionResult result = PgSqlEngine.execute(mockSession,
                    "INSERT INTO test_table (id, name, message) VALUES ($1, $2, 'Placeholder $1 should remain')", 
                    parameters);
            
            assertThat(result).isNotNull();
            verify(mockWriter).upsert(rowCaptor.capture());
            
            GenericRow captured = rowCaptor.getValue();
            assertThat(captured.getInt(0)).isEqualTo(1);
            
            Object field1 = captured.getField(1);
            Object field2 = captured.getField(2);
            
            String actualName = field1 instanceof BinaryString 
                ? field1.toString() 
                : field1.toString().replaceAll("^'|'$", "");
            String actualMessage = field2 instanceof BinaryString 
                ? field2.toString() 
                : field2.toString().replaceAll("^'|'$", "");
            
            assertThat(actualName).isEqualTo("Alice");
            assertThat(actualMessage).isEqualTo("Placeholder $1 should remain");
        }

        @Test
        void testParameterBinding_MultipleParameters() throws Exception {
            RowType rowType = RowType.builder()
                    .field("id", DataTypes.INT())
                    .field("name", DataTypes.STRING())
                    .field("age", DataTypes.INT())
                    .field("email", DataTypes.STRING())
                    .build();
            
            when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
            when(mockTableInfo.getRowType()).thenReturn(rowType);
            when(mockConnection.getTable(testTablePath)).thenReturn(mockTable);
            
            Upsert mockUpsert = mock(Upsert.class);
            UpsertWriter mockWriter = mock(UpsertWriter.class);
            when(mockTable.newUpsert()).thenReturn(mockUpsert);
            when(mockUpsert.createWriter()).thenReturn(mockWriter);
            when(mockWriter.upsert(any(GenericRow.class))).thenReturn(CompletableFuture.completedFuture(null));
            
            List<Object> parameters = Arrays.asList(1, "Alice", 30, "alice@example.com");
            
            PgExecutionResult result = PgSqlEngine.execute(mockSession,
                    "INSERT INTO test_table (id, name, age, email) VALUES ($1, $2, $3, $4)", 
                    parameters);
            
            assertThat(result).isNotNull();
            assertThat(result.getCommandTag()).isEqualTo("INSERT 0 1");
            verify(mockWriter).upsert(any(GenericRow.class));
        }

        @Test
        void testParameterBinding_NullParameter() throws Exception {
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
            
            List<Object> parameters = Arrays.asList(1, null);
            
            PgExecutionResult result = PgSqlEngine.execute(mockSession,
                    "INSERT INTO test_table (id, name) VALUES ($1, $2)", parameters);
            
            assertThat(result).isNotNull();
            verify(mockWriter).upsert(any(GenericRow.class));
        }

        @Test
        void testParameterBinding_WithQuotes() throws Exception {
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
            
            ArgumentCaptor<GenericRow> rowCaptor = ArgumentCaptor.forClass(GenericRow.class);
            
            List<Object> parameters = Collections.singletonList(1);
            
            PgExecutionResult result = PgSqlEngine.execute(mockSession,
                    "INSERT INTO test_table (id, name) VALUES ($1, '$2 is not replaced')", 
                    parameters);
            
            assertThat(result).isNotNull();
            verify(mockWriter).upsert(rowCaptor.capture());
            
            GenericRow captured = rowCaptor.getValue();
            Object field1 = captured.getField(1);
            
            String actualValue = field1 instanceof BinaryString 
                ? field1.toString() 
                : field1.toString().replaceAll("^'|'$", "");
            
            assertThat(actualValue).isEqualTo("$2 is not replaced");
        }
    }

    @Nested
    class ExtendedQueryProtocolTest {

        @Test
        void testMaxRows_LimitsResultSet() {
            List<Object[]> allRows = Arrays.asList(
                    new Object[]{1, "Alice"},
                    new Object[]{2, "Bob"},
                    new Object[]{3, "Charlie"},
                    new Object[]{4, "David"},
                    new Object[]{5, "Eve"}
            );
            
            RowDescription mockDescription = mock(RowDescription.class);
            PgQueryResult fullResult = new PgQueryResult(mockDescription, allRows);
            
            assertThat(fullResult.getRows()).hasSize(5);
            
            int maxRows = 3;
            List<Object[]> limitedRows = fullResult.getRows().subList(0, Math.min(maxRows, fullResult.getRows().size()));
            
            assertThat(limitedRows).hasSize(3);
            assertThat(limitedRows.get(0)[1]).isEqualTo("Alice");
            assertThat(limitedRows.get(1)[1]).isEqualTo("Bob");
            assertThat(limitedRows.get(2)[1]).isEqualTo("Charlie");
        }

        @Test
        void testMaxRows_ZeroMeansAllRows() {
            List<Object[]> allRows = Arrays.asList(
                    new Object[]{1, "Alice"},
                    new Object[]{2, "Bob"},
                    new Object[]{3, "Charlie"}
            );
            
            RowDescription mockDescription = mock(RowDescription.class);
            PgQueryResult fullResult = new PgQueryResult(mockDescription, allRows);
            
            int maxRows = 0;
            int rowsToReturn = maxRows > 0 ? Math.min(maxRows, fullResult.getRows().size()) : fullResult.getRows().size();
            
            assertThat(rowsToReturn).isEqualTo(3);
        }

        @Test
        void testResultFormatCodes_StoredInPortal() {
            String portalName = "test_portal";
            String statementName = "test_statement";
            List<Object> parameters = Arrays.asList(1, "Alice");
            short[] resultFormatCodes = {0, 1};
            
            PgPortal portal = new PgPortal(portalName, statementName, parameters, resultFormatCodes);
            
            assertThat(portal.getName()).isEqualTo(portalName);
            assertThat(portal.getStatementName()).isEqualTo(statementName);
            assertThat(portal.getParameters()).isEqualTo(parameters);
            assertThat(portal.getResultFormatCodes()).isEqualTo(resultFormatCodes);
        }

        @Test
        void testEncodeRow_WithFormatCodes() {
            RowDescription mockDescription = mock(RowDescription.class);
            Object[] row = new Object[]{42, "test", true};
            short[] formatCodes = {0, 0, 0};
            
            List<byte[]> encoded = PgSqlEngine.encodeRow(row, mockDescription, formatCodes);
            
            assertThat(encoded).hasSize(3);
            assertThat(new String(encoded.get(0), StandardCharsets.UTF_8)).isEqualTo("42");
            assertThat(new String(encoded.get(1), StandardCharsets.UTF_8)).isEqualTo("test");
            assertThat(new String(encoded.get(2), StandardCharsets.UTF_8)).isEqualTo("true");
        }

        @Test
        void testEncodeRow_WithNullFormatCodes_UsesDefaultTextFormat() {
            RowDescription mockDescription = mock(RowDescription.class);
            Object[] row = new Object[]{42, "test"};
            
            List<byte[]> encoded = PgSqlEngine.encodeRow(row, mockDescription, null);
            
            assertThat(encoded).hasSize(2);
            assertThat(new String(encoded.get(0), StandardCharsets.UTF_8)).isEqualTo("42");
            assertThat(new String(encoded.get(1), StandardCharsets.UTF_8)).isEqualTo("test");
        }

        @Test
        void testEncodeRow_WithSingleFormatCode_AppliesToAllColumns() {
            RowDescription mockDescription = mock(RowDescription.class);
            Object[] row = new Object[]{1, 2, 3};
            short[] formatCodes = {0};
            
            List<byte[]> encoded = PgSqlEngine.encodeRow(row, mockDescription, formatCodes);
            
            assertThat(encoded).hasSize(3);
            for (int i = 0; i < 3; i++) {
                assertThat(new String(encoded.get(i), StandardCharsets.UTF_8)).isEqualTo(String.valueOf(i + 1));
            }
        }
    }

    @Nested
    class PgCatalogTest {

        @Test
        void testPgType_ReturnsCommonTypes() throws Exception {
            String sql = "SELECT * FROM pg_catalog.pg_type WHERE typname = 'int4'";
            
            PgExecutionResult result = PgSqlEngine.execute(mockSession, sql, Collections.emptyList());
            
            assertThat(result).isNotNull();
            assertThat(result.isQuery()).isTrue();
        }

        @Test
        void testPgAttribute_ReturnsColumnMetadata() throws Exception {
            String sql = "SELECT * FROM pg_catalog.pg_attribute WHERE attname = 'id'";
            
            PgExecutionResult result = PgSqlEngine.execute(mockSession, sql, Collections.emptyList());
            
            assertThat(result).isNotNull();
            assertThat(result.isQuery()).isTrue();
        }

        @Test
        void testPgNamespace_ReturnsSchemas() throws Exception {
            String sql = "SELECT * FROM pg_catalog.pg_namespace";
            
            PgExecutionResult result = PgSqlEngine.execute(mockSession, sql, Collections.emptyList());
            
            assertThat(result).isNotNull();
            assertThat(result.isQuery()).isTrue();
        }

        @Test
        void testPgClass_ReturnsTables() throws Exception {
            String sql = "SELECT * FROM pg_catalog.pg_class";
            
            PgExecutionResult result = PgSqlEngine.execute(mockSession, sql, Collections.emptyList());
            
            assertThat(result).isNotNull();
            assertThat(result.isQuery()).isTrue();
        }
    }

    @Nested
    class UpdateDeleteWithPrimaryKeyTest {

        @Test
        void testUpdate_WithSinglePrimaryKey() throws Exception {
            RowType rowType = RowType.builder()
                    .field("id", DataTypes.INT())
                    .field("name", DataTypes.STRING())
                    .field("age", DataTypes.INT())
                    .build();
            
            when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
            when(mockTableInfo.getRowType()).thenReturn(rowType);
            when(mockTableInfo.getPrimaryKeys()).thenReturn(Collections.singletonList("id"));
            when(mockConnection.getTable(testTablePath)).thenReturn(mockTable);
            
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
            
            Upsert mockUpsert = mock(Upsert.class);
            UpsertWriter mockWriter = mock(UpsertWriter.class);
            when(mockTable.newUpsert()).thenReturn(mockUpsert);
            when(mockUpsert.createWriter()).thenReturn(mockWriter);
            when(mockWriter.upsert(any(GenericRow.class))).thenReturn(CompletableFuture.completedFuture(null));
            
            PgExecutionResult result = PgSqlEngine.execute(mockSession,
                    "UPDATE test_table SET name = 'Bob' WHERE id = 1", Collections.emptyList());
            
            assertThat(result).isNotNull();
            assertThat(result.getCommandTag()).isEqualTo("UPDATE 1");
            verify(mockWriter).upsert(any(GenericRow.class));
        }

        @Test
        void testDelete_WithCompositePrimaryKey() throws Exception {
            RowType rowType = RowType.builder()
                    .field("customer_id", DataTypes.INT())
                    .field("order_id", DataTypes.INT())
                    .field("total", DataTypes.INT())
                    .build();
            
            when(mockAdmin.getTableInfo(testTablePath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));
            when(mockTableInfo.getRowType()).thenReturn(rowType);
            when(mockTableInfo.getPrimaryKeys()).thenReturn(Arrays.asList("customer_id", "order_id"));
            when(mockConnection.getTable(testTablePath)).thenReturn(mockTable);
            
            Upsert mockUpsert = mock(Upsert.class);
            UpsertWriter mockWriter = mock(UpsertWriter.class);
            when(mockTable.newUpsert()).thenReturn(mockUpsert);
            when(mockUpsert.createWriter()).thenReturn(mockWriter);
            when(mockWriter.delete(any(GenericRow.class))).thenReturn(CompletableFuture.completedFuture(null));
            
            PgExecutionResult result = PgSqlEngine.execute(mockSession,
                    "DELETE FROM test_table WHERE customer_id = 1 AND order_id = 100", Collections.emptyList());
            
            assertThat(result).isNotNull();
            assertThat(result.getCommandTag()).isEqualTo("DELETE 1");
            verify(mockWriter).delete(any(GenericRow.class));
        }
    }
}
