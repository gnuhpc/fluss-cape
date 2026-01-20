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

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.gnuhpc.fluss.cape.pg.session.PgSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PgDdlExecutorTest {

    @Mock
    private PgSession mockSession;

    @Mock
    private Admin mockAdmin;

    @Mock
    private TableInfo mockTableInfo;

    @BeforeEach
    void setup() {
        lenient().when(mockSession.getFlussAdmin()).thenReturn(mockAdmin);
    }

    // ==================== SHOW DATABASES Tests ====================

    @Test
    void testExecuteShowDatabases_withMultipleDatabases() throws Exception {
        List<String> databases = Arrays.asList("testdb", "proddb", "devdb");
        when(mockAdmin.listDatabases()).thenReturn(CompletableFuture.completedFuture(databases));

        PgExecutionResult result = PgDdlExecutor.executeShowDatabases(mockSession);

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isTrue();
        assertThat(result.getQueryResult().getRows()).hasSize(3);
        assertThat(result.getQueryResult().getRows().get(0)[0]).isEqualTo("testdb");
        assertThat(result.getQueryResult().getRows().get(1)[0]).isEqualTo("proddb");
        assertThat(result.getQueryResult().getRows().get(2)[0]).isEqualTo("devdb");
        verify(mockAdmin).listDatabases();
    }

    @Test
    void testExecuteShowDatabases_withEmptyList() throws Exception {
        when(mockAdmin.listDatabases()).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));

        PgExecutionResult result = PgDdlExecutor.executeShowDatabases(mockSession);

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isTrue();
        assertThat(result.getQueryResult().getRows()).isEmpty();
    }

    @Test
    void testExecuteShowDatabases_withSingleDatabase() throws Exception {
        List<String> databases = Collections.singletonList("onlydb");
        when(mockAdmin.listDatabases()).thenReturn(CompletableFuture.completedFuture(databases));

        PgExecutionResult result = PgDdlExecutor.executeShowDatabases(mockSession);

        assertThat(result).isNotNull();
        assertThat(result.getQueryResult().getRows()).hasSize(1);
        assertThat(result.getQueryResult().getRows().get(0)[0]).isEqualTo("onlydb");
    }

    // ==================== SHOW TABLES Tests ====================

    @Test
    void testExecuteShowTables_withExplicitDatabase() throws Exception {
        List<String> tables = Arrays.asList("users", "orders", "products");
        when(mockAdmin.listTables("testdb")).thenReturn(CompletableFuture.completedFuture(tables));

        PgExecutionResult result = PgDdlExecutor.executeShowTables(mockSession, "testdb");

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isTrue();
        assertThat(result.getQueryResult().getRows()).hasSize(3);
        assertThat(result.getQueryResult().getRows().get(0)[0]).isEqualTo("users");
        assertThat(result.getQueryResult().getRows().get(1)[0]).isEqualTo("orders");
        verify(mockAdmin).listTables("testdb");
    }

    @Test
    void testExecuteShowTables_withSessionDatabase() throws Exception {
        when(mockSession.getDatabase()).thenReturn("mydb");
        List<String> tables = Collections.singletonList("table1");
        when(mockAdmin.listTables("mydb")).thenReturn(CompletableFuture.completedFuture(tables));

        PgExecutionResult result = PgDdlExecutor.executeShowTables(mockSession, null);

        assertThat(result).isNotNull();
        assertThat(result.getQueryResult().getRows()).hasSize(1);
        assertThat(result.getQueryResult().getRows().get(0)[0]).isEqualTo("table1");
        verify(mockAdmin).listTables("mydb");
    }

    @Test
    void testExecuteShowTables_withLowercaseConversion() throws Exception {
        List<String> tables = Arrays.asList("Table1", "TABLE2");
        when(mockAdmin.listTables("testdb")).thenReturn(CompletableFuture.completedFuture(tables));

        PgExecutionResult result = PgDdlExecutor.executeShowTables(mockSession, "TESTDB");

        assertThat(result).isNotNull();
        verify(mockAdmin).listTables("testdb");
    }

    @Test
    void testExecuteShowTables_withNoDatabaseSpecified() {
        when(mockSession.getDatabase()).thenReturn(null);

        assertThatThrownBy(() -> PgDdlExecutor.executeShowTables(mockSession, null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No database specified");
    }

    @Test
    void testExecuteShowTables_withEmptyList() throws Exception {
        when(mockSession.getDatabase()).thenReturn("emptydb");
        when(mockAdmin.listTables("emptydb")).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));

        PgExecutionResult result = PgDdlExecutor.executeShowTables(mockSession, null);

        assertThat(result.getQueryResult().getRows()).isEmpty();
    }

    // ==================== DESCRIBE TABLE Tests ====================

    @Test
    void testExecuteDescribeTable_withPrimaryKey() throws Exception {
        when(mockSession.getDatabase()).thenReturn("testdb");
        
        RowType rowType = RowType.builder()
            .field("id", DataTypes.INT())
            .field("name", DataTypes.STRING())
            .field("age", DataTypes.INT())
            .build();
        
        when(mockTableInfo.getRowType()).thenReturn(rowType);
        when(mockTableInfo.getPrimaryKeys()).thenReturn(Collections.singletonList("id"));
        
        TablePath expectedPath = TablePath.of("testdb", "users");
        when(mockAdmin.getTableInfo(expectedPath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));

        PgExecutionResult result = PgDdlExecutor.executeDescribeTable(mockSession, "users");

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isTrue();
        List<Object[]> rows = result.getQueryResult().getRows();
        assertThat(rows).hasSize(3);
        
        assertThat(rows.get(0)[0]).isEqualTo("id");
        assertThat(rows.get(0)[2]).isEqualTo("YES");
        assertThat(rows.get(0)[3]).isEqualTo("PRI");
        
        assertThat(rows.get(1)[0]).isEqualTo("name");
        assertThat(rows.get(1)[2]).isEqualTo("YES");
        assertThat(rows.get(1)[3]).isEqualTo("");
        
        verify(mockAdmin).getTableInfo(expectedPath);
    }

    @Test
    void testExecuteDescribeTable_withQualifiedName() throws Exception {
        RowType rowType = RowType.builder()
            .field("col1", DataTypes.STRING())
            .build();
        
        when(mockTableInfo.getRowType()).thenReturn(rowType);
        when(mockTableInfo.getPrimaryKeys()).thenReturn(Collections.emptyList());
        
        TablePath expectedPath = TablePath.of("otherdb", "table1");
        when(mockAdmin.getTableInfo(expectedPath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));

        PgExecutionResult result = PgDdlExecutor.executeDescribeTable(mockSession, "otherdb.table1");

        assertThat(result).isNotNull();
        assertThat(result.getQueryResult().getRows()).hasSize(1);
        verify(mockAdmin).getTableInfo(expectedPath);
    }

    @Test
    void testExecuteDescribeTable_tableNotFound() throws Exception {
        when(mockSession.getDatabase()).thenReturn("testdb");
        TablePath expectedPath = TablePath.of("testdb", "nonexistent");
        when(mockAdmin.getTableInfo(expectedPath)).thenReturn(CompletableFuture.completedFuture(null));

        assertThatThrownBy(() -> PgDdlExecutor.executeDescribeTable(mockSession, "nonexistent"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Table not found");
    }

    @Test
    void testExecuteDescribeTable_withCompositePrimaryKey() throws Exception {
        when(mockSession.getDatabase()).thenReturn("testdb");
        
        RowType rowType = RowType.builder()
            .field("id", DataTypes.INT())
            .field("region", DataTypes.STRING())
            .field("value", DataTypes.BIGINT())
            .build();
        
        when(mockTableInfo.getRowType()).thenReturn(rowType);
        when(mockTableInfo.getPrimaryKeys()).thenReturn(Arrays.asList("id", "region"));
        
        TablePath expectedPath = TablePath.of("testdb", "composite_pk_table");
        when(mockAdmin.getTableInfo(expectedPath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));

        PgExecutionResult result = PgDdlExecutor.executeDescribeTable(mockSession, "composite_pk_table");

        List<Object[]> rows = result.getQueryResult().getRows();
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0)[3]).isEqualTo("PRI");
        assertThat(rows.get(1)[3]).isEqualTo("PRI");
        assertThat(rows.get(2)[3]).isEqualTo("");
    }

    @Test
    void testExecuteDescribeTable_lowercaseTableName() throws Exception {
        when(mockSession.getDatabase()).thenReturn("testdb");
        
        RowType rowType = RowType.builder()
            .field("col1", DataTypes.INT())
            .build();
        when(mockTableInfo.getRowType()).thenReturn(rowType);
        when(mockTableInfo.getPrimaryKeys()).thenReturn(Collections.emptyList());
        
        TablePath expectedPath = TablePath.of("testdb", "mytable");
        when(mockAdmin.getTableInfo(expectedPath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));

        PgDdlExecutor.executeDescribeTable(mockSession, "MyTable");

        verify(mockAdmin).getTableInfo(expectedPath);
    }

    // ==================== CREATE DATABASE Tests ====================

    @Test
    void testExecuteCreateDatabase_success() throws Exception {
        when(mockAdmin.createDatabase(eq("newdb"), any(DatabaseDescriptor.class), eq(false)))
            .thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgDdlExecutor.executeCreateDatabase(mockSession, "newdb", false);

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isFalse();
        assertThat(result.getCommandTag()).isEqualTo("CREATE DATABASE");
        
        ArgumentCaptor<DatabaseDescriptor> descriptorCaptor = ArgumentCaptor.forClass(DatabaseDescriptor.class);
        verify(mockAdmin).createDatabase(eq("newdb"), descriptorCaptor.capture(), eq(false));
        assertThat(descriptorCaptor.getValue()).isNotNull();
    }

    @Test
    void testExecuteCreateDatabase_withIgnoreIfExists() throws Exception {
        when(mockAdmin.createDatabase(eq("existingdb"), any(DatabaseDescriptor.class), eq(true)))
            .thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgDdlExecutor.executeCreateDatabase(mockSession, "existingdb", true);

        assertThat(result).isNotNull();
        verify(mockAdmin).createDatabase(eq("existingdb"), any(DatabaseDescriptor.class), eq(true));
    }

    // ==================== DROP DATABASE Tests ====================

    @Test
    void testExecuteDropDatabase_success() throws Exception {
        when(mockAdmin.dropDatabase("olddb", false, false))
            .thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgDdlExecutor.executeDropDatabase(mockSession, "olddb", false, false);

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isFalse();
        assertThat(result.getCommandTag()).isEqualTo("DROP DATABASE");
        verify(mockAdmin).dropDatabase("olddb", false, false);
    }

    @Test
    void testExecuteDropDatabase_withIgnoreIfNotExists() throws Exception {
        when(mockAdmin.dropDatabase("maybedb", true, false))
            .thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgDdlExecutor.executeDropDatabase(mockSession, "maybedb", true, false);

        assertThat(result).isNotNull();
        verify(mockAdmin).dropDatabase("maybedb", true, false);
    }

    @Test
    void testExecuteDropDatabase_withCascade() throws Exception {
        when(mockAdmin.dropDatabase("dbwithdata", false, true))
            .thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgDdlExecutor.executeDropDatabase(mockSession, "dbwithdata", false, true);

        assertThat(result).isNotNull();
        verify(mockAdmin).dropDatabase("dbwithdata", false, true);
    }

    @Test
    void testExecuteDropDatabase_withBothFlags() throws Exception {
        when(mockAdmin.dropDatabase("anydb", true, true))
            .thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgDdlExecutor.executeDropDatabase(mockSession, "anydb", true, true);

        assertThat(result).isNotNull();
        verify(mockAdmin).dropDatabase("anydb", true, true);
    }

    // ==================== DROP TABLE Tests ====================

    @Test
    void testExecuteDropTable_simpleTable() throws Exception {
        when(mockSession.getDatabase()).thenReturn("testdb");
        TablePath expectedPath = TablePath.of("testdb", "obsoletetable");
        when(mockAdmin.dropTable(expectedPath, false))
            .thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgDdlExecutor.executeDropTable(mockSession, "obsoletetable", false);

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isFalse();
        assertThat(result.getCommandTag()).isEqualTo("DROP TABLE");
        verify(mockAdmin).dropTable(expectedPath, false);
    }

    @Test
    void testExecuteDropTable_withQualifiedName() throws Exception {
        TablePath expectedPath = TablePath.of("otherdb", "oldtable");
        when(mockAdmin.dropTable(expectedPath, false))
            .thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgDdlExecutor.executeDropTable(mockSession, "otherdb.oldtable", false);

        assertThat(result).isNotNull();
        verify(mockAdmin).dropTable(expectedPath, false);
    }

    @Test
    void testExecuteDropTable_withIgnoreIfNotExists() throws Exception {
        when(mockSession.getDatabase()).thenReturn("testdb");
        TablePath expectedPath = TablePath.of("testdb", "maybenotthere");
        when(mockAdmin.dropTable(expectedPath, true))
            .thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgDdlExecutor.executeDropTable(mockSession, "maybenotthere", true);

        assertThat(result).isNotNull();
        verify(mockAdmin).dropTable(expectedPath, true);
    }

    // ==================== CREATE TABLE Tests ====================

    @Test
    void testExecuteCreateTable_withPrimaryKey() throws Exception {
        List<PgDdlExecutor.ColumnDef> columns = Arrays.asList(
            new PgDdlExecutor.ColumnDef("id", DataTypes.INT()),
            new PgDdlExecutor.ColumnDef("name", DataTypes.STRING())
        );
        List<String> primaryKeys = Collections.singletonList("id");
        
        TablePath expectedPath = TablePath.of("testdb", "newtable");
        when(mockAdmin.createTable(eq(expectedPath), any(TableDescriptor.class), eq(false)))
            .thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgDdlExecutor.executeCreateTable(mockSession, "testdb.newtable", columns, primaryKeys);

        assertThat(result).isNotNull();
        assertThat(result.isQuery()).isFalse();
        assertThat(result.getCommandTag()).isEqualTo("CREATE TABLE");
        
        ArgumentCaptor<TableDescriptor> descriptorCaptor = ArgumentCaptor.forClass(TableDescriptor.class);
        verify(mockAdmin).createTable(eq(expectedPath), descriptorCaptor.capture(), eq(false));
        
        TableDescriptor descriptor = descriptorCaptor.getValue();
        assertThat(descriptor.getSchema().getPrimaryKeyColumnNames()).containsExactly("id");
    }

    @Test
    void testExecuteCreateTable_withoutPrimaryKey() throws Exception {
        List<PgDdlExecutor.ColumnDef> columns = Arrays.asList(
            new PgDdlExecutor.ColumnDef("col1", DataTypes.STRING()),
            new PgDdlExecutor.ColumnDef("col2", DataTypes.BIGINT())
        );
        List<String> primaryKeys = Collections.emptyList();
        
        TablePath expectedPath = TablePath.of("testdb", "nopk");
        when(mockAdmin.createTable(eq(expectedPath), any(TableDescriptor.class), eq(false)))
            .thenReturn(CompletableFuture.completedFuture(null));

        PgExecutionResult result = PgDdlExecutor.executeCreateTable(mockSession, "testdb.nopk", columns, primaryKeys);

        assertThat(result).isNotNull();
        verify(mockAdmin).createTable(eq(expectedPath), any(TableDescriptor.class), eq(false));
    }

    @Test
    void testExecuteCreateTable_invalidTableName() {
        List<PgDdlExecutor.ColumnDef> columns = Collections.singletonList(
            new PgDdlExecutor.ColumnDef("col1", DataTypes.INT())
        );

        assertThatThrownBy(() -> 
            PgDdlExecutor.executeCreateTable(mockSession, "singlenameonly", columns, Collections.emptyList()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Table name must be in format 'database.table'");
    }

    // ==================== Helper Method Tests ====================

    @Test
    void testResolveTablePath_withDatabase() throws Exception {
        when(mockSession.getDatabase()).thenReturn("testdb");
        RowType rowType = RowType.builder()
            .field("col1", DataTypes.INT())
            .build();
        when(mockTableInfo.getRowType()).thenReturn(rowType);
        when(mockTableInfo.getPrimaryKeys()).thenReturn(Collections.emptyList());
        
        TablePath expectedPath = TablePath.of("testdb", "table1");
        when(mockAdmin.getTableInfo(expectedPath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));

        PgDdlExecutor.executeDescribeTable(mockSession, "table1");

        verify(mockAdmin).getTableInfo(expectedPath);
    }

    @Test
    void testResolveTablePath_withDefaultDatabase() throws Exception {
        when(mockSession.getDatabase()).thenReturn(null);
        RowType rowType = RowType.builder()
            .field("col1", DataTypes.INT())
            .build();
        when(mockTableInfo.getRowType()).thenReturn(rowType);
        when(mockTableInfo.getPrimaryKeys()).thenReturn(Collections.emptyList());
        
        TablePath expectedPath = TablePath.of("default", "table1");
        when(mockAdmin.getTableInfo(expectedPath)).thenReturn(CompletableFuture.completedFuture(mockTableInfo));

        PgDdlExecutor.executeDescribeTable(mockSession, "table1");

        verify(mockAdmin).getTableInfo(expectedPath);
    }

    @Test
    void testResolveTablePath_withInvalidFormat() {
        when(mockSession.getDatabase()).thenReturn("testdb");
        
        assertThatThrownBy(() -> PgDdlExecutor.executeDescribeTable(mockSession, "db.schema.table"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid table reference");
    }
}
