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

package org.gnuhpc.fluss.cape.pg.calcite;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.RowType;

import java.util.ArrayList;
import java.util.List;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.RowType;
import org.gnuhpc.fluss.cape.pg.sql.PgTableScanner;

import java.util.ArrayList;
import java.util.List;

public class FlussTable extends AbstractTable implements ScannableTable {

    private final Connection connection;
    private final Admin admin;
    private final TableInfo tableInfo;

    public FlussTable(Connection connection, Admin admin, TableInfo tableInfo) {
        this.connection = connection;
        this.admin = admin;
        this.tableInfo = tableInfo;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RowType rowType = tableInfo.getRowType();
        List<String> fieldNames = new ArrayList<>();
        List<RelDataType> fieldTypes = new ArrayList<>();
        for (DataField field : rowType.getFields()) {
            fieldNames.add(field.getName());
            fieldTypes.add(FlussTypeConverter.toCalciteType(typeFactory, field.getType()));
        }
        return typeFactory.createStructType(fieldTypes, fieldNames);
    }

    @Override
    public Enumerable<Object[]> scan(org.apache.calcite.DataContext root) {
        try {
            Table table = connection.getTable(tableInfo.getTablePath());
            List<InternalRow> scannedRows = PgTableScanner.scanTable(connection, table, null, -1);
            RowType rowType = tableInfo.getRowType();
            InternalRow.FieldGetter[] getters = InternalRow.createFieldGetters(rowType);
            
            List<Object[]> rows = new ArrayList<>();
            for (InternalRow row : scannedRows) {
                Object[] result = new Object[rowType.getFieldCount()];
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    result[i] = row.isNullAt(i) ? null : getters[i].getFieldOrNull(row);
                }
                rows.add(result);
            }
            return Linq4j.asEnumerable(rows);
        } catch (Exception e) {
            throw new RuntimeException("Failed to scan table: " + tableInfo.getTablePath(), e);
        }
    }
}
