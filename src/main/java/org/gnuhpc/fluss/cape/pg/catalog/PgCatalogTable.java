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

package org.gnuhpc.fluss.cape.pg.catalog;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

public class PgCatalogTable extends AbstractTable implements ScannableTable {

    private final List<String> columnNames;
    private final List<SqlTypeName> columnTypes;
    private final List<Object[]> rows;

    public PgCatalogTable(
            List<String> columnNames,
            List<SqlTypeName> columnTypes,
            List<Object[]> rows) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.rows = rows;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        List<RelDataType> types = new ArrayList<>();
        for (SqlTypeName typeName : columnTypes) {
            types.add(typeFactory.createSqlType(typeName));
        }
        return typeFactory.createStructType(types, columnNames);
    }

    @Override
    public Enumerable<Object[]> scan(org.apache.calcite.DataContext root) {
        return Linq4j.asEnumerable(rows);
    }
}
