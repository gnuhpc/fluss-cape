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

public class FlussTable extends AbstractTable implements ScannableTable {

    private final TableInfo tableInfo;

    public FlussTable(TableInfo tableInfo) {
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
        return Linq4j.emptyEnumerable();
    }
}
