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

package org.gnuhpc.fluss.cape.pg.protocol;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.RowDescription;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.RowDescription.Field;

public final class PgRowDescriptionBuilder {

    private PgRowDescriptionBuilder() {
    }

    public static RowDescription fromRelType(RelDataType rowType) {
        List<Field> fields = new ArrayList<>();
        for (RelDataTypeField field : rowType.getFieldList()) {
            SqlTypeName typeName = field.getType().getSqlTypeName();
            int oid = PgOidMapping.oidFor(typeName);
            fields.add(new Field(field.getName(), oid, typeSize(typeName), -1, (short) 0));
        }
        return new RowDescription(fields);
    }

    public static RowDescription fromRowType(RowType rowType, List<String> selectedColumns) {
        List<Field> fields = new ArrayList<>();
        for (String column : selectedColumns) {
            int index = fieldIndex(rowType, column);
            DataType dataType = rowType.getFields().get(index).getType();
            SqlTypeName typeName = toSqlType(dataType.getTypeRoot());
            int oid = PgOidMapping.oidFor(typeName);
            fields.add(new Field(column, oid, typeSize(typeName), -1, (short) 0));
        }
        return new RowDescription(fields);
    }

    private static int fieldIndex(RowType rowType, String column) {
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (rowType.getFields().get(i).getName().equalsIgnoreCase(column)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Unknown column: " + column);
    }

    private static SqlTypeName toSqlType(DataTypeRoot root) {
        switch (root) {
            case STRING:
            case CHAR:
                return SqlTypeName.VARCHAR;
            case BOOLEAN:
                return SqlTypeName.BOOLEAN;
            case TINYINT:
                return SqlTypeName.TINYINT;
            case SMALLINT:
                return SqlTypeName.SMALLINT;
            case INTEGER:
                return SqlTypeName.INTEGER;
            case BIGINT:
                return SqlTypeName.BIGINT;
            case FLOAT:
                return SqlTypeName.FLOAT;
            case DOUBLE:
                return SqlTypeName.DOUBLE;
            case DECIMAL:
                return SqlTypeName.DECIMAL;
            case BYTES:
            case BINARY:
                return SqlTypeName.VARBINARY;
            case DATE:
                return SqlTypeName.DATE;
            case TIME_WITHOUT_TIME_ZONE:
                return SqlTypeName.TIME;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return SqlTypeName.TIMESTAMP;
            default:
                return SqlTypeName.ANY;
        }
    }

    private static short typeSize(SqlTypeName typeName) {
        switch (typeName) {
            case SMALLINT:
                return 2;
            case INTEGER:
                return 4;
            case BIGINT:
                return 8;
            case BOOLEAN:
                return 1;
            case FLOAT:
            case REAL:
                return 4;
            case DOUBLE:
                return 8;
            default:
                return -1;
        }
    }
}
