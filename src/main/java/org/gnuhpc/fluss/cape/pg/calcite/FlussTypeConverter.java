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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DecimalType;

final class FlussTypeConverter {

    private FlussTypeConverter() {
    }

    static RelDataType toCalciteType(RelDataTypeFactory typeFactory, DataType dataType) {
        RelDataType type;
        DataTypeRoot root = dataType.getTypeRoot();
        switch (root) {
            case STRING:
            case CHAR:
                type = typeFactory.createSqlType(SqlTypeName.VARCHAR);
                break;
            case BOOLEAN:
                type = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
                break;
            case TINYINT:
                type = typeFactory.createSqlType(SqlTypeName.TINYINT);
                break;
            case SMALLINT:
                type = typeFactory.createSqlType(SqlTypeName.SMALLINT);
                break;
            case INTEGER:
                type = typeFactory.createSqlType(SqlTypeName.INTEGER);
                break;
            case BIGINT:
                type = typeFactory.createSqlType(SqlTypeName.BIGINT);
                break;
            case FLOAT:
                type = typeFactory.createSqlType(SqlTypeName.FLOAT);
                break;
            case DOUBLE:
                type = typeFactory.createSqlType(SqlTypeName.DOUBLE);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                type = typeFactory.createSqlType(
                        SqlTypeName.DECIMAL,
                        decimalType.getPrecision(),
                        decimalType.getScale());
                break;
            case BYTES:
            case BINARY:
                type = typeFactory.createSqlType(SqlTypeName.VARBINARY);
                break;
            case DATE:
                type = typeFactory.createSqlType(SqlTypeName.DATE);
                break;
            case TIME_WITHOUT_TIME_ZONE:
                type = typeFactory.createSqlType(SqlTypeName.TIME);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                type = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
                break;
            default:
                type = typeFactory.createSqlType(SqlTypeName.ANY);
                break;
        }

        return typeFactory.createTypeWithNullability(type, dataType.isNullable());
    }
}
