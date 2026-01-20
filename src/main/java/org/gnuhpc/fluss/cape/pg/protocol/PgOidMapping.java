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

import org.apache.calcite.sql.type.SqlTypeName;

public final class PgOidMapping {

    private static final int BOOL = 16;
    private static final int INT2 = 21;
    private static final int INT4 = 23;
    private static final int INT8 = 20;
    private static final int FLOAT4 = 700;
    private static final int FLOAT8 = 701;
    private static final int NUMERIC = 1700;
    private static final int DATE = 1082;
    private static final int TIME = 1083;
    private static final int TIMESTAMP = 1114;
    private static final int BYTEA = 17;
    private static final int VARCHAR = 1043;

    private PgOidMapping() {
    }

    public static int oidFor(SqlTypeName typeName) {
        switch (typeName) {
            case BOOLEAN:
                return BOOL;
            case TINYINT:
            case SMALLINT:
                return INT2;
            case INTEGER:
                return INT4;
            case BIGINT:
                return INT8;
            case FLOAT:
                return FLOAT4;
            case DOUBLE:
                return FLOAT8;
            case DECIMAL:
                return NUMERIC;
            case DATE:
                return DATE;
            case TIME:
                return TIME;
            case TIMESTAMP:
                return TIMESTAMP;
            case VARBINARY:
                return BYTEA;
            case VARCHAR:
            case CHAR:
            default:
                return VARCHAR;
        }
    }
}
