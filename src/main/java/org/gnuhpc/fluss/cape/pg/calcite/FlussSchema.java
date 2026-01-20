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

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlussSchema extends AbstractSchema {

    private final Admin admin;
    private final String database;

    public FlussSchema(Admin admin, String database) {
        this.admin = admin;
        this.database = database;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> tables = new HashMap<>();
        try {
            List<String> tableNames = admin.listTables(database).get();
            for (String tableName : tableNames) {
                TablePath tablePath = TablePath.of(database, tableName);
                TableInfo tableInfo = admin.getTableInfo(tablePath).get();
                tables.put(tableName, new FlussTable(tableInfo));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load Fluss schema", e);
        }
        return tables;
    }

}
