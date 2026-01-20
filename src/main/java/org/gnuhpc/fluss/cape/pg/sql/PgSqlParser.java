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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class PgSqlParser {

    private static final Pattern COPY_FROM_STDIN = Pattern.compile(
            "(?i)^\\s*COPY\\s+([a-zA-Z0-9_\\.]+)\\s*(?:\\(([^)]*)\\))?\\s+FROM\\s+STDIN.*$");

    private PgSqlParser() {
    }

    public static CopyCommand parseCopyFromStdin(String sql) {
        Matcher matcher = COPY_FROM_STDIN.matcher(sql == null ? "" : sql.trim());
        if (!matcher.matches()) {
            return null;
        }
        String table = matcher.group(1);
        String columnGroup = matcher.group(2);
        List<String> columns = new ArrayList<>();
        if (columnGroup != null && !columnGroup.isBlank()) {
            for (String raw : columnGroup.split(",")) {
                String column = raw.trim();
                if (!column.isEmpty()) {
                    columns.add(column);
                }
            }
        }
        return new CopyCommand(table, columns);
    }

    public static final class CopyCommand {
        private final String table;
        private final List<String> columns;

        public CopyCommand(String table, List<String> columns) {
            this.table = table;
            this.columns = columns;
        }

        public String getTable() {
            return table;
        }

        public List<String> getColumns() {
            return columns;
        }
    }
}
