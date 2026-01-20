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

package org.gnuhpc.fluss.cape.pg.session;

import java.nio.charset.StandardCharsets;
import org.gnuhpc.fluss.cape.pg.sql.PgSqlParser;

public class PgCopyState {

    private final PgSqlParser.CopyCommand command;
    private final StringBuilder buffer = new StringBuilder();

    public PgCopyState(PgSqlParser.CopyCommand command) {
        this.command = command;
    }

    public PgSqlParser.CopyCommand getCommand() {
        return command;
    }

    public void append(byte[] payload) {
        buffer.append(new String(payload, StandardCharsets.UTF_8));
    }

    public String getData() {
        return buffer.toString();
    }
}
