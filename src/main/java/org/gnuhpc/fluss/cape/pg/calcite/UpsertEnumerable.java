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

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.row.InternalRow;

import java.util.List;

public class UpsertEnumerable extends AbstractEnumerable<Void> {

    private final UpsertWriter writer;
    private final List<InternalRow> rows;
    private final boolean delete;

    public UpsertEnumerable(UpsertWriter writer, List<InternalRow> rows, boolean delete) {
        this.writer = writer;
        this.rows = rows;
        this.delete = delete;
    }

    @Override
    public Enumerator<Void> enumerator() {
        return new UpsertEnumerator(writer, rows, delete);
    }

    private static final class UpsertEnumerator implements Enumerator<Void> {

        private final UpsertWriter writer;
        private final List<InternalRow> rows;
        private final boolean delete;
        private boolean executed;

        private UpsertEnumerator(UpsertWriter writer, List<InternalRow> rows, boolean delete) {
            this.writer = writer;
            this.rows = rows;
            this.delete = delete;
        }

        @Override
        public Void current() {
            return null;
        }

        @Override
        public boolean moveNext() {
            if (executed) {
                return false;
            }
            executed = true;
            if (delete) {
                for (InternalRow row : rows) {
                    writer.delete(row).join();
                }
            } else {
                for (InternalRow row : rows) {
                    writer.upsert(row).join();
                }
            }
            return false;
        }

        @Override
        public void reset() {
            executed = false;
        }

        @Override
        public void close() {
        }
    }
}
