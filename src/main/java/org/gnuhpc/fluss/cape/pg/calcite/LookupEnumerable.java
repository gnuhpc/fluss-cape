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
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.row.InternalRow;

import java.util.Collections;
import java.util.List;

public class LookupEnumerable extends AbstractEnumerable<InternalRow> {

    private final Lookuper lookuper;
    private final InternalRow key;

    public LookupEnumerable(Lookuper lookuper, InternalRow key) {
        this.lookuper = lookuper;
        this.key = key;
    }

    @Override
    public Enumerator<InternalRow> enumerator() {
        LookupResult result = lookuper.lookup(key).join();
        List<InternalRow> rows = result == null ? Collections.emptyList() : result.getRowList();
        return new LookupEnumerator(rows);
    }

    private static final class LookupEnumerator implements Enumerator<InternalRow> {

        private final List<InternalRow> rows;
        private int index = -1;

        private LookupEnumerator(List<InternalRow> rows) {
            this.rows = rows;
        }

        @Override
        public InternalRow current() {
            return rows.get(index);
        }

        @Override
        public boolean moveNext() {
            index += 1;
            return index < rows.size();
        }

        @Override
        public void reset() {
            index = -1;
        }

        @Override
        public void close() {
        }
    }
}
