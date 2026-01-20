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
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.InternalRow;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LogScanEnumerable extends AbstractEnumerable<InternalRow> {

    private final LogScanner scanner;
    private final List<TableBucket> buckets;
    private final Duration pollTimeout;

    public LogScanEnumerable(LogScanner scanner, List<TableBucket> buckets, Duration pollTimeout) {
        this.scanner = scanner;
        this.buckets = buckets;
        this.pollTimeout = pollTimeout;
    }

    @Override
    public Enumerator<InternalRow> enumerator() {
        return new LogScanEnumerator(scanner, buckets, pollTimeout);
    }

    private static final class LogScanEnumerator implements Enumerator<InternalRow> {

        private final LogScanner scanner;
        private final List<TableBucket> buckets;
        private final Duration pollTimeout;
        private Iterator<InternalRow> iterator;
        private InternalRow current;

        private LogScanEnumerator(LogScanner scanner, List<TableBucket> buckets, Duration pollTimeout) {
            this.scanner = scanner;
            this.buckets = buckets;
            this.pollTimeout = pollTimeout;
        }

        @Override
        public InternalRow current() {
            return current;
        }

        @Override
        public boolean moveNext() {
            if (iterator == null) {
                List<InternalRow> rows = new ArrayList<>();
                for (TableBucket bucket : buckets) {
                    scanner.subscribeFromBeginning(bucket.getTableId(), bucket.getBucket());
                }
                ScanRecords records = scanner.poll(pollTimeout);
                if (records != null && !records.isEmpty()) {
                    for (ScanRecord record : records) {
                        rows.add(record.getRow());
                    }
                }
                iterator = rows.iterator();
            }

            if (!iterator.hasNext()) {
                return false;
            }

            current = iterator.next();
            return true;
        }

        @Override
        public void reset() {
            iterator = null;
            current = null;
        }

        @Override
        public void close() {
            scanner.wakeup();
        }
    }
}
