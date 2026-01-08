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

package org.apache.fluss.hbase.protocol;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.codec.KeyValueCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/** Builds CellBlock binary format for HBase scan results. */
public class CellBlockBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(CellBlockBuilder.class);

    public static byte[] buildCellBlock(List<Result> results) throws IOException {
        if (results == null || results.isEmpty()) {
            LOG.warn("buildCellBlock called with null or empty results");
            return null;
        }

        LOG.info("Building CellBlock for {} results", results.size());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        KeyValueCodec codec = new KeyValueCodec();
        KeyValueCodec.KeyValueEncoder encoder =
                (KeyValueCodec.KeyValueEncoder) codec.getEncoder(baos);

        int totalCells = 0;
        for (Result result : results) {
            if (result == null || result.isEmpty()) {
                continue;
            }
            Cell[] cells = result.rawCells();
            totalCells += cells.length;
            for (Cell cell : cells) {
                encoder.write(cell);
            }
        }

        encoder.flush();
        byte[] cellBlock = baos.toByteArray();
        LOG.info("Built CellBlock: {} cells encoded, {} bytes total", totalCells, cellBlock.length);
        return cellBlock;
    }
}
