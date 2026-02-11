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

package org.gnuhpc.fluss.cape.hbase.storage;

import org.apache.fluss.metadata.TablePath;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("HBaseCounterTableManager - Column Key Utilities")
class HBaseCounterTableManagerTest {

    @Test
    @DisplayName("buildColumnKey creates correct format")
    void testBuildColumnKey() {
        String columnKey = HBaseCounterTableManager.buildColumnKey("cf", "counter");
        assertThat(columnKey).isEqualTo("cf:counter");
    }

    @Test
    @DisplayName("buildColumnKey handles empty qualifier")
    void testBuildColumnKeyEmptyQualifier() {
        String columnKey = HBaseCounterTableManager.buildColumnKey("cf", "");
        assertThat(columnKey).isEqualTo("cf:");
    }

    @Test
    @DisplayName("parseFamily extracts family from column key")
    void testParseFamily() {
        String family = HBaseCounterTableManager.parseFamily("cf:counter");
        assertThat(family).isEqualTo("cf");
    }

    @Test
    @DisplayName("parseFamily handles family without qualifier")
    void testParseFamilyNoQualifier() {
        String family = HBaseCounterTableManager.parseFamily("cf:");
        assertThat(family).isEqualTo("cf");
    }

    @Test
    @DisplayName("parseQualifier extracts qualifier from column key")
    void testParseQualifier() {
        String qualifier = HBaseCounterTableManager.parseQualifier("cf:counter");
        assertThat(qualifier).isEqualTo("counter");
    }

    @Test
    @DisplayName("parseQualifier handles empty qualifier")
    void testParseQualifierEmpty() {
        String qualifier = HBaseCounterTableManager.parseQualifier("cf:");
        assertThat(qualifier).isEqualTo("");
    }

    @Test
    @DisplayName("parseQualifier handles multiple colons")
    void testParseQualifierMultipleColons() {
        String qualifier = HBaseCounterTableManager.parseQualifier("cf:qual:with:colons");
        assertThat(qualifier).isEqualTo("qual:with:colons");
    }

    @Test
    @DisplayName("parseFamily handles no colon - returns full string")
    void testParseFamilyNoColon() {
        String family = HBaseCounterTableManager.parseFamily("nocolon");
        assertThat(family).isEqualTo("nocolon");
    }

    @Test
    @DisplayName("parseQualifier handles no colon - returns empty string")
    void testParseQualifierNoColon() {
        String qualifier = HBaseCounterTableManager.parseQualifier("nocolon");
        assertThat(qualifier).isEqualTo("");
    }

    @Test
    @DisplayName("getCounterTablePath creates correct table path")
    void testGetCounterTablePath() {
        TablePath path = HBaseCounterTableManager.getCounterTablePath("mydb", "mytable");
        assertThat(path.getDatabaseName()).isEqualTo("mydb");
        assertThat(path.getTableName()).isEqualTo("mytable_incr_counters");
    }

    @Test
    @DisplayName("getCounterTablePath handles special characters in table name")
    void testGetCounterTablePathSpecialChars() {
        TablePath path = HBaseCounterTableManager.getCounterTablePath("db", "table-with-dashes");
        assertThat(path.getTableName()).isEqualTo("table-with-dashes_incr_counters");
    }
}
