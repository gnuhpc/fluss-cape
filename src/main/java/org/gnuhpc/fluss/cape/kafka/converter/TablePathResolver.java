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

package org.gnuhpc.fluss.cape.kafka.converter;

import org.apache.fluss.metadata.TablePath;

public class TablePathResolver {
    
    private final String defaultDatabase;
    
    public TablePathResolver(String defaultDatabase) {
        this.defaultDatabase = defaultDatabase;
    }
    
    public TablePath resolve(String topicName) {
        if (topicName.contains(".")) {
            String[] parts = topicName.split("\\.", 2);
            return new TablePath(parts[0], parts[1]);
        }
        return new TablePath(defaultDatabase, topicName);
    }
    
    public String toTopicName(TablePath tablePath) {
        if (defaultDatabase.equals(tablePath.getDatabaseName())) {
            return tablePath.getTableName();
        }
        return tablePath.getDatabaseName() + "." + tablePath.getTableName();
    }
}
