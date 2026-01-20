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

import java.util.List;

public class PgPortal {

    private final String name;
    private final String statementName;
    private final List<Object> parameters;
    private final short[] resultFormatCodes;

    public PgPortal(String name, String statementName, List<Object> parameters, short[] resultFormatCodes) {
        this.name = name;
        this.statementName = statementName;
        this.parameters = parameters;
        this.resultFormatCodes = resultFormatCodes;
    }

    public String getName() {
        return name;
    }

    public String getStatementName() {
        return statementName;
    }

    public List<Object> getParameters() {
        return parameters;
    }

    public short[] getResultFormatCodes() {
        return resultFormatCodes;
    }
}
