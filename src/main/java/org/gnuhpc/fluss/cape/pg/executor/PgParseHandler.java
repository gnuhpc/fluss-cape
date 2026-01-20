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

package org.gnuhpc.fluss.cape.pg.executor;

import java.util.List;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessage;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.ParseComplete;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.ParseMessage;
import org.gnuhpc.fluss.cape.pg.session.PgPreparedStatement;
import org.gnuhpc.fluss.cape.pg.session.PgSession;
import org.gnuhpc.fluss.cape.pg.sql.PgSqlEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PgParseHandler implements PgCommandHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PgParseHandler.class);

    @Override
    public List<PgBackendMessage> handle(PgSession session, PgFrontendMessage message) {
        ParseMessage parseMessage = (ParseMessage) message;
        String statementId = parseMessage.getName() == null ? "" : parseMessage.getName();
        String sql = parseMessage.getSql();
        LOG.info("Parse SQL: {}", sql);
        if (sql == null || sql.trim().isEmpty()) {
            return PgErrorResponseFactory.unsupported("empty query");
        }
        try {
            PgSqlEngine.validate(sql);
        } catch (IllegalArgumentException e) {
            return PgErrorResponseFactory.unsupported(e.getMessage());
        }
        session.getPreparedStatements().put(
                statementId,
                new PgPreparedStatement(statementId, sql, parseMessage.getParameterTypeOids()));
        return List.of(new ParseComplete());
    }
}
