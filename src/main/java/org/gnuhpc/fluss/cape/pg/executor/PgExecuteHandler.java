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

import java.util.ArrayList;
import java.util.List;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessage;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.CommandComplete;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.DataRow;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.ExecuteMessage;
import org.gnuhpc.fluss.cape.pg.session.PgPortal;
import org.gnuhpc.fluss.cape.pg.session.PgPreparedStatement;
import org.gnuhpc.fluss.cape.pg.session.PgSession;
import org.gnuhpc.fluss.cape.pg.sql.PgExecutionResult;
import org.gnuhpc.fluss.cape.pg.sql.PgQueryResult;
import org.gnuhpc.fluss.cape.pg.sql.PgSqlEngine;

public class PgExecuteHandler implements PgCommandHandler {

    @Override
    public List<PgBackendMessage> handle(PgSession session, PgFrontendMessage message) {
        ExecuteMessage executeMessage = (ExecuteMessage) message;
        String portalName = executeMessage.getPortal() == null ? "" : executeMessage.getPortal();
        PgPortal portal = session.getPortals().get(portalName);
        if (portal == null) {
            return PgErrorResponseFactory.unsupported("portal not found");
        }
        PgPreparedStatement statement = session.getPreparedStatements().get(portal.getStatementName());
        if (statement == null) {
            return PgErrorResponseFactory.unsupported("statement not found");
        }
        try {
            PgExecutionResult execution = PgSqlEngine.execute(session, statement.getSql(), portal.getParameters());
            return buildResponses(execution, portal.getResultFormatCodes(), executeMessage.getMaxRows());
        } catch (IllegalArgumentException e) {
            return PgErrorResponseFactory.unsupported(e.getMessage());
        } catch (Exception e) {
            return PgErrorResponseFactory.error("XX000", e.getMessage());
        }
    }

    private List<PgBackendMessage> buildResponses(PgExecutionResult execution, short[] formatCodes, int maxRows) {
        List<PgBackendMessage> responses = new ArrayList<>();
        if (execution.isQuery()) {
            PgQueryResult result = execution.getQueryResult();
            List<Object[]> rows = result.getRows();
            int rowsToReturn = maxRows > 0 ? Math.min(maxRows, rows.size()) : rows.size();
            
            for (int i = 0; i < rowsToReturn; i++) {
                responses.add(new DataRow(PgSqlEngine.encodeRow(rows.get(i), result.getRowDescription(), formatCodes)));
            }
        }
        responses.add(new CommandComplete(execution.getCommandTag()));
        return responses;
    }
}
