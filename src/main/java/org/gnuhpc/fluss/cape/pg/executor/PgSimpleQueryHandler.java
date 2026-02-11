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
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.CopyInResponse;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.DataRow;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.ReadyForQuery;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.ReadyStatus;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.RowDescription;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.QueryMessage;
import org.gnuhpc.fluss.cape.pg.session.PgCopyState;
import org.gnuhpc.fluss.cape.pg.session.PgSession;
import org.gnuhpc.fluss.cape.pg.sql.PgExecutionResult;
import org.gnuhpc.fluss.cape.pg.sql.PgQueryResult;
import org.gnuhpc.fluss.cape.pg.sql.PgSqlEngine;
import org.gnuhpc.fluss.cape.pg.sql.PgSqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PgSimpleQueryHandler implements PgCommandHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PgSimpleQueryHandler.class);

    @Override
    public List<PgBackendMessage> handle(PgSession session, PgFrontendMessage message) {
        QueryMessage queryMessage = (QueryMessage) message;
        String sql = queryMessage.getSql();
        LOG.info("Simple query SQL: {}", sql);
        if (sql == null || sql.trim().isEmpty()) {
            return PgErrorResponseFactory.errorWithReady("0A000", "empty query");
        }
        String normalized = sql.trim().toUpperCase();
        if (normalized.startsWith("BEGIN")
                || normalized.startsWith("START TRANSACTION")
                || normalized.startsWith("COMMIT")
                || normalized.startsWith("ROLLBACK")) {
            return PgErrorResponseFactory.errorWithReady("25000", "transactions not supported");
        }
        PgSqlParser.CopyCommand copyCommand = PgSqlParser.parseCopyFromStdin(sql);
        if (copyCommand != null) {
            session.setCopyState(new PgCopyState(copyCommand));
            return List.of(new CopyInResponse((short) 0, new short[0]));
        }
        try {
            PgExecutionResult execution = PgSqlEngine.execute(session, sql, List.of());
            return respondWithResult(execution, true);
        } catch (IllegalArgumentException e) {
            return PgErrorResponseFactory.errorWithReady("0A000", PgErrorSanitizer.sanitize(e));
        } catch (Exception e) {
            return PgErrorResponseFactory.errorWithReady("XX000", PgErrorSanitizer.sanitize(e));
        }
    }

    private List<PgBackendMessage> respondWithResult(PgExecutionResult execution, boolean includeReady) {
        List<PgBackendMessage> responses = new ArrayList<>();
        if (execution.isQuery()) {
            PgQueryResult result = execution.getQueryResult();
            RowDescription rowDescription = result.getRowDescription();
            if (rowDescription != null) {
                responses.add(rowDescription);
            }
            for (Object[] row : result.getRows()) {
                responses.add(new DataRow(PgSqlEngine.encodeRow(row, result.getRowDescription())));
            }
            responses.add(new CommandComplete(execution.getCommandTag()));
        } else {
            responses.add(new CommandComplete(execution.getCommandTag()));
        }
        if (includeReady) {
            responses.add(new ReadyForQuery(ReadyStatus.IDLE));
        }
        return responses;
    }
}
