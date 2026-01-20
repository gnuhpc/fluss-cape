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
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.BindComplete;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.BindMessage;
import org.gnuhpc.fluss.cape.pg.session.PgPortal;
import org.gnuhpc.fluss.cape.pg.session.PgSession;
import org.gnuhpc.fluss.cape.pg.sql.PgSqlEngine;

public class PgBindHandler implements PgCommandHandler {

    @Override
    public List<PgBackendMessage> handle(PgSession session, PgFrontendMessage message) {
        BindMessage bindMessage = (BindMessage) message;
        List<Object> parameters;
        try {
            parameters = PgSqlEngine.decodeParameters(bindMessage.getParameters(), bindMessage.getParameterFormatCodes());
        } catch (IllegalArgumentException e) {
            return PgErrorResponseFactory.unsupported(e.getMessage());
        }
        String portalName = bindMessage.getPortal() == null ? "" : bindMessage.getPortal();
        String statementName = bindMessage.getStatement() == null ? "" : bindMessage.getStatement();
        if (!session.getPreparedStatements().containsKey(statementName)) {
            return PgErrorResponseFactory.unsupported("statement not found");
        }
        PgPortal portal = new PgPortal(portalName, statementName, parameters, bindMessage.getResultFormatCodes());
        session.getPortals().put(portalName, portal);
        return List.of(new BindComplete());
    }
}
