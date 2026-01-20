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
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.NoData;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.ParameterDescription;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.RowDescription;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.DescribeMessage;
import org.gnuhpc.fluss.cape.pg.session.PgPortal;
import org.gnuhpc.fluss.cape.pg.session.PgPreparedStatement;
import org.gnuhpc.fluss.cape.pg.session.PgSession;
import org.gnuhpc.fluss.cape.pg.sql.PgQueryResult;
import org.gnuhpc.fluss.cape.pg.sql.PgSqlEngine;

public class PgDescribeHandler implements PgCommandHandler {

    @Override
    public List<PgBackendMessage> handle(PgSession session, PgFrontendMessage message) {
        DescribeMessage describeMessage = (DescribeMessage) message;
        String name = describeMessage.getName() == null ? "" : describeMessage.getName();

        PgPreparedStatement prepared;
        if (describeMessage.getType() == 'S') {
            prepared = session.getPreparedStatements().get(name);
        } else if (describeMessage.getType() == 'P') {
            PgPortal portal = session.getPortals().get(name);
            if (portal == null) {
                return PgErrorResponseFactory.unsupported("portal not found");
            }
            prepared = session.getPreparedStatements().get(portal.getStatementName());
        } else {
            return List.of(new NoData());
        }

        if (prepared == null || prepared.getSql() == null) {
            return PgErrorResponseFactory.unsupported("statement not found");
        }

        List<PgBackendMessage> responses = new ArrayList<>();
        if (describeMessage.getType() == 'S') {
            responses.add(new ParameterDescription(prepared.getParameterTypeOids()));
        }
        try {
            PgQueryResult result = PgSqlEngine.describe(session, prepared.getSql());
            RowDescription rowDescription = result.getRowDescription();
            responses.add(rowDescription == null ? new NoData() : rowDescription);
            return responses;
        } catch (IllegalArgumentException e) {
            return PgErrorResponseFactory.unsupported(e.getMessage());
        } catch (Exception e) {
            return PgErrorResponseFactory.error("XX000", e.getMessage());
        }
    }
}
