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
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.CloseComplete;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.CloseMessage;
import org.gnuhpc.fluss.cape.pg.session.PgSession;

public class PgCloseHandler implements PgCommandHandler {

    @Override
    public List<PgBackendMessage> handle(PgSession session, PgFrontendMessage message) {
        CloseMessage closeMessage = (CloseMessage) message;
        String name = closeMessage.getName() == null ? "" : closeMessage.getName();
        if (closeMessage.getType() == 'S') {
            session.getPreparedStatements().remove(name);
        } else if (closeMessage.getType() == 'P') {
            session.getPortals().remove(name);
        }
        return List.of(new CloseComplete());
    }
}
