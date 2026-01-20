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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessage;
import org.gnuhpc.fluss.cape.pg.session.PgSession;

public class PgCommandRouter {

    private final Map<Class<? extends PgFrontendMessage>, PgCommandHandler> handlers = new HashMap<>();
    private final PgCommandHandler defaultHandler;

    public PgCommandRouter(PgCommandHandler defaultHandler) {
        this.defaultHandler = defaultHandler;
    }

    public void register(Class<? extends PgFrontendMessage> type, PgCommandHandler handler) {
        handlers.put(type, handler);
    }

    public List<PgBackendMessage> route(PgSession session, PgFrontendMessage message) {
        PgCommandHandler handler = handlers.get(message.getClass());
        if (handler == null) {
            return defaultHandler == null ? Collections.emptyList() : defaultHandler.handle(session, message);
        }
        return handler.handle(session, message);
    }
}
