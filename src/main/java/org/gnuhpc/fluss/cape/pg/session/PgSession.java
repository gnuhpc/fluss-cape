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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;

public class PgSession {

    private final Map<String, PgPreparedStatement> preparedStatements = new ConcurrentHashMap<>();
    private final Map<String, PgPortal> portals = new ConcurrentHashMap<>();

    private String username;
    private boolean authenticated;
    private Connection flussConnection;
    private Admin flussAdmin;
    private String database;
    private PgCopyState copyState;

    public Map<String, PgPreparedStatement> getPreparedStatements() {
        return preparedStatements;
    }

    public Map<String, PgPortal> getPortals() {
        return portals;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public boolean isAuthenticated() {
        return authenticated;
    }

    public void setAuthenticated(boolean authenticated) {
        this.authenticated = authenticated;
    }

    public Connection getFlussConnection() {
        return flussConnection;
    }

    public void setFlussConnection(Connection flussConnection) {
        this.flussConnection = flussConnection;
    }

    public Admin getFlussAdmin() {
        return flussAdmin;
    }

    public void setFlussAdmin(Admin flussAdmin) {
        this.flussAdmin = flussAdmin;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public PgCopyState getCopyState() {
        return copyState;
    }

    public void setCopyState(PgCopyState copyState) {
        this.copyState = copyState;
    }
}
