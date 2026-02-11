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

package org.gnuhpc.fluss.cape.pg.server;

import org.gnuhpc.fluss.cape.common.configuration.CapeConfig;

public final class PgServerConfig {

    private final boolean enabled;
    private final String bindAddress;
    private final int port;
    private final PgAuthMode authMode;
    private final String authUser;
    private final String authPassword;
    private final String database;

    private PgServerConfig(
            boolean enabled,
            String bindAddress,
            int port,
            PgAuthMode authMode,
            String authUser,
            String authPassword,
            String database) {
        this.enabled = enabled;
        this.bindAddress = bindAddress;
        this.port = port;
        this.authMode = authMode;
        this.authUser = authUser;
        this.authPassword = authPassword;
        this.database = database;
    }

    public static PgServerConfig fromSystemProperties() {
        boolean enabled = Boolean.parseBoolean(System.getProperty("pg.enabled", "false"));
        String bindAddress = System.getProperty("pg.bind.address", "0.0.0.0");
        int port = Integer.parseInt(System.getProperty("pg.port", "5432"));
        PgAuthMode authMode = PgAuthMode.fromString(System.getProperty("pg.auth.mode", "trust"));
        String authUser = System.getProperty("pg.auth.user");
        String authPassword = System.getProperty("pg.auth.password");
        String database = System.getProperty("pg.database", "default");
        return new PgServerConfig(enabled, bindAddress, port, authMode, authUser, authPassword, database);
    }

    public static PgServerConfig fromCapeConfig(CapeConfig capeConfig) {
        boolean enabled = capeConfig.isPgEnabled();
        String bindAddress = capeConfig.getPgBindAddress();
        int port = capeConfig.getPgPort();
        PgAuthMode authMode = PgAuthMode.fromString(capeConfig.getPgAuthMode());
        String authUser = capeConfig.getPgAuthUser();
        String authPassword = capeConfig.getPgAuthPassword();
        String database = capeConfig.getPgDatabase();
        return new PgServerConfig(enabled, bindAddress, port, authMode, authUser, authPassword, database);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public String getBindAddress() {
        return bindAddress;
    }

    public int getPort() {
        return port;
    }

    public PgAuthMode getAuthMode() {
        return authMode;
    }

    public String getAuthUser() {
        return authUser;
    }

    public String getAuthPassword() {
        return authPassword;
    }

    public String getDatabase() {
        return database;
    }
}
