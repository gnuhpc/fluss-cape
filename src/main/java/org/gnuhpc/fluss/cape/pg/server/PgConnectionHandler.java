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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.gnuhpc.fluss.cape.pg.executor.PgBindHandler;
import org.gnuhpc.fluss.cape.pg.executor.PgCloseHandler;
import org.gnuhpc.fluss.cape.pg.executor.PgCommandRouter;
import org.gnuhpc.fluss.cape.pg.executor.PgDescribeHandler;
import org.gnuhpc.fluss.cape.pg.executor.PgExecuteHandler;
import org.gnuhpc.fluss.cape.pg.executor.PgParseHandler;
import org.gnuhpc.fluss.cape.pg.executor.PgSimpleQueryHandler;
import org.gnuhpc.fluss.cape.pg.executor.PgSyncHandler;
import org.gnuhpc.fluss.cape.pg.executor.PgUnsupportedCommandHandler;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessage;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.AuthenticationCleartextPassword;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.AuthenticationOk;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.BackendKeyData;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.ErrorResponse;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.ParameterStatus;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.ReadyForQuery;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.ReadyStatus;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.SslResponse;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.CancelRequest;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.CopyDataMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.CopyDoneMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.CopyFailMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.FlushMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.PasswordMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.SslRequest;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.StartupMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.TerminateMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.QueryMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.ParseMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.DescribeMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.BindMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.ExecuteMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.CloseMessage;
import org.gnuhpc.fluss.cape.pg.protocol.frontend.PgFrontendMessages.SyncMessage;
import org.gnuhpc.fluss.cape.pg.session.PgSession;
import org.gnuhpc.fluss.cape.pg.sql.PgSqlEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PgConnectionHandler extends SimpleChannelInboundHandler<PgFrontendMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(PgConnectionHandler.class);

    private final PgAuthConfig authConfig;
    private final String defaultDatabase;
    private final PgSession session;
    private final PgCommandRouter router;
    private final int backendProcessId;
    private final int backendSecretKey;
    private boolean authenticated;
    private boolean passwordRequested;

    public PgConnectionHandler(
            PgAuthConfig authConfig,
            Connection flussConnection,
            Admin admin,
            String defaultDatabase) {
        super(false);
        this.authConfig = authConfig;
        this.defaultDatabase = defaultDatabase == null || defaultDatabase.isBlank()
                ? "default"
                : defaultDatabase;
        this.session = new PgSession();
        this.session.setFlussConnection(flussConnection);
        this.session.setFlussAdmin(admin);
        this.router = new PgCommandRouter(new PgUnsupportedCommandHandler("command not supported yet"));
        this.backendProcessId = ThreadLocalRandom.current().nextInt();
        this.backendSecretKey = ThreadLocalRandom.current().nextInt();
        registerHandlers();
    }

    private void registerHandlers() {
        router.register(QueryMessage.class, new PgSimpleQueryHandler());
        router.register(ParseMessage.class, new PgParseHandler());
        router.register(DescribeMessage.class, new PgDescribeHandler());
        router.register(BindMessage.class, new PgBindHandler());
        router.register(ExecuteMessage.class, new PgExecuteHandler());
        router.register(SyncMessage.class, new PgSyncHandler());
        router.register(CloseMessage.class, new PgCloseHandler());
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        LOG.info("PG client connected from {}", ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        LOG.info("PG client disconnected from {}", ctx.channel().remoteAddress());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, PgFrontendMessage message) {
        if (message instanceof SslRequest) {
            ctx.writeAndFlush(new SslResponse());
            return;
        }
        if (message instanceof CancelRequest) {
            ctx.close();
            return;
        }
        if (message instanceof StartupMessage) {
            handleStartup(ctx, (StartupMessage) message);
            return;
        }
        if (message instanceof PasswordMessage) {
            handlePassword(ctx, (PasswordMessage) message);
            return;
        }
        if (message instanceof TerminateMessage) {
            ctx.close();
            return;
        }
        if (message instanceof FlushMessage) {
            ctx.flush();
            return;
        }
        if (!authenticated) {
            ctx.writeAndFlush(new ErrorResponse("ERROR", "28P01", "authentication required"));
            return;
        }
        if (session.getCopyState() != null) {
            handleCopy(ctx, message);
            return;
        }
        List<PgBackendMessage> responses = router.route(session, message);
        for (PgBackendMessage response : responses) {
            ctx.write(response);
        }
        ctx.flush();
    }

    private void handleStartup(ChannelHandlerContext ctx, StartupMessage message) {
        String username = message.getParameter("user");
        String database = message.getParameter("database");
        
        session.setUsername(username);
        if (database != null && !database.isBlank()) {
            session.setDatabase(database);
        } else {
            session.setDatabase(defaultDatabase);
        }
        
        LOG.info("Client connecting to database: {}", session.getDatabase());
        
        if (authConfig.getMode() == PgAuthMode.TRUST) {
            authenticated = true;
            session.setAuthenticated(true);
            sendReady(ctx, username);
            return;
        }
        passwordRequested = true;
        ctx.writeAndFlush(new AuthenticationCleartextPassword());
    }

    private void handlePassword(ChannelHandlerContext ctx, PasswordMessage message) {
        if (!passwordRequested) {
            ctx.writeAndFlush(new ErrorResponse("ERROR", "28P01", "unexpected password message"));
            return;
        }
        String expectedUser = authConfig.getUser();
        String expectedPassword = authConfig.getPassword();
        String username = session.getUsername();
        if (expectedUser != null && username != null && !expectedUser.equals(username)) {
            ctx.writeAndFlush(new ErrorResponse("ERROR", "28P01", "invalid user"));
            return;
        }
        if (expectedPassword != null && !expectedPassword.equals(message.getPassword())) {
            ctx.writeAndFlush(new ErrorResponse("ERROR", "28P01", "invalid password"));
            return;
        }
        authenticated = true;
        session.setAuthenticated(true);
        sendReady(ctx, username);
    }

    private void sendReady(ChannelHandlerContext ctx, String username) {
        ctx.write(new AuthenticationOk());
        ctx.write(new ParameterStatus("server_version", "14.0"));
        ctx.write(new ParameterStatus("server_encoding", "UTF8"));
        ctx.write(new ParameterStatus("client_encoding", "UTF8"));
        ctx.write(new ParameterStatus("DateStyle", "ISO, MDY"));
        ctx.write(new ParameterStatus("standard_conforming_strings", "on"));
        ctx.write(new ParameterStatus("integer_datetimes", "on"));
        ctx.write(new ParameterStatus("application_name", "pgcli"));
        ctx.write(new ParameterStatus("TimeZone", "UTC"));
        ctx.write(new ParameterStatus("is_superuser", "false"));
        ctx.write(new ParameterStatus("session_authorization", username == null ? "" : username));
        ctx.write(new ParameterStatus("max_identifier_length", "63"));
        ctx.write(new ParameterStatus("max_connections", "100"));
        ctx.write(new BackendKeyData(backendProcessId, backendSecretKey));
        ctx.writeAndFlush(new ReadyForQuery(ReadyStatus.IDLE));
    }

    private void handleCopy(ChannelHandlerContext ctx, PgFrontendMessage message) {
        if (message instanceof CopyDataMessage) {
            session.getCopyState().append(((CopyDataMessage) message).getPayload());
            return;
        }
        if (message instanceof CopyFailMessage) {
            session.setCopyState(null);
            ctx.write(new ErrorResponse("ERROR", "57014", "COPY canceled"));
            ctx.writeAndFlush(new ReadyForQuery(ReadyStatus.IDLE));
            return;
        }
        if (message instanceof CopyDoneMessage) {
            try {
                long count = PgSqlEngine.copyFromStdin(session, session.getCopyState());
                session.setCopyState(null);
                ctx.write(new org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessages.CommandComplete(
                        "COPY " + count));
                ctx.writeAndFlush(new ReadyForQuery(ReadyStatus.IDLE));
            } catch (Exception e) {
                session.setCopyState(null);
                ctx.write(new ErrorResponse("ERROR", "XX000", e.getMessage()));
                ctx.writeAndFlush(new ReadyForQuery(ReadyStatus.IDLE));
            }
            return;
        }
        ctx.write(new ErrorResponse("ERROR", "57014", "unexpected COPY message"));
        ctx.writeAndFlush(new ReadyForQuery(ReadyStatus.IDLE));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.warn("PG connection error", cause);
        ctx.close();
    }
}
