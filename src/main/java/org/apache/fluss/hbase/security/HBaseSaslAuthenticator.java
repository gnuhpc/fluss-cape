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

package org.apache.fluss.hbase.security;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.security.auth.sasl.jaas.JaasContext;
import org.apache.fluss.security.auth.sasl.jaas.LoginManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Locale;
import java.util.Map;

/** SASL authenticator supporting PLAIN, GSSAPI (Kerberos), and DIGEST-MD5 mechanisms. */
public class HBaseSaslAuthenticator {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseSaslAuthenticator.class);

    private static final String DEFAULT_MECHANISM = "PLAIN";
    private static final String GSSAPI_MECHANISM = "GSSAPI";
    private static final String DIGEST_MD5_MECHANISM = "DIGEST-MD5";

    private final Configuration configuration;
    private final String mechanism;
    private SaslServer saslServer;
    private LoginManager loginManager;
    private String authenticatedUser;

    public HBaseSaslAuthenticator(Configuration configuration) {
        this(configuration, DEFAULT_MECHANISM);
    }

    public HBaseSaslAuthenticator(Configuration configuration, String mechanism) {
        this.configuration = configuration;
        this.mechanism = mechanism != null ? mechanism.toUpperCase(Locale.ROOT) : DEFAULT_MECHANISM;
    }

    public void initialize(String clientAddress) throws AuthenticationException {
        try {
            String jaasConfigKey = getJaasConfigKey(mechanism);
            String dynamicJaasConfig = configuration.toMap().get(jaasConfigKey);

            if (dynamicJaasConfig == null || dynamicJaasConfig.isEmpty()) {
                LOG.warn(
                        "No JAAS config found for mechanism: {}. Falling back to JVM option: -D{}",
                        mechanism,
                        JaasContext.JAVA_LOGIN_CONFIG_PARAM);
            }

            JaasContext jaasContext =
                    JaasContext.loadServerContext("HBaseCompat", dynamicJaasConfig);

            this.loginManager = LoginManager.acquireLoginManager(jaasContext);

            this.saslServer = createSaslServer(mechanism, clientAddress);

        } catch (Exception e) {
            throw new AuthenticationException("Failed to initialize SASL authenticator", e);
        }
    }

    private SaslServer createSaslServer(String mechanism, String clientAddress)
            throws SaslException {
        Map<String, String> saslProps = configuration.toMap();

        CallbackHandler callbackHandler = createCallbackHandler(mechanism);

        try {
            return Subject.doAs(
                    loginManager.subject(),
                    (PrivilegedExceptionAction<SaslServer>)
                            () -> {
                                return Sasl.createSaslServer(
                                        mechanism,
                                        "hbase",
                                        clientAddress,
                                        saslProps,
                                        callbackHandler);
                            });
        } catch (PrivilegedActionException e) {
            throw new SaslException("Failed to create SASL server", e.getCause());
        }
    }

    private CallbackHandler createCallbackHandler(String mechanism) {
        switch (mechanism) {
            case DEFAULT_MECHANISM:
                return new PlainServerCallbackHandler(configuration);
            case GSSAPI_MECHANISM:
                return new GssapiServerCallbackHandler();
            case DIGEST_MD5_MECHANISM:
                return new DigestMd5ServerCallbackHandler(configuration);
            default:
                throw new IllegalArgumentException("Unsupported SASL mechanism: " + mechanism);
        }
    }

    private String getJaasConfigKey(String mechanism) {
        return String.format("security.sasl.%s.jaas.config", mechanism.toLowerCase(Locale.ROOT));
    }

    public byte[] evaluateResponse(byte[] token) throws AuthenticationException {
        if (saslServer == null) {
            throw new AuthenticationException("SASL server not initialized");
        }

        try {
            return saslServer.evaluateResponse(token);
        } catch (SaslException e) {
            throw new AuthenticationException("SASL authentication failed: " + e.getMessage(), e);
        }
    }

    public boolean isComplete() {
        return saslServer != null && saslServer.isComplete();
    }

    public String getAuthenticatedUser() {
        if (saslServer == null || !saslServer.isComplete()) {
            return null;
        }

        if (authenticatedUser == null) {
            authenticatedUser = saslServer.getAuthorizationID();
        }

        return authenticatedUser;
    }

    public void dispose() {
        if (saslServer != null) {
            try {
                saslServer.dispose();
            } catch (SaslException e) {
                LOG.warn("Failed to dispose SASL server", e);
            }
        }

        if (loginManager != null) {
            loginManager.release();
        }
    }

    private static class PlainServerCallbackHandler implements CallbackHandler {
        private final Map<String, String> credentials;

        PlainServerCallbackHandler(Configuration configuration) {
            this.credentials = configuration.toMap();
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            String username = null;
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    NameCallback nc = (NameCallback) callback;
                    username = nc.getDefaultName();
                    nc.setName(username);
                } else if (callback instanceof PasswordCallback) {
                    PasswordCallback pc = (PasswordCallback) callback;
                    if (username != null) {
                        String userKey = "user_" + username;
                        String password = credentials.get(userKey);
                        if (password != null) {
                            pc.setPassword(password.toCharArray());
                        }
                    }
                } else if (callback instanceof AuthorizeCallback) {
                    AuthorizeCallback ac = (AuthorizeCallback) callback;
                    String authId = ac.getAuthenticationID();
                    String authzId = ac.getAuthorizationID();
                    ac.setAuthorized(authId.equals(authzId));
                    if (ac.isAuthorized()) {
                        ac.setAuthorizedID(authzId);
                    }
                } else if (callback instanceof RealmCallback) {
                    RealmCallback rc = (RealmCallback) callback;
                    rc.setText(rc.getDefaultText());
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        }
    }

    private static class GssapiServerCallbackHandler implements CallbackHandler {
        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof AuthorizeCallback) {
                    AuthorizeCallback ac = (AuthorizeCallback) callback;
                    String authId = ac.getAuthenticationID();
                    String authzId = ac.getAuthorizationID();

                    String shortName = extractShortName(authId);
                    ac.setAuthorized(true);
                    ac.setAuthorizedID(shortName);
                } else if (callback instanceof RealmCallback) {
                    RealmCallback rc = (RealmCallback) callback;
                    rc.setText(rc.getDefaultText());
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        }

        private String extractShortName(String principal) {
            if (principal == null) {
                return null;
            }
            int slashIdx = principal.indexOf('/');
            int atIdx = principal.indexOf('@');

            if (slashIdx > 0 && atIdx > slashIdx) {
                return principal.substring(0, slashIdx);
            } else if (atIdx > 0) {
                return principal.substring(0, atIdx);
            }
            return principal;
        }
    }

    private static class DigestMd5ServerCallbackHandler implements CallbackHandler {
        private final Map<String, String> credentials;

        DigestMd5ServerCallbackHandler(Configuration configuration) {
            this.credentials = configuration.toMap();
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            String username = null;
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    NameCallback nc = (NameCallback) callback;
                    username = nc.getDefaultName();
                    nc.setName(username);
                } else if (callback instanceof PasswordCallback) {
                    PasswordCallback pc = (PasswordCallback) callback;
                    if (username != null) {
                        String userKey = "user_" + username;
                        String password = credentials.get(userKey);
                        if (password != null) {
                            pc.setPassword(password.toCharArray());
                        }
                    }
                } else if (callback instanceof AuthorizeCallback) {
                    AuthorizeCallback ac = (AuthorizeCallback) callback;
                    String authId = ac.getAuthenticationID();
                    String authzId = ac.getAuthorizationID();
                    ac.setAuthorized(authId.equals(authzId));
                    if (ac.isAuthorized()) {
                        ac.setAuthorizedID(authzId);
                    }
                } else if (callback instanceof RealmCallback) {
                    RealmCallback rc = (RealmCallback) callback;
                    rc.setText(rc.getDefaultText());
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        }
    }
}
