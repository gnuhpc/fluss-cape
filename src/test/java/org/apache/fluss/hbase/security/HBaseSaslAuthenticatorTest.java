package org.gnuhpc.fluss.cape.hbase.security;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HBaseSaslAuthenticatorTest {

    private HBaseSaslAuthenticator authenticator;

    @AfterEach
    void teardown() {
        if (authenticator != null) {
            authenticator.dispose();
        }
    }

    @Test
    void testPlainAuthenticationSuccess() throws Exception {
        Configuration config = new Configuration();
        config.setString(
                "security.sasl.plain.jaas.config",
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required "
                        + "user_testuser=\"testpass\";");

        authenticator = new HBaseSaslAuthenticator(config, "PLAIN");
        authenticator.initialize("127.0.0.1");

        byte[] clientInitial = buildPlainInitialResponse("testuser", "testpass");
        authenticator.evaluateResponse(clientInitial);

        assertThat(authenticator.isComplete()).isTrue();
        assertThat(authenticator.getAuthenticatedUser()).isEqualTo("testuser");
    }

    @Test
    void testPlainAuthenticationWrongPassword() throws Exception {
        Configuration config = new Configuration();
        config.setString(
                "security.sasl.plain.jaas.config",
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required "
                        + "user_testuser=\"testpass\";");

        authenticator = new HBaseSaslAuthenticator(config, "PLAIN");
        authenticator.initialize("127.0.0.1");

        byte[] clientInitial = buildPlainInitialResponse("testuser", "wrongpass");

        assertThatThrownBy(() -> authenticator.evaluateResponse(clientInitial))
                .isInstanceOf(AuthenticationException.class)
                .hasMessageContaining("SASL authentication failed");
    }

    @Test
    void testPlainAuthenticationUnknownUser() throws Exception {
        Configuration config = new Configuration();
        config.setString(
                "security.sasl.plain.jaas.config",
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required "
                        + "user_testuser=\"testpass\";");

        authenticator = new HBaseSaslAuthenticator(config, "PLAIN");
        authenticator.initialize("127.0.0.1");

        byte[] clientInitial = buildPlainInitialResponse("unknownuser", "anypass");

        assertThatThrownBy(() -> authenticator.evaluateResponse(clientInitial))
                .isInstanceOf(AuthenticationException.class);
    }

    @Test
    void testUninitializedAuthenticator() {
        Configuration config = new Configuration();
        authenticator = new HBaseSaslAuthenticator(config, "PLAIN");

        assertThatThrownBy(() -> authenticator.evaluateResponse(new byte[0]))
                .isInstanceOf(AuthenticationException.class)
                .hasMessageContaining("SASL server not initialized");
    }

    @Test
    void testGetAuthenticatedUserBeforeCompletion() throws Exception {
        Configuration config = new Configuration();
        config.setString(
                "security.sasl.plain.jaas.config",
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required "
                        + "user_testuser=\"testpass\";");

        authenticator = new HBaseSaslAuthenticator(config, "PLAIN");
        authenticator.initialize("127.0.0.1");

        assertThat(authenticator.getAuthenticatedUser()).isNull();
        assertThat(authenticator.isComplete()).isFalse();
    }

    @Test
    void testMultipleUsers() throws Exception {
        Configuration config = new Configuration();
        config.setString(
                "security.sasl.plain.jaas.config",
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required "
                        + "user_alice=\"alice-pass\" "
                        + "user_bob=\"bob-pass\" "
                        + "user_charlie=\"charlie-pass\";");

        authenticator = new HBaseSaslAuthenticator(config, "PLAIN");
        authenticator.initialize("127.0.0.1");

        byte[] aliceInitial = buildPlainInitialResponse("alice", "alice-pass");
        authenticator.evaluateResponse(aliceInitial);

        assertThat(authenticator.isComplete()).isTrue();
        assertThat(authenticator.getAuthenticatedUser()).isEqualTo("alice");

        authenticator.dispose();

        authenticator = new HBaseSaslAuthenticator(config, "PLAIN");
        authenticator.initialize("127.0.0.1");

        byte[] bobInitial = buildPlainInitialResponse("bob", "bob-pass");
        authenticator.evaluateResponse(bobInitial);

        assertThat(authenticator.isComplete()).isTrue();
        assertThat(authenticator.getAuthenticatedUser()).isEqualTo("bob");
    }

    private byte[] buildPlainInitialResponse(String username, String password) {
        byte[] usernameBytes = username.getBytes();
        byte[] passwordBytes = password.getBytes();

        byte[] response = new byte[1 + usernameBytes.length + 1 + passwordBytes.length];
        int pos = 0;

        response[pos++] = 0;

        System.arraycopy(usernameBytes, 0, response, pos, usernameBytes.length);
        pos += usernameBytes.length;

        response[pos++] = 0;

        System.arraycopy(passwordBytes, 0, response, pos, passwordBytes.length);

        return response;
    }
}
