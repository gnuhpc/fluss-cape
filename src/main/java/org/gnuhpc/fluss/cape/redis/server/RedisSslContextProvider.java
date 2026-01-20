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

package org.gnuhpc.fluss.cape.redis.server;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;

import java.io.File;

public class RedisSslContextProvider {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSslContextProvider.class);

    private final boolean sslEnabled;
    private final SslContext sslContext;

    public static RedisSslContextProvider create() throws SSLException {
        String certChainPath = System.getenv("REDIS_TLS_CERT_CHAIN_FILE");
        String privateKeyPath = System.getenv("REDIS_TLS_PRIVATE_KEY_FILE");
        String caFilePath = System.getenv("REDIS_TLS_CA_FILE");
        
        boolean sslEnabled = certChainPath != null && privateKeyPath != null;
        
        if (!sslEnabled) {
            LOG.info("Redis TLS/SSL DISABLED - set REDIS_TLS_CERT_CHAIN_FILE and REDIS_TLS_PRIVATE_KEY_FILE to enable");
            return new RedisSslContextProvider(false, null);
        }
        
        LOG.info("Redis TLS/SSL ENABLED");
        LOG.info("  Certificate chain: {}", certChainPath);
        LOG.info("  Private key: {}", privateKeyPath);
        if (caFilePath != null) {
            LOG.info("  CA file: {}", caFilePath);
        }
        
        File certChainFile = new File(certChainPath);
        File privateKeyFile = new File(privateKeyPath);
        
        if (!certChainFile.exists()) {
            throw new IllegalArgumentException("Certificate chain file not found: " + certChainPath);
        }
        if (!privateKeyFile.exists()) {
            throw new IllegalArgumentException("Private key file not found: " + privateKeyPath);
        }
        
        SslContextBuilder builder = SslContextBuilder
                .forServer(certChainFile, privateKeyFile)
                .sslProvider(SslProvider.JDK);
        
        if (caFilePath != null) {
            File caFile = new File(caFilePath);
            if (!caFile.exists()) {
                throw new IllegalArgumentException("CA file not found: " + caFilePath);
            }
            builder.trustManager(caFile);
            LOG.info("Client certificate validation ENABLED (mutual TLS)");
        } else {
            LOG.info("Client certificate validation DISABLED (server-side TLS only)");
        }
        
        SslContext sslContext = builder.build();
        LOG.info("Redis TLS/SSL context created successfully");
        
        return new RedisSslContextProvider(true, sslContext);
    }

    private RedisSslContextProvider(boolean sslEnabled, SslContext sslContext) {
        this.sslEnabled = sslEnabled;
        this.sslContext = sslContext;
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }

    public SslContext getSslContext() {
        return sslContext;
    }
}
