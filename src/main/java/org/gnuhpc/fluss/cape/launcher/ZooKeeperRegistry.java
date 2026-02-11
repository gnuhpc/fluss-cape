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

package org.gnuhpc.fluss.cape.launcher;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.gnuhpc.fluss.cape.common.configuration.CapeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZooKeeperRegistry implements ServerComponent {
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperRegistry.class);
    
    private final String zkQuorum;
    private final ServerName serverName;
    private final String serverId;
    private final CapeConfig capeConfig;
    
    private ZooKeeper zooKeeper;
    private String regionServerZnodePath;
    private volatile boolean running = false;
    
    public ZooKeeperRegistry(String zkQuorum, ServerName serverName, String serverId, CapeConfig capeConfig) {
        this.zkQuorum = zkQuorum;
        this.serverName = serverName;
        this.serverId = serverId;
        this.capeConfig = capeConfig;
    }
    
    @Override
    public void start() throws Exception {
        LOG.info("Connecting to ZooKeeper: {}", zkQuorum);
        
        CountDownLatch connected = new CountDownLatch(1);
        this.zooKeeper = new ZooKeeper(zkQuorum, 30000, e -> {
            if (e.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        
        if (!connected.await(10, TimeUnit.SECONDS)) {
            throw new RuntimeException("ZooKeeper connection timeout");
        }
        
        authenticateIfConfigured();
        List<ACL> acls = resolveAcls();
        
        ensureZNodeExists("/hbase", acls);
        ensureZNodeExists("/hbase/rs", acls);
        
        registerRegionServer(acls);
        registerMaster(acls);
        
        running = true;
        LOG.info("ZooKeeper registration completed");
    }
    
    @Override
    public void close() throws Exception {
        running = false;
        
        if (zooKeeper == null) {
            return;
        }
        
        try {
            if (regionServerZnodePath != null) {
                zooKeeper.delete(regionServerZnodePath, -1);
                LOG.info("Unregistered from ZooKeeper: {}", regionServerZnodePath);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister from ZooKeeper", e);
        } finally {
            zooKeeper.close();
        }
    }
    
    @Override
    public String getName() {
        return "ZooKeeperRegistry";
    }
    
    @Override
    public boolean isRunning() {
        return running;
    }
    
    private void authenticateIfConfigured() {
        String authScheme = capeConfig.getZkAuthScheme();
        String authToken = capeConfig.getZkAuthToken();
        
        if (authScheme != null && authScheme.isBlank()) {
            authScheme = null;
        }
        if (authToken != null && authToken.isBlank()) {
            authToken = null;
        }
        
        if ((authScheme == null) != (authToken == null)) {
            throw new IllegalArgumentException(
                "ZooKeeper auth requires both hbase.zookeeper.auth.scheme and hbase.zookeeper.auth.token");
        }
        
        if (authScheme != null) {
            zooKeeper.addAuthInfo(authScheme, authToken.getBytes(StandardCharsets.UTF_8));
            LOG.info("ZooKeeper authentication configured with scheme: {}", authScheme);
        }
    }
    
    private List<ACL> resolveAcls() {
        String mode = capeConfig.getZkAclMode();
        String normalized = mode == null
                ? "creator-read"
                : mode.trim().toLowerCase(java.util.Locale.ROOT);
        
        boolean hasAuth = capeConfig.getZkAuthScheme() != null && !capeConfig.getZkAuthScheme().isBlank();
        
        if ((normalized.contains("creator") || normalized.contains("auth")) && !hasAuth) {
            throw new IllegalStateException(
                "ZooKeeper ACL mode '" + normalized + "' requires auth. " +
                "Set hbase.zookeeper.auth.scheme and hbase.zookeeper.auth.token, " +
                "or use hbase.zookeeper.acl.mode=open.");
        }
        
        switch (normalized) {
            case "open":
                return ZooDefs.Ids.OPEN_ACL_UNSAFE;
            case "read":
                return ZooDefs.Ids.READ_ACL_UNSAFE;
            case "creator":
                return ZooDefs.Ids.CREATOR_ALL_ACL;
            case "creator-read": {
                List<ACL> merged = new ArrayList<>();
                merged.addAll(ZooDefs.Ids.CREATOR_ALL_ACL);
                merged.addAll(ZooDefs.Ids.READ_ACL_UNSAFE);
                return merged;
            }
            default:
                throw new IllegalArgumentException("Unsupported ZooKeeper ACL mode: " + mode);
        }
    }
    
    private void ensureZNodeExists(String path, List<ACL> acls) throws Exception {
        try {
            if (zooKeeper.exists(path, false) == null) {
                zooKeeper.create(path, new byte[0], acls, CreateMode.PERSISTENT);
                LOG.debug("Created ZNode: {}", path);
            }
        } catch (KeeperException.NodeExistsException e) {
            // Ignore
        }
    }
    
    private void registerRegionServer(List<ACL> acls) throws Exception {
        regionServerZnodePath = "/hbase/rs/" + serverId;
        byte[] data = org.apache.hadoop.hbase.util.Bytes.toBytes(serverName.getServerName());
        
        zooKeeper.create(regionServerZnodePath, data, acls, CreateMode.EPHEMERAL);
        LOG.info("Registered as RegionServer: {}", regionServerZnodePath);
    }
    
    private void registerMaster(List<ACL> acls) throws Exception {
        String masterPath = "/hbase/master";
        ZooKeeperProtos.Master masterProto = ZooKeeperProtos.Master.newBuilder()
                .setMaster(ProtobufUtil.toServerName(serverName))
                .build();
        byte[] data = masterProto.toByteArray();
        
        try {
            zooKeeper.create(masterPath, data, acls, CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException e) {
            zooKeeper.setData(masterPath, data, -1);
        }
        LOG.info("Registered as HBase Master: {}", masterPath);
    }
    
    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }
}
