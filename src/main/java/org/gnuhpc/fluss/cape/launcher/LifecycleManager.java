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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Manages the lifecycle of all CAPE components.
 * Handles startup, shutdown, and graceful termination.
 */
public class LifecycleManager {
    private static final Logger LOG = LoggerFactory.getLogger(LifecycleManager.class);
    
    private final List<ServerComponent> components = new ArrayList<>();
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private volatile boolean running = false;
    
    /**
     * Register a component to be managed.
     * Components are started in registration order and stopped in reverse order.
     */
    public void registerComponent(ServerComponent component) {
        components.add(component);
        LOG.debug("Registered component: {}", component.getName());
    }
    
    /**
     * Start all registered components.
     */
    public void startAll() throws Exception {
        LOG.info("Starting {} components...", components.size());
        
        List<ServerComponent> startedComponents = new ArrayList<>();
        try {
            for (ServerComponent component : components) {
                LOG.info("Starting component: {}", component.getName());
                component.start();
                startedComponents.add(component);
                LOG.info("Component started: {}", component.getName());
            }
            running = true;
            LOG.info("All components started successfully");
        } catch (Exception e) {
            LOG.error("Failed to start component, rolling back...", e);
            // Rollback: stop all started components in reverse order
            stopComponents(startedComponents);
            throw e;
        }
    }
    
    /**
     * Stop all components gracefully.
     */
    public void stopAll() {
        if (!running) {
            LOG.warn("Lifecycle manager is not running");
            return;
        }
        
        LOG.info("Stopping all components...");
        stopComponents(components);
        running = false;
        shutdownLatch.countDown();
        LOG.info("All components stopped");
    }
    
    /**
     * Wait for shutdown signal.
     */
    public void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
    }
    
    /**
     * Wait for shutdown signal with timeout.
     */
    public boolean awaitShutdown(long timeout, TimeUnit unit) throws InterruptedException {
        return shutdownLatch.await(timeout, unit);
    }
    
    /**
     * Register shutdown hook for graceful termination.
     */
    public void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Received shutdown signal");
            stopAll();
        }, "shutdown-hook"));
    }
    
    /**
     * Stop components in reverse order.
     */
    private void stopComponents(List<ServerComponent> componentsToStop) {
        // Stop in reverse order
        for (int i = componentsToStop.size() - 1; i >= 0; i--) {
            ServerComponent component = componentsToStop.get(i);
            try {
                LOG.info("Stopping component: {}", component.getName());
                component.close();
                LOG.info("Component stopped: {}", component.getName());
            } catch (Exception e) {
                LOG.error("Failed to stop component: {}", component.getName(), e);
                // Continue stopping other components
            }
        }
    }
    
    public boolean isRunning() {
        return running;
    }
    
    public int getComponentCount() {
        return components.size();
    }
}
