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

/**
 * Represents a component that can be started and stopped.
 * All CAPE components (servers, registries, managers) should implement this interface.
 */
public interface ServerComponent extends AutoCloseable {
    
    /**
     * Start the component.
     * @throws Exception if startup fails
     */
    void start() throws Exception;
    
    /**
     * Stop the component gracefully.
     * @throws Exception if shutdown fails
     */
    @Override
    void close() throws Exception;
    
    /**
     * Get the component name for logging.
     * @return component name
     */
    String getName();
    
    /**
     * Check if the component is running.
     * @return true if running, false otherwise
     */
    boolean isRunning();
}
