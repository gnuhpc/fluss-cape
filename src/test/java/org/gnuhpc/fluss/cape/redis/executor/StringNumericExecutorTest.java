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

package org.gnuhpc.fluss.cape.redis.executor;

import org.gnuhpc.fluss.cape.redis.storage.RedisStorageAdapter;
import org.gnuhpc.fluss.cape.redis.executor.string.StringNumericExecutor;
import org.gnuhpc.fluss.cape.redis.protocol.RedisCommand;

import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("String Numeric Executor Tests - INCR/DECR Operations")
class StringNumericExecutorTest {

    @Mock
    private RedisStorageAdapter mockAdapter;

    private StringNumericExecutor executor;
    private AutoCloseable mocks;

    @BeforeEach
    void setUp() throws Exception {
        mocks = MockitoAnnotations.openMocks(this);
        executor = new StringNumericExecutor(mockAdapter);
    }

    @Test
    @DisplayName("INCR should increment from 0 when key doesn't exist")
    void testIncrNewKey() throws Exception {
        when(mockAdapter.incr(any(byte[].class))).thenReturn(1L);
        
        RedisCommand incrCmd = createCommand("INCR", "counter");
        RedisMessage response = executor.execute(incrCmd);
        
        assertThat(response).isInstanceOf(IntegerRedisMessage.class);
        assertThat(((IntegerRedisMessage) response).value()).isEqualTo(1L);
        
        verify(mockAdapter).incr(any(byte[].class));
    }

    @Test
    @DisplayName("INCRBY should add specified increment")
    void testIncrBy() throws Exception {
        when(mockAdapter.incrBy(any(byte[].class), eq(10L))).thenReturn(10L);
        
        RedisCommand incrByCmd = createCommand("INCRBY", "counter", "10");
        RedisMessage response = executor.execute(incrByCmd);
        
        assertThat(response).isInstanceOf(IntegerRedisMessage.class);
        assertThat(((IntegerRedisMessage) response).value()).isEqualTo(10L);
        
        verify(mockAdapter).incrBy(any(byte[].class), eq(10L));
    }

    @Test
    @DisplayName("DECR should decrement existing value")
    void testDecr() throws Exception {
        when(mockAdapter.decr(any(byte[].class))).thenReturn(9L);
        
        RedisCommand decrCmd = createCommand("DECR", "counter");
        RedisMessage response = executor.execute(decrCmd);
        
        assertThat(response).isInstanceOf(IntegerRedisMessage.class);
        assertThat(((IntegerRedisMessage) response).value()).isEqualTo(9L);
        
        verify(mockAdapter).decr(any(byte[].class));
    }

    private RedisCommand createCommand(String command, String... args) {
        List<byte[]> argList = new ArrayList<>();
        for (String arg : args) {
            argList.add(arg.getBytes(StandardCharsets.UTF_8));
        }
        return new RedisCommand(command, argList);
    }
}
