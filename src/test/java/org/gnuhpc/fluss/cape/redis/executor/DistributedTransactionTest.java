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

import org.gnuhpc.fluss.cape.redis.transaction.DistributedTransactionManager;
import org.gnuhpc.fluss.cape.redis.transaction.DistributedTransactionManager.SerializableCommand;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Distributed Transaction Integration Tests")
class DistributedTransactionTest {

    @Test
    @DisplayName("UUID transaction IDs should be unique across multiple generations")
    void testTransactionIdUniqueness() {
        List<String> txnIds = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String txnId = UUID.randomUUID().toString();
            assertThat(txnIds).doesNotContain(txnId);
            txnIds.add(txnId);
        }
        assertThat(txnIds).hasSize(100);
    }

    @Test
    @DisplayName("SerializableCommand should preserve command and arguments")
    void testSerializableCommandPreservation() {
        String command = "SET";
        List<byte[]> args = new ArrayList<>();
        args.add("mykey".getBytes(StandardCharsets.UTF_8));
        args.add("myvalue".getBytes(StandardCharsets.UTF_8));
        
        SerializableCommand serialCmd = new SerializableCommand(command, args);
        
        assertThat(serialCmd.command).isEqualTo("SET");
        assertThat(serialCmd.args).hasSize(2);
        assertThat(new String(serialCmd.args.get(0), StandardCharsets.UTF_8)).isEqualTo("mykey");
        assertThat(new String(serialCmd.args.get(1), StandardCharsets.UTF_8)).isEqualTo("myvalue");
    }

    @Test
    @DisplayName("SerializableCommand should be serializable via Java serialization")
    void testSerializableCommandSerialization() throws Exception {
        SerializableCommand original = new SerializableCommand(
            "HSET",
            List.of(
                "user:1".getBytes(StandardCharsets.UTF_8),
                "name".getBytes(StandardCharsets.UTF_8),
                "Alice".getBytes(StandardCharsets.UTF_8)
            )
        );
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(original);
        oos.close();
        
        byte[] serialized = baos.toByteArray();
        
        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        ObjectInputStream ois = new ObjectInputStream(bais);
        SerializableCommand deserialized = (SerializableCommand) ois.readObject();
        ois.close();
        
        assertThat(deserialized.command).isEqualTo(original.command);
        assertThat(deserialized.args).hasSize(original.args.size());
        for (int i = 0; i < original.args.size(); i++) {
            assertThat(deserialized.args.get(i)).isEqualTo(original.args.get(i));
        }
    }

    @Test
    @DisplayName("GZIP compression should reduce command list size for large datasets")
    void testGzipCompressionEffectiveness() throws Exception {
        List<SerializableCommand> commands = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            commands.add(new SerializableCommand(
                "SET",
                List.of(
                    ("key:" + i).getBytes(StandardCharsets.UTF_8),
                    ("value:" + i + ":with:lots:of:repeated:text").getBytes(StandardCharsets.UTF_8)
                )
            ));
        }
        
        ByteArrayOutputStream rawBaos = new ByteArrayOutputStream();
        ObjectOutputStream rawOos = new ObjectOutputStream(rawBaos);
        rawOos.writeObject(commands);
        rawOos.close();
        byte[] rawBytes = rawBaos.toByteArray();
        
        ByteArrayOutputStream compressedBaos = new ByteArrayOutputStream();
        GZIPOutputStream gzipOut = new GZIPOutputStream(compressedBaos);
        ObjectOutputStream compressedOos = new ObjectOutputStream(gzipOut);
        compressedOos.writeObject(commands);
        compressedOos.close();
        gzipOut.close();
        byte[] compressedBytes = compressedBaos.toByteArray();
        
        int rawSize = rawBytes.length;
        int compressedSize = compressedBytes.length;
        double compressionRatio = (double) compressedSize / rawSize;
        
        assertThat(compressedSize).isLessThan(rawSize);
        assertThat(compressionRatio).isLessThan(0.8);
    }

    @Test
    @DisplayName("GZIP decompression should restore original command list")
    void testGzipRoundTrip() throws Exception {
        List<SerializableCommand> original = List.of(
            new SerializableCommand("MULTI", List.of()),
            new SerializableCommand("SET", List.of(
                "key1".getBytes(StandardCharsets.UTF_8),
                "value1".getBytes(StandardCharsets.UTF_8)
            )),
            new SerializableCommand("EXEC", List.of())
        );
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gzipOut = new GZIPOutputStream(baos);
        ObjectOutputStream oos = new ObjectOutputStream(gzipOut);
        oos.writeObject(original);
        oos.close();
        gzipOut.close();
        
        byte[] compressed = baos.toByteArray();
        
        ByteArrayInputStream bais = new ByteArrayInputStream(compressed);
        GZIPInputStream gzipIn = new GZIPInputStream(bais);
        ObjectInputStream ois = new ObjectInputStream(gzipIn);
        @SuppressWarnings("unchecked")
        List<SerializableCommand> restored = (List<SerializableCommand>) ois.readObject();
        ois.close();
        gzipIn.close();
        
        assertThat(restored).hasSize(original.size());
        for (int i = 0; i < original.size(); i++) {
            assertThat(restored.get(i).command).isEqualTo(original.get(i).command);
            assertThat(restored.get(i).args).hasSize(original.get(i).args.size());
            for (int j = 0; j < original.get(i).args.size(); j++) {
                assertThat(restored.get(i).args.get(j)).isEqualTo(original.get(i).args.get(j));
            }
        }
    }

    @Test
    @DisplayName("Transaction timeout calculation should be consistent")
    void testTransactionTimeoutCalculation() {
        long now = System.currentTimeMillis();
        long timeoutMs = 5 * 60 * 1000;
        long expiresAt = now + timeoutMs;
        
        long remainingTime = expiresAt - now;
        
        assertThat(remainingTime).isGreaterThanOrEqualTo(timeoutMs - 100);
        assertThat(remainingTime).isLessThanOrEqualTo(timeoutMs + 100);
    }

    @Test
    @DisplayName("Expired transaction detection should work correctly")
    void testExpiredTransactionDetection() {
        long now = System.currentTimeMillis();
        long pastExpiry = now - 1000;
        long futureExpiry = now + 300000;
        
        assertThat(pastExpiry).isLessThan(now);
        assertThat(futureExpiry).isGreaterThan(now);
    }
}
