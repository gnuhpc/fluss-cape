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

package org.gnuhpc.fluss.cape.redis.transaction;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Distributed Transaction Manager using append-only transaction log.
 * 
 * <p>Schema (Append-Only):
 * <ul>
 *   <li>txn_id (PK1) - Transaction UUID</li>
 *   <li>seq_num (PK2) - Command sequence number (0 for metadata row)</li>
 *   <li>client_id - Client channel ID (only in seq=0 row)</li>
 *   <li>command - Command name (e.g., "SET", "HSET")</li>
 *   <li>args - Serialized command arguments</li>
 *   <li>created_at - Transaction start timestamp</li>
 *   <li>expires_at - Transaction expiration timestamp</li>
 *   <li>status - OPEN/EXECUTING/COMMITTED/DISCARDED</li>
 * </ul>
 * 
 * <p>Benefits over blob-based approach:
 * <ul>
 *   <li>O(N) complexity instead of O(N²)</li>
 *   <li>Idempotent command execution</li>
 *   <li>Easier cleanup and debugging</li>
 *   <li>No GZIP overhead for small transactions</li>
 * </ul>
 */
public class DistributedTransactionManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DistributedTransactionManager.class);
    
    private static final String TRANSACTION_TABLE = "redis_internal_transactions";
    private static final long DEFAULT_TRANSACTION_TIMEOUT_MS = 300_000;
    private static final int CLEANUP_INTERVAL_SECONDS = 60;
    private static final int METADATA_SEQ_NUM = 0;
    
    private final Connection connection;
    private final String database;
    private final Table transactionTable;
    private final ThreadLocal<UpsertWriter> writerLocal;
    private final ThreadLocal<Lookuper> lookuperLocal;
    private final ScheduledExecutorService cleanupExecutor;
    
    private final ConcurrentLinkedQueue<UpsertWriter> allWriters = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Lookuper> allLookupers = new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<String, AtomicInteger> txnSequences = new ConcurrentHashMap<>();

    public DistributedTransactionManager(Connection connection, String database) throws Exception {
        this.connection = connection;
        this.database = database;
        
        ensureTransactionTable(connection.getAdmin(), database);
        
        TablePath tablePath = TablePath.of(database, TRANSACTION_TABLE);
        this.transactionTable = connection.getTable(tablePath);
        
        this.writerLocal = ThreadLocal.withInitial(this::createWriter);
        this.lookuperLocal = ThreadLocal.withInitial(this::createLookuper);
        
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "txn-cleanup-" + database);
            t.setDaemon(true);
            return t;
        });
        
        this.cleanupExecutor.scheduleAtFixedRate(
            this::cleanupExpiredTransactions,
            CLEANUP_INTERVAL_SECONDS,
            CLEANUP_INTERVAL_SECONDS,
            TimeUnit.SECONDS
        );
        
        LOG.info("DistributedTransactionManager initialized for database: {}", database);
    }

    private static void ensureTransactionTable(Admin admin, String database) throws Exception {
        TablePath tablePath = TablePath.of(database, TRANSACTION_TABLE);
        
        if (admin.tableExists(tablePath).get()) {
            LOG.debug("Transaction table '{}' already exists", tablePath);
            return;
        }
        
        LOG.info("Creating append-only transaction table '{}'...", tablePath);
        
        Schema schema = Schema.newBuilder()
                .column("txn_id", DataTypes.STRING())
                .column("seq_num", DataTypes.INT())
                .column("client_id", DataTypes.STRING())
                .column("command", DataTypes.STRING())
                .column("args", DataTypes.BYTES())
                .column("created_at", DataTypes.BIGINT())
                .column("expires_at", DataTypes.BIGINT())
                .column("status", DataTypes.STRING())
                .primaryKey("txn_id", "seq_num")
                .build();
        
        TableDescriptor tableDescriptor = TableDescriptor.builder()
                .schema(schema)
                .distributedBy(16)
                .build();
        
        admin.createTable(tablePath, tableDescriptor, false).get();
        LOG.info("Successfully created append-only transaction table '{}'", tablePath);
    }

    /**
     * Begin a new transaction.
     * 
     * @param clientId Client channel ID
     * @return Transaction ID
     * @throws Exception if transaction creation fails
     */
    public String beginTransaction(String clientId) throws Exception {
        String txnId = generateTransactionId();
        long now = System.currentTimeMillis();
        long expiresAt = now + DEFAULT_TRANSACTION_TIMEOUT_MS;
        
        GenericRow metadataRow = new GenericRow(8);
        metadataRow.setField(0, BinaryString.fromString(txnId));
        metadataRow.setField(1, METADATA_SEQ_NUM);
        metadataRow.setField(2, BinaryString.fromString(clientId));
        metadataRow.setField(3, null);
        metadataRow.setField(4, null);
        metadataRow.setField(5, now);
        metadataRow.setField(6, expiresAt);
        metadataRow.setField(7, BinaryString.fromString("OPEN"));
        
        UpsertWriter writer = writerLocal.get();
        writer.upsert(metadataRow).get();
        
        txnSequences.put(txnId, new AtomicInteger(1));
        
        LOG.debug("Transaction started: txnId={}, clientId={}, expiresAt={}", 
            txnId, clientId, expiresAt);
        return txnId;
    }

    /**
     * Queue a command in the transaction. O(1) operation.
     * 
     * @param txnId Transaction ID
     * @param command Command to queue
     * @throws Exception if queueing fails or transaction is invalid
     */
    public void queueCommand(String txnId, SerializableCommand command) throws Exception {
        TransactionMetadata metadata = getTransactionMetadata(txnId);
        if (metadata == null) {
            throw new IllegalStateException("Transaction not found: " + txnId);
        }
        
        if (!"OPEN".equals(metadata.status)) {
            throw new IllegalStateException("Transaction not in OPEN state: " + txnId + " (status: " + metadata.status + ")");
        }
        
        if (System.currentTimeMillis() > metadata.expiresAt) {
            throw new IllegalStateException("Transaction expired: " + txnId);
        }
        
        AtomicInteger seqCounter = txnSequences.get(txnId);
        if (seqCounter == null) {
            seqCounter = new AtomicInteger(1);
            txnSequences.put(txnId, seqCounter);
        }
        int seqNum = seqCounter.getAndIncrement();
        
        byte[] argsBytes = serializeArgs(command.args);
        
        GenericRow commandRow = new GenericRow(8);
        commandRow.setField(0, BinaryString.fromString(txnId));
        commandRow.setField(1, seqNum);
        commandRow.setField(2, null);
        commandRow.setField(3, BinaryString.fromString(command.command));
        commandRow.setField(4, argsBytes);
        commandRow.setField(5, metadata.createdAt);
        commandRow.setField(6, metadata.expiresAt);
        commandRow.setField(7, BinaryString.fromString("OPEN"));
        
        UpsertWriter writer = writerLocal.get();
        writer.upsert(commandRow).get();
        
        LOG.trace("Command queued: txnId={}, seq={}, command={}", txnId, seqNum, command.command);
    }

    /**
     * Mark transaction as EXECUTING. Must be called before executing commands.
     * 
     * @param txnId Transaction ID
     * @throws Exception if state transition fails
     */
    public void markExecuting(String txnId) throws Exception {
        updateTransactionStatus(txnId, "EXECUTING");
        LOG.debug("Transaction marked EXECUTING: txnId={}", txnId);
    }

    /**
     * Commit transaction and retrieve commands. Only marks COMMITTED after successful call.
     * 
     * @param txnId Transaction ID
     * @return List of commands to execute, sorted by sequence
     * @throws Exception if transaction is invalid
     */
    public List<SerializableCommand> getCommandsForExecution(String txnId) throws Exception {
        TransactionMetadata metadata = getTransactionMetadata(txnId);
        if (metadata == null) {
            throw new IllegalStateException("Transaction not found: " + txnId);
        }
        
        if (!"OPEN".equals(metadata.status)) {
            throw new IllegalStateException("Transaction not in OPEN state: " + txnId + " (status: " + metadata.status + ")");
        }
        
        List<SerializableCommand> commands = getTransactionCommands(txnId);
        
        LOG.info("Retrieved commands for execution: txnId={}, commandCount={}", txnId, commands.size());
        return commands;
    }

    /**
     * Mark transaction as COMMITTED. Call only after all commands execute successfully.
     * 
     * @param txnId Transaction ID
     * @throws Exception if status update fails
     */
    public void markCommitted(String txnId) throws Exception {
        updateTransactionStatus(txnId, "COMMITTED");
        txnSequences.remove(txnId);
        LOG.info("Transaction committed: txnId={}", txnId);
    }

    /**
     * Discard transaction.
     * 
     * @param txnId Transaction ID
     * @throws Exception if discard fails
     */
    public void discardTransaction(String txnId) throws Exception {
        updateTransactionStatus(txnId, "DISCARDED");
        txnSequences.remove(txnId);
        LOG.info("Transaction discarded: txnId={}", txnId);
    }

    /**
     * Mark transaction as FAILED. Call when EXEC fails during execution.
     * 
     * @param txnId Transaction ID
     * @throws Exception if status update fails
     */
    public void markFailed(String txnId) throws Exception {
        updateTransactionStatus(txnId, "FAILED");
        txnSequences.remove(txnId);
        LOG.error("Transaction failed: txnId={}", txnId);
    }

    /**
     * Get transaction metadata (seq_num = 0 row).
     */
    private TransactionMetadata getTransactionMetadata(String txnId) throws Exception {
        Lookuper lookuper = lookuperLocal.get();
        
        GenericRow keyRow = new GenericRow(2);
        keyRow.setField(0, BinaryString.fromString(txnId));
        keyRow.setField(1, METADATA_SEQ_NUM);
        
        List<InternalRow> results = lookuper.lookup(keyRow).get().getRowList();
        if (results.isEmpty()) {
            return null;
        }
        
        InternalRow row = results.get(0);
        TransactionMetadata metadata = new TransactionMetadata();
        metadata.txnId = row.getString(0).toString();
        metadata.clientId = row.isNullAt(2) ? null : row.getString(2).toString();
        metadata.createdAt = row.getLong(5);
        metadata.expiresAt = row.getLong(6);
        metadata.status = row.getString(7).toString();
        
        return metadata;
    }

    /**
     * Get all commands for a transaction, sorted by sequence number.
     * Uses Fluss bucket scan to retrieve only rows for this transaction.
     */
    private List<SerializableCommand> getTransactionCommands(String txnId) throws Exception {
        List<SerializableCommand> commands = new ArrayList<>();
        
        // Scan all buckets for rows with this txn_id
        TableInfo tableInfo = transactionTable.getTableInfo();
        for (int bucketId = 0; bucketId < 16; bucketId++) {
            TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), bucketId);
            
            try (BatchScanner scanner = transactionTable.newScan()
                    .limit(Integer.MAX_VALUE)
                    .createBatchScanner(tableBucket)) {
                
                CloseableIterator<InternalRow> iterator;
                while ((iterator = scanner.pollBatch(Duration.ofMillis(100))) != null) {
                    try {
                        while (iterator.hasNext()) {
                            InternalRow row = iterator.next();
                            String rowTxnId = row.getString(0).toString();
                            int seqNum = row.getInt(1);
                            
                            // Only include command rows (seq_num > 0) for this transaction
                            if (rowTxnId.equals(txnId) && seqNum > 0) {
                                String command = row.isNullAt(3) ? null : row.getString(3).toString();
                                byte[] argsBytes = row.isNullAt(4) ? null : row.getBytes(4);
                                
                                if (command != null) {
                                    List<byte[]> args = deserializeArgs(argsBytes);
                                    commands.add(new SerializableCommand(command, args, seqNum));
                                }
                            }
                        }
                    } finally {
                        iterator.close();
                    }
                }
            }
        }
        
        commands.sort(Comparator.comparingInt(cmd -> cmd.seqNum));
        
        return commands;
    }

    private void updateTransactionStatus(String txnId, String newStatus) throws Exception {
        TransactionMetadata metadata = getTransactionMetadata(txnId);
        if (metadata == null) {
            throw new IllegalStateException("Transaction not found: " + txnId);
        }
        
        GenericRow metadataRow = new GenericRow(8);
        metadataRow.setField(0, BinaryString.fromString(txnId));
        metadataRow.setField(1, METADATA_SEQ_NUM);
        metadataRow.setField(2, BinaryString.fromString(metadata.clientId));
        metadataRow.setField(3, null);
        metadataRow.setField(4, null);
        metadataRow.setField(5, metadata.createdAt);
        metadataRow.setField(6, metadata.expiresAt);
        metadataRow.setField(7, BinaryString.fromString(newStatus));
        
        UpsertWriter writer = writerLocal.get();
        writer.upsert(metadataRow).get();
        
        LOG.debug("Transaction status updated: txnId={}, status={}", txnId, newStatus);
    }

    /**
     * Cleanup expired and completed transactions.
     */
    private void cleanupExpiredTransactions() {
        try {
            long now = System.currentTimeMillis();
            int deletedCount = 0;
            
            LOG.debug("Starting transaction cleanup...");
            
            TableInfo tableInfo = transactionTable.getTableInfo();
            UpsertWriter writer = writerLocal.get();
            
            for (int bucketId = 0; bucketId < 16; bucketId++) {
                TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), bucketId);
                
                try (BatchScanner scanner = transactionTable.newScan()
                        .limit(Integer.MAX_VALUE)
                        .createBatchScanner(tableBucket)) {
                    
                    CloseableIterator<InternalRow> iterator;
                    while ((iterator = scanner.pollBatch(Duration.ofMillis(100))) != null) {
                        try {
                            while (iterator.hasNext()) {
                                InternalRow row = iterator.next();
                                int seqNum = row.getInt(1);
                                
                                if (seqNum == METADATA_SEQ_NUM) {
                                    String status = row.getString(7).toString();
                                    long expiresAt = row.getLong(6);
                                    
                                    boolean shouldDelete = (now > expiresAt) || 
                                                          "COMMITTED".equals(status) || 
                                                          "DISCARDED".equals(status) ||
                                                          "FAILED".equals(status);
                                    
                                    if (shouldDelete) {
                                        String txnId = row.getString(0).toString();
                                        deleteTransaction(txnId, writer);
                                        deletedCount++;
                                    }
                                }
                            }
                        } finally {
                            iterator.close();
                        }
                    }
                }
            }
            
            if (deletedCount > 0) {
                LOG.info("Cleanup completed: deleted {} transactions", deletedCount);
            }
            
        } catch (Exception e) {
            LOG.error("Error during transaction cleanup", e);
        }
    }

    /**
     * Delete all rows for a transaction (metadata + commands).
     */
    private void deleteTransaction(String txnId, UpsertWriter writer) throws Exception {
        TableInfo tableInfo = transactionTable.getTableInfo();
        
        for (int bucketId = 0; bucketId < 16; bucketId++) {
            TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), bucketId);
            
            try (BatchScanner scanner = transactionTable.newScan()
                    .limit(Integer.MAX_VALUE)
                    .createBatchScanner(tableBucket)) {
                
                CloseableIterator<InternalRow> iterator;
                while ((iterator = scanner.pollBatch(Duration.ofMillis(100))) != null) {
                    try {
                        while (iterator.hasNext()) {
                            InternalRow row = iterator.next();
                            String rowTxnId = row.getString(0).toString();
                            
                            if (rowTxnId.equals(txnId)) {
                                int seqNum = row.getInt(1);
                                GenericRow keyRow = new GenericRow(2);
                                keyRow.setField(0, BinaryString.fromString(txnId));
                                keyRow.setField(1, seqNum);
                                writer.delete(keyRow).get();
                            }
                        }
                    } finally {
                        iterator.close();
                    }
                }
            }
        }
        
        txnSequences.remove(txnId);
        
        LOG.trace("Deleted all rows for transaction: txnId={}", txnId);
    }

    private String generateTransactionId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private byte[] serializeArgs(List<byte[]> args) throws IOException {
        if (args == null || args.isEmpty()) {
            return new byte[0];
        }
        
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        try (ObjectOutputStream objOut = new ObjectOutputStream(byteOut)) {
            objOut.writeInt(args.size());
            for (byte[] arg : args) {
                objOut.writeInt(arg.length);
                objOut.write(arg);
            }
        }
        
        return byteOut.toByteArray();
    }

    private List<byte[]> deserializeArgs(byte[] data) throws IOException {
        if (data == null || data.length == 0) {
            return new ArrayList<>();
        }
        
        List<byte[]> args = new ArrayList<>();
        ByteArrayInputStream byteIn = new ByteArrayInputStream(data);
        
        try (ObjectInputStream objIn = new ObjectInputStream(byteIn)) {
            int count = objIn.readInt();
            for (int i = 0; i < count; i++) {
                int length = objIn.readInt();
                byte[] arg = new byte[length];
                objIn.readFully(arg);
                args.add(arg);
            }
        }
        
        return args;
    }

    private UpsertWriter createWriter() {
        try {
            UpsertWriter writer = transactionTable.newUpsert().createWriter();
            allWriters.add(writer);
            return writer;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create UpsertWriter", e);
        }
    }

    private Lookuper createLookuper() {
        try {
            Lookuper lookuper = transactionTable.newLookup().createLookuper();
            allLookupers.add(lookuper);
            return lookuper;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Lookuper", e);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing DistributedTransactionManager for database: {}", database);
        
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        for (UpsertWriter writer : allWriters) {
            try {
                writer.flush();
            } catch (Exception e) {
                LOG.warn("Failed to flush writer during shutdown", e);
            }
        }
        
        allWriters.clear();
        allLookupers.clear();
        txnSequences.clear();
    }

    private static class TransactionMetadata {
        String txnId;
        String clientId;
        long createdAt;
        long expiresAt;
        String status;
    }

    public static class SerializableCommand implements Serializable {
        private static final long serialVersionUID = 2L;
        
        public final String command;
        public final List<byte[]> args;
        public final int seqNum;
        
        public SerializableCommand(String command, List<byte[]> args) {
            this(command, args, -1);
        }
        
        public SerializableCommand(String command, List<byte[]> args, int seqNum) {
            this.command = command;
            this.args = args;
            this.seqNum = seqNum;
        }
    }
}
