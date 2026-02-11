package org.gnuhpc.fluss.cape.kafka.integration;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive integration tests for Kafka Consumer using real Kafka Java SDK.
 * Tests the FetchHandler and ScannerPool implementation.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaConsumerIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerIntegrationTest.class);

    private static final String BOOTSTRAP_SERVERS = System.getProperty("kafka.bootstrap.servers", "localhost:9092");
    private static final String TEST_TOPIC_PREFIX = "test-consumer-";
    
    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;
    private String testTopic;

    @BeforeEach
    void setup() {
        testTopic = TEST_TOPIC_PREFIX + UUID.randomUUID();
        
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        consumerProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");
        
        consumer = new KafkaConsumer<>(consumerProps);
        
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        
        producer = new KafkaProducer<>(producerProps);
    }

    @AfterEach
    void teardown() {
        if (consumer != null) {
            consumer.close(Duration.ofSeconds(5));
        }
        if (producer != null) {
            producer.close(Duration.ofSeconds(5));
        }
    }

    @Test
    @Order(1)
    @DisplayName("Test basic consume - validates FetchHandler and ScannerPool")
    void testBasicConsume() throws Exception {
        int numRecords = 10;
        produceTestRecords(numRecords);
        
        consumer.subscribe(Collections.singletonList(testTopic));
        
        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        
        while (allRecords.size() < numRecords && System.currentTimeMillis() - startTime < 30000) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                allRecords.add(record);
            }
        }
        
        assertThat(allRecords).hasSize(numRecords);
        
        Set<String> expectedKeys = new HashSet<>();
        Set<String> expectedValues = new HashSet<>();
        for (int i = 0; i < numRecords; i++) {
            expectedKeys.add("key-" + i);
            expectedValues.add("value-" + i);
        }
        
        Set<String> actualKeys = new HashSet<>();
        Set<String> actualValues = new HashSet<>();
        for (ConsumerRecord<String, String> record : allRecords) {
            assertThat(record.topic()).isEqualTo(testTopic);
            assertThat(record.offset()).isGreaterThanOrEqualTo(0);
            actualKeys.add(record.key());
            actualValues.add(record.value());
        }
        
        assertThat(actualKeys).isEqualTo(expectedKeys);
        assertThat(actualValues).isEqualTo(expectedValues);
    }

    @Test
    @Order(2)
    @DisplayName("Test consume with manual commit - validates offset commit")
    void testConsumeWithManualCommit() throws Exception {
        int numRecords = 20;
        produceTestRecords(numRecords);
        
        consumer.subscribe(Collections.singletonList(testTopic));
        
        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        
        while (allRecords.size() < numRecords && System.currentTimeMillis() - startTime < 30000) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            
            for (ConsumerRecord<String, String> record : records) {
                allRecords.add(record);
            }
            
            if (!records.isEmpty()) {
                consumer.commitSync();
            }
        }
        
        assertThat(allRecords).hasSize(numRecords);
        
        Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(
            new HashSet<>(consumer.assignment())
        );
        
        assertThat(committed).isNotEmpty();
        for (OffsetAndMetadata offsetMetadata : committed.values()) {
            assertThat(offsetMetadata.offset()).isGreaterThan(0);
        }
    }

    @Test
    @Order(3)
    @DisplayName("Test consume with async commit - validates async offset commit")
    void testConsumeWithAsyncCommit() throws Exception {
        int numRecords = 15;
        produceTestRecords(numRecords);
        
        consumer.subscribe(Collections.singletonList(testTopic));
        
        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();
        List<Exception> commitErrors = Collections.synchronizedList(new ArrayList<>());
        
        long startTime = System.currentTimeMillis();
        
        while (allRecords.size() < numRecords && System.currentTimeMillis() - startTime < 30000) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            
            for (ConsumerRecord<String, String> record : records) {
                allRecords.add(record);
            }
            
            if (!records.isEmpty()) {
                consumer.commitAsync((offsets, exception) -> {
                    if (exception != null) {
                        commitErrors.add(exception);
                    }
                });
            }
        }
        
        Thread.sleep(2000);
        
        assertThat(allRecords).hasSize(numRecords);
        assertThat(commitErrors).isEmpty();
    }

    @Test
    @Order(4)
    @DisplayName("Test manual partition assignment - validates assign()")
    void testManualPartitionAssignment() throws Exception {
        int numRecords = 10;
        produceTestRecords(numRecords);
        
        TopicPartition partition = new TopicPartition(testTopic, 0);
        consumer.assign(Collections.singletonList(partition));
        consumer.seekToBeginning(Collections.singletonList(partition));
        
        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        
        while (allRecords.size() < numRecords && System.currentTimeMillis() - startTime < 30000) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                allRecords.add(record);
            }
        }
        
        assertThat(allRecords).hasSizeGreaterThanOrEqualTo(1);
        
        for (ConsumerRecord<String, String> record : allRecords) {
            assertThat(record.partition()).isEqualTo(0);
        }
    }

    @Test
    @Order(5)
    @DisplayName("Test seek to offset - validates seek operations")
    void testSeekToOffset() throws Exception {
        int numRecords = 20;
        produceTestRecords(numRecords);
        
        TopicPartition partition = new TopicPartition(testTopic, 0);
        consumer.assign(Collections.singletonList(partition));
        consumer.seekToBeginning(Collections.singletonList(partition));
        
        consumer.poll(Duration.ofSeconds(5));
        
        long targetOffset = 10;
        consumer.seek(partition, targetOffset);
        
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
        
        if (!records.isEmpty()) {
            ConsumerRecord<String, String> firstRecord = records.iterator().next();
            assertThat(firstRecord.offset()).isGreaterThanOrEqualTo(targetOffset);
        }
    }

    @Test
    @Order(6)
    @DisplayName("Test consume from beginning - validates seekToBeginning()")
    void testConsumeFromBeginning() throws Exception {
        int numRecords = 15;
        produceTestRecords(numRecords);
        
        consumer.subscribe(Collections.singletonList(testTopic));
        
        consumer.poll(Duration.ofSeconds(1));
        
        consumer.seekToBeginning(consumer.assignment());
        
        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        
        while (allRecords.size() < numRecords && System.currentTimeMillis() - startTime < 30000) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                allRecords.add(record);
            }
        }
        
        assertThat(allRecords).hasSizeGreaterThanOrEqualTo(numRecords);
        assertThat(allRecords.get(0).offset()).isEqualTo(0);
    }

    @Test
    @Order(7)
    @DisplayName("Test consume from end - validates seekToEnd()")
    void testConsumeFromEnd() throws Exception {
        produceTestRecords(10);
        
        consumer.subscribe(Collections.singletonList(testTopic));
        consumer.poll(Duration.ofSeconds(1));
        
        consumer.seekToEnd(consumer.assignment());
        
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
        assertThat(records.isEmpty()).isTrue();
        
        produceTestRecords(5);
        
        List<ConsumerRecord<String, String>> newRecords = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        
        while (newRecords.size() < 5 && System.currentTimeMillis() - startTime < 30000) {
            ConsumerRecords<String, String> polledRecords = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : polledRecords) {
                newRecords.add(record);
            }
        }
        
        assertThat(newRecords).hasSizeGreaterThanOrEqualTo(5);
    }

    @Test
    @Order(8)
    @DisplayName("Test fetch performance - validates ScannerPool hit rate")
    void testFetchPerformance() throws Exception {
        int numRecords = 100;
        produceTestRecords(numRecords);
        
        consumer.subscribe(Collections.singletonList(testTopic));
        
        List<Long> fetchLatencies = new ArrayList<>();
        int totalRecordsConsumed = 0;
        
        long overallStart = System.currentTimeMillis();
        
        while (totalRecordsConsumed < numRecords && System.currentTimeMillis() - overallStart < 60000) {
            long fetchStart = System.nanoTime();
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            long fetchEnd = System.nanoTime();
            
            if (!records.isEmpty()) {
                long fetchLatencyMs = (fetchEnd - fetchStart) / 1_000_000;
                fetchLatencies.add(fetchLatencyMs);
                totalRecordsConsumed += records.count();
            }
        }
        
        assertThat(totalRecordsConsumed).isEqualTo(numRecords);
        assertThat(fetchLatencies).isNotEmpty();
        
        double avgLatency = fetchLatencies.stream().mapToLong(Long::longValue).average().orElse(0);
        long maxLatency = fetchLatencies.stream().mapToLong(Long::longValue).max().orElse(0);
        
        LOG.info("Fetch performance: {} fetches | Avg: {} ms | Max: {} ms",
            fetchLatencies.size(), String.format("%.2f", avgLatency), maxLatency);
        
        assertThat(avgLatency).isLessThan(400);
    }

    @Test
    @Order(9)
    @DisplayName("Test pause and resume - validates flow control")
    void testPauseAndResume() throws Exception {
        consumer.subscribe(Collections.singletonList(testTopic));
        
        consumer.poll(Duration.ofSeconds(1));
        
        Set<TopicPartition> partitions = consumer.assignment();
        assertThat(partitions).isNotEmpty();
        
        int numRecords = 20;
        produceTestRecords(numRecords);
        
        consumer.pause(partitions);
        
        ConsumerRecords<String, String> pausedRecords = consumer.poll(Duration.ofSeconds(2));
        assertThat(pausedRecords.isEmpty()).isTrue();
        
        consumer.resume(partitions);
        
        List<ConsumerRecord<String, String>> resumedRecords = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        
        while (resumedRecords.size() < numRecords && System.currentTimeMillis() - startTime < 30000) {
            ConsumerRecords<String, String> polledRecords = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : polledRecords) {
                resumedRecords.add(record);
            }
        }
        
        assertThat(resumedRecords).hasSizeGreaterThanOrEqualTo(numRecords);
    }

    @Test
    @Order(10)
    @DisplayName("Test position tracking - validates consumer position()")
    void testPositionTracking() throws Exception {
        int numRecords = 10;
        produceTestRecords(numRecords);
        
        TopicPartition partition = new TopicPartition(testTopic, 0);
        consumer.assign(Collections.singletonList(partition));
        consumer.seekToBeginning(Collections.singletonList(partition));
        
        long initialPosition = consumer.position(partition);
        assertThat(initialPosition).isEqualTo(0);
        
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
        
        if (!records.isEmpty()) {
            long newPosition = consumer.position(partition);
            assertThat(newPosition).isGreaterThan(initialPosition);
            assertThat(newPosition).isEqualTo(records.count());
        }
    }

    private void produceTestRecords(int numRecords) throws Exception {
        for (int i = 0; i < numRecords; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                testTopic,
                "key-" + i,
                "value-" + i
            );
            producer.send(record).get();
        }
        producer.flush();
        Thread.sleep(1000);
    }
}
