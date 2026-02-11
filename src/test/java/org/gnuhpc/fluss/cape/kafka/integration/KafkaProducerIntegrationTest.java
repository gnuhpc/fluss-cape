package org.gnuhpc.fluss.cape.kafka.integration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive integration tests for Kafka Producer using real Kafka Java SDK.
 * 
 * <p>Tests the ProduceHandler and WriterPool implementation through actual Kafka producer clients.
 * 
 * <p>Prerequisites:
 * <ul>
 *   <li>Fluss CAPE running on localhost:9092 (default Kafka port)</li>
 *   <li>Fluss cluster running and accessible</li>
 * </ul>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class KafkaProducerIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerIntegrationTest.class);

    private static final String BOOTSTRAP_SERVERS = System.getProperty("kafka.bootstrap.servers", "localhost:9092");
    private static final String TEST_TOPIC_PREFIX = "test-producer-";
    
    private KafkaProducer<String, String> producer;
    private String testTopic;

    @BeforeEach
    void setup() {
        testTopic = TEST_TOPIC_PREFIX + UUID.randomUUID();
        
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false); // Disable idempotence (InitProducerId not implemented)
        
        producer = new KafkaProducer<>(props);
    }

    @AfterEach
    void teardown() {
        if (producer != null) {
            producer.close(Duration.ofSeconds(5));
        }
    }

    @Test
    @Order(1)
    @DisplayName("Test single record produce - validates basic produce functionality")
    void testProduceSingleRecord() throws Exception {
        String key = "key1";
        String value = "Hello Kafka!";
        
        ProducerRecord<String, String> record = new ProducerRecord<>(testTopic, key, value);
        Future<RecordMetadata> future = producer.send(record);
        
        RecordMetadata metadata = future.get(10, TimeUnit.SECONDS);
        
        assertThat(metadata).isNotNull();
        assertThat(metadata.topic()).isEqualTo(testTopic);
        assertThat(metadata.partition()).isGreaterThanOrEqualTo(0);
        assertThat(metadata.offset()).isGreaterThanOrEqualTo(0);
        assertThat(metadata.hasTimestamp()).isTrue();
    }

    @Test
    @Order(2)
    @DisplayName("Test batch produce - validates async batch writes with WriterPool")
    void testProduceBatch() throws Exception {
        int numRecords = 100;
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < numRecords; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(testTopic, key, value);
            futures.add(producer.send(record));
        }
        
        producer.flush();
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        
        for (Future<RecordMetadata> future : futures) {
            RecordMetadata metadata = future.get(1, TimeUnit.SECONDS);
            assertThat(metadata).isNotNull();
            assertThat(metadata.topic()).isEqualTo(testTopic);
        }
        
        LOG.info("Produced {} records in {} ms (avg: {:.2f} ms/record, throughput: {:.0f} records/sec)",
            numRecords, totalTime, (double) totalTime / numRecords, numRecords * 1000.0 / totalTime);
        
        assertThat(totalTime).isLessThan(5000);
    }

    @Test
    @Order(3)
    @DisplayName("Test produce with callback - validates async produce with callbacks")
    void testProduceWithCallback() throws Exception {
        int numRecords = 10;
        List<RecordMetadata> receivedMetadata = Collections.synchronizedList(new ArrayList<>());
        List<Exception> errors = Collections.synchronizedList(new ArrayList<>());
        
        for (int i = 0; i < numRecords; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                testTopic, 
                "callback-key-" + i, 
                "callback-value-" + i
            );
            
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    errors.add(exception);
                } else {
                    receivedMetadata.add(metadata);
                }
            });
        }
        
        producer.flush();
        Thread.sleep(1000);
        
        assertThat(errors).isEmpty();
        assertThat(receivedMetadata).hasSize(numRecords);
        
        for (RecordMetadata metadata : receivedMetadata) {
            assertThat(metadata.topic()).isEqualTo(testTopic);
            assertThat(metadata.offset()).isGreaterThanOrEqualTo(0);
        }
    }

    @Test
    @Order(4)
    @DisplayName("Test produce with null key - validates null key handling")
    void testProduceWithNullKey() throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(testTopic, null, "value-no-key");
        Future<RecordMetadata> future = producer.send(record);
        
        RecordMetadata metadata = future.get(10, TimeUnit.SECONDS);
        
        assertThat(metadata).isNotNull();
        assertThat(metadata.topic()).isEqualTo(testTopic);
        assertThat(metadata.offset()).isGreaterThanOrEqualTo(0);
    }

    @Test
    @Order(5)
    @DisplayName("Test produce with explicit partition - validates partition routing")
    void testProduceWithExplicitPartition() throws Exception {
        int targetPartition = 0;
        
        ProducerRecord<String, String> record = new ProducerRecord<>(
            testTopic, 
            targetPartition, 
            "partition-key", 
            "partition-value"
        );
        
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get(10, TimeUnit.SECONDS);
        
        assertThat(metadata).isNotNull();
        assertThat(metadata.partition()).isEqualTo(targetPartition);
    }

    @Test
    @Order(6)
    @DisplayName("Test produce large messages - validates large message handling")
    void testProduceLargeMessages() throws Exception {
        StringBuilder largeValue = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            largeValue.append("Large message content with lots of data. ");
        }
        
        ProducerRecord<String, String> record = new ProducerRecord<>(
            testTopic, 
            "large-key", 
            largeValue.toString()
        );
        
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get(10, TimeUnit.SECONDS);
        
        assertThat(metadata).isNotNull();
        assertThat(metadata.serializedValueSize()).isGreaterThan(100000);
    }

    @Test
    @Order(7)
    @DisplayName("Test concurrent producers - validates thread safety")
    void testConcurrentProducers() throws Exception {
        int numThreads = 5;
        int recordsPerThread = 20;
        List<Thread> threads = new ArrayList<>();
        List<Exception> errors = Collections.synchronizedList(new ArrayList<>());
        
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            Thread thread = new Thread(() -> {
                try {
                    for (int i = 0; i < recordsPerThread; i++) {
                        ProducerRecord<String, String> record = new ProducerRecord<>(
                            testTopic,
                            "thread-" + threadId + "-key-" + i,
                            "thread-" + threadId + "-value-" + i
                        );
                        producer.send(record).get(5, TimeUnit.SECONDS);
                    }
                } catch (Exception e) {
                    errors.add(e);
                }
            });
            threads.add(thread);
            thread.start();
        }
        
        for (Thread thread : threads) {
            thread.join(30000);
        }
        
        assertThat(errors).isEmpty();
    }

    @Test
    @Order(8)
    @DisplayName("Test produce performance - validates WriterPool performance improvements")
    void testProducePerformance() throws Exception {
        int warmupRecords = 50;
        int benchmarkRecords = 100;
        
        for (int i = 0; i < warmupRecords; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                testTopic, 
                "warmup-key-" + i, 
                "warmup-value-" + i
            );
            producer.send(record);
        }
        producer.flush();
        Thread.sleep(1000);
        
        long startTime = System.nanoTime();
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        
        for (int i = 0; i < benchmarkRecords; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                testTopic, 
                "bench-key-" + i, 
                "bench-value-" + i
            );
            futures.add(producer.send(record));
        }
        
        producer.flush();
        
        for (Future<RecordMetadata> future : futures) {
            future.get(5, TimeUnit.SECONDS);
        }
        
        long endTime = System.nanoTime();
        long totalTimeMs = (endTime - startTime) / 1_000_000;
        double avgLatencyMs = (double) totalTimeMs / benchmarkRecords;
        double throughput = benchmarkRecords * 1000.0 / totalTimeMs;
        
        LOG.info("Performance: {} records in {} ms | Avg latency: {:.2f} ms | Throughput: {:.0f} records/sec",
            benchmarkRecords, totalTimeMs, avgLatencyMs, throughput);
        
        assertThat(avgLatencyMs).isLessThan(50);
        assertThat(throughput).isGreaterThan(100);
    }

    @Test
    @Order(9)
    @DisplayName("Test produce with headers - validates record headers support")
    void testProduceWithHeaders() throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(testTopic, "header-key", "header-value");
        record.headers().add("custom-header-1", "value1".getBytes());
        record.headers().add("custom-header-2", "value2".getBytes());
        record.headers().add("timestamp", String.valueOf(System.currentTimeMillis()).getBytes());
        
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get(10, TimeUnit.SECONDS);
        
        assertThat(metadata).isNotNull();
        assertThat(metadata.topic()).isEqualTo(testTopic);
    }

    @Test
    @Order(10)
    @DisplayName("Test idempotent producer - validates idempotence configuration")
    void testIdempotentProducer() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false); // TODO: Change to true once InitProducerId handler is implemented
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        
        try (KafkaProducer<String, String> idempotentProducer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                testTopic, 
                "idempotent-key", 
                "idempotent-value"
            );
            
            Future<RecordMetadata> future = idempotentProducer.send(record);
            RecordMetadata metadata = future.get(10, TimeUnit.SECONDS);
            
            assertThat(metadata).isNotNull();
            assertThat(metadata.offset()).isGreaterThanOrEqualTo(0);
        }
    }
}
