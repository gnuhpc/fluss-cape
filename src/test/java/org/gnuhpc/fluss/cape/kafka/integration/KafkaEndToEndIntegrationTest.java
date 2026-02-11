package org.gnuhpc.fluss.cape.kafka.integration;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test simulating real-world Kafka usage patterns.
 * Tests complete producer-consumer workflows with all implemented features.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaEndToEndIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaEndToEndIntegrationTest.class);

    private static final String BOOTSTRAP_SERVERS = System.getProperty("kafka.bootstrap.servers", "localhost:9092");
    private static final String TEST_TOPIC_PREFIX = "test-e2e-";
    
    private List<KafkaProducer<String, String>> producers;
    private List<KafkaConsumer<String, String>> consumers;
    private ExecutorService executorService;
    private String testTopic;

    @BeforeEach
    void setup() {
        testTopic = TEST_TOPIC_PREFIX + UUID.randomUUID();
        producers = new ArrayList<>();
        consumers = new ArrayList<>();
        executorService = Executors.newCachedThreadPool();
    }

    @AfterEach
    void teardown() {
        for (KafkaProducer<String, String> producer : producers) {
            try {
                producer.close(Duration.ofSeconds(2));
            } catch (Exception e) {
            }
        }
        
        for (KafkaConsumer<String, String> consumer : consumers) {
            try {
                consumer.close(Duration.ofSeconds(2));
            } catch (Exception e) {
            }
        }
        
        if (executorService != null) {
            executorService.shutdownNow();
        }
        
        producers.clear();
        consumers.clear();
    }

    @Test
    @Order(1)
    @DisplayName("Test simple producer-consumer workflow")
    void testSimpleProducerConsumerWorkflow() throws Exception {
        int numRecords = 20;
        
        KafkaProducer<String, String> producer = createProducer();
        producers.add(producer);
        
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
        
        String groupId = "e2e-simple-group-" + UUID.randomUUID();
        KafkaConsumer<String, String> consumer = createConsumer(groupId);
        consumers.add(consumer);
        
        consumer.subscribe(Collections.singletonList(testTopic));
        
        List<ConsumerRecord<String, String>> receivedRecords = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        
        while (receivedRecords.size() < numRecords && System.currentTimeMillis() - startTime < 30000) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                receivedRecords.add(record);
            }
        }
        
        assertThat(receivedRecords).hasSize(numRecords);
        
        for (int i = 0; i < numRecords; i++) {
            ConsumerRecord<String, String> record = receivedRecords.get(i);
            assertThat(record.key()).isEqualTo("key-" + i);
            assertThat(record.value()).isEqualTo("value-" + i);
        }
    }

    @Test
    @Order(2)
    @DisplayName("Test multiple producers single consumer")
    void testMultipleProducersSingleConsumer() throws Exception {
        int numProducers = 3;
        int recordsPerProducer = 10;
        
        CountDownLatch producerLatch = new CountDownLatch(numProducers);
        
        for (int p = 0; p < numProducers; p++) {
            final int producerId = p;
            executorService.submit(() -> {
                try {
                    KafkaProducer<String, String> producer = createProducer();
                    synchronized (producers) {
                        producers.add(producer);
                    }
                    
                    for (int i = 0; i < recordsPerProducer; i++) {
                        ProducerRecord<String, String> record = new ProducerRecord<>(
                            testTopic,
                            "producer-" + producerId + "-key-" + i,
                            "producer-" + producerId + "-value-" + i
                        );
                        producer.send(record).get();
                    }
                    
                    producer.flush();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    producerLatch.countDown();
                }
            });
        }
        
        producerLatch.await(30, TimeUnit.SECONDS);
        Thread.sleep(2000);
        
        String groupId = "e2e-multi-prod-" + UUID.randomUUID();
        KafkaConsumer<String, String> consumer = createConsumer(groupId);
        consumers.add(consumer);
        
        consumer.subscribe(Collections.singletonList(testTopic));
        
        int totalExpected = numProducers * recordsPerProducer;
        List<ConsumerRecord<String, String>> receivedRecords = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        
        while (receivedRecords.size() < totalExpected && System.currentTimeMillis() - startTime < 30000) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                receivedRecords.add(record);
            }
        }
        
        assertThat(receivedRecords).hasSize(totalExpected);
    }

    @Test
    @Order(3)
    @DisplayName("Test single producer multiple consumers with consumer group")
    void testSingleProducerMultipleConsumers() throws Exception {
        int numRecords = 50;
        int numConsumers = 3;
        
        KafkaProducer<String, String> producer = createProducer();
        producers.add(producer);
        
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
        
        String groupId = "e2e-multi-consumer-" + UUID.randomUUID();
        CountDownLatch consumerLatch = new CountDownLatch(numConsumers);
        AtomicInteger totalConsumed = new AtomicInteger(0);
        
        for (int c = 0; c < numConsumers; c++) {
            executorService.submit(() -> {
                try {
                    KafkaConsumer<String, String> consumer = createConsumer(groupId);
                    synchronized (consumers) {
                        consumers.add(consumer);
                    }
                    
                    consumer.subscribe(Collections.singletonList(testTopic));
                    
                    int consumed = 0;
                    long startTime = System.currentTimeMillis();
                    
                    while (consumed < numRecords && System.currentTimeMillis() - startTime < 30000) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                        consumed += records.count();
                        totalConsumed.addAndGet(records.count());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    consumerLatch.countDown();
                }
            });
        }
        
        consumerLatch.await(60, TimeUnit.SECONDS);
        
        assertThat(totalConsumed.get()).isEqualTo(numRecords);
    }

    @Test
    @Order(4)
    @DisplayName("Test producer-consumer with offset commits and restarts")
    void testProducerConsumerWithOffsetCommits() throws Exception {
        int numRecords = 30;
        
        KafkaProducer<String, String> producer = createProducer();
        producers.add(producer);
        
        for (int i = 0; i < numRecords; i++) {
            producer.send(new ProducerRecord<>(testTopic, "key-" + i, "value-" + i)).get();
        }
        
        producer.flush();
        Thread.sleep(1000);
        
        String groupId = "e2e-offset-commit-" + UUID.randomUUID();
        KafkaConsumer<String, String> consumer1 = createConsumer(groupId);
        consumers.add(consumer1);
        
        consumer1.subscribe(Collections.singletonList(testTopic));
        
        int firstBatchSize = 0;
        long startTime = System.currentTimeMillis();
        
        while (firstBatchSize < 15 && System.currentTimeMillis() - startTime < 30000) {
            ConsumerRecords<String, String> records = consumer1.poll(Duration.ofSeconds(5));
            firstBatchSize += records.count();
            
            if (!records.isEmpty()) {
                consumer1.commitSync();
            }
        }
        
        assertThat(firstBatchSize).isGreaterThanOrEqualTo(15);
        
        consumer1.close();
        consumers.remove(consumer1);
        
        Thread.sleep(2000);
        
        KafkaConsumer<String, String> consumer2 = createConsumer(groupId);
        consumers.add(consumer2);
        
        consumer2.subscribe(Collections.singletonList(testTopic));
        
        int secondBatchSize = 0;
        startTime = System.currentTimeMillis();
        
        while (secondBatchSize < 15 && System.currentTimeMillis() - startTime < 30000) {
            ConsumerRecords<String, String> records = consumer2.poll(Duration.ofSeconds(5));
            secondBatchSize += records.count();
        }
        
        assertThat(firstBatchSize + secondBatchSize).isEqualTo(numRecords);
    }

    @Test
    @Order(5)
    @DisplayName("Test high-throughput producer-consumer scenario")
    void testHighThroughputScenario() throws Exception {
        int numRecords = 500;
        int batchSize = 50;
        
        KafkaProducer<String, String> producer = createProducer();
        producers.add(producer);
        
        long produceStartTime = System.nanoTime();
        
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                testTopic,
                "high-throughput-key-" + i,
                "high-throughput-value-" + i
            );
            futures.add(producer.send(record));
        }
        
        producer.flush();
        
        for (Future<RecordMetadata> future : futures) {
            future.get(5, TimeUnit.SECONDS);
        }
        
        long produceEndTime = System.nanoTime();
        long produceTimeMs = (produceEndTime - produceStartTime) / 1_000_000;
        double produceThroughput = numRecords * 1000.0 / produceTimeMs;
        
        LOG.info("Produced {} records in {} ms ({:.0f} records/sec)",
            numRecords, produceTimeMs, produceThroughput);
        
        Thread.sleep(1000);
        
        String groupId = "e2e-high-throughput-" + UUID.randomUUID();
        KafkaConsumer<String, String> consumer = createConsumer(groupId);
        consumers.add(consumer);
        
        consumer.subscribe(Collections.singletonList(testTopic));
        
        long consumeStartTime = System.nanoTime();
        
        int consumedCount = 0;
        long pollStartTime = System.currentTimeMillis();
        
        while (consumedCount < numRecords && System.currentTimeMillis() - pollStartTime < 60000) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            consumedCount += records.count();
        }
        
        long consumeEndTime = System.nanoTime();
        long consumeTimeMs = (consumeEndTime - consumeStartTime) / 1_000_000;
        double consumeThroughput = numRecords * 1000.0 / consumeTimeMs;
        
        LOG.info("Consumed {} records in {} ms ({:.0f} records/sec)",
            numRecords, consumeTimeMs, consumeThroughput);
        
        assertThat(consumedCount).isEqualTo(numRecords);
        assertThat(produceThroughput).isGreaterThan(100);
        assertThat(consumeThroughput).isGreaterThan(50);
    }

    @Test
    @Order(6)
    @DisplayName("Test concurrent producers and consumers")
    void testConcurrentProducersAndConsumers() throws Exception {
        int numProducers = 2;
        int numConsumers = 2;
        int recordsPerProducer = 25;
        
        String groupId = "e2e-concurrent-" + UUID.randomUUID();
        CountDownLatch allDone = new CountDownLatch(numProducers + numConsumers);
        AtomicInteger totalConsumed = new AtomicInteger(0);
        
        for (int p = 0; p < numProducers; p++) {
            final int producerId = p;
            executorService.submit(() -> {
                try {
                    KafkaProducer<String, String> producer = createProducer();
                    synchronized (producers) {
                        producers.add(producer);
                    }
                    
                    for (int i = 0; i < recordsPerProducer; i++) {
                        ProducerRecord<String, String> record = new ProducerRecord<>(
                            testTopic,
                            "concurrent-p" + producerId + "-key-" + i,
                            "concurrent-p" + producerId + "-value-" + i
                        );
                        producer.send(record).get();
                    }
                    
                    producer.flush();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    allDone.countDown();
                }
            });
        }
        
        Thread.sleep(2000);
        
        for (int c = 0; c < numConsumers; c++) {
            executorService.submit(() -> {
                try {
                    KafkaConsumer<String, String> consumer = createConsumer(groupId);
                    synchronized (consumers) {
                        consumers.add(consumer);
                    }
                    
                    consumer.subscribe(Collections.singletonList(testTopic));
                    
                    long startTime = System.currentTimeMillis();
                    
                    while (System.currentTimeMillis() - startTime < 30000) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                        totalConsumed.addAndGet(records.count());
                        
                        if (totalConsumed.get() >= numProducers * recordsPerProducer) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    allDone.countDown();
                }
            });
        }
        
        boolean finished = allDone.await(60, TimeUnit.SECONDS);
        assertThat(finished).isTrue();
        assertThat(totalConsumed.get()).isEqualTo(numProducers * recordsPerProducer);
    }

    private KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, String> createConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");
        
        return new KafkaConsumer<>(props);
    }
}
