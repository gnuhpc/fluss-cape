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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Kafka Consumer Group coordination.
 * Tests JoinGroup, SyncGroup, Heartbeat, and offset management.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaConsumerGroupIntegrationTest {

    private static final String BOOTSTRAP_SERVERS = System.getProperty("kafka.bootstrap.servers", "localhost:9092");
    private static final String TEST_TOPIC_PREFIX = "test-cg-";
    
    private List<KafkaConsumer<String, String>> consumers;
    private KafkaProducer<String, String> producer;
    private String testTopic;
    private String groupId;

    @BeforeEach
    void setup() {
        testTopic = TEST_TOPIC_PREFIX + UUID.randomUUID();
        groupId = "test-group-" + UUID.randomUUID();
        consumers = new ArrayList<>();
        
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        
        producer = new KafkaProducer<>(producerProps);
    }

    @AfterEach
    void teardown() {
        for (KafkaConsumer<String, String> consumer : consumers) {
            try {
                consumer.close(Duration.ofSeconds(2));
            } catch (Exception e) {
            }
        }
        consumers.clear();
        
        if (producer != null) {
            producer.close(Duration.ofSeconds(2));
        }
    }

    @Test
    @Order(1)
    @DisplayName("Test single consumer joins group - validates JoinGroup basic flow")
    void testSingleConsumerJoinsGroup() throws Exception {
        produceTestRecords(10);
        
        KafkaConsumer<String, String> consumer = createConsumer(groupId);
        consumers.add(consumer);
        
        consumer.subscribe(Collections.singletonList(testTopic));
        
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        
        assertThat(consumer.assignment()).isNotEmpty();
        assertThat(consumer.groupMetadata().groupId()).isEqualTo(groupId);
        assertThat(consumer.groupMetadata().generationId()).isGreaterThanOrEqualTo(0);
    }

    @Test
    @Order(2)
    @DisplayName("Test multiple consumers join group - validates rebalance and partition assignment")
    void testMultipleConsumersJoinGroup() throws Exception {
        int numRecords = 30;
        produceTestRecords(numRecords);
        
        KafkaConsumer<String, String> consumer1 = createConsumer(groupId);
        KafkaConsumer<String, String> consumer2 = createConsumer(groupId);
        KafkaConsumer<String, String> consumer3 = createConsumer(groupId);
        
        consumers.add(consumer1);
        consumers.add(consumer2);
        consumers.add(consumer3);
        
        consumer1.subscribe(Collections.singletonList(testTopic));
        consumer2.subscribe(Collections.singletonList(testTopic));
        consumer3.subscribe(Collections.singletonList(testTopic));
        
        CompletableFuture<Set<TopicPartition>> future1 = CompletableFuture.supplyAsync(() -> {
            consumer1.poll(Duration.ofSeconds(10));
            return new HashSet<>(consumer1.assignment());
        });
        
        CompletableFuture<Set<TopicPartition>> future2 = CompletableFuture.supplyAsync(() -> {
            consumer2.poll(Duration.ofSeconds(10));
            return new HashSet<>(consumer2.assignment());
        });
        
        CompletableFuture<Set<TopicPartition>> future3 = CompletableFuture.supplyAsync(() -> {
            consumer3.poll(Duration.ofSeconds(10));
            return new HashSet<>(consumer3.assignment());
        });
        
        Set<TopicPartition> consumer1Partitions = future1.get();
        Set<TopicPartition> consumer2Partitions = future2.get();
        Set<TopicPartition> consumer3Partitions = future3.get();
        
        Set<TopicPartition> allAssignments = new HashSet<>();
        allAssignments.addAll(consumer1Partitions);
        allAssignments.addAll(consumer2Partitions);
        allAssignments.addAll(consumer3Partitions);
        
        assertThat(allAssignments).isNotEmpty();
        assertThat(consumer1Partitions).isNotEmpty();
        
        assertThat(consumer1Partitions).doesNotContainAnyElementsOf(consumer2Partitions);
        assertThat(consumer1Partitions).doesNotContainAnyElementsOf(consumer3Partitions);
        assertThat(consumer2Partitions).doesNotContainAnyElementsOf(consumer3Partitions);
    }

    @Test
    @Order(3)
    @DisplayName("Test consumer group rebalance - validates dynamic membership changes")
    void testConsumerGroupRebalance() throws Exception {
        produceTestRecords(20);
        
        KafkaConsumer<String, String> consumer1 = createConsumer(groupId);
        consumers.add(consumer1);
        
        consumer1.subscribe(Collections.singletonList(testTopic));
        consumer1.poll(Duration.ofSeconds(5));
        
        Set<TopicPartition> initialAssignment = new HashSet<>(consumer1.assignment());
        assertThat(initialAssignment).isNotEmpty();
        
        KafkaConsumer<String, String> consumer2 = createConsumer(groupId);
        consumers.add(consumer2);
        
        consumer2.subscribe(Collections.singletonList(testTopic));
        
        consumer1.poll(Duration.ofSeconds(5));
        consumer2.poll(Duration.ofSeconds(5));
        
        Set<TopicPartition> rebalancedAssignment1 = consumer1.assignment();
        Set<TopicPartition> rebalancedAssignment2 = consumer2.assignment();
        
        assertThat(rebalancedAssignment1).isNotEmpty();
        assertThat(rebalancedAssignment2).isNotEmpty();
        assertThat(rebalancedAssignment1).doesNotContainAnyElementsOf(rebalancedAssignment2);
    }

    @Test
    @Order(4)
    @DisplayName("Test consumer group offset commit and fetch - validates offset management")
    void testConsumerGroupOffsetCommitAndFetch() throws Exception {
        int numRecords = 15;
        produceTestRecords(numRecords);
        
        KafkaConsumer<String, String> consumer1 = createConsumer(groupId);
        consumers.add(consumer1);
        
        consumer1.subscribe(Collections.singletonList(testTopic));
        
        int consumedCount = 0;
        long startTime = System.currentTimeMillis();
        
        while (consumedCount < numRecords && System.currentTimeMillis() - startTime < 30000) {
            ConsumerRecords<String, String> records = consumer1.poll(Duration.ofSeconds(5));
            consumedCount += records.count();
            
            if (!records.isEmpty()) {
                consumer1.commitSync();
            }
        }
        
        assertThat(consumedCount).isEqualTo(numRecords);
        
        Map<TopicPartition, OffsetAndMetadata> committed = consumer1.committed(
            new HashSet<>(consumer1.assignment())
        );
        
        assertThat(committed).isNotEmpty();
        
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committed.entrySet()) {
            assertThat(entry.getValue().offset()).isGreaterThan(0);
        }
        
        consumer1.close();
        consumers.remove(consumer1);
        
        KafkaConsumer<String, String> consumer2 = createConsumer(groupId);
        consumers.add(consumer2);
        
        consumer2.subscribe(Collections.singletonList(testTopic));
        
        ConsumerRecords<String, String> records = consumer2.poll(Duration.ofSeconds(10));
        
        assertThat(records.isEmpty()).isTrue();
    }

    @Test
    @Order(5)
    @DisplayName("Test consumer heartbeat - validates session maintenance")
    void testConsumerHeartbeat() throws Exception {
        produceTestRecords(10);
        
        Properties props = getConsumerProperties(groupId);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumers.add(consumer);
        
        consumer.subscribe(Collections.singletonList(testTopic));
        
        for (int i = 0; i < 5; i++) {
            consumer.poll(Duration.ofSeconds(2));
            Thread.sleep(2000);
        }
        
        assertThat(consumer.assignment()).isNotEmpty();
    }

    @Test
    @Order(6)
    @DisplayName("Test consumer leave group - validates clean shutdown")
    void testConsumerLeaveGroup() throws Exception {
        produceTestRecords(10);
        
        KafkaConsumer<String, String> consumer1 = createConsumer(groupId);
        KafkaConsumer<String, String> consumer2 = createConsumer(groupId);
        
        consumers.add(consumer1);
        consumers.add(consumer2);
        
        consumer1.subscribe(Collections.singletonList(testTopic));
        consumer2.subscribe(Collections.singletonList(testTopic));
        
        consumer1.poll(Duration.ofSeconds(5));
        consumer2.poll(Duration.ofSeconds(5));
        
        Set<TopicPartition> consumer2InitialAssignment = new HashSet<>(consumer2.assignment());
        assertThat(consumer2InitialAssignment).isNotEmpty();
        
        consumer1.close();
        consumers.remove(consumer1);
        
        Thread.sleep(2000);
        
        consumer2.poll(Duration.ofSeconds(5));
        
        Set<TopicPartition> consumer2FinalAssignment = consumer2.assignment();
        
        assertThat(consumer2FinalAssignment.size()).isGreaterThanOrEqualTo(consumer2InitialAssignment.size());
    }

    @Test
    @Order(7)
    @DisplayName("Test parallel consumption - validates concurrent group members")
    void testParallelConsumption() throws Exception {
        int numRecords = 50;
        produceTestRecords(numRecords);
        
        int numConsumers = 3;
        CountDownLatch startLatch = new CountDownLatch(numConsumers);
        CountDownLatch doneLatch = new CountDownLatch(numConsumers);
        AtomicInteger totalConsumed = new AtomicInteger(0);
        
        List<Thread> threads = new ArrayList<>();
        
        for (int i = 0; i < numConsumers; i++) {
            Thread thread = new Thread(() -> {
                KafkaConsumer<String, String> consumer = createConsumer(groupId);
                synchronized (consumers) {
                    consumers.add(consumer);
                }
                
                consumer.subscribe(Collections.singletonList(testTopic));
                
                startLatch.countDown();
                
                try {
                    startLatch.await(10, TimeUnit.SECONDS);
                    
                    int consumed = 0;
                    long startTime = System.currentTimeMillis();
                    
                    while (consumed < 20 && System.currentTimeMillis() - startTime < 30000) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                        consumed += records.count();
                        totalConsumed.addAndGet(records.count());
                    }
                    
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
            
            threads.add(thread);
            thread.start();
        }
        
        boolean finished = doneLatch.await(60, TimeUnit.SECONDS);
        assertThat(finished).isTrue();
        
        for (Thread thread : threads) {
            thread.join(5000);
        }
        
        assertThat(totalConsumed.get()).isEqualTo(numRecords);
    }

    @Test
    @Order(8)
    @DisplayName("Test offset commit with metadata - validates commit metadata storage")
    void testOffsetCommitWithMetadata() throws Exception {
        produceTestRecords(10);
        
        KafkaConsumer<String, String> consumer = createConsumer(groupId);
        consumers.add(consumer);
        
        consumer.subscribe(Collections.singletonList(testTopic));
        
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        
        if (!records.isEmpty()) {
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
            
            for (TopicPartition partition : consumer.assignment()) {
                long position = consumer.position(partition);
                offsetsToCommit.put(partition, new OffsetAndMetadata(position, "test-metadata"));
            }
            
            consumer.commitSync(offsetsToCommit);
            
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(
                new HashSet<>(consumer.assignment())
            );
            
            assertThat(committed).isNotEmpty();
            
            for (OffsetAndMetadata metadata : committed.values()) {
                assertThat(metadata.metadata()).isEqualTo("test-metadata");
            }
        }
    }

    @Test
    @Order(9)
    @DisplayName("Test consumer group with different topics - validates multi-topic coordination")
    void testConsumerGroupWithDifferentTopics() throws Exception {
        String topic1 = testTopic + "-1";
        String topic2 = testTopic + "-2";
        
        produceTestRecords(topic1, 10);
        produceTestRecords(topic2, 10);
        
        KafkaConsumer<String, String> consumer1 = createConsumer(groupId);
        KafkaConsumer<String, String> consumer2 = createConsumer(groupId);
        
        consumers.add(consumer1);
        consumers.add(consumer2);
        
        consumer1.subscribe(Arrays.asList(topic1, topic2));
        consumer2.subscribe(Arrays.asList(topic1, topic2));
        
        consumer1.poll(Duration.ofSeconds(5));
        consumer2.poll(Duration.ofSeconds(5));
        
        Set<String> consumer1Topics = new HashSet<>();
        Set<String> consumer2Topics = new HashSet<>();
        
        for (TopicPartition tp : consumer1.assignment()) {
            consumer1Topics.add(tp.topic());
        }
        
        for (TopicPartition tp : consumer2.assignment()) {
            consumer2Topics.add(tp.topic());
        }
        
        Set<String> allTopics = new HashSet<>();
        allTopics.addAll(consumer1Topics);
        allTopics.addAll(consumer2Topics);
        
        assertThat(allTopics).containsAnyOf(topic1, topic2);
    }

    @Test
    @Order(10)
    @DisplayName("Test generation ID increment - validates rebalance generation tracking")
    void testGenerationIdIncrement() throws Exception {
        produceTestRecords(10);
        
        KafkaConsumer<String, String> consumer1 = createConsumer(groupId);
        consumers.add(consumer1);
        
        consumer1.subscribe(Collections.singletonList(testTopic));
        consumer1.poll(Duration.ofSeconds(5));
        
        int initialGeneration = consumer1.groupMetadata().generationId();
        
        KafkaConsumer<String, String> consumer2 = createConsumer(groupId);
        consumers.add(consumer2);
        
        consumer2.subscribe(Collections.singletonList(testTopic));
        
        consumer1.poll(Duration.ofSeconds(5));
        consumer2.poll(Duration.ofSeconds(5));
        
        int newGeneration1 = consumer1.groupMetadata().generationId();
        int newGeneration2 = consumer2.groupMetadata().generationId();
        
        assertThat(newGeneration1).isGreaterThanOrEqualTo(initialGeneration);
        assertThat(newGeneration1).isEqualTo(newGeneration2);
    }

    private KafkaConsumer<String, String> createConsumer(String groupId) {
        return new KafkaConsumer<>(getConsumerProperties(groupId));
    }

    private Properties getConsumerProperties(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");
        
        return props;
    }

    private void produceTestRecords(int numRecords) throws Exception {
        produceTestRecords(testTopic, numRecords);
    }

    private void produceTestRecords(String topic, int numRecords) throws Exception {
        for (int i = 0; i < numRecords; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                topic,
                "key-" + i,
                "value-" + i
            );
            producer.send(record).get();
        }
        producer.flush();
        Thread.sleep(1000);
    }
}
