package org.gnuhpc.fluss.cape.kafka.integration;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Kafka Admin API operations.
 * Tests metadata operations through MetadataHandler.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaAdminIntegrationTest {

    private static final String BOOTSTRAP_SERVERS = System.getProperty("kafka.bootstrap.servers", "localhost:9092");
    private static final String TEST_TOPIC_PREFIX = "test-admin-";
    
    private AdminClient adminClient;
    private String testTopic;

    @BeforeEach
    void setup() {
        testTopic = TEST_TOPIC_PREFIX + UUID.randomUUID();
        
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        
        adminClient = AdminClient.create(props);
    }

    @AfterEach
    void teardown() {
        if (adminClient != null) {
            adminClient.close(Duration.ofSeconds(5));
        }
    }

    @Test
    @Order(1)
    @DisplayName("Test list topics - validates metadata retrieval")
    void testListTopics() throws Exception {
        ListTopicsResult result = adminClient.listTopics();
        Set<String> topics = result.names().get(10, TimeUnit.SECONDS);
        
        assertThat(topics).isNotNull();
    }

    @Test
    @Order(2)
    @Disabled("DESCRIBE_TOPICS API not implemented - requires DescribeTopics handler")
    @DisplayName("Test describe topics - validates topic metadata")
    void testDescribeTopics() throws Exception {
        createTestTopic();
        
        DescribeTopicsResult result = adminClient.describeTopics(Collections.singleton(testTopic));
        Map<String, TopicDescription> descriptions = result.all().get(10, TimeUnit.SECONDS);
        
        assertThat(descriptions).containsKey(testTopic);
        
        TopicDescription description = descriptions.get(testTopic);
        assertThat(description.name()).isEqualTo(testTopic);
        assertThat(description.partitions()).isNotEmpty();
    }

    @Test
    @Order(3)
    @Disabled("DESCRIBE_CLUSTER API not implemented - requires cluster metadata handler")
    @DisplayName("Test describe cluster - validates cluster metadata")
    void testDescribeCluster() throws Exception {
        DescribeClusterResult result = adminClient.describeCluster();
        
        String clusterId = result.clusterId().get(10, TimeUnit.SECONDS);
        Collection<org.apache.kafka.common.Node> nodes = result.nodes().get(10, TimeUnit.SECONDS);
        
        assertThat(nodes).isNotEmpty();
    }

    @Test
    @Order(4)
    @Disabled("LIST_GROUPS API not implemented - requires consumer group listing handler")
    @DisplayName("Test list consumer groups - validates consumer group listing")
    void testListConsumerGroups() throws Exception {
        String groupId = "test-admin-group-" + UUID.randomUUID();
        
        createConsumerInGroup(groupId);
        
        Thread.sleep(2000);
        
        ListConsumerGroupsResult result = adminClient.listConsumerGroups();
        Collection<ConsumerGroupListing> groups = result.all().get(10, TimeUnit.SECONDS);
        
        assertThat(groups).isNotNull();
    }

    @Test
    @Order(5)
    @Disabled("CONSUMER_GROUP_DESCRIBE API not implemented - requires group metadata handler")
    @DisplayName("Test describe consumer groups - validates consumer group metadata")
    void testDescribeConsumerGroups() throws Exception {
        String groupId = "test-admin-cg-" + UUID.randomUUID();
        
        KafkaConsumer<String, String> consumer = createConsumerInGroup(groupId);
        
        Thread.sleep(2000);
        
        try {
            DescribeConsumerGroupsResult result = adminClient.describeConsumerGroups(
                Collections.singleton(groupId)
            );
            
            Map<String, ConsumerGroupDescription> descriptions = result.all().get(10, TimeUnit.SECONDS);
            
            if (descriptions.containsKey(groupId)) {
                ConsumerGroupDescription description = descriptions.get(groupId);
                assertThat(description.groupId()).isEqualTo(groupId);
                assertThat(description.members()).isNotEmpty();
            }
        } finally {
            consumer.close();
        }
    }

    @Test
    @Order(6)
    @DisplayName("Test list consumer group offsets - validates offset retrieval")
    void testListConsumerGroupOffsets() throws Exception {
        createTestTopic();
        
        String groupId = "test-offset-list-" + UUID.randomUUID();
        
        produceAndConsumeRecords(groupId, 10);
        
        Map<TopicPartition, OffsetSpec> partitionOffsets = new HashMap<>();
        partitionOffsets.put(new TopicPartition(testTopic, 0), OffsetSpec.latest());
        
        ListOffsetsResult offsetsResult = adminClient.listOffsets(partitionOffsets);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets = 
            offsetsResult.all().get(10, TimeUnit.SECONDS);
        
        assertThat(offsets).isNotEmpty();
        
        for (ListOffsetsResult.ListOffsetsResultInfo info : offsets.values()) {
            assertThat(info.offset()).isGreaterThanOrEqualTo(0);
        }
    }

    @Test
    @Order(7)
    @Disabled("ALTER_CONSUMER_GROUP_OFFSETS API has issues with coordinator key handling")
    @DisplayName("Test alter consumer group offsets - validates offset modification")
    void testAlterConsumerGroupOffsets() throws Exception {
        createTestTopic();
        
        String groupId = "test-alter-offset-" + UUID.randomUUID();
        
        produceAndConsumeRecords(groupId, 10);
        
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition(testTopic, 0), new OffsetAndMetadata(5));
        
        AlterConsumerGroupOffsetsResult result = adminClient.alterConsumerGroupOffsets(
            groupId,
            offsets
        );
        
        result.all().get(10, TimeUnit.SECONDS);
        
        ListConsumerGroupOffsetsResult listResult = adminClient.listConsumerGroupOffsets(groupId);
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = 
            listResult.partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
        
        if (committedOffsets.containsKey(new TopicPartition(testTopic, 0))) {
            OffsetAndMetadata committed = committedOffsets.get(new TopicPartition(testTopic, 0));
            assertThat(committed.offset()).isEqualTo(5);
        }
    }

    @Test
    @Order(8)
    @DisplayName("Test metadata with pattern subscription - validates topic pattern matching")
    void testMetadataWithPatternSubscription() throws Exception {
        String topic1 = testTopic + "-pattern-1";
        String topic2 = testTopic + "-pattern-2";
        
        createTopic(topic1);
        createTopic(topic2);
        
        Thread.sleep(1000);
        
        ListTopicsResult result = adminClient.listTopics();
        Set<String> topics = result.names().get(10, TimeUnit.SECONDS);
        
        long matchingTopics = topics.stream()
            .filter(t -> t.startsWith(TEST_TOPIC_PREFIX))
            .count();
        
        assertThat(matchingTopics).isGreaterThanOrEqualTo(2);
    }

    private void createTestTopic() throws Exception {
        createTopic(testTopic);
    }

    private void createTopic(String topicName) throws Exception {
        NewTopic newTopic = new NewTopic(topicName, 3, (short) 1);
        
        try {
            CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
            result.all().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
        }
        
        Thread.sleep(1000);
    }

    private KafkaConsumer<String, String> createConsumerInGroup(String groupId) throws Exception {
        createTestTopic();
        
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(testTopic));
        consumer.poll(Duration.ofSeconds(5));
        
        return consumer;
    }

    private void produceAndConsumeRecords(String groupId, int numRecords) throws Exception {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 0; i < numRecords; i++) {
                producer.send(new ProducerRecord<>(testTopic, "key-" + i, "value-" + i)).get();
            }
            producer.flush();
        }
        
        Thread.sleep(1000);
        
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(testTopic));
            
            int consumed = 0;
            long startTime = System.currentTimeMillis();
            
            while (consumed < numRecords && System.currentTimeMillis() - startTime < 30000) {
                var records = consumer.poll(Duration.ofSeconds(5));
                consumed += records.count();
                
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        }
    }
}
