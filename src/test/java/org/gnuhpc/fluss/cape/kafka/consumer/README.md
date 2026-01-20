# Kafka Consumer Group - Integration Tests

This directory previously contained integration tests for consumer group coordination, but those tests have been removed as part of the filter architecture simplification.

## Test Overview

The integration tests validate the consumer group functionality including:
- **JoinGroup workflow**: Member joining, leader election, generation management
- **Heartbeat handling**: Timestamp updates, error conditions, validation
- **Offset management**: Committing and fetching consumer offsets

## Test Files

### 1. JoinGroupWorkflowTest.java
Tests the complete JoinGroup workflow for consumer group coordination.

**Test Cases**:
- `testSingleMemberJoin_BecomesLeader()` - First member joining becomes leader
- `testMultipleMembersJoin_GenerationIncrement()` - Generation ID increments on new member
- `testMemberRejoinWithSameMemberId()` - Member can rejoin with same ID
- `testLeaderElection_FirstMemberIsLeader()` - First member remains leader
- `testEmptyGroupId_ReturnsError()` - Empty group ID validation

**Coverage**: 
- Leader election logic
- Generation ID management
- Member ID assignment
- Error handling

### 2. HeartbeatWorkflowTest.java
Tests heartbeat handling and validation logic.

**Test Cases**:
- `testValidHeartbeat_UpdatesTimestamp()` - Valid heartbeat updates member timestamp
- `testInvalidGenerationId_ReturnsIllegalGeneration()` - Wrong generation returns error
- `testUnknownMember_ReturnsUnknownMemberId()` - Unknown member returns error
- `testRebalanceInProgress_ReturnsRebalanceInProgress()` - Rebalance state handling

**Coverage**:
- Heartbeat timestamp updates
- Generation ID validation
- Member existence checks
- Rebalance state detection

### 3. OffsetCommitFetchTest.java
Tests offset commit and fetch operations.

**Test Cases**:
- `testCommitAndFetchOffset_ReturnsCommittedValue()` - Basic commit/fetch cycle
- `testFetchNonExistentOffset_ReturnsMinusOne()` - Non-existent offsets return -1
- `testCommitMultiplePartitions()` - Batch commit across partitions
- `testUpdateExistingOffset()` - Updating previously committed offsets

**Coverage**:
- Offset persistence to `__consumer_offsets` table
- Offset retrieval logic
- Multi-partition operations
- Update semantics

## Running Tests

### Prerequisites
```bash
# Start Fluss cluster
# Make sure Fluss is running on localhost:9123 (or configure via system property)

# Optional: Override Fluss connection
export JAVA_OPTS="-Dfluss.bootstrap=your-fluss-host:9123"
```

### Run All Tests
```bash
# Run all consumer group tests
mvn test -Dtest="org.gnuhpc.fluss.cape.kafka.consumer.*"

# Run specific test class
mvn test -Dtest=JoinGroupWorkflowTest
mvn test -Dtest=HeartbeatWorkflowTest
mvn test -Dtest=OffsetCommitFetchTest

# Run with verbose output
mvn test -Dtest=JoinGroupWorkflowTest -X
```

### Run Single Test Method
```bash
# Run specific test method
mvn test -Dtest=JoinGroupWorkflowTest#testSingleMemberJoin_BecomesLeader
```

## Test Configuration

All tests use the following default configuration:

```java
SESSION_TIMEOUT_MS = 30000      // 30 seconds
REBALANCE_TIMEOUT_MS = 60000    // 60 seconds
PROTOCOL_TYPE = "consumer"      // Kafka consumer protocol
```

Tests create unique group IDs per test case to avoid interference:
```java
String groupId = TEST_GROUP + "-suffix-" + UUID.randomUUID();
```

## Test Architecture

### Setup Pattern
Each test follows this pattern:

```java
@BeforeEach
void setup() throws Exception {
    // 1. Create Fluss connection
    Configuration config = new Configuration();
    config.setString("bootstrap.servers", "localhost:9123");
    flussConnection = ConnectionFactory.createConnection(config);
    
    // 2. Initialize coordinator
    coordinator = new ConsumerGroupCoordinatorV2(flussConnection);
    
    // 3. Initialize handlers directly (no filter helpers)
    handler = new XxxHandler(coordinator);
}

@AfterEach
void teardown() throws Exception {
    // Clean up resources
    coordinator.close();
    flussConnection.close();
}
```

### Test Pattern (BDD Style)
Tests use Given-When-Then structure:

```java
@Test
void testExample() throws Exception {
    // Given: Setup preconditions
    String groupId = TEST_GROUP + "-example-" + UUID.randomUUID();
    var request = createRequest(groupId, ...);
    
    // When: Execute the operation
    var response = helper.handleAsync(request).get();
    
    // Then: Verify expectations
    assertThat(response.errorCode()).isEqualTo(Errors.NONE.code());
    assertThat(response.someField()).isEqualTo(expectedValue);
}
```

## Expected Behavior

### JoinGroup Workflow
1. **First member joins** → Becomes leader, generation = 1
2. **Second member joins** → Triggers rebalance, generation = 2
3. **Leader election** → First member remains leader across generations
4. **Member rejoin** → Member can rejoin with same ID

### Heartbeat Workflow
1. **Valid heartbeat** → Returns NONE error, updates timestamp
2. **Invalid generation** → Returns ILLEGAL_GENERATION error
3. **Unknown member** → Returns UNKNOWN_MEMBER_ID error
4. **During rebalance** → Returns REBALANCE_IN_PROGRESS error

### Offset Commit/Fetch Workflow
1. **Commit offset** → Persists to `__consumer_offsets` table
2. **Fetch committed offset** → Returns last committed value
3. **Fetch non-existent** → Returns -1 (Kafka convention)
4. **Update offset** → Overwrites previous value

## Troubleshooting

### Test Failures

**Connection refused to Fluss**:
```
Solution: Start Fluss cluster before running tests
OR: Configure correct bootstrap servers via -Dfluss.bootstrap=...
```

**Table not found errors**:
```
Solution: Coordinator creates tables automatically on first use
Wait a few seconds and retry if tables are being created
```

**Timeout errors**:
```
Solution: Tests have 30-second timeout per test
Check if Fluss cluster is responsive
Check network connectivity
```

### Debug Logging

Enable debug logging for detailed execution traces:

```xml
<!-- Add to logback-test.xml -->
<logger name="org.gnuhpc.fluss.cape.kafka.consumer" level="DEBUG"/>
<logger name="org.gnuhpc.fluss.cape.kafka.handler" level="DEBUG"/>
```

## Integration with CI/CD

These tests require a running Fluss instance. In CI/CD pipelines:

```bash
# Example: GitHub Actions
- name: Start Fluss
  run: docker-compose up -d fluss
  
- name: Wait for Fluss
  run: ./wait-for-fluss.sh localhost:9123
  
- name: Run Integration Tests
  run: mvn test -Dtest="org.gnuhpc.fluss.cape.kafka.consumer.*"
  
- name: Stop Fluss
  run: docker-compose down
```

## Future Test Coverage

Potential additional tests:

- **SyncGroup workflow** - Assignment distribution and state transitions
- **Rebalance scenarios** - Member timeout, forced rebalance
- **Concurrent operations** - Multiple members joining simultaneously
- **Edge cases** - Empty assignments, invalid topic names
- **Performance tests** - Large groups (100+ members), high heartbeat rate

## References

- [Kafka Consumer Group Protocol](https://kafka.apache.org/protocol#The_Messages_JoinGroup)
- [ConsumerGroupCoordinatorV2 Implementation](../../../main/java/org/gnuhpc/fluss/cape/kafka/consumer/ConsumerGroupCoordinatorV2.java)

**Note**: The individual unit tests (JoinGroupWorkflowTest, HeartbeatWorkflowTest, OffsetCommitFetchTest) have been removed as part of the filter architecture simplification. End-to-end functionality is validated by integration tests in `src/test/java/org/gnuhpc/fluss/cape/kafka/integration/`.
