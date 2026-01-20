package org.gnuhpc.fluss.cape.kafka.tools;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

/**
 * Utility to create Kafka-compatible topics in Fluss.
 * 
 * <p>Creates tables with the schema expected by Kafka protocol handlers:
 * - key (BYTES): Message key
 * - value (BYTES): Message value
 * - timestamp (BIGINT): Message timestamp
 * 
 * <p>Usage:
 * <pre>
 * java CreateKafkaTopic &lt;bootstrap-servers&gt; &lt;database&gt; &lt;topic-name&gt; [partitions]
 * 
 * Examples:
 *   java CreateKafkaTopic localhost:9133 default my_topic
 *   java CreateKafkaTopic localhost:9133 oss_test_db events 10
 * </pre>
 */
public class CreateKafkaTopic {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: CreateKafkaTopic <bootstrap-servers> <database> <topic-name> [partitions]");
            System.err.println();
            System.err.println("Arguments:");
            System.err.println("  bootstrap-servers  Fluss coordinator address (e.g., localhost:9133)");
            System.err.println("  database           Database name (e.g., default, oss_test_db)");
            System.err.println("  topic-name         Kafka topic name");
            System.err.println("  partitions         Number of partitions (default: 3)");
            System.err.println();
            System.err.println("Examples:");
            System.err.println("  CreateKafkaTopic localhost:9133 default my_topic");
            System.err.println("  CreateKafkaTopic localhost:9133 oss_test_db events 10");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String database = args[1];
        String topicName = args[2];
        int partitions = args.length > 3 ? Integer.parseInt(args[3]) : 3;

        System.out.println("Connecting to Fluss: " + bootstrapServers);
        
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrapServers);

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        try {
            createKafkaTopic(admin, database, topicName, partitions);
            System.out.println("\n✓ Kafka topic created successfully!");
        } finally {
            connection.close();
        }
    }
    
    private static void createKafkaTopic(Admin admin, String database, String topicName, int partitions) throws Exception {
        TablePath tablePath = TablePath.of(database, topicName);

        if (admin.tableExists(tablePath).get()) {
            System.out.println("Topic '" + database + "." + topicName + "' already exists.");
            System.out.println("Use a different name or delete the existing table first.");
            return;
        }

        System.out.println("Creating Kafka topic '" + database + "." + topicName + "'...");
        
        // Kafka protocol expects this exact schema
        Schema schema = Schema.newBuilder()
                .column("key", DataTypes.BYTES())
                .column("value", DataTypes.BYTES())
                .column("timestamp", DataTypes.BIGINT())
                .build();

        TableDescriptor tableDescriptor = TableDescriptor.builder()
                .schema(schema)
                .distributedBy(partitions)
                .build();

        admin.createTable(tablePath, tableDescriptor, false).get();
        
        System.out.println("  ✓ Created '" + database + "." + topicName + "'");
        System.out.println("     - Type: LOG TABLE");
        System.out.println("     - Partitions: " + partitions);
        System.out.println("     - Schema: [key BYTES, value BYTES, timestamp BIGINT]");
        System.out.println();
        System.out.println("You can now produce/consume using:");
        System.out.println("  kafkacat -b localhost:9093 -t " + database + "." + topicName + " -P");
        System.out.println("  kafkacat -b localhost:9093 -t " + database + "." + topicName + " -C -e");
    }
}
