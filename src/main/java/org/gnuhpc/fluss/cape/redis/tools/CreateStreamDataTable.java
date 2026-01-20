package org.gnuhpc.fluss.cape.redis.tools;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

public class CreateStreamDataTable {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: CreateStreamDataTable <bootstrap-servers>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        System.out.println("Connecting to Fluss: " + bootstrapServers);
        
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrapServers);

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        createStreamDataTable(admin);
        
        connection.close();
        System.out.println("\n✓ Stream data table created successfully!");
    }
    
    private static void createStreamDataTable(Admin admin) throws Exception {
        TablePath tablePath = TablePath.of("default", "redis_stream_data");

        if (admin.tableExists(tablePath).get()) {
            System.out.println("Table 'default.redis_stream_data' already exists.");
            return;
        }

        System.out.println("Creating table 'default.redis_stream_data'...");
        
        Schema schema = Schema.newBuilder()
                .column("stream_key", DataTypes.STRING())
                .column("entry_id", DataTypes.STRING())
                .column("serialized_fields", DataTypes.BYTES())
                .column("timestamp", DataTypes.BIGINT())
                .primaryKey("stream_key", "entry_id")
                .build();

        TableDescriptor tableDescriptor = TableDescriptor.builder()
                .schema(schema)
                .distributedBy(10, "stream_key")
                .property("table.type", "log")
                .build();

        admin.createTable(tablePath, tableDescriptor, false).get();
        System.out.println("  ✓ Created 'default.redis_stream_data'");
        System.out.println("     - Type: LOG TABLE");
        System.out.println("     - Primary Key: (stream_key, entry_id)");
        System.out.println("     - Buckets: 10 (distributed by stream_key)");
        System.out.println("     - Columns: stream_key, entry_id, serialized_fields, timestamp");
    }
}
