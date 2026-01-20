package org.gnuhpc.fluss.cape.redis.tools;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

public class CreateRedisTable {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: CreateRedisTable <bootstrap-servers>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        System.out.println("Connecting to Fluss: " + bootstrapServers);
        
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrapServers);

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        createMainTable(admin);
        createReverseIndexTable(admin);
        createSubKeyIndexTable(admin);
        
        connection.close();
        System.out.println("\n✓ All Redis tables created successfully!");
    }
    
    private static void createMainTable(Admin admin) throws Exception {
        TablePath tablePath = TablePath.of("default", "redis_data");

        if (admin.tableExists(tablePath).get()) {
            System.out.println("Table 'default.redis_data' already exists.");
            return;
        }

        System.out.println("Creating table 'default.redis_data'...");
        
        Schema schema = Schema.newBuilder()
                .column("redis_key", DataTypes.STRING())
                .column("redis_type", DataTypes.STRING())
                .column("sub_key", DataTypes.STRING())
                .column("score", DataTypes.DOUBLE())
                .column("value", DataTypes.BYTES())
                .primaryKey("redis_key", "sub_key")
                .build();

        TableDescriptor tableDescriptor = TableDescriptor.builder()
                .schema(schema)
                .build();

        admin.createTable(tablePath, tableDescriptor, false).get();
        System.out.println("  ✓ Created 'default.redis_data'");
    }
    
    private static void createReverseIndexTable(Admin admin) throws Exception {
        TablePath tablePath = TablePath.of("default", "redis_zset_members");

        if (admin.tableExists(tablePath).get()) {
            System.out.println("Table 'default.redis_zset_members' already exists.");
            return;
        }

        System.out.println("Creating table 'default.redis_zset_members' (Sorted Set reverse index)...");
        
        Schema schema = Schema.newBuilder()
                .column("redis_key", DataTypes.STRING())
                .column("member", DataTypes.STRING())
                .column("score", DataTypes.DOUBLE())
                .primaryKey("redis_key", "member")
                .build();

        TableDescriptor tableDescriptor = TableDescriptor.builder()
                .schema(schema)
                .build();

        admin.createTable(tablePath, tableDescriptor, false).get();
        System.out.println("  ✓ Created 'default.redis_zset_members'");
    }
    
    private static void createSubKeyIndexTable(Admin admin) throws Exception {
        TablePath tablePath = TablePath.of("default", "redis_subkey_index");

        if (admin.tableExists(tablePath).get()) {
            System.out.println("Table 'default.redis_subkey_index' already exists.");
            return;
        }

        System.out.println("Creating table 'default.redis_subkey_index' (Sub-key index for KEYS/SCAN)...");
        
        Schema schema = Schema.newBuilder()
                .column("redis_key", DataTypes.STRING())
                .column("sub_keys", DataTypes.STRING())
                .primaryKey("redis_key")
                .build();

        TableDescriptor tableDescriptor = TableDescriptor.builder()
                .schema(schema)
                .build();

        admin.createTable(tablePath, tableDescriptor, false).get();
        System.out.println("  ✓ Created 'default.redis_subkey_index'");
    }
}
