# 为什么HBase客户端需要ZooKeeper？

## 问题现象

我们的HBase兼容服务器：
- ✅ 监听在 16020 端口
- ✅ 实现了 HBase RPC 协议
- ✅ 可以处理 Get/Put/Delete/Scan 请求
- ❌ **HBase客户端无法连接**（超时）

## 根本原因：服务发现机制

### HBase的分布式架构

HBase不是单机数据库，而是分布式系统：

```
HBase集群架构：
┌─────────────┐
│  ZooKeeper  │  ← 服务注册中心
└──────┬──────┘
       │
   ┌───┴────────────────┬────────────┐
   ↓                    ↓            ↓
┌──────────┐    ┌──────────┐  ┌──────────┐
│  Master  │    │  Master  │  │  Master  │
└────┬─────┘    └────┬─────┘  └────┬─────┘
     │               │              │
   ┌─┴───────────────┴──────────────┴─┐
   ↓                 ↓                 ↓
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│RegionServer1│ │RegionServer2│ │RegionServer3│
│  (端口 16020)│ │  (端口 16020)│ │  (端口 16020)│
└─────────────┘ └─────────────┘ └─────────────┘
     ↓                 ↓                 ↓
  Region 1-50     Region 51-100    Region 101-150
```

**关键点：**
- 一个表的数据被分成多个Region（类似分片）
- Region分布在不同的RegionServer上
- RegionServer的位置会动态变化（服务器上下线、Region迁移）

### HBase客户端连接流程

```java
// 用户代码
Configuration config = HBaseConfiguration.create();
config.set("hbase.zookeeper.quorum", "localhost");
Connection connection = ConnectionFactory.createConnection(config);
Table table = connection.getTable(TableName.valueOf("usertable"));

// 第一次GET操作
Get get = new Get(Bytes.toBytes("row1"));
Result result = table.get(get);  // ← 在这里超时！
```

**客户端内部实际执行的步骤：**

#### 步骤1：连接ZooKeeper
```java
// 客户端首先连接ZooKeeper
ZooKeeperWatcher zkw = new ZooKeeperWatcher(config);
zkw.connect();
```

#### 步骤2：查找Meta表位置
```java
// 从ZooKeeper获取hbase:meta表的位置
String metaLocation = zkw.getData("/hbase/meta-region-server");
// 返回: "hostname1:16020"
```

#### 步骤3：查询Meta表
```java
// 连接到Meta Region Server
Connection metaConn = connect(metaLocation);

// 查询：usertable的Region分布
Scan scan = new Scan()
    .setStartRow("hbase:meta,,usertable")
    .setStopRow("hbase:meta,,usertable~");
ResultScanner scanner = metaTable.getScanner(scan);

// Meta表返回的信息：
// Row Key: usertable,row001,timestamp.region_id
// Column: info:server = "hostname2:16020"
// Column: info:startkey = "row001"
// Column: info:endkey = "row500"
```

#### 步骤4：连接目标Region Server
```java
// 根据row key找到对应的Region Server
String targetServer = metaCache.getRegionLocation("row1");
// 返回: "hostname2:16020"

// 连接到实际存储数据的Region Server
Connection targetConn = connect(targetServer);

// 发送GET请求
Result result = targetConn.get(get);  // ✅ 成功
```

## 我们的服务器缺少什么？

### 当前实现 ✅
```
客户端 → [我们的服务器:16020] → Fluss存储
         ↑
         监听端口，处理RPC请求
```

### HBase客户端期望 ❌
```
客户端 → ZooKeeper → Meta表 → [Region Server:16020] → 数据
         ↓           ↓            ↑
         查询RS列表   查询Region    我们的服务器
                     位置信息
```

**缺少的组件：**

1. **ZooKeeper注册**
```java
// 需要在启动时注册
zkw.create("/hbase/rs/hostname:16020", serverInfo);
```

2. **hbase:meta表模拟**
```java
// 当客户端查询meta表时返回：
if (tableName.equals("hbase:meta")) {
    // 返回：usertable的所有Region信息
    Row: "usertable,,timestamp.region_id"
    Column: "info:server" = "localhost:16020"
    Column: "info:regioninfo" = <序列化的RegionInfo>
}
```

3. **Region Server信息**
```java
// 提供服务器信息
ServerName serverName = ServerName.valueOf("localhost", 16020, startCode);
```

## 实际测试结果

### QuickTest.java 执行流程

```java
Connection connection = ConnectionFactory.createConnection(config);
// ✅ 成功 - 只是创建了连接对象

Table table = connection.getTable(TableName.valueOf("usertable"));
// ✅ 成功 - 只是创建了Table对象

Put put = new Put(Bytes.toBytes("testkey1"));
table.put(put);  
// ❌ 超时 - 这里客户端尝试查询Meta表，找不到Region位置
```

**超时原因：**
```
1. 客户端从ZooKeeper读取 /hbase/meta-region-server
   → 找不到（我们没注册）
   
2. 客户端重试，等待Meta Region Server上线
   → 等待超时（60秒）
   
3. 抛出异常: RetriesExhaustedException
```

## 解决方案

### 方案1：完整实现（推荐用于生产）

在`HBaseCompatServerLauncher.java`中添加：

```java
public class HBaseCompatServerLauncher {
    private ZooKeeperWatcher zkWatcher;
    private MetaTableEmulator metaTable;
    
    public void start() throws Exception {
        // 1. 连接ZooKeeper
        Configuration zkConf = new Configuration();
        zkConf.set("hbase.zookeeper.quorum", "localhost:2181");
        zkWatcher = new ZooKeeperWatcher(zkConf, "HBaseCompat", null);
        
        // 2. 注册Region Server
        String hostname = InetAddress.getLocalHost().getHostName();
        int port = 16020;
        long startCode = System.currentTimeMillis();
        ServerName serverName = ServerName.valueOf(hostname, port, startCode);
        
        // 在ZooKeeper中注册
        String zkPath = "/hbase/rs/" + serverName.toString();
        zkWatcher.getRecoverableZooKeeper().create(
            zkPath,
            Bytes.toBytes(serverName.toString()),
            Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL  // 临时节点，服务停止时自动删除
        );
        
        // 3. 创建Meta表模拟器
        metaTable = new MetaTableEmulator();
        
        // 为每个Fluss表创建Region信息
        for (String flussTable : tables) {
            TableName tableName = TableName.valueOf(flussTable);
            RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName)
                .setStartKey(HConstants.EMPTY_START_ROW)
                .setEndKey(HConstants.EMPTY_END_ROW)
                .build();
            
            metaTable.addRegion(regionInfo, serverName);
        }
        
        // 4. 启动RPC服务器
        server.start();
        
        System.out.println("✅ HBase Compatibility Server started");
        System.out.println("   - Registered in ZooKeeper: " + zkPath);
        System.out.println("   - Meta table emulation: enabled");
        System.out.println("   - Listening on: " + hostname + ":" + port);
    }
}
```

```java
public class MetaTableEmulator {
    private Map<TableName, List<RegionLocation>> regionMap = new HashMap<>();
    
    public void addRegion(RegionInfo regionInfo, ServerName serverName) {
        regionMap.computeIfAbsent(
            regionInfo.getTable(), 
            k -> new ArrayList<>()
        ).add(new RegionLocation(regionInfo, serverName));
    }
    
    public Result getRegionLocation(byte[] tableName, byte[] row) {
        // 返回Meta表格式的结果
        TableName tn = TableName.valueOf(tableName);
        List<RegionLocation> regions = regionMap.get(tn);
        
        for (RegionLocation loc : regions) {
            if (loc.getRegionInfo().containsRow(row)) {
                // 构造Meta表的结果
                Put put = new Put(regionInfo.getRegionName());
                put.addColumn(
                    HConstants.CATALOG_FAMILY,
                    HConstants.SERVER_QUALIFIER,
                    Bytes.toBytes(loc.getServerName().getAddress().toString())
                );
                // ... 更多列
                
                return Result.create(put.getFamilyCellMap().values());
            }
        }
        return null;
    }
}
```

### 方案2：绕过ZooKeeper（仅测试用）

修改HBase客户端代码，直接连接：

```java
// 需要实现自定义Connection类
public class DirectHBaseConnection implements Connection {
    private final String serverAddress;
    
    public DirectHBaseConnection(String address) {
        this.serverAddress = address;  // "localhost:16020"
    }
    
    @Override
    public Table getTable(TableName tableName) {
        // 直接返回连接到我们服务器的Table实现
        return new DirectTable(serverAddress, tableName);
    }
    
    // 跳过所有ZooKeeper和Meta表查询
}

// 使用
Configuration config = new Configuration();
config.set("hbase.client.connection.impl", 
          "org.apache.fluss.hbase.client.DirectHBaseConnection");
config.set("hbase.direct.server", "localhost:16020");
Connection conn = ConnectionFactory.createConnection(config);
```

**缺点：**
- 需要修改HBase客户端代码
- 不支持标准HBase工具（hbase shell, YCSB等）
- 仅适合测试，不适合生产

## 类比：传统Web服务

可以类比为：

### 没有ZooKeeper的HBase = 没有DNS的互联网
```
❌ 直接使用IP访问：http://192.168.1.100
   → 如果服务器IP变了，所有客户端都要更新配置

✅ 使用DNS：http://example.com
   → DNS自动解析到正确的IP
   → 服务器迁移只需更新DNS记录
```

### 有ZooKeeper的HBase
```
✅ 客户端连接ZooKeeper
   → ZooKeeper告诉客户端当前活跃的Region Server列表
   → Region Server宕机/上线，ZooKeeper自动更新
   → 客户端无需修改配置
```

## 总结

| 组件 | 我们的实现 | HBase期望 | 影响 |
|-----|-----------|----------|-----|
| RPC协议 | ✅ 完整实现 | ✅ | 协议层OK |
| 服务器监听 | ✅ 16020端口 | ✅ | 可以接收连接 |
| ZooKeeper注册 | ❌ 未实现 | ✅ 必需 | **客户端找不到服务器** |
| Meta表模拟 | ❌ 未实现 | ✅ 必需 | **客户端不知道数据位置** |
| Region信息 | ❌ 未实现 | ✅ 必需 | **客户端无法路由请求** |

**核心问题：** HBase客户端不是"直连"模式，而是"服务发现"模式。必须实现ZooKeeper注册和Meta表模拟，客户端才能正常工作。

## 下一步行动

参考 `NEXT-STEPS.md` 选择：
1. **实现ZooKeeper集成** - 2-3天工作量，完整功能
2. **实现自定义客户端** - 跳过ZooKeeper，仅测试用
3. **直接测试Fluss原生API** - 测试Fluss性能，不经过HBase层

---

*文档创建时间：2026年1月6日*
