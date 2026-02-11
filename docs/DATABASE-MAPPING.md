四个协议在 Fluss CAPE 中创建的表，其 Database 归属逻辑如下：

1. PostgreSQL (PgSQL)
逻辑：映射 Schema 到 Database
*   默认情况：连接时如果未指定 Database，默认连接到 default database。
*   创建表时：
    *   CREATE TABLE table_name -> 在当前 session 的 database（通常是 default）下创建表 table_name。
    *   CREATE TABLE schema_name.table_name -> 在名为 schema_name 的 Fluss database 下创建表 table_name。
*   底层实现：PgSqlEngine 会解析 SQL 语句，提取 schema 部分作为 database 名称。
2. Redis
逻辑：固定使用 default Database 下的特定内部表
*   Redis 协议本身没有 Schema 的概念（只有数字 DB 0-15，但在 CAPE 中被忽略或展平）。
*   所有数据都存储在 default database 下的几张固定表中：
    *   数据表：default.redis_internal_data (存储 String, Hash, List, Set, ZSet 等数据)
    *   索引表：default.redis_internal_subkey_index (用于加速子键查找)
    *   ZSet 索引：default.redis_internal_zset_index (用于 ZSet 排序)
*   底层实现：RedisSingleTableAdapter 初始化时硬编码或注入了 default 作为 database 名称。
3. HBase
逻辑：映射 Namespace 到 Database
*   HBase 的表名结构通常为 namespace:table_qualifier。
*   映射规则：
    *   namespace:table -> Fluss 的 namespace database 下的 table 表。
    *   table (无 namespace) -> Fluss 的 default database 下的 table 表。
*   底层实现：CreateTableExecutor 会解析 Protobuf 请求中的 TableName 对象，将 HBase Namespace 直接作为 Fluss Database Name。
4. Kafka
逻辑：映射 Topic 到 Table，支持 db.table 格式
*   默认情况：创建一个名为 my-topic 的 Topic -> Fluss 的 default database 下的 my-topic 表。
*   高级用法：如果 Topic 名字包含点号（.），例如创建 Topic mydb.mytopic -> Fluss 的 mydb database 下的 mytopic 表。
*   底层实现：TablePathResolver 会检查 Topic 名称中是否有点号：
    *   有 (db.table) -> 解析为 Database db 和 Table table。
    *   无 -> 使用配置的默认 Database (默认为 default)。
总结对照表
| 协议 | 用户操作示例 | Fluss 表路径 (Database.Table) | 逻辑说明 |
| :--- | :--- | :--- | :--- |
| PgSQL | CREATE TABLE users ... | default.users | 使用当前 Session DB |
| PgSQL | CREATE TABLE sales.orders ... | sales.orders | Schema -> Database |
| Redis | SET key value | default.redis_internal_data | 固定映射到内部宽表 |
| HBase | create 'users', 'cf' | default.users | 默认 Namespace -> default |
| HBase | create 'crm:users', 'cf' | crm.users | Namespace -> Database |
| Kafka | kafka-topics ... --topic logs | default.logs | 默认映射到 default DB |
| Kafka | kafka-topics ... --topic app.logs | app.logs | 点号分隔符解析 DB |
