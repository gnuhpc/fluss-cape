# Redis Phase 2 实现分析与设计

## 目标

实现 Redis 复杂数据结构支持：Hash、List、Set、Sorted Set

## 数据结构映射策略

### 方案对比

#### 方案 1：统一复合主键表（推荐）

**表结构**：
```sql
CREATE TABLE default.redis_data (
  redis_key STRING,        -- Redis key (user:1, session:abc, etc.)
  redis_type STRING,       -- 数据类型: string/hash/list/set/zset
  sub_key STRING,          -- 子键/字段名/成员名/索引
  score DOUBLE,            -- Sorted Set 的分数 (其他类型为 NULL)
  value BYTES,             -- 实际数据
  PRIMARY KEY (redis_key, sub_key)
);
```

**优点**：
- ✅ 单一表结构，管理简单
- ✅ 复合主键支持所有数据类型
- ✅ 利用 Fluss 的 range scan 特性
- ✅ 原子性操作保证

**缺点**：
- ⚠️ String 类型有冗余列（sub_key 固定为空字符串）
- ⚠️ 需要类型检查防止误操作

#### 方案 2：每种类型一张表

**表结构**：
```sql
-- String 表
CREATE TABLE redis_strings (
  redis_key STRING,
  value BYTES,
  PRIMARY KEY (redis_key)
);

-- Hash 表
CREATE TABLE redis_hashes (
  redis_key STRING,
  field STRING,
  value BYTES,
  PRIMARY KEY (redis_key, field)
);

-- List 表
CREATE TABLE redis_lists (
  redis_key STRING,
  index BIGINT,
  value BYTES,
  PRIMARY KEY (redis_key, index)
);

-- Set 表
CREATE TABLE redis_sets (
  redis_key STRING,
  member BYTES,
  PRIMARY KEY (redis_key, member)
);

-- Sorted Set 表
CREATE TABLE redis_sorted_sets (
  redis_key STRING,
  member BYTES,
  score DOUBLE,
  PRIMARY KEY (redis_key, score, member)
);
```

**优点**：
- ✅ 每种类型的表结构最优化
- ✅ 无冗余列
- ✅ 类型隔离，不会误操作

**缺点**：
- ❌ 需要管理 5 张表
- ❌ 配置复杂
- ❌ TYPE 命令需要查询 5 张表
- ❌ KEYS 命令需要扫描 5 张表

### 最终选择：方案 1（统一表）

理由：
1. **简单性优先**：单表更易于配置和维护
2. **Fluss 特性匹配**：复合主键 + range scan 正好匹配需求
3. **原子性**：单表操作保证更强
4. **可扩展性**：未来添加新类型只需增加 redis_type

---

## 数据映射详细设计

### 1. String 类型

**存储格式**：
```
redis_key: "user:1:name"
redis_type: "string"
sub_key: ""  (空字符串)
score: NULL
value: "Alice"
```

**操作映射**：
- `GET key` → lookup(redis_key, "")
- `SET key value` → upsert(redis_key, "", value)
- `DEL key` → delete(redis_key, "")

### 2. Hash 类型

**存储格式**：
```
# HSET user:1 name Alice age 30
redis_key: "user:1"  | sub_key: "name" | value: "Alice"
redis_key: "user:1"  | sub_key: "age"  | value: "30"
```

**操作映射**：
- `HGET user:1 name` → lookup(redis_key="user:1", sub_key="name")
- `HSET user:1 name Alice` → upsert(redis_key="user:1", sub_key="name", value="Alice")
- `HDEL user:1 name` → delete(redis_key="user:1", sub_key="name")
- `HGETALL user:1` → scan(redis_key="user:1")  # 返回所有 sub_key
- `HKEYS user:1` → scan(redis_key="user:1")  # 只返回 sub_key 列
- `HVALS user:1` → scan(redis_key="user:1")  # 只返回 value 列
- `HLEN user:1` → count(redis_key="user:1")

### 3. List 类型

**存储格式**（使用索引作为 sub_key）：
```
# LPUSH mylist a b c  (索引从左到右: 0, 1, 2)
redis_key: "mylist"  | sub_key: "0000000000000000" | value: "c"
redis_key: "mylist"  | sub_key: "0000000000000001" | value: "b"
redis_key: "mylist"  | sub_key: "0000000000000002" | value: "a"
```

**索引编码**：使用 16 位零填充的 long 值，保证字典序 = 数字序

**操作映射**：
- `LPUSH mylist value` → 
  1. 找到最小索引（或从 metadata 获取）
  2. upsert(redis_key, new_index-1, value)
- `RPUSH mylist value` → 
  1. 找到最大索引（或从 metadata 获取）
  2. upsert(redis_key, new_index+1, value)
- `LPOP mylist` → 
  1. lookup 最小索引
  2. delete 该行
- `RPOP mylist` → 
  1. lookup 最大索引
  2. delete 该行
- `LRANGE mylist 0 10` → scan(redis_key, start_index, end_index)
- `LLEN mylist` → count(redis_key)
- `LINDEX mylist 5` → lookup(redis_key, "0000000000000005")

**挑战**：
- ⚠️ LPUSH 需要找到最小索引（可能需要额外的 metadata 行）
- ⚠️ 索引重编号问题（当大量 LPOP 后索引变得稀疏）

**解决方案**：
- 使用负索引：LPUSH 使用 -1, -2, -3...，RPUSH 使用 0, 1, 2...
- sub_key 格式：`[sign][16-digit-number]`，如 `-0000000000000001`, `+0000000000000000`

### 4. Set 类型

**存储格式**（member 作为 sub_key）：
```
# SADD myset member1 member2
redis_key: "myset"  | sub_key: "member1" | value: ""  (空值，只需存在性)
redis_key: "myset"  | sub_key: "member2" | value: ""
```

**操作映射**：
- `SADD myset member` → upsert(redis_key="myset", sub_key="member", value="")
- `SREM myset member` → delete(redis_key="myset", sub_key="member")
- `SISMEMBER myset member` → exists(redis_key="myset", sub_key="member")
- `SMEMBERS myset` → scan(redis_key="myset")  # 返回所有 sub_key
- `SCARD myset` → count(redis_key="myset")

**集合运算**：
- `SINTER set1 set2` → 客户端实现（分别 scan 两个集合，计算交集）
- `SUNION set1 set2` → 客户端实现（分别 scan 两个集合，计算并集）

### 5. Sorted Set 类型

**存储格式**（score+member 作为复合 sub_key）：
```
# ZADD myzset 100 member1 200 member2
redis_key: "myzset"  | sub_key: "0000000000000100:member1" | score: 100.0 | value: ""
redis_key: "myzset"  | sub_key: "0000000000000200:member2" | score: 200.0 | value: ""
```

**sub_key 格式**：`[score_encoded]:[member]`
- score_encoded: 16位零填充的分数（需要处理浮点数排序问题）
- 分隔符: `:`
- member: 原始成员名

**操作映射**：
- `ZADD myzset 100 member` → upsert(redis_key, "score:member", score=100, value="")
- `ZREM myzset member` → 需要先查找所有匹配 member 的行，然后删除
- `ZSCORE myzset member` → 需要扫描找到 member
- `ZRANGE myzset 0 10` → scan(redis_key) 按 sub_key 排序取前 10
- `ZRANGEBYSCORE myzset 50 150` → scan(redis_key, start_key="50:*", end_key="150:~")
- `ZCARD myzset` → count(redis_key)
- `ZRANK myzset member` → scan 计算排名

**挑战**：
- ⚠️ 浮点数排序需要特殊编码（IEEE 754 → 字典序）
- ⚠️ ZREM/ZSCORE 需要先扫描找到 member（因为 primary key 是 score+member）

**解决方案**：
- 使用双向索引：
  - 主索引：`(redis_key, score_encoded:member)` - 用于 ZRANGE
  - 反向索引：额外存储 `(redis_key, member:score_encoded)` - 用于 ZSCORE/ZREM
  - 实现：在同一行存储两个方向的 sub_key？或者使用两行？

**简化方案**（Phase 2.1）：
- 暂时接受 ZREM/ZSCORE 需要全表扫描的性能损失
- 未来优化：使用额外的索引表

---

## 实现计划

### Phase 2.1: 核心数据结构（高优先级）

1. **设计确认** ✅
   - 确定统一表结构
   - 定义编码规则

2. **更新 RedisFlussAdapter** 
   - 添加 `scanByPrefix(redis_key)` 方法
   - 添加 `deleteByPrefix(redis_key, sub_key_prefix)` 方法
   - 添加 `countByKey(redis_key)` 方法
   - 添加 `getByCompositeKey(redis_key, sub_key)` 方法

3. **实现 Hash 命令**
   - HashCommandExecutor
   - HGET, HSET, HDEL, HEXISTS, HGETALL, HKEYS, HVALS, HLEN

4. **实现 List 命令**（简化版）
   - ListCommandExecutor
   - LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX
   - 使用负索引方案

5. **实现 Set 命令**
   - SetCommandExecutor
   - SADD, SREM, SISMEMBER, SMEMBERS, SCARD
   - 集合运算（SINTER, SUNION）客户端实现

6. **实现 Sorted Set 命令**（简化版）
   - SortedSetCommandExecutor
   - ZADD, ZREM, ZRANGE, ZSCORE, ZCARD
   - 接受 ZREM/ZSCORE 的全表扫描性能

7. **添加 TYPE 命令**
   - 查询 redis_type 字段

### Phase 2.2: 优化与增强（中优先级）

8. **List 优化**
   - 实现 metadata 行存储 head/tail 索引
   - 避免每次 LPUSH/RPUSH 都扫描

9. **Sorted Set 优化**
   - 实现反向索引支持 O(1) ZSCORE
   - 优化 ZREM 性能

10. **KEYS 命令**
    - 实现模式匹配（带性能警告）

11. **TTL/EXPIRE 支持**
    - 添加 expiration_time 列
    - 实现后台清理机制

### Phase 2.3: 测试与文档（低优先级）

12. **单元测试**
    - 每个 Executor 的测试

13. **集成测试**
    - 完整流程测试

14. **文档更新**
    - README 更新
    - 性能基准测试

---

## 关键代码结构

### 文件列表

```
src/main/java/org/apache/fluss/redis/
├── executor/
│   ├── HashCommandExecutor.java       (NEW)
│   ├── ListCommandExecutor.java       (NEW)
│   ├── SetCommandExecutor.java        (NEW)
│   ├── SortedSetCommandExecutor.java  (NEW)
│   └── TypeCommandExecutor.java       (NEW)
├── storage/
│   └── RedisFlussAdapter.java         (UPDATE - 添加 scan/count 方法)
└── util/
    ├── IndexEncoder.java              (NEW - List 索引编码)
    └── ScoreEncoder.java              (NEW - Sorted Set 分数编码)
```

### RedisFlussAdapter 新方法

```java
// 扫描指定 redis_key 的所有 sub_key
public List<KeyValue> scanByKey(String redisKey) throws Exception;

// 扫描指定 redis_key 和 sub_key 前缀
public List<KeyValue> scanByPrefix(String redisKey, String subKeyPrefix) throws Exception;

// 删除指定 redis_key 的所有数据
public void deleteByKey(String redisKey) throws Exception;

// 统计指定 redis_key 的行数
public long countByKey(String redisKey) throws Exception;

// 复合主键查询
public byte[] getByCompositeKey(String redisKey, String subKey) throws Exception;

// 复合主键插入
public void setByCompositeKey(String redisKey, String subKey, byte[] value) throws Exception;

// 复合主键删除
public void deleteByCompositeKey(String redisKey, String subKey) throws Exception;
```

---

## 风险与挑战

### 高风险

1. **List 性能**
   - LPUSH/RPUSH 需要找到边界索引
   - 可能需要全表扫描或 metadata 支持

2. **Sorted Set 复杂性**
   - 浮点数排序编码复杂
   - 反向索引实现挑战

3. **Fluss Scan API 限制**
   - 需要验证 Fluss 是否支持高效的 range scan
   - 是否支持 count 操作

### 中风险

4. **类型安全**
   - 需要防止不同类型命令混用（如 HGET 一个 String key）
   - 需要实现 TYPE 命令和类型检查

5. **原子性**
   - 多行操作（HGETALL, SMEMBERS）是否原子？
   - 可能需要事务支持

### 低风险

6. **内存占用**
   - 大集合可能占用大量内存
   - 需要分批处理

---

## 下一步行动

1. ✅ 分析完成 - 本文档
2. ⏭️ 实现 RedisFlussAdapter 扩展方法
3. ⏭️ 实现 HashCommandExecutor
4. ⏭️ 实现 ListCommandExecutor
5. ⏭️ 实现 SetCommandExecutor
6. ⏭️ 实现 SortedSetCommandExecutor
7. ⏭️ 实现 TypeCommandExecutor
8. ⏭️ 注册新 Executor 到 Router
9. ⏭️ 构建测试验证

---

## 时间估算

- **Phase 2.1（核心）**: 6-8 小时
- **Phase 2.2（优化）**: 4-6 小时
- **Phase 2.3（测试文档）**: 2-4 小时
- **总计**: 12-18 小时

---

## 成功标准

✅ Hash 所有命令正常工作
✅ List 基本命令正常工作（LPUSH/RPUSH/LPOP/RPOP/LRANGE/LLEN）
✅ Set 基本命令正常工作（SADD/SREM/SMEMBERS/SCARD）
✅ Sorted Set 基本命令正常工作（ZADD/ZRANGE/ZCARD）
✅ TYPE 命令返回正确类型
✅ 构建成功无错误
✅ 基本功能测试通过
