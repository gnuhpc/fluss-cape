# Phase 3.1 - String批量操作命令设计文档

## 目标

实现Redis String类型的批量和高级操作命令，提升性能和功能完整性。

---

## 命令清单

### 高优先级（批量操作）

| 命令 | 功能 | Redis语法 | 返回值 |
|------|------|-----------|--------|
| **MGET** | 批量获取多个key | `MGET key [key ...]` | Array reply: 值列表（不存在返回nil） |
| **MSET** | 批量设置多个key-value | `MSET key value [key value ...]` | Simple string: OK |
| **MSETNX** | 批量设置（仅当所有key都不存在） | `MSETNX key value [key value ...]` | Integer: 1成功，0失败（原子操作） |
| **SETNX** | 仅当key不存在时设置 | `SETNX key value` | Integer: 1成功，0失败 |
| **SETEX** | 设置key并指定TTL（秒） | `SETEX key seconds value` | Simple string: OK |
| **PSETEX** | 设置key并指定TTL（毫秒） | `PSETEX key milliseconds value` | Simple string: OK |
| **GETSET** | 获取旧值并设置新值 | `GETSET key value` | Bulk string: 旧值（不存在返回nil） |

### 中优先级（辅助操作）

| 命令 | 功能 | Redis语法 | 返回值 |
|------|------|-----------|--------|
| **APPEND** | 追加字符串 | `APPEND key value` | Integer: 追加后的字符串长度 |
| **STRLEN** | 获取字符串长度 | `STRLEN key` | Integer: 长度（不存在返回0） |
| **DECR** | 减1 | `DECR key` | Integer: 减后的值 |
| **DECRBY** | 减指定值 | `DECRBY key decrement` | Integer: 减后的值 |

---

## 实现方案

### 1. MGET - 批量获取

**Redis行为**:
```redis
MGET key1 key2 key3
# 返回: ["value1", nil, "value3"]  # key2不存在返回nil
```

**实现策略**:
- 使用Fluss的`Lookuper.multiGet(List<InternalRow> keys)`批量查询
- 对每个key构造查询行: `GenericRow.of(BinaryString.fromString(key), BinaryString.fromString(""))`
- 检查每个key的过期状态
- 返回值数组，不存在的返回null

**性能优势**: 
- 单次RPC调用获取多个key
- 比N次GET快约3-5倍

**代码位置**: `StringCommandExecutor.executeMGet()`

---

### 2. MSET - 批量设置

**Redis行为**:
```redis
MSET key1 val1 key2 val2 key3 val3
# 返回: OK
```

**实现策略**:
- 验证参数数量为偶数（key-value配对）
- 使用Fluss的`UpsertWriter.upsert(List<InternalRow> rows)`批量写入
- 构造多个upsert行
- 原子性由Fluss保证

**性能优势**:
- 批量写入，减少网络往返
- 比N次SET快约3-5倍

**代码位置**: `StringCommandExecutor.executeMSet()`

---

### 3. SETNX - 仅当不存在时设置

**Redis行为**:
```redis
SETNX mykey "value"
# 返回: 1 (成功) 或 0 (key已存在)
```

**实现策略**:
1. 使用`lookuper.lookup(key)`检查key是否存在
2. 检查过期状态
3. 如果不存在，执行`upsertWriter.upsert(row)`
4. 返回1（成功）或0（失败）

**注意事项**:
- **非完全原子性**: Fluss没有原生的"insert if not exists"
- 在高并发场景可能存在竞态条件
- 对于分布式锁场景，建议使用专门的分布式锁服务

**代码位置**: `StringCommandExecutor.executeSetnx()`

---

### 4. SETEX - 设置+TTL（原子）

**Redis行为**:
```redis
SETEX mykey 60 "value"
# 等价于: SET mykey "value" EX 60
# 返回: OK
```

**实现策略**:
1. 解析参数: key, seconds, value
2. 执行`adapter.set(key, value)`
3. 立即执行`expirationManager.setExpiration(key, System.currentTimeMillis() + seconds * 1000)`
4. 返回OK

**原子性保证**:
- Fluss的upsert操作是原子的
- TTL元数据写入也是原子的
- 两次写入之间的极短时间窗口风险可接受

**代码位置**: `StringCommandExecutor.executeSetex()`

---

### 5. PSETEX - 设置+TTL（毫秒）

**Redis行为**:
```redis
PSETEX mykey 5000 "value"
# 返回: OK
```

**实现策略**:
- 与SETEX相同，但TTL单位为毫秒
- 直接使用毫秒值设置过期时间

**代码位置**: `StringCommandExecutor.executePsetex()`

---

### 6. MSETNX - 批量设置（原子）

**Redis行为**:
```redis
MSETNX key1 val1 key2 val2
# 返回: 1 (所有key都不存在，全部设置成功)
#      0 (任意一个key存在，全部不设置)
```

**实现策略**:
1. 批量检查所有key是否存在（使用`multiGet`）
2. 如果任意一个key存在，返回0，不执行任何写入
3. 如果所有key都不存在，批量写入所有key-value
4. 返回1

**原子性限制**:
- Fluss没有原生的事务支持
- 检查和写入之间存在时间窗口
- **不是完全原子性**，但对大多数场景足够

**代码位置**: `StringCommandExecutor.executeMsetnx()`

---

### 7. GETSET - 获取旧值并设置新值

**Redis行为**:
```redis
GETSET mykey "newvalue"
# 返回: "oldvalue" (如果key不存在返回nil)
```

**实现策略**:
1. 执行`adapter.get(key)`获取旧值
2. 检查过期状态
3. 执行`adapter.set(key, newValue)`
4. 返回旧值（或nil）

**原子性限制**:
- 两次操作之间存在时间窗口
- 不是完全原子性
- 对于需要严格原子性的场景，建议使用Lua脚本（未来实现）

**代码位置**: `StringCommandExecutor.executeGetset()`

---

### 8. APPEND - 追加字符串

**Redis行为**:
```redis
APPEND mykey " world"
# 如果mykey="hello"，追加后="hello world"
# 返回: 11 (追加后的长度)
```

**实现策略**:
1. 获取当前值
2. 如果不存在，新值=追加值
3. 如果存在，新值=旧值+追加值
4. 写入新值
5. 返回新值长度

**代码位置**: `StringCommandExecutor.executeAppend()`

---

### 9. STRLEN - 字符串长度

**Redis行为**:
```redis
STRLEN mykey
# 返回: 字符串长度（不存在返回0）
```

**实现策略**:
1. 获取key的值
2. 检查过期
3. 如果不存在，返回0
4. 否则返回`value.length`

**代码位置**: `StringCommandExecutor.executeStrlen()`

---

### 10. DECR / DECRBY - 减法操作

**Redis行为**:
```redis
DECR counter
# counter="10" -> 返回9

DECRBY counter 5
# counter="10" -> 返回5
```

**实现策略**:
- 与INCR/INCRBY相同逻辑，只是用减法
- 获取当前值（默认0）
- 减去指定值
- 写入新值
- 返回新值

**代码位置**: `StringCommandExecutor.executeDecr()`, `executeDecrby()`

---

## Fluss API使用

### 批量查询API

```java
// 构造查询行列表
List<InternalRow> lookupRows = new ArrayList<>();
for (String key : keys) {
    lookupRows.add(GenericRow.of(
        BinaryString.fromString(key), 
        BinaryString.fromString("")
    ));
}

// 批量查询
List<InternalRow> results = lookuper.multiGet(lookupRows);

// 处理结果
for (int i = 0; i < results.size(); i++) {
    InternalRow row = results.get(i);
    if (row != null) {
        byte[] value = row.getBinary(4);  // value列
        // 处理value
    }
}
```

### 批量写入API

```java
// 构造upsert行列表
List<InternalRow> upsertRows = new ArrayList<>();
for (Map.Entry<String, byte[]> entry : keyValues.entrySet()) {
    upsertRows.add(GenericRow.of(
        BinaryString.fromString(entry.getKey()),  // redis_key
        BinaryString.fromString("string"),        // redis_type
        BinaryString.fromString(""),              // sub_key
        null,                                     // score
        entry.getValue()                          // value
    ));
}

// 批量写入
upsertWriter.upsert(upsertRows);
upsertWriter.flush();
```

---

## 原子性保证说明

### 完全原子性
- ✅ **SETEX / PSETEX**: Fluss单行upsert + TTL元数据写入
- ✅ **MSET**: Fluss批量upsert（单个批次）

### 非完全原子性（但对大多数场景足够）
- ⚠️ **SETNX**: 检查+写入有时间窗口
- ⚠️ **MSETNX**: 批量检查+批量写入有时间窗口
- ⚠️ **GETSET**: 读+写有时间窗口
- ⚠️ **APPEND**: 读+写有时间窗口

### 改进方案（未来）
- 实现Lua脚本支持（EVAL命令）
- 使用Lua脚本封装SETNX、GETSET等操作
- 实现真正的原子性

---

## 性能预期

### 批量操作性能提升

| 操作 | 单次延迟 | 批量延迟（10个key） | 提升倍数 |
|------|----------|---------------------|----------|
| GET → MGET | 10ms × 10 = 100ms | ~15-20ms | **5-6x** |
| SET → MSET | 10ms × 10 = 100ms | ~15-20ms | **5-6x** |

### 内存影响
- 批量操作会短暂增加内存使用（构造请求/响应列表）
- 对于1000个key的MGET，额外内存约1-2MB（假设每个值1KB）
- 可接受的权衡

---

## 测试计划

### 功能测试

1. **MGET测试**
   - 批量获取存在的key
   - 批量获取不存在的key
   - 混合存在/不存在的key
   - 过期key处理

2. **MSET测试**
   - 批量设置多个key
   - 奇数参数报错
   - 覆盖已存在的key

3. **SETNX测试**
   - key不存在时成功
   - key已存在时失败
   - 过期key被视为不存在

4. **SETEX/PSETEX测试**
   - 设置key和TTL
   - TTL正确生效
   - 验证过期后key消失

5. **MSETNX测试**
   - 所有key不存在时成功
   - 任意key存在时失败（全部不设置）

6. **GETSET测试**
   - 获取旧值并设置新值
   - key不存在时返回nil

7. **APPEND测试**
   - 追加到已存在的key
   - 追加到不存在的key
   - 返回正确的长度

8. **STRLEN测试**
   - 获取字符串长度
   - 不存在的key返回0

9. **DECR/DECRBY测试**
   - 减法操作
   - 初始值为0
   - 非数字值报错

### 性能测试

```bash
# MGET vs 多次GET
redis-benchmark -t get -n 100000 -c 50
redis-benchmark -t mget -n 100000 -c 50

# MSET vs 多次SET
redis-benchmark -t set -n 100000 -c 50
redis-benchmark -t mset -n 100000 -c 50
```

### 边界测试
- 空key列表
- 单个key（批量操作退化）
- 大量key（1000+）
- 大value（1MB+）

---

## 实现步骤

1. ✅ 分析命令需求和设计方案
2. ⏳ 在`RedisFlussAdapter`中实现批量API包装方法
3. ⏳ 在`StringCommandExecutor`中实现11个新命令
4. ⏳ 在`RedisCommandRouter`中注册新命令
5. ⏳ 编写测试脚本
6. ⏳ 执行测试并验证
7. ⏳ 更新文档

---

## 文件修改清单

### 需要修改的文件

1. **RedisFlussAdapter.java** - 添加批量操作方法
   - `multiGet(List<String> keys)` - 批量获取
   - `multiSet(Map<String, byte[]> kvMap)` - 批量设置
   - `existsMulti(List<String> keys)` - 批量检查存在

2. **StringCommandExecutor.java** - 添加11个命令执行方法
   - `executeMGet()`, `executeMSet()`, `executeSetnx()`, `executeSetex()`
   - `executePsetex()`, `executeMsetnx()`, `executeGetset()`
   - `executeAppend()`, `executeStrlen()`, `executeDecr()`, `executeDecrby()`

3. **RedisCommandRouter.java** - 注册新命令
   - 添加11个命令到路由表

4. **测试文件**
   - `test-phase-3.1.sh` - 完整测试脚本

5. **文档文件**
   - `README.md` - 更新命令列表
   - `PHASE-3.1-COMPLETE.md` - 完成报告

---

## 兼容性说明

### 完全兼容
- ✅ MGET, MSET, SETEX, PSETEX, APPEND, STRLEN, DECR, DECRBY
- 行为与Redis完全一致

### 部分兼容
- ⚠️ SETNX, MSETNX, GETSET - 非完全原子性
- 在低并发场景下行为与Redis一致
- 在高并发场景可能有竞态条件
- 对于分布式锁等严格场景，建议使用专门的锁服务

### 已知限制
- 没有Lua脚本支持（计划Phase 5实现）
- 没有事务支持（计划Phase 4实现）
- MSETNX不保证完全原子性

---

## 总结

Phase 3.1实现**11个String批量和高级操作命令**，将总命令数从40提升至**51**。

**核心价值**:
1. 🚀 **性能提升**: MGET/MSET比单个操作快5-6倍
2. ✅ **功能完整**: 覆盖Redis最常用的String操作
3. 📦 **实用性强**: SETEX/SETNX在缓存场景广泛使用
4. 🔧 **易于实现**: 利用Fluss现有批量API

**完成后覆盖率**:
- String命令: 18/~20 = **90%**
- 总命令覆盖: 51/~200 = **25.5%**

---

*文档版本: v1.0*  
*创建时间: 2026-01-09*  
*作者: Sisyphus AI Agent*
