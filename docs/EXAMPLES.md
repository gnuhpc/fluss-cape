# Examples and Use Cases

Real-world examples and use cases for Fluss CAPE.

---

## Table of Contents

1. [Use Case Overview](#use-case-overview)
2. [E-Commerce Platform](#e-commerce-platform)
3. [Gaming Leaderboard](#gaming-leaderboard)
4. [Session Management](#session-management)
5. [Time-Series Analytics](#time-series-analytics)
6. [Real-Time Recommendations](#real-time-recommendations)
7. [Message Queue](#message-queue)
8. [Geospatial Services](#geospatial-services)
9. [Multi-Language Examples](#multi-language-examples)

---

## Use Case Overview

| Use Case | Protocol | Data Types | Key Features |
|----------|----------|------------|--------------|
| E-Commerce | HBase + Redis | Hash, Sorted Set | Product catalog, inventory |
| Gaming | Redis | Sorted Set, Hash | Real-time leaderboards |
| Session Store | Redis | String, Hash | TTL, fast access |
| Analytics | HBase | - | Batch processing, scans |
| Recommendations | Redis | Set, Sorted Set | Real-time scoring |
| Message Queue | Redis | List, Stream | FIFO, pub/sub |
| Location Services | Redis | Geo | Proximity search |

---

## E-Commerce Platform

### Use Case: Product Catalog with Inventory Management

**Requirements**:
- Fast product lookups
- Real-time inventory updates
- Price updates
- Search by category

### Architecture

```
┌──────────────┐
│ Web Frontend │
└──────┬───────┘
       │
       ▼
┌────────────────────────┐
│  Application Server    │
│  (Redis + HBase APIs)  │
└──────┬────────┬────────┘
       │        │
       ▼        ▼
  ┌────────┐  ┌────────┐
  │ Redis  │  │ HBase  │
  │Protocol│  │Protocol│
  └────┬───┘  └───┬────┘
       │          │
       └────┬─────┘
            ▼
      Fluss CAPE
            ▼
      Fluss Cluster
```

### Implementation

#### 1. Product Catalog (HBase)

```java
// Store product details in HBase
public class ProductCatalog {
    private final Table productTable;
    
    public void addProduct(String productId, Product product) throws Exception {
        Put put = new Put(Bytes.toBytes(productId));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), 
                     Bytes.toBytes(product.getName()));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("description"), 
                     Bytes.toBytes(product.getDescription()));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("price"), 
                     Bytes.toBytes(product.getPrice()));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("category"), 
                     Bytes.toBytes(product.getCategory()));
        productTable.put(put);
    }
    
    public Product getProduct(String productId) throws Exception {
        Get get = new Get(Bytes.toBytes(productId));
        Result result = productTable.get(get);
        
        return new Product(
            Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))),
            Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("description"))),
            Bytes.toDouble(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("price"))),
            Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("category")))
        );
    }
    
    public List<Product> getProductsByCategory(String category) throws Exception {
        Scan scan = new Scan();
        // Filter by category (simplified - use custom filter in production)
        ResultScanner scanner = productTable.getScanner(scan);
        
        List<Product> products = new ArrayList<>();
        for (Result result : scanner) {
            String cat = Bytes.toString(result.getValue(
                Bytes.toBytes("info"), Bytes.toBytes("category")));
            if (category.equals(cat)) {
                products.add(parseProduct(result));
            }
        }
        return products;
    }
}
```

#### 2. Real-Time Inventory (Redis)

```python
# Inventory management using Redis
import redis

class InventoryManager:
    def __init__(self):
        self.redis = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    def set_stock(self, product_id, quantity):
        """Set product stock quantity"""
        self.redis.set(f'inventory:{product_id}', quantity)
    
    def get_stock(self, product_id):
        """Get current stock"""
        stock = self.redis.get(f'inventory:{product_id}')
        return int(stock) if stock else 0
    
    def reserve_stock(self, product_id, quantity):
        """Atomically reserve stock for order"""
        key = f'inventory:{product_id}'
        
        # Use Lua script for atomic check-and-decrement
        lua_script = """
        local current = tonumber(redis.call('GET', KEYS[1]) or 0)
        if current >= tonumber(ARGV[1]) then
            redis.call('DECRBY', KEYS[1], ARGV[1])
            return 1
        else
            return 0
        end
        """
        result = self.redis.eval(lua_script, 1, key, quantity)
        return result == 1
    
    def release_stock(self, product_id, quantity):
        """Release reserved stock (e.g., order cancelled)"""
        self.redis.incrby(f'inventory:{product_id}', quantity)
    
    def get_low_stock_products(self, threshold=10):
        """Find products with stock below threshold"""
        low_stock = []
        for key in self.redis.scan_iter('inventory:*'):
            stock = int(self.redis.get(key))
            if stock < threshold:
                product_id = key.split(':')[1]
                low_stock.append((product_id, stock))
        return low_stock

# Usage
inv = InventoryManager()

# Set initial stock
inv.set_stock('PROD-001', 100)

# Customer places order
if inv.reserve_stock('PROD-001', 2):
    print("Order placed successfully")
else:
    print("Out of stock")

# Check current stock
print(f"Remaining stock: {inv.get_stock('PROD-001')}")
```

#### 3. Price Cache (Redis)

```javascript
// Node.js - Price caching with TTL
const Redis = require('ioredis');
const redis = new Redis({ port: 6379 });

class PriceCache {
  // Cache price with 5-minute TTL
  async setPrice(productId, price) {
    await redis.setex(`price:${productId}`, 300, price);
  }
  
  async getPrice(productId) {
    const price = await redis.get(`price:${productId}`);
    return price ? parseFloat(price) : null;
  }
  
  // Bulk price update
  async updatePrices(priceMap) {
    const pipeline = redis.pipeline();
    for (const [productId, price] of Object.entries(priceMap)) {
      pipeline.setex(`price:${productId}`, 300, price);
    }
    await pipeline.exec();
  }
  
  // Get all prices for cart
  async getCartPrices(productIds) {
    const pipeline = redis.pipeline();
    productIds.forEach(id => pipeline.get(`price:${id}`));
    const results = await pipeline.exec();
    
    return results.map(([err, price], idx) => ({
      productId: productIds[idx],
      price: price ? parseFloat(price) : null
    }));
  }
}
```

---

## Gaming Leaderboard

### Use Case: Real-Time Global Leaderboard

**Requirements**:
- Instant score updates
- Top N players query
- Player rank lookup
- Score-based range queries

### Implementation

```python
import redis
from datetime import datetime

class GameLeaderboard:
    def __init__(self):
        self.redis = redis.Redis(host='localhost', port=6379, decode_responses=True)
        self.leaderboard_key = 'leaderboard:global'
    
    def update_score(self, player_id, score):
        """Update player score (higher score = better rank)"""
        self.redis.zadd(self.leaderboard_key, {player_id: score})
        
        # Also store player metadata
        self.redis.hset(f'player:{player_id}', mapping={
            'last_updated': datetime.now().isoformat(),
            'score': score
        })
    
    def increment_score(self, player_id, points):
        """Add points to existing score"""
        new_score = self.redis.zincrby(self.leaderboard_key, points, player_id)
        self.redis.hset(f'player:{player_id}', 'score', new_score)
        return new_score
    
    def get_top_players(self, n=10):
        """Get top N players"""
        top = self.redis.zrevrange(
            self.leaderboard_key, 0, n-1, withscores=True
        )
        return [(player, int(score)) for player, score in top]
    
    def get_player_rank(self, player_id):
        """Get player's rank (1-indexed)"""
        rank = self.redis.zrevrank(self.leaderboard_key, player_id)
        return rank + 1 if rank is not None else None
    
    def get_player_score(self, player_id):
        """Get player's current score"""
        score = self.redis.zscore(self.leaderboard_key, player_id)
        return int(score) if score else None
    
    def get_nearby_players(self, player_id, count=5):
        """Get players ranked near this player"""
        rank = self.redis.zrevrank(self.leaderboard_key, player_id)
        if rank is None:
            return []
        
        start = max(0, rank - count)
        end = rank + count
        
        return self.redis.zrevrange(
            self.leaderboard_key, start, end, withscores=True
        )
    
    def get_players_in_score_range(self, min_score, max_score):
        """Get all players within score range"""
        return self.redis.zrangebyscore(
            self.leaderboard_key, min_score, max_score, withscores=True
        )
    
    def reset_leaderboard(self):
        """Reset leaderboard (new season)"""
        # Archive old leaderboard
        timestamp = datetime.now().strftime('%Y%m%d')
        self.redis.rename(
            self.leaderboard_key,
            f'leaderboard:archive:{timestamp}'
        )

# Usage Example
lb = GameLeaderboard()

# Players play and earn scores
lb.update_score('player001', 1500)
lb.update_score('player002', 2000)
lb.update_score('player003', 1750)
lb.increment_score('player001', 100)  # player001: 1600

# Get top 10
top10 = lb.get_top_players(10)
for rank, (player, score) in enumerate(top10, 1):
    print(f"#{rank}: {player} - {score} points")

# Get specific player info
player_id = 'player001'
rank = lb.get_player_rank(player_id)
score = lb.get_player_score(player_id)
print(f"{player_id} is ranked #{rank} with {score} points")

# Get players near you
nearby = lb.get_nearby_players(player_id, count=3)
print(f"Players near {player_id}:")
for player, score in nearby:
    print(f"  {player}: {score}")
```

---

## Session Management

### Use Case: User Session Store with Auto-Expiration

```go
package main

import (
    "context"
    "encoding/json"
    "time"
    "github.com/redis/go-redis/v9"
)

type SessionManager struct {
    client *redis.Client
    ctx    context.Context
}

type Session struct {
    UserID    string    `json:"user_id"`
    Username  string    `json:"username"`
    Email     string    `json:"email"`
    LoginTime time.Time `json:"login_time"`
    IPAddress string    `json:"ip_address"`
}

func NewSessionManager() *SessionManager {
    return &SessionManager{
        client: redis.NewClient(&redis.Options{
            Addr: "localhost:6379",
        }),
        ctx: context.Background(),
    }
}

// Create new session with 1-hour TTL
func (sm *SessionManager) CreateSession(sessionID string, session Session) error {
    session.LoginTime = time.Now()
    data, err := json.Marshal(session)
    if err != nil {
        return err
    }
    
    return sm.client.Set(
        sm.ctx,
        "session:"+sessionID,
        data,
        time.Hour, // 1 hour TTL
    ).Err()
}

// Get session and extend TTL
func (sm *SessionManager) GetSession(sessionID string) (*Session, error) {
    key := "session:" + sessionID
    
    data, err := sm.client.Get(sm.ctx, key).Result()
    if err != nil {
        return nil, err
    }
    
    var session Session
    if err := json.Unmarshal([]byte(data), &session); err != nil {
        return nil, err
    }
    
    // Extend TTL on access
    sm.client.Expire(sm.ctx, key, time.Hour)
    
    return &session, nil
}

// Delete session (logout)
func (sm *SessionManager) DeleteSession(sessionID string) error {
    return sm.client.Del(sm.ctx, "session:"+sessionID).Err()
}

// Get all active sessions for a user
func (sm *SessionManager) GetUserSessions(userID string) ([]string, error) {
    var sessions []string
    iter := sm.client.Scan(sm.ctx, 0, "session:*", 0).Iterator()
    
    for iter.Next(sm.ctx) {
        key := iter.Val()
        data, err := sm.client.Get(sm.ctx, key).Result()
        if err != nil {
            continue
        }
        
        var session Session
        if err := json.Unmarshal([]byte(data), &session); err != nil {
            continue
        }
        
        if session.UserID == userID {
            sessionID := key[8:] // Remove "session:" prefix
            sessions = append(sessions, sessionID)
        }
    }
    
    return sessions, iter.Err()
}

// Main usage
func main() {
    sm := NewSessionManager()
    
    // User logs in
    sessionID := "abc123"
    session := Session{
        UserID:    "user001",
        Username:  "alice",
        Email:     "alice@example.com",
        IPAddress: "192.168.1.100",
    }
    sm.CreateSession(sessionID, session)
    
    // Validate session
    retrievedSession, err := sm.GetSession(sessionID)
    if err != nil {
        panic("Session not found")
    }
    println("Welcome back,", retrievedSession.Username)
    
    // User logs out
    sm.DeleteSession(sessionID)
}
```

---

## Time-Series Analytics

### Use Case: Website Analytics with HBase

```java
// Store and query page view analytics
public class WebAnalytics {
    private final Connection hbaseConn;
    private final Table analyticsTable;
    
    public void recordPageView(String url, String userId, String timestamp) 
            throws Exception {
        // Row key: date:url:timestamp
        String rowKey = timestamp.substring(0, 10) + ":" + url + ":" + timestamp;
        
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("user_id"), 
                     Bytes.toBytes(userId));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("url"), 
                     Bytes.toBytes(url));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("timestamp"), 
                     Bytes.toBytes(timestamp));
        
        analyticsTable.put(put);
    }
    
    public Map<String, Integer> getPageViewsByDate(String date) throws Exception {
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(date + ":"));
        scan.withStopRow(Bytes.toBytes(date + ";\\")); // ; is after : in ASCII
        
        ResultScanner scanner = analyticsTable.getScanner(scan);
        Map<String, Integer> pageViews = new HashMap<>();
        
        for (Result result : scanner) {
            String url = Bytes.toString(
                result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("url")));
            pageViews.merge(url, 1, Integer::sum);
        }
        
        scanner.close();
        return pageViews;
    }
    
    public List<PageView> getPageViewsInTimeRange(
            String startTime, String endTime) throws Exception {
        Scan scan = new Scan();
        scan.setTimeRange(
            parseTimestamp(startTime).getTime(),
            parseTimestamp(endTime).getTime()
        );
        
        ResultScanner scanner = analyticsTable.getScanner(scan);
        List<PageView> pageViews = new ArrayList<>();
        
        for (Result result : scanner) {
            pageViews.add(parsePageView(result));
        }
        
        scanner.close();
        return pageViews;
    }
}
```

---

## Real-Time Recommendations

### Use Case: Collaborative Filtering with Redis Sets

```python
class RecommendationEngine:
    def __init__(self):
        self.redis = redis.Redis(decode_responses=True)
    
    def add_user_interest(self, user_id, item_id):
        """Record user's interest in item"""
        self.redis.sadd(f'user:{user_id}:interests', item_id)
        self.redis.sadd(f'item:{item_id}:interested_users', user_id)
    
    def get_similar_users(self, user_id, limit=10):
        """Find users with similar interests"""
        user_interests = self.redis.smembers(f'user:{user_id}:interests')
        
        similar_users = {}
        for item in user_interests:
            users = self.redis.smembers(f'item:{item}:interested_users')
            for user in users:
                if user != user_id:
                    similar_users[user] = similar_users.get(user, 0) + 1
        
        # Sort by overlap count
        sorted_users = sorted(
            similar_users.items(),
            key=lambda x: x[1],
            reverse=True
        )
        return [user for user, _ in sorted_users[:limit]]
    
    def recommend_items(self, user_id, limit=10):
        """Recommend items based on similar users"""
        user_interests = self.redis.smembers(f'user:{user_id}:interests')
        similar_users = self.get_similar_users(user_id, 20)
        
        # Collect items from similar users
        recommended_items = {}
        for similar_user in similar_users:
            items = self.redis.smembers(f'user:{similar_user}:interests')
            for item in items:
                if item not in user_interests:
                    recommended_items[item] = recommended_items.get(item, 0) + 1
        
        # Sort by frequency
        sorted_items = sorted(
            recommended_items.items(),
            key=lambda x: x[1],
            reverse=True
        )
        return [item for item, _ in sorted_items[:limit]]
```

---

## Message Queue

### Use Case: Task Queue with Redis Lists

```python
import json
import time

class TaskQueue:
    def __init__(self, queue_name='tasks'):
        self.redis = redis.Redis(decode_responses=True)
        self.queue_key = f'queue:{queue_name}'
        self.processing_key = f'queue:{queue_name}:processing'
    
    def enqueue(self, task):
        """Add task to queue"""
        task_data = json.dumps({
            'id': task['id'],
            'type': task['type'],
            'data': task['data'],
            'enqueued_at': time.time()
        })
        self.redis.rpush(self.queue_key, task_data)
    
    def dequeue(self, timeout=0):
        """Get next task (blocking)"""
        # Atomic move from queue to processing
        if timeout > 0:
            result = self.redis.blpop(self.queue_key, timeout=timeout)
            if result:
                _, task_data = result
                return json.loads(task_data)
        else:
            task_data = self.redis.lpop(self.queue_key)
            if task_data:
                return json.loads(task_data)
        return None
    
    def complete(self, task_id):
        """Mark task as complete"""
        self.redis.srem(self.processing_key, task_id)
    
    def get_queue_length(self):
        """Get number of pending tasks"""
        return self.redis.llen(self.queue_key)

# Producer
queue = TaskQueue('email')
queue.enqueue({
    'id': 'task-001',
    'type': 'send_email',
    'data': {'to': 'user@example.com', 'subject': 'Hello'}
})

# Consumer
while True:
    task = queue.dequeue(timeout=5)
    if task:
        print(f"Processing task: {task['id']}")
        # Process task...
        queue.complete(task['id'])
```

---

## Geospatial Services

### Use Case: Store Locator

```python
class StoreLocator:
    def __init__(self):
        self.redis = redis.Redis(decode_responses=True)
        self.geo_key = 'stores:locations'
    
    def add_store(self, store_id, longitude, latitude, name):
        """Add store location"""
        self.redis.geoadd(self.geo_key, (longitude, latitude, store_id))
        self.redis.hset(f'store:{store_id}', 'name', name)
    
    def find_nearby_stores(self, longitude, latitude, radius_km, limit=10):
        """Find stores within radius"""
        stores = self.redis.georadius(
            self.geo_key,
            longitude,
            latitude,
            radius_km,
            unit='km',
            withdist=True,
            withcoord=True,
            count=limit,
            sort='ASC'
        )
        
        results = []
        for store_id, distance, coords in stores:
            name = self.redis.hget(f'store:{store_id}', 'name')
            results.append({
                'id': store_id,
                'name': name,
                'distance_km': float(distance),
                'latitude': coords[1],
                'longitude': coords[0]
            })
        return results

# Usage
locator = StoreLocator()

# Add stores
locator.add_store('store1', -122.4194, 37.7749, 'SF Store')
locator.add_store('store2', -118.2437, 34.0522, 'LA Store')

# Find nearby (user at -122.0, 37.5)
nearby = locator.find_nearby_stores(-122.0, 37.5, 100)
for store in nearby:
    print(f"{store['name']}: {store['distance_km']:.2f} km away")
```

---

## Multi-Language Examples

### Java - Complete CRUD Application

See [HBase Guide](HBASE-GUIDE.md#java-client-usage) for complete Java examples.

### Python - Web API with Flask

See [Redis Guide](REDIS-GUIDE.md#python-redis-py) for complete Python examples.

### Node.js - Express API

See [Redis Guide](REDIS-GUIDE.md#nodejs-ioredis) for complete Node.js examples.

### Go - Microservice

See [Redis Guide](REDIS-GUIDE.md#go-go-redis) for complete Go examples.

---

## Next Steps

- **[Getting Started](GETTING-STARTED.md)** - Set up your environment
- **[HBase Guide](HBASE-GUIDE.md)** - Master HBase protocol
- **[Redis Guide](REDIS-GUIDE.md)** - Master Redis protocol
- **[Configuration](CONFIGURATION.md)** - Optimize for your use case

---

**Have a use case not covered here?** Open an issue or contribute examples!
