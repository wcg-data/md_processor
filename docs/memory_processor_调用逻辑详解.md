# memory_processor_on_tick.rs 调用逻辑详解

**文件**: `src/memory_processor_on_tick.rs`
**用途**: 从共享内存实时读取tick数据，聚合成1分钟和5分钟K线

---

## 一、调用方式

### 1. 程序启动

```bash
./target/release/memory_processor_on_tick /md_shm
```

参数：
- `/md_shm`：共享内存名称

### 2. 调用链路

```
main()
  ↓
map_shared_ring_buffer("/md_shm")  // 映射共享内存
  ↓
process_memory_on_tick(ring_buffer, AggregationMode::Multi)
  ↓
multi_contract_loop()  // 多合约聚合循环
```

---

## 二、核心处理流程

### 模式：Multi（多合约自动聚合）

代码中固定使用 `AggregationMode::Multi`：

```rust
let mode = AggregationMode::Multi;  // line 295
```

### 处理循环（multi_contract_loop）

```
while running:
    当前时间 = Local::now()

    // ===== 每分钟触发一次 =====
    if 分钟变化:
        ① 更新活跃合约状态（基于交易时段）
        ② 检测交易时段结束的合约
        ③ flush 交易时段结束的合约（5min bar）
        ④ flush 所有活跃合约的1min bar
        ⑤ 将1min bar 喂入 5min 聚合器

    // ===== Tick 驱动（实时） =====
    if 有新tick:
        ⑥ tick → 1min bar (通过 Bar1MinAggregator)
        ⑦ 1min bar → 5min bar (通过 Bar5MinAggregator)
```

---

## 三、与 parquet_processor_on_tick 的对比

| 特性 | memory_processor | parquet_processor |
|------|------------------|-------------------|
| **数据源** | 共享内存（实时） | Parquet文件（离线） |
| **聚合器** | ✅ Bar1MinAggregator | ✅ Bar1MinAggregator |
| **集合竞价** | ✅ 支持（通过聚合器） | ✅ 支持（通过聚合器） |
| **fill_missing_minutes** | ❌ **不补全** | ✅ 补全 |
| **输出** | CSV/Kafka（实时流） | Parquet文件（批处理） |
| **使用场景** | 实盘交易、实时监控 | 历史数据回测、研究 |

---

## 四、补全逻辑（关键区别！）

### ❌ memory_processor 不调用 fill_missing_minutes

**为什么？**

1. **实时性要求**
   - 实时处理不能等待补全
   - tick驱动，有tick就产生bar
   - 没有tick就没有bar

2. **数据完整性由上游保证**
   - C++行情接收程序负责写入共享内存
   - 如果某分钟无tick，自然就无bar
   - 下游系统（策略、监控）需要处理缺失分钟

3. **不需要补全**
   - 实时交易系统关注最新数据
   - 历史缺失分钟由离线处理补全
   - 两者职责分离

### ✅ parquet_processor 调用 fill_missing_minutes

```rust
// parquet_processor_on_tick.rs 的处理流程
let bars_df = aggregate_to_bars(...);
let filled_df = fill_missing_minutes(bars_df)?;  // ← 补全
write_to_parquet(filled_df);
```

**补全时机**：聚合完成后，写入文件前

---

## 五、集合竞价处理

### 1. 使用相同的 Bar1MinAggregator

```rust
// memory_processor_on_tick.rs line 183
let mut manager = AggregatorManager::new();

// AggregatorManager 内部使用 Bar1MinAggregator
// Bar1MinAggregator 包含集合竞价逻辑
```

### 2. 集合竞价逻辑（在 Bar1MinAggregator 中）

**检测集合竞价时段**（bar1min_aggregator.rs:74-90）：

```rust
fn is_auction_period(&self, tick_time: NaiveTime) -> bool {
    for auction_time in &self.auction_times {
        // 推断开盘时间
        let opening_time = match auction_time {
            t if t.hour() == 20 && t.minute() == 55 =>
                NaiveTime::from_hms_opt(21, 0, 0).unwrap(),
            t if t.hour() == 8 && t.minute() == 55 =>
                NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            t if t.hour() == 9 && t.minute() == 25 =>
                NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
            _ => continue,
        };

        // auction_time <= tick_time < opening_time
        if tick_time >= *auction_time && tick_time < opening_time {
            return true;
        }
    }
    false
}
```

**收集集合竞价tick**（bar1min_aggregator.rs:137-152）：

```rust
if self.is_auction_period(tick_time) {
    self.auction_ticks.push(tick.clone());
    return None;  // 不立即产生bar
}

// 开盘时刻到达，flush集合竞价bar
if self.should_flush_auction_bar(tick_time) {
    return self.flush_auction_bar(tick_time);
}
```

**生成集合竞价bar**（bar1min_aggregator.rs:154-193）：

```rust
fn flush_auction_bar(&mut self, opening_time: NaiveTime) -> Option<BarData> {
    // 聚合所有auction_ticks
    // 时间戳设为 opening_time (09:00/09:30/21:00)
    // 即使 volume=0 也生成bar（保留昨结算价等信息）

    let bar = BarData {
        trade_time: format!("{} {:02}:{:02}:00", date, hour, minute),
        open: first_tick.close,
        high: max_price,
        low: min_price,
        close: last_tick.close,
        volume: total_volume,
        // ... 其他字段
    };

    self.auction_ticks.clear();
    Some(bar)
}
```

### 3. 集合竞价实时处理示例

**时间线**：

```
20:55:00 - 20:59:59
  - tick到达 → 存入 auction_ticks
  - 不产生bar

21:00:00
  - tick到达 → 触发 flush_auction_bar
  - 产生集合竞价bar，时间戳 = 21:00:00
  - 输出到 CSV/Kafka

21:00:05
  - tick到达 → 正常聚合
  - 累积到当前分钟窗口

21:01:00
  - 分钟切换 → flush 21:00的bar
  - 输出到 CSV/Kafka
```

---

## 六、活跃合约管理

### 1. 每分钟更新活跃状态

```rust
// line 196
manager.update_active_contracts(&TRADE_SESSION_MAP, now_naive);
```

**逻辑**（aggregator_manager.rs）：

```rust
pub fn update_active_contracts(&mut self, ...) {
    for (contract, aggregator) in &self.aggregators_1min {
        if is_in_trading_session(contract, now) {
            self.active_contracts.insert(contract.clone());
        }
    }
}
```

### 2. 检测交易时段结束

```rust
// line 200-204
let ended_contracts: Vec<String> = prev_active_contracts
    .iter()
    .filter(|c| !current_active.contains(*c))
    .cloned()
    .collect();
```

**场景**：
- 15:00 收盘
- prev_active 包含 "IF2502"
- current_active 不包含 "IF2502"
- → ended_contracts = ["IF2502"]

### 3. 主动 flush 收盘合约

```rust
// line 210-213
for bar5min in manager.flush_contracts_session_ended(&ended_contracts) {
    write_bar_to_csv(&bar5min, "5min");
}
```

**作用**：确保最后的5min窗口被输出（避免丢失）

---

## 七、输出方式

### 当前：临时CSV输出

```rust
// line 41-48
"output/md_snapshot_RTdongzheng_1min.csv.20251027"
"output/md_snapshot_RTdongzheng_5min.csv.20251027"
```

CSV格式（与Parquet一致）：
```
contract,contract_yymm,comd,exchange,date,trade_time,open,high,low,close,...
```

### 未来：Kafka输出（已注释）

```rust
// 临时注释的Kafka代码（line 160, 221, 236等）
// kafka_client::send_bar_data(&bar, "1min");
```

**恢复Kafka后**：
- Topic: `md_snapshot_RTdongzheng_1min`
- Topic: `md_snapshot_RTdongzheng_5min`

---

## 八、完整调用流程图

```
C++ 行情接收程序
    ↓ (写入)
共享内存 /md_shm (RingBuffer)
    ↓ (读取)
memory_processor_on_tick
    ↓
multi_contract_loop
    ├─→ 每分钟触发
    │   ├─→ update_active_contracts()
    │   ├─→ flush_all_bar1min_active()
    │   └─→ on_bar_1min() → 5min聚合
    │
    └─→ Tick驱动
        ├─→ manager.on_tick()
        │   └─→ Bar1MinAggregator.on_tick()
        │       ├─→ is_auction_period? → 收集
        │       ├─→ should_flush_auction? → 生成集合竞价bar
        │       └─→ 正常tick → 累积到分钟窗口
        │
        └─→ on_bar_1min() → 5min聚合

输出: CSV (临时) / Kafka (未来)
```

---

## 九、关键代码位置

| 功能 | 文件 | 行号 |
|------|------|------|
| 共享内存映射 | memory_processor_on_tick.rs | 282-288 |
| 多合约循环 | memory_processor_on_tick.rs | 179-267 |
| 活跃合约更新 | memory_processor_on_tick.rs | 196 |
| 集合竞价检测 | bar1min_aggregator.rs | 74-90 |
| 集合竞价flush | bar1min_aggregator.rs | 154-193 |
| CSV输出 | memory_processor_on_tick.rs | 67-108 |

---

## 十、总结

### memory_processor_on_tick 的特点

1. **实时处理**
   - 从共享内存读取tick
   - 立即聚合，立即输出
   - 无延迟

2. **集合竞价支持**
   - 使用 Bar1MinAggregator
   - 与 parquet_processor 逻辑完全一致
   - 实时生成集合竞价bar

3. **不补全缺失分钟**
   - tick驱动，有数据才有bar
   - 实时性优先
   - 历史补全由离线处理负责

4. **活跃合约管理**
   - 自动检测交易时段
   - 自动flush收盘合约
   - 避免数据丢失

5. **输出灵活**
   - 当前：CSV（调试方便）
   - 未来：Kafka（生产环境）

### 与离线处理的配合

| 处理类型 | 职责 | 数据完整性 |
|---------|------|----------|
| **实时** (memory_processor) | 快速响应、实时监控 | 缺失分钟不补全 |
| **离线** (parquet_processor) | 完整回测、历史研究 | fill_missing_minutes 补全 |

**最佳实践**：
- 实盘交易：使用 memory_processor（低延迟）
- 策略回测：使用 parquet_processor 生成的完整数据
- 数据研究：使用 fill_missing_minutes 补全后的Parquet文件
