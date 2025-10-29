# memory_processor_on_tick.rs 集合竞价深度调用链路

**聚焦**: 从tick到达到集合竞价bar输出的完整代码调用细节

---

## 完整调用链路图

```
C++行情程序 → 共享内存
    ↓
memory_processor_on_tick::main()
    ↓ (line 282-288)
map_shared_ring_buffer("/md_shm")
    ↓ (line 295-298)
process_memory_on_tick(ring_buffer, AggregationMode::Multi)
    ↓ (line 136)
multi_contract_loop(ring_buffer, &running)
    ↓ (line 233)
ring_buffer.pop_market_data() → Some(tick)
    ↓ (line 235)
manager.on_tick(&mut tick)  ← 【关键入口】
    ↓
AggregatorManager::on_tick() (aggregator_manager.rs:49-59)
    ├─→ Bar1MinAggregator::validate_tick() (line 50-51)
    │   ├─→ 基础字段验证 (line 360-378)
    │   ├─→ parse_datetime_from_tick() (line 447-450)
    │   ├─→ 交易时段检查 (line 464-465)
    │   └─→ 集合竞价时段检查 (line 469-475) ✅
    │
    └─→ Bar1MinAggregator::on_tick() (line 58)
        ├─→ parse_datetime_from_tick() (line 36)
        ├─→ check_auction_period() (line 41) ← 【集合竞价检测】
        │   └─→ TRADE_SESSION_MAP.get_auction_periods()
        │
        ├─→ 【分支1：在集合竞价时段内】
        │   ├─→ 第一次进入？(line 45-69)
        │   │   ├─→ flush() 前一个连续交易窗口 (line 54)
        │   │   └─→ auction_ticks.push(tick) (line 65)
        │   └─→ 已经在竞价时段？(line 70-74)
        │       └─→ auction_ticks.push(tick) (line 72)
        │
        ├─→ 【分支2：刚离开集合竞价时段】(line 75-89)
        │   ├─→ flush_auction_bar() (line 78) ← 【生成集合竞价bar】
        │   │   ├─→ 聚合OHLC (line 135-148)
        │   │   ├─→ 计算volume/turnover (line 151-152)
        │   │   ├─→ 计算oi_diff (line 164-169)
        │   │   ├─→ 构造BarData (line 171-197)
        │   │   └─→ 更新prev_last_tick (line 201)
        │   │
        │   ├─→ 重置集合竞价状态 (line 80-82)
        │   └─→ 处理当前tick (line 85-86)
        │
        └─→ 【分支3：正常连续交易】(line 91-122)
            ├─→ 同一分钟？累积high/low (line 95-113)
            └─→ 分钟切换？flush + init (line 115-117)
    ↓
返回 Some(bar) 或 None
    ↓ (memory_processor_on_tick.rs:236-238)
write_bar_to_csv(&bar, "1min")
```

---

## 一、Tick到达：ring_buffer.pop_market_data()

### 代码位置：memory_processor_on_tick.rs:233

```rust
if let Some(mut tick) = ring_buffer.pop_market_data() {
    // ③ Tick → 1 min (集合竞价处理在这里！)
    if let Some(bar1min) = manager.on_tick(&mut tick) {
        write_bar_to_csv(&bar1min, "1min");
    }
}
```

**作用**：
- 从共享内存环形缓冲区取出一个tick
- 传递给 `manager.on_tick()`

---

## 二、AggregatorManager::on_tick() - 验证并路由

### 代码位置：aggregator_manager.rs:49-59

```rust
pub fn on_tick(&mut self, tick: &mut TickData) -> Option<BarData> {
    // Step 1: 验证tick（包含集合竞价时段检查）
    if !Bar1MinAggregator::validate_tick(tick) {
        return None;  // 验证失败，丢弃tick
    }

    // Step 2: 获取或创建该合约的聚合器
    let contract = to_string_field(&tick.contract);
    let aggr = self.bar1min_aggregators
        .entry(contract.clone())
        .or_insert_with(Bar1MinAggregator::new);

    // Step 3: 调用聚合器处理tick
    aggr.on_tick(tick)
}
```

**关键点**：
1. **验证tick**（validate_tick）- 包含集合竞价时段检查
2. **路由到合约** - 每个合约有独立的 Bar1MinAggregator
3. **调用聚合器** - 实际处理逻辑在 Bar1MinAggregator

---

## 三、Bar1MinAggregator::validate_tick() - 集合竞价时段检查

### 代码位置：bar1min_aggregator.rs:357-478

这个函数非常重要！它确保集合竞价时段的tick不会被过滤掉。

```rust
pub fn validate_tick(tick: &mut TickData) -> bool {
    // ... 基础字段验证 (line 360-408)

    // 解析时间
    let dt = match Self::parse_datetime_from_tick(tick) {
        Some(t) => t,
        None => return false,
    };

    let comd = to_string_field(&tick.comd);
    let trading_ranges = TRADE_SESSION_MAP.get_trading_ranges(&comd);

    if trading_ranges.is_empty() {
        return true;  // 没有配置则不过滤
    }

    // ===== 检查1：是否在连续交易时段 =====
    if TRADE_SESSION_MAP.is_in_trading_session(&comd, dt.naive_utc()) {
        return true;  // 在连续交易时段，允许通过
    }

    // ===== 检查2：是否在集合竞价时段 ===== ← 关键！
    let auction_periods = TRADE_SESSION_MAP.get_auction_periods(&comd);
    let tick_time = dt.naive_utc().time();

    for (auction_start, auction_end) in auction_periods {
        // 检查 tick_time 是否在 [auction_start, auction_end) 区间
        if tick_time >= auction_start && tick_time < auction_end {
            return true;  // ← 在集合竞价时段，允许通过！
        }
    }

    false  // 既不在连续交易时段，也不在集合竞价时段，过滤掉
}
```

**关键逻辑**：
- 如果tick在集合竞价时段（如20:55-21:00），返回 `true`
- 这样集合竞价的tick才能继续处理

**示例**：
```
tick.trade_time = "2024-11-27 20:56:30"
comd = "rb"
auction_periods = [(20:55:00, 21:00:00)]

tick_time = 20:56:30
20:56:30 >= 20:55:00 && 20:56:30 < 21:00:00 → true
→ validate_tick 返回 true ✅
```

---

## 四、Bar1MinAggregator::on_tick() - 核心处理逻辑

### 代码位置：bar1min_aggregator.rs:34-123

这是集合竞价处理的**核心函数**！

### Step 1: 解析时间（line 36-38）

```rust
pub fn on_tick(&mut self, tick: &TickData) -> Option<BarData> {
    // 缓存时间解析结果
    let tick_time = Self::parse_datetime_from_tick(tick)?;
    self.cached_tick_time = Some(tick_time);
    let tick_naive = tick_time.naive_utc();
```

### Step 2: 检查是否在集合竞价时段（line 41）

```rust
    // 检查是否在集合竞价时段
    let auction_opening = Self::check_auction_period(tick, &tick_naive);
```

**调用**: `check_auction_period()` - 详细见后文

**返回值**：
- `Some(opening_time)` - 在集合竞价时段，返回开盘时间
- `None` - 不在集合竞价时段

### Step 3: 分支1 - 在集合竞价时段内（line 43-74）

```rust
    if let Some(opening_time) = auction_opening {
        // 在集合竞价时段内

        if !self.in_auction_period {
            // ===== 第一次进入集合竞价 =====

            // 检查是否需要flush前一个连续交易窗口
            let prev_bar = if self.last_tick.is_some() {
                let prev_date = to_string_field(&self.last_tick.unwrap().date);
                let curr_date = to_string_field(&tick.date);

                if prev_date == curr_date {
                    // 同一天: flush前一个窗口（如日盘结束后的夜盘集合竞价）
                    self.flush()
                } else {
                    // 跨天: 不flush，避免生成超出交易时段的bar
                    None
                }
            } else {
                None
            };

            // 开始累积集合竞价ticks
            self.auction_ticks.push(*tick);
            self.in_auction_period = true;
            self.auction_opening_time = Some(opening_time);

            return prev_bar;  // 返回前一个连续交易bar（如果有）
        } else {
            // ===== 已经在集合竞价时段内 =====

            // 继续累积
            self.auction_ticks.push(*tick);
            return None;
        }
    }
```

**场景示例**：

```
20:55:05 第一个集合竞价tick到达
  → auction_opening = Some(2024-11-27 21:00:00)
  → in_auction_period = false
  → 设置 in_auction_period = true
  → auction_opening_time = Some(21:00:00)
  → auction_ticks = [tick1]
  → 返回 None（或前一个bar）

20:55:30 第二个集合竞价tick到达
  → auction_opening = Some(2024-11-27 21:00:00)
  → in_auction_period = true
  → auction_ticks = [tick1, tick2]
  → 返回 None

20:59:50 最后一个集合竞价tick到达
  → auction_opening = Some(2024-11-27 21:00:00)
  → in_auction_period = true
  → auction_ticks = [tick1, tick2, ..., tickN]
  → 返回 None
```

### Step 4: 分支2 - 刚离开集合竞价时段（line 75-89）

```rust
    else if self.in_auction_period {
        // ===== 刚离开集合竞价时段 =====
        // （第一个连续交易tick）

        // 生成集合竞价bar
        let auction_bar = self.flush_auction_bar();

        // 重置集合竞价状态
        self.in_auction_period = false;
        self.auction_opening_time = None;

        // 处理当前tick（连续交易的第一个tick）
        let tick_minute = floor_to_1min(&tick_time);
        self.init_bar_window(tick, tick_minute);

        return auction_bar;  // ← 返回集合竞价bar！
    }
```

**场景示例**：

```
21:00:05 第一个连续交易tick到达
  → auction_opening = None（不在集合竞价时段了）
  → in_auction_period = true（之前在）
  → 触发 flush_auction_bar()
  → 生成集合竞价bar（时间戳 = 21:00:00）
  → 重置状态
  → 初始化新窗口（21:00-21:01）
  → 返回集合竞价bar ✅
```

### Step 5: 分支3 - 正常连续交易（line 91-122）

```rust
    // 正常连续交易逻辑（未修改）
    let tick_minute = floor_to_1min(&tick_time);

    if let Some(current_bar_minute) = self.current_bar_minute {
        if tick_minute == current_bar_minute {
            // 同一分钟，累积数据
            self.last_tick = Some(*tick);
            // 更新 high/low
            ...
            return None;
        } else {
            // 分钟切换，flush旧窗口，初始化新窗口
            let flushed = self.flush();
            self.init_bar_window(tick, tick_minute);
            return flushed;
        }
    } else {
        // 第一个tick，初始化窗口
        self.init_bar_window(tick, tick_minute);
        return None;
    }
```

---

## 五、check_auction_period() - 检测集合竞价时段

### 代码位置：bar1min_aggregator.rs:505-522

```rust
fn check_auction_period(
    tick: &TickData,
    tick_datetime: &NaiveDateTime
) -> Option<NaiveDateTime> {
    let comd = to_string_field(&tick.comd);
    let tick_time = tick_datetime.time();

    // 获取该品种的集合竞价时段配置
    let auction_periods = TRADE_SESSION_MAP.get_auction_periods(&comd);

    for (auction_start, auction_end) in auction_periods {
        // 检查tick时间是否在集合竞价时段内 [auction_start, auction_end)
        if tick_time >= auction_start && tick_time < auction_end {
            // 构造开盘时间：使用tick的日期 + auction_end时间
            let opening_datetime = tick_datetime.date().and_time(auction_end);
            return Some(opening_datetime);
        }
    }

    None
}
```

**作用**：
- 查询 `trade_session.csv` 中的集合竞价时段配置
- 判断tick是否在集合竞价时段内
- 返回对应的开盘时间

**示例**：

```
品种: rb（螺纹钢）
配置: rb,20:55:00,21:00:00,23:00:00
      rb,,09:00:00,10:15:00
      ...

auction_periods = [(20:55:00, 21:00:00)]

tick.trade_time = "2024-11-27 20:56:30"
tick_time = 20:56:30

20:56:30 >= 20:55:00 && 20:56:30 < 21:00:00 → true
opening_datetime = 2024-11-27 21:00:00
返回 Some(2024-11-27 21:00:00) ✅
```

---

## 六、flush_auction_bar() - 生成集合竞价bar

### 代码位置：bar1min_aggregator.rs:127-207

这是**生成集合竞价bar的核心函数**！

```rust
fn flush_auction_bar(&mut self) -> Option<BarData> {
    if self.auction_ticks.is_empty() {
        return None;  // 无集合竞价tick，跳过
    }

    let last_tick = *self.auction_ticks.last()?;
    let opening_time = self.auction_opening_time?;

    // ===== Step 1: 聚合OHLC =====
    let open_price = self.auction_ticks[0].close;
    let mut high_price = self.auction_ticks[0].close;
    let mut low_price = self.auction_ticks[0].close;
    let close_price = last_tick.close;

    for tick in &self.auction_ticks {
        if tick.close > high_price {
            high_price = tick.close;
        }
        if tick.close < low_price {
            low_price = tick.close;
        }
    }

    // ===== Step 2: 聚合volume/turnover =====
    // 集合竞价bar使用累计值，不做差分
    let volume = last_tick.volume;
    let turnover = last_tick.turnover;

    // ===== Step 3: 计算oi_diff =====
    let oi_diff = if let Some(prev) = &self.prev_last_tick {
        last_tick.open_interest.wrapping_sub(prev.open_interest) as i32
    } else {
        // 文件第一个bar，无前置数据
        0
    };

    // ===== Step 4: 构造BarData =====
    let contract = to_string_field(&last_tick.contract);
    let trade_time = opening_time.format("%Y-%m-%d %H:%M:%S").to_string();

    // ← 时间戳设为开盘时间（21:00:00）

    let bar = BarData {
        contract: contract.clone(),
        comd: to_string_field(&last_tick.comd),
        exchange: to_string_field(&last_tick.exchange),
        date: to_string_field(&last_tick.date),
        trade_time,  // ← 21:00:00
        open: open_price,
        high: high_price,
        low: low_price,
        close: close_price,
        prev_close: f64::NAN,  // 集合竞价bar无prev_close
        pre_settle: last_tick.pre_settle,
        volume,
        turnover,
        open_interest: last_tick.open_interest,
        open_interest_diff: oi_diff,
        bid_price_1: last_tick.bid_price_1,
        bid_volume_1: last_tick.bid_volume_1,
        ask_price_1: last_tick.ask_price_1,
        ask_volume_1: last_tick.ask_volume_1,
        mid_price: last_tick.mid_price,
        vwap: if volume == 0 { 0.0 } else { turnover / volume as f64 },
        log_return: f64::NAN,  // 集合竞价bar无法计算log_return
        maturity_month: 0,
        maturity_day: 0,
    };

    // ===== Step 5: 更新prev_last_tick =====
    // 这样后续的连续交易bar可以正确计算差分
    self.prev_last_tick = Some(last_tick);

    // ===== Step 6: 清空集合竞价缓冲区 =====
    self.auction_ticks.clear();

    Some(bar)
}
```

**关键点**：

1. **时间戳 = 开盘时间**
   - 不是集合竞价时段的任意时间
   - 而是开盘时间（21:00:00, 09:00:00, 09:30:00）

2. **OHLC聚合**
   - 遍历所有 auction_ticks
   - 计算最高价、最低价

3. **volume/turnover使用累计值**
   - 不做差分
   - 直接使用最后一个tick的累计值

4. **oi_diff正常计算**
   - 与前一个bar的oi做差
   - 保证oi_diff连续性

5. **更新prev_last_tick**
   - 确保后续bar的差分计算正确

---

## 七、完整时间线示例

### 夜盘集合竞价（螺纹钢 rb2501）

```
时间          tick.trade_time    状态                    动作
------------------------------------------------------------------------
20:55:00      2024-11-27 20:55:00  进入集合竞价时段
20:55:05      2024-11-27 20:55:05  ↓                     auction_ticks=[tick1]
20:55:30      2024-11-27 20:55:30  ↓                     auction_ticks=[tick1,tick2]
20:56:10      2024-11-27 20:56:10  ↓                     auction_ticks=[...,tick3]
...
20:59:50      2024-11-27 20:59:50  ↓                     auction_ticks=[...,tickN]
------------------------------------------------------------------------
21:00:05      2024-11-27 21:00:05  离开集合竞价时段      flush_auction_bar()
                                                        → 输出bar:
                                                          trade_time=21:00:00
                                                          volume=累计值
                                                          oi_diff=正常差分
                                                        → 初始化新窗口(21:00-21:01)
21:00:15      2024-11-27 21:00:15  正常连续交易          累积到21:00窗口
21:00:45      2024-11-27 21:00:45  ↓                     累积到21:00窗口
21:01:02      2024-11-27 21:01:02  分钟切换              flush 21:00窗口
                                                        → 输出bar:
                                                          trade_time=21:00:00
                                                          (连续交易bar)
                                                        → 初始化新窗口(21:01-21:02)
```

---

## 八、输出到CSV

### 代码位置：memory_processor_on_tick.rs:236-238

```rust
if let Some(bar1min) = manager.on_tick(&mut tick) {
    write_bar_to_csv(&bar1min, "1min");

    // 及时投喂 5 min 聚合器
    if let Some(bar5min) = manager.on_bar_1min(&bar1min) {
        write_bar_to_csv(&bar5min, "5min");
    }
}
```

### write_bar_to_csv() - 代码位置：67-108

```rust
fn write_bar_to_csv(bar: &BarData, freq: &str) {
    let csv_line = format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\\n",
        bar.contract.trim_end_matches('\\0'),
        // ... 所有字段
    );

    match freq {
        "1min" => {
            let mut writer = CSV_WRITER.file_1min.lock().unwrap();
            writer.write_all(csv_line.as_bytes()).expect("写入1min CSV失败");
        }
        "5min" => {
            let mut writer = CSV_WRITER.file_5min.lock().unwrap();
            writer.write_all(csv_line.as_bytes()).expect("写入5min CSV失败");
        }
        _ => {}
    }
}
```

---

## 九、关键状态变量

### Bar1MinAggregator 的集合竞价状态字段

```rust
pub struct Bar1MinAggregator {
    // 集合竞价相关字段
    auction_ticks: Vec<TickData>,       // 集合竞价时段的tick缓冲区
    in_auction_period: bool,            // 当前是否在集合竞价时段
    auction_opening_time: Option<NaiveDateTime>, // 开盘时间

    // 连续交易相关字段
    current_bar_minute: Option<DateTime<Utc>>,
    prev_last_tick: Option<TickData>,
    last_tick: Option<TickData>,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
}
```

**状态转换**：

```
初始状态:
  in_auction_period = false
  auction_ticks = []

20:55:05 第一个集合竞价tick:
  in_auction_period = true
  auction_opening_time = Some(21:00:00)
  auction_ticks = [tick1]

20:55-20:59 持续收集:
  auction_ticks = [tick1, tick2, ..., tickN]

21:00:05 第一个连续交易tick:
  flush_auction_bar() → 生成bar
  in_auction_period = false
  auction_opening_time = None
  auction_ticks = []（已清空）
```

---

## 十、总结

### 集合竞价处理的完整流程

1. **tick验证**（validate_tick）
   - 检查是否在集合竞价时段
   - 如果在，允许通过 ✅

2. **检测集合竞价**（check_auction_period）
   - 查询 trade_session.csv 配置
   - 返回开盘时间

3. **收集tick**（on_tick）
   - 在集合竞价时段内：push到 auction_ticks
   - 不立即生成bar

4. **生成bar**（flush_auction_bar）
   - 第一个连续交易tick到达时触发
   - 聚合所有 auction_ticks
   - 时间戳 = 开盘时间
   - volume = 累计值
   - oi_diff = 正常差分

5. **输出**（write_bar_to_csv）
   - 立即写入CSV文件
   - 或发送到Kafka（生产环境）

### 关键特性

- ✅ **实时处理** - tick到达立即处理
- ✅ **状态机驱动** - 通过 in_auction_period 标志
- ✅ **完全自动** - 基于 trade_session.csv 配置
- ✅ **数据驱动** - 自动适配节假日、停夜盘
- ✅ **oi_diff连续** - 集合竞价bar与连续交易bar完全连续

### 不做的事情

- ❌ **不补全缺失分钟** - 实时处理，tick驱动
- ❌ **不缓存历史** - 无状态，只保留必要的 prev_last_tick
