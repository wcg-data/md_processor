# on_tick错误生成21:00空bar的bug分析

**日期**: 2025-10-30
**问题**: on_tick在最后交易日（无夜盘tick）错误生成21:00空bar

---

## 一、已确认事实

### 1.1 原始tick数据
- **最后交易日**: 2023-03-14
- **tick时间范围**: 09:38:11 ~ 14:59:58（只有日盘）
- **夜盘tick数**: 0个
- **21:00集合竞价tick**: 0个

### 1.2 on_batch行为（正确）
- 没有生成21:00 bar ✓
- 最后bar: 2023-03-14 15:00:00

### 1.3 on_tick行为（错误）
- 生成了21:00 bar ✗
- 21:00 bar详情：
  - trade_time: 2023-03-14 21:00:00
  - date: 2023-03-15（正常，夜盘交易日是次日）
  - close: 2924.00（= 最后实际tick 14:59:58的close）
  - volume: 0
  - 这是一个**空bar**

---

## 二、21:00 bar的来源推断

### 2.1 价格来源
close=2924.00 = 最后实际tick（14:59:58）的close价格
→ 这个bar使用了`prev_last_tick`的价格

### 2.2 可能的生成路径

#### 路径A: flush()的空bar分支
```rust
// bar1min_aggregator.rs:274-327
if self.last_tick.is_none() {
    if let Some(prev_last_tick) = self.prev_last_tick {
        // 生成空bar
        let trade_time = align_to_bar_minute(
            &self.current_bar_minute.unwrap_or_else(|| Utc::now())
        )
        ...
    }
}
```

**问题**: 如果current_bar_minute=None，会使用Utc::now()，时间应该是2025年，而不是2023-03-14 21:00

#### 路径B: flush_auction_bar()
```rust
// bar1min_aggregator.rs:265-272
if !self.auction_ticks.is_empty() && self.in_auction_period {
    return self.flush_auction_bar();
}
```

**问题**:
- auction_ticks可能没有被清空？
- in_auction_period可能没有被重置为false？

---

## 三、关键疑点

### 3.1 倒数第二日的状态
- 最后tick: 2023-03-13 22:59:49（夜盘tick）
- 有21:00集合竞价tick: 1个（21:00:24）
- on_tick处理完这个tick后：
  - in_auction_period = ?（是否还是true？）
  - auction_ticks = ?（是否为空？）

### 3.2 文件结束时的flush()
在parquet_processor_on_tick.rs:160调用aggregator.flush()时：
- last_tick = None
- prev_last_tick = 2023-03-14最后tick（14:59:58, close=2924）
- current_bar_minute = ?
- in_auction_period = ?
- auction_ticks = ?

---

## 四、下一步调查方向

### 方向1: 检查跨日后in_auction_period的状态
查看on_tick()函数在处理倒数第二日夜盘tick后，是否正确重置了集合竞价状态。

**关键代码**: bar1min_aggregator.rs:63-84（跨天flush集合竞价bar）

### 方向2: 检查flush()是否生成了"幽灵bar"
如果current_bar_minute有值（但不是21:00），flush()空bar分支可能生成错误的时间。

### 方向3: 检查auction_ticks是否残留
如果auction_ticks缓冲区没有被清空，且in_auction_period=true，flush_auction_bar()可能生成错误的bar。

---

## 五、推荐调试方法

在bar1min_aggregator.rs的flush()函数开头添加日志：

```rust
pub fn flush(&mut self) -> Option<BarData> {
    // DEBUG: 输出状态
    eprintln!("DEBUG flush(): last_tick={:?}, prev_last_tick={:?}, current_bar_minute={:?}, in_auction={}, auction_ticks.len={}",
        self.last_tick.is_some(),
        self.prev_last_tick.map(|t| to_string_field(&t.contract)),
        self.current_bar_minute,
        self.in_auction_period,
        self.auction_ticks.len()
    );

    // 原有逻辑...
}
```

然后单独处理SA2303，观察最后几次flush()的状态。

---

## 六、临时结论

**最可能的原因**：
文件结束时调用flush()，由于某个状态没有正确重置，导致生成了错误的21:00 bar。

**需要验证**：
- in_auction_period是否在跨日时正确重置
- auction_ticks是否在文件结束时还有残留数据
- current_bar_minute在最后一次flush后的值

**影响范围**：
~54%的合约（最后交易日停夜盘的情况），导致多填充~1%的空bar。
