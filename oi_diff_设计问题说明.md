# oi_diff 设计问题说明

## 问题发现

当前集合竞价bar的`oi_diff=0`会导致**跨时段的OI变化丢失**。

### 实际案例：IF1706

```
2016-10-24 15:00:00 - oi=193, oi_diff=2
--- 隔夜休市 ---
2016-10-25 09:30:00 - oi=2, oi_diff=0  ← 集合竞价bar
2016-10-25 09:31:00 - oi=2, oi_diff=0

问题：
  隔夜OI从193变到2，变化了-191手
  但这-191手的变化无法从bar数据中得知！
```

### 实际案例：rb2412（有夜盘）

```
2024-05-06 22:59:00 - oi=56447  (夜盘收盘)
--- 短暂休市 ---
2024-05-07 09:00:00 - oi=56447, oi_diff=0  ← 日盘集合竞价

幸运的是：
  隔夜OI没变化（56447→56447）
  所以没有数据丢失
```

## 当前设计的逻辑

### 代码实现（bar1min_aggregator.rs）

```rust
// 集合竞价bar（line 156）
open_interest_diff: 0,  // 固定为0

// 更新prev_last_tick（line 170）
self.prev_last_tick = Some(last_tick);

// 连续交易bar（line 261）
oi_diff = last_tick.oi - prev.oi  // 基于prev计算
```

### 设计意图

1. 集合竞价bar是"新时段的起点"
2. 无法将跨时段的变化归因到某个具体bar
3. 每个时段独立计算

## 问题分析

### 场景1：OI连续（rb2412夜盘→日盘）

```
夜盘最后: oi=56447
日盘集合竞价: oi=56447, oi_diff=0

实际变化：0手
丢失的信息：0手 ✅ 没有问题
```

### 场景2：OI跳变（IF1706隔日）

```
第一天收盘: oi=193
第二天集合竞价: oi=2, oi_diff=0

实际变化：-191手
丢失的信息：-191手 ❌ 问题！
```

## 改进方案

### 方案A：完全连续计算（推荐）

只有文件第一个bar的`oi_diff=0`，所有后续bar（包括集合竞价）都计算真实diff。

```rust
// 集合竞价bar
let oi_diff = if let Some(prev) = self.prev_last_tick {
    // 有前置bar，计算真实diff
    last_tick.open_interest.wrapping_sub(prev.open_interest) as i32
} else {
    // 文件第一个bar
    0
};
```

**效果**：
```
第一天收盘: oi=193, oi_diff=2
第二天集合竞价: oi=2, oi_diff=-191  ← 正确反映隔夜变化
第二天连续交易: oi=..., oi_diff=...  ← 正常计算
```

**优点**：
- 完整追踪整个合约生命周期的OI变化
- 所有oi_diff累加 = 总的OI变化
- 符合时间序列数据的惯例

**缺点**：
- 集合竞价bar的oi_diff可能很大（包含隔夜变化）
- 可能不太符合"集合竞价是新起点"的直觉

### 方案B：每日重置

每个交易日的第一个bar的`oi_diff=0`。

```rust
// 检测是否跨日
let is_new_day = if let Some(prev) = self.prev_last_tick {
    let prev_date = parse_date(prev.date);
    let curr_date = parse_date(tick.date);
    curr_date != prev_date
} else {
    true  // 第一个bar
};

let oi_diff = if is_new_day {
    0  // 新交易日，重置
} else {
    // 正常计算diff
    last_tick.oi - prev.oi
};
```

**效果**：
```
第一天收盘: oi=193, oi_diff=2
--- 新交易日 ---
第二天集合竞价: oi=2, oi_diff=0  ← 新交易日起点
第二天连续交易: oi=..., oi_diff=...  ← 基于集合竞价计算
```

**优点**：
- 每个交易日独立
- 符合"新交易日新起点"的概念

**缺点**：
- 跨日的OI变化仍然丢失
- 日内有夜盘时，夜盘→日盘的连续性被破坏

### 方案C：保持现状但文档说明

继续使用当前逻辑（集合竞价bar的oi_diff=0），但在文档中明确说明：

> ⚠️ 注意：集合竞价bar的oi_diff固定为0，跨时段（如隔夜）的OI变化无法从bar数据中追踪。
> 如需完整的OI变化信息，请使用tick数据。

## 推荐方案

**推荐方案A：完全连续计算**

理由：
1. **OI本质上是连续的**：交易所的OI在整个合约生命周期内连续变化
2. **数据完整性**：不应该丢失任何真实的市场变化信息
3. **符合时间序列惯例**：只有第一个数据点的diff为0
4. **便于分析**：可以通过累加oi_diff追踪总的持仓变化

实现位置：
- `bar1min_aggregator.rs` line 156 (flush_auction_bar方法)

## 验证

### 验证方案A是否正确

```python
# IF1706完整数据验证
文件第一个bar: oi=2, oi_diff=0
...
第一天收盘: oi=193, oi_diff=...
第二天集合竞价: oi=2, oi_diff=-191  ← 应该看到真实变化
...

# 累加所有oi_diff应该等于最后的oi - 第一个oi
sum(all oi_diff) = last_oi - first_oi
```

## 结论

当前设计会导致跨时段OI变化丢失，建议改为**方案A：完全连续计算**。

修改后：
- ✅ 第一个bar的oi_diff=0（观察起点）
- ✅ 所有后续bar（包括集合竞价）计算真实diff
- ✅ 完整追踪整个合约生命周期的OI变化

---

## ✅ 修复完成（2025-10-27）

### 修改内容

**bar1min_aggregator.rs** (line 140-146):
```rust
// 计算oi_diff：只有文件第一个bar为0，其余所有bar（包括集合竞价）计算真实diff
let oi_diff = if let Some(prev) = &self.prev_last_tick {
    last_tick.open_interest.wrapping_sub(prev.open_interest) as i32
} else {
    // 文件第一个bar，无前置数据
    0
};
```

### 验证结果

**测试用例1**：IF1706 2016-11-15→2016-11-16（有集合竞价）
```
前一天收盘(15:00): oi=718
集合竞价bar(09:30): oi=717, volume=1
实际变化: -1
记录的oi_diff: -1
✅ 正确！集合竞价bar正确捕捉隔夜OI变化
```

**测试用例2**：rb2412夜盘→日盘（OI连续）
```
夜盘收盘: oi=56447
日盘集合竞价: oi=56447
实际变化: 0
记录的oi_diff: 0
✅ 正确！OI无变化时oi_diff=0
```

### oi_diff计算方式说明

**重要**：oi_diff是从**tick数据**直接计算的，不是从bar的oi字段计算：

- **on_tick模式** (bar1min_aggregator.rs:269):
  ```rust
  oi_diff = last_tick.open_interest.wrapping_sub(prev.open_interest) as i32
  ```

- **on_batch模式** (parquet_processor_on_batch.rs:372):
  ```rust
  oi_diff = last_open_interest - prev_open_interest
  ```

这意味着：
- 真实bar的oi_diff**不受补全bar的oi字段错误影响**
- 补全bar本身的oi_diff=0（无tick数据）
- 所有真实bar的oi_diff都是从真实tick数据计算的

### 实现的方案

✅ **方案A：完全连续计算**

- 只有文件第一个bar的oi_diff=0
- 所有后续bar（包括集合竞价、跨日bar）都计算真实diff
- 完整追踪整个合约生命周期的OI变化
- sum(all oi_diff) = last_oi - first_oi
