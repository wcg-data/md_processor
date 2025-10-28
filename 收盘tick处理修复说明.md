# 收盘tick处理修复说明

## 问题概述

在期货市场数据处理中，收盘时刻（如23:00:00）的tick归属问题导致on_tick和on_batch处理器产生不一致的结果。

### 问题表现（修复前）

**rb2405 2024-03-06示例：**

| 处理器 | 23:00 bar | 23:01 bar | 09:00 bar | 说明 |
|--------|-----------|-----------|-----------|------|
| on_tick | 5523手 | 8手 | 690手 | 生成了不应存在的23:01 bar |
| on_batch | 5531手 | 无 | 690手 | 正确包含收盘tick |

**差异原因：**
- 23:00:00是收盘撮合tick（8手）
- on_tick: 23:00:00 → floor → 23:00窗口 → 文件结束时生成23:01 bar
- on_batch: 23:00:00 → 分配到22:59窗口 → 合并到23:00 bar ✅

## 行业标准

### 时间区间规则

根据Pandas、Alpaca Markets、CME、NinjaTrader等行业标准：
- **标准区间**: **左闭右开 [t, t+1)**
- **时间标注**: 使用结束时间（避免未来信息）
- **示例**: 9:31 bar = [9:30:00, 9:31:00)

### 收盘tick处理

专业平台（vnpy、tqsdk、米筐）的标准：
1. 收盘时刻的tick（如23:00:00）应包含在最后一个bar中
2. 不应生成超出交易时段的bar
3. 收盘撮合的成交量不应丢失

## 修复方案

### 方案选择

**方案A**: 全局改为左开右闭
❌ 不推荐 - 违反行业标准

**方案B**: 保持左闭右开 + 收盘tick特殊处理
✅ **采用** - 符合行业标准，避免数据丢失

### on_batch修复（✅ 已完成）

**位置**: `src/parquet_processor_on_batch.rs` Lines 223-249

**逻辑**:
```rust
// 检查是否是收盘时刻
let mut is_closing_tick = false;
for (_start, end) in &trading_ranges {
    if time_part == *end {
        is_closing_tick = true;
        break;
    }
}

if is_closing_tick {
    // 收盘tick合并到前一分钟窗口
    // 例如：23:00:00 → 分配到 22:59 窗口
    let prev_minute = time_part - chrono::Duration::minutes(1);
    let prev_minute_window = naive.date().and_time(prev_minute);
    prev_minute_window.format("%Y-%m-%d %H:%M:00").to_string()
}
```

**效果**:
- 23:00:00的收盘tick分配到22:59窗口
- 聚合后生成23:00 bar，volume包含收盘tick
- 不生成23:01 bar

### on_tick修复状态（⚠️ 需进一步优化）

**尝试的修复方案**:

#### 尝试1: 修改on_tick主逻辑
- 在tick到达时检测收盘tick
- 将收盘tick视为与当前分钟相同
- **问题**: 破坏了数据处理流程，导致volume计算错误

#### 尝试2: 修改flush() trade_time计算
- 使用current_bar_minute而不是last_tick时间
- **问题**: 仍生成23:01 bar，只是时间戳变了

#### 当前建议方案: 后处理合并
在parquet_processor_on_tick中：
1. 生成所有bars（包括23:01 bar）
2. 在DataFrame生成后，检查是否有超时段的bars
3. 将超时段bars的volume合并到前一个bar
4. 删除超时段bars
5. 应用fill_missing_minutes

**优势**:
- 不破坏现有的tick处理逻辑
- 简单、安全、可控
- 易于测试和验证

**待实现**: 需要添加`merge_closing_bars()`函数

## 技术细节

### 收盘时刻识别

**通用函数**:
```rust
fn is_closing_time(tick: &TickData, tick_datetime: &NaiveDateTime) -> bool {
    let comd = to_string_field(&tick.comd);
    let tick_time = tick_datetime.time();

    let trading_ranges = TRADE_SESSION_MAP.get_trading_ranges(&comd);

    for (_start, end) in trading_ranges {
        if tick_time == end {
            return true;
        }
    }
    false
}
```

**特点**:
- 从trade_session.csv获取配置
- 支持多时段品种（夜盘+日盘）
- 品种间收盘时间可能不同

### 时间戳标注

**规则**: 使用窗口结束时间（current_bar_minute + 1）

**示例**:
```
tick范围              窗口时间    bar.trade_time
22:59:00-22:59:59.999  22:59  →  23:00:00
23:00:00（收盘）        22:59  →  23:00:00（合并）
09:25-09:29（集合竞价）   09:29  →  09:30:00
```

## 测试验证

### 关键测试用例

**rb2405 2024-03-06夜盘收盘→日盘开盘**:
```
原始tick数据：
22:59:59.500  volume=428,851  ← 22:59窗口最后tick
23:00:00      volume=428,859  ← 收盘撮合（+8手）
08:59:00.500  volume=429,549  ← 集合竞价

预期结果：
23:00 bar: volume=5531手（包含收盘tick）
23:01 bar: 不存在
09:00 bar: volume=690手（429549-428859）
```

### 验证脚本

```python
import polars as pl

df_tick = pl.read_parquet("rb2405_1min_on_tick.parquet")
df_batch = pl.read_parquet("rb2405_1min.parquet")

# 检查23:00 bar
tick_23 = df_tick.filter(pl.col("trade_time") == "2024-03-05 23:00:00")["volume"][0]
batch_23 = df_batch.filter(pl.col("trade_time") == "2024-03-05 23:00:00")["volume"][0]
assert tick_23 == batch_23 == 5531, "23:00 bar应包含收盘tick"

# 检查23:01 bar不存在
tick_23_01 = "2024-03-05 23:01:00" in df_tick["trade_time"].to_list()
batch_23_01 = "2024-03-05 23:01:00" in df_batch["trade_time"].to_list()
assert not tick_23_01 and not batch_23_01, "23:01 bar不应存在"

# 检查09:00 bar一致
tick_09 = df_tick.filter(pl.col("trade_time") == "2024-03-06 09:00:00")["volume"][0]
batch_09 = df_batch.filter(pl.col("trade_time") == "2024-03-06 09:00:00")["volume"][0]
assert tick_09 == batch_09 == 690, "09:00集合竞价bar应一致"
```

## 当前状态

- ✅ **on_batch**: 完全修复，符合标准
- ⚠️ **on_tick**: 需要实现后处理合并方案
- ✅ **通用函数**: is_closing_time已实现
- ✅ **文档**: 本文档已完成

## 下一步

1. 实现on_tick的后处理合并逻辑
2. 全面测试多个品种和交易所
3. 更新CLAUDE.md和相关文档
4. 提交代码并创建修复说明commit

##参考资料

- [Pandas resample文档](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.resample.html)
- [Alpaca Markets分钟bar规范](https://alpaca.markets/learn/stock-minute-bars)
- trade_session.csv - 交易时段配置（168品种）

---

**文档版本**: 1.0
**创建日期**: 2025-10-28
**最后更新**: 2025-10-28
