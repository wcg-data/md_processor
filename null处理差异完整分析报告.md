# on_batch vs on_tick null处理差异完整分析报告

**生成时间**: 2025-10-31
**调查合约**: au2502（黄金2502合约，142133个bar）
**抽样验证**: 500个随机合约

---

## 执行摘要

经过深度调查，发现on_batch和on_tick存在**3个核心差异**：

| 差异类型 | 影响字段 | 影响范围 | 性质 | 优先级 |
|---------|---------|---------|------|--------|
| Bug #1 | `pre_settle` | 100%合约，100%bar | 🔴 **Bug** | **P0 - 必须修复** |
| Bug #2 | `prev_close` | ~89%合约，集合竞价bar | 🟡 **设计差异** | **P1 - 建议修复** |
| Bug #3 | `log_return` | ~89%合约，集合竞价bar | 🟢 **派生影响** | **P2 - 跟随修复** |

---

## 差异详细分析

### Bug #1: pre_settle字段的null处理错误 🔴

#### 问题描述

**原始数据**: au2502的所有11,637,065个tick的pre_settle字段**全部为null**

**on_batch处理** ✅ 正确：
- 聚合null得到null
- 输出的142,133个bar的pre_settle全部为null

**on_tick处理** ❌ 错误：
- 读取时：null被转为0.0
- 输出的142,133个bar的pre_settle全部为0.0

#### 根本原因

**代码位置**: `src/parquet_processor_on_tick.rs`

```rust
// 第84-86行：读取f64时的问题
let get_f64 = |idx| match row.0[idx] {
    AnyValue::Float64(v) => v,
    _ => 0.0,  // ← BUG: null被转成0.0，应该转成f64::NAN
};

// 第31行：输出时未处理NaN→None
Series::new("pre_settle", bars.iter().map(|b| b.pre_settle).collect::<Vec<_>>()),

// 应该像prev_close一样处理（第30行）：
Series::new("prev_close", bars.iter().map(|b|
    if b.prev_close.is_nan() { None } else { Some(b.prev_close) }
).collect::<Vec<_>>()),
```

#### 影响评估

- **影响范围**: 所有合约的所有bar（100%）
- **数据错误**: null被错误地存储为0.0
- **业务影响**:
  - ✅ 不影响核心交易数据（OHLC、volume等）
  - ⚠️ 如果策略使用pre_settle字段，会读到错误的0.0而不是null
  - ⚠️ 数据语义错误（0.0和null含义完全不同）

#### 修复方案

**步骤1**: 修改`get_f64`函数处理null

```rust
let get_f64 = |idx| match row.0[idx] {
    AnyValue::Float64(v) => v,
    AnyValue::Null => f64::NAN,  // ← 修复：null → NaN
    _ => 0.0,
};
```

**步骤2**: 修改输出时处理NaN

```rust
Series::new("pre_settle", bars.iter().map(|b|
    if b.pre_settle.is_nan() { None } else { Some(b.pre_settle) }
).collect::<Vec<_>>()),
```

**步骤3**: 同时检查其他f64字段
- `bid_price_1`
- `ask_price_1`
- `mid_price`
- `vwap`

---

### Bug #2: prev_close字段的设计差异 🟡

#### 问题描述

**测试合约**: au2502（142,133个bar）

**on_batch处理** ✅ 推荐：
- 使用Polars的shift操作：`col("close").shift(lit(1)).alias("prev_close")`
- prev_close = 前一个bar的close值
- **只有2个null**：第0行（第一个bar）和第3行（第一个有成交的bar）
- 集合竞价bar的prev_close = 前一交易时段的收盘价

**on_tick处理** ⚠️ 存疑：
- 在`bar1min_aggregator.rs:260`刻意设置：`prev_close: f64::NAN`
- 注释说明："集合竞价bar无prev_close"
- **507个null**：包括所有集合竞价bar（09:00/21:00）
- 具体分布：
  - 255个09:00（日盘集合竞价）
  - 251个21:00（夜盘集合竞价）
  - 1个21:03（第一个真实bar）

#### 数据验证

**on_batch示例**（2024-01-16）:

```
时间          | Close  | Prev_Close | Volume | 说明
02:30:00      | 486.30 | 486.30     | 0      | 夜盘收盘
09:00:00      | 486.30 | 486.30     | 0      | ← 集合竞价，prev_close有值
09:01:00      | 486.30 | 486.30     | 0      |
15:00:00      | 485.78 | 485.78     | 0      | 日盘收盘
21:00:00      | 485.78 | 485.78     | 0      | ← 集合竞价，prev_close有值
```

**on_tick示例**（2024-01-16）:

```
时间          | Close  | Prev_Close | Volume | 说明
02:30:00      | 486.30 | 486.30     | 0      | 夜盘收盘
09:00:00      | 486.30 | None       | 0      | ← 集合竞价，prev_close=null
09:01:00      | 486.30 | 486.30     | 0      |
15:00:00      | 485.78 | 485.78     | 0      | 日盘收盘
21:00:00      | 485.78 | None       | 0      | ← 集合竞价，prev_close=null
```

#### 业务语义分析

**集合竞价bar应该有prev_close吗？**

从业务角度分析：

1. ✅ **应该有prev_close**：
   - 集合竞价bar代表集合竞价阶段的成交
   - 它的"前一个收盘价"是明确的（前一交易时段的收盘价）
   - 可以计算集合竞价的价格变动率
   - 与常规bar保持一致的数据结构

2. ❌ **不应该有prev_close**（on_tick当前逻辑）：
   - 集合竞价是新交易时段的开始
   - 可能认为不应该与前一时段的收盘价关联
   - 但这个理由不够充分

**结论**: on_batch的做法更合理，集合竞价bar应该有prev_close

#### 影响评估

- **影响范围**: ~89%的合约（有集合竞价的合约）
- **影响bar数**: 每个交易日2个（09:00 + 21:00），占总bar的~0.4%
- **业务影响**:
  - ⚠️ 集合竞价bar无法计算收益率
  - ⚠️ 策略如果依赖prev_close会遇到null
  - ⚠️ 与on_batch不一致

#### 修复方案

**方案A: 修改bar1min_aggregator.rs（推荐）**

在生成集合竞价bar时，传入正确的prev_close值而不是NaN：

```rust
// 当前代码（bar1min_aggregator.rs:260）
prev_close: f64::NAN,  // 集合竞价bar无prev_close

// 修复后：从上下文获取prev_close
prev_close: self.get_prev_close(),  // 获取前一个bar的close
```

需要在`Bar1MinAggregator`中维护前一个bar的close值。

**方案B: 在输出后补全（简单但不推荐）**

在`parquet_processor_on_tick.rs`中，输出DataFrame后使用shift补全：

```rust
let df = build_bars_dataframe(&bars)?;
let df = df.lazy()
    .with_column(
        when(col("prev_close").is_null())
            .then(col("close").shift(lit(1)))
            .otherwise(col("prev_close"))
            .alias("prev_close")
    )
    .collect()?;
```

---

### Bug #3: log_return字段的null差异 🟢

#### 问题描述

log_return字段依赖prev_close计算，所以null位置跟随prev_close。

**计算公式**: `log_return = ln(close / prev_close)`

**on_batch**: 2个null
**on_tick**: 507个null（所有集合竞价bar + 第一个bar）

#### 影响评估

- **派生影响**: 完全由prev_close决定
- **业务影响**: 集合竞价bar无法计算收益率

#### 修复方案

修复prev_close后，log_return会自动修复。

---

## 其他字段检查

### bid_price_1, ask_price_1, mid_price

深度对比显示这些字段有"null处理差异"，但实际验证：

**au2502验证结果**:
- null数量：batch=0, tick=0（都没有null）
- 数值差异：最大差异 0.00e+00（完全一致）

**结论**:
- ✅ 这些字段的数值完全一致
- ⚠️ 对比脚本可能存在NaN比较的误报
- 🔍 需要进一步验证是否是脚本问题

---

## 统计汇总

### 字段差异统计（基于500个随机抽样）

| 字段 | 有差异的合约数 | 占比 | 差异类型 |
|------|---------------|------|---------|
| `pre_settle` | 492/500 | 98.4% | null→0.0 |
| `mid_price` | 466/500 | 93.2% | 可能误报 |
| `ask_price_1` | 453/500 | 90.6% | 可能误报 |
| `prev_close` | 443/500 | 88.6% | 集合竞价null |
| `log_return` | 443/500 | 88.6% | 派生影响 |
| `bid_price_1` | 442/500 | 88.4% | 可能误报 |

### 核心数据一致性 ✅

以下字段**完全一致**（数值完全相同）：

| 字段 | 一致性 | 验证结果 |
|------|--------|---------|
| `open` | ✅ 100% | 最大差异 < 1e-10 |
| `high` | ✅ 100% | 最大差异 < 1e-10 |
| `low` | ✅ 100% | 最大差异 < 1e-10 |
| `close` | ✅ 100% | 最大差异 < 1e-10 |
| `volume` | ✅ 100% | 完全一致 |
| `turnover` | ✅ 100% | 最大差异 < 1e-10 |
| `open_interest` | ✅ 100% | 完全一致 |
| `open_interest_diff` | ✅ 100% | 完全一致 |

---

## 修复优先级和计划

### P0 - 必须立即修复 🔴

**Bug #1: pre_settle的null处理**

- **影响**: 100%合约，数据语义错误
- **修复难度**: 低（2处代码修改）
- **预计耗时**: 30分钟
- **验证方法**: 重新处理au2502，检查pre_settle全为null

### P1 - 强烈建议修复 🟡

**Bug #2: prev_close的集合竞价bar处理**

- **影响**: ~89%合约，集合竞价bar无法计算收益率
- **修复难度**: 中（需要维护状态）
- **预计耗时**: 1-2小时
- **验证方法**: 重新处理au2502，检查集合竞价bar的prev_close值

### P2 - 自动修复 🟢

**Bug #3: log_return**

- 修复prev_close后自动修复
- 无需额外工作

### P3 - 调查验证 🔵

**bid/ask/mid_price的"差异"**

- **可能性**: 对比脚本误报
- **行动**: 修复对比脚本的NaN处理逻辑
- **验证**: 重新深度对比

---

## 修复后的预期结果

修复所有bug后，预期达到：

### 完美一致 ✅

- ✅ Bar数量100%一致
- ✅ 所有时间点100%一致
- ✅ 核心数据字段100%一致
- ✅ 辅助字段的null位置100%一致
- ✅ 辅助字段的数值100%一致

### 一致性指标

| 指标 | 修复前 | 修复后（预期） |
|------|--------|---------------|
| Bar数量一致率 | 100% | 100% |
| 核心数据一致率 | 100% | 100% |
| 所有字段一致率 | ~2% | **100%** |
| null位置一致率 | ~11% | **100%** |

---

## 测试用例

### 测试用例1: pre_settle修复验证

```bash
# 修复前
python3 << 'EOF'
import polars as pl
tick = pl.read_parquet('.../au2502_1min.parquet')
print(f"pre_settle null数: {tick['pre_settle'].null_count()}")  # 预期: 0（全是0.0）
print(f"pre_settle=0.0数: {(tick['pre_settle'] == 0.0).sum()}")  # 预期: 142133
EOF

# 修复后
python3 << 'EOF'
import polars as pl
tick = pl.read_parquet('.../au2502_1min.parquet')
print(f"pre_settle null数: {tick['pre_settle'].null_count()}")  # 预期: 142133（全null）
print(f"pre_settle=0.0数: {(tick['pre_settle'] == 0.0).sum()}")  # 预期: 0
EOF
```

### 测试用例2: prev_close修复验证

```bash
# 修复前
python3 << 'EOF'
import polars as pl
tick = pl.read_parquet('.../au2502_1min.parquet')
auction = tick.filter(
    (pl.col('trade_time').str.contains('09:00:00')) |
    (pl.col('trade_time').str.contains('21:00:00'))
)
print(f"集合竞价bar数: {len(auction)}")  # 预期: ~510
print(f"prev_close null数: {auction['prev_close'].null_count()}")  # 预期: ~506
EOF

# 修复后
python3 << 'EOF'
import polars as pl
tick = pl.read_parquet('.../au2502_1min.parquet')
auction = tick.filter(
    (pl.col('trade_time').str.contains('09:00:00')) |
    (pl.col('trade_time').str.contains('21:00:00'))
)
print(f"集合竞价bar数: {len(auction)}")  # 预期: ~510
print(f"prev_close null数: {auction['prev_close'].null_count()}")  # 预期: 0（都有值）
EOF
```

### 测试用例3: 完整一致性验证

```bash
# 重新生成on_tick数据
./target/release/parquet_processor_on_tick dongzheng_data 1min au2502

# 深度对比
python3 script/deep_compare_batch_tick.py --contracts au2502 --show-details

# 预期输出:
# ✅ 完美一致！所有字段、所有行的值和null位置都完全相同。
```

---

## 下一步行动

### 立即行动（今天）

1. ✅ **完成调查** - 已完成
2. ✅ **生成报告** - 已完成
3. ⏳ **修复Bug #1（pre_settle）** - 待执行
   - 修改`get_f64`函数
   - 修改输出Series处理
   - 编译测试

### 短期计划（本周）

4. ⏳ **修复Bug #2（prev_close）** - 待执行
   - 设计状态维护方案
   - 修改bar1min_aggregator
   - 测试验证

5. ⏳ **验证修复效果** - 待执行
   - 重新处理测试合约
   - 深度对比验证
   - 确认完美一致

### 中期计划（下周）

6. ⏳ **重新生成全量数据** - 待执行
   - 修复所有bug后
   - 使用230核心重新处理
   - 全量深度对比

7. ⏳ **添加自动化测试** - 待执行
   - 单元测试：null处理函数
   - 集成测试：on_batch vs on_tick
   - 回归测试：防止未来bug

---

## 附录：调查过程记录

### 调查步骤

1. ✅ 初步对比：发现8656个合约bar数量一致，但字段有差异
2. ✅ 深度对比：发现6个字段有差异，主要是null位置
3. ✅ 手动验证au2502：
   - pre_settle: batch全null, tick全0.0
   - prev_close: batch 2个null, tick 507个null
4. ✅ 分析prev_close的507个null位置：
   - 255个09:00（日盘集合竞价）
   - 251个21:00（夜盘集合竞价）
   - 1个21:03（第一个bar）
5. ✅ 代码审查：
   - 找到pre_settle的bug（get_f64）
   - 找到prev_close的设计差异（bar1min_aggregator.rs:260）
6. ✅ 业务语义分析：确定on_batch的做法更合理

### 关键发现

- 核心交易数据（OHLC、volume等）完全一致 ✅
- null处理存在3个差异，2个需要修复 ⚠️
- on_batch使用向量化shift，on_tick手动维护状态 📝
- 集合竞价bar的prev_close业务语义需要明确 💡

---

**报告生成**: Claude Code @ 2025-10-31
**数据来源**:
- au2502合约详细分析
- 500个随机抽样合约统计
- 代码审查：bar1min_aggregator.rs, parquet_processor_on_batch.rs, parquet_processor_on_tick.rs
