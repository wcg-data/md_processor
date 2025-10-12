# validate_tick 过滤规则文档

## 概述

`validate_tick` 是 `bar1min_aggregator.rs` 中定义的 tick 数据验证函数，被以下模块共享使用：

- **parquet_processor_on_tick.rs** - 流式处理器（直接调用）
- **parquet_processor_on_batch.rs** - 批量处理器（使用等价的向量化过滤逻辑）
- **aggregator_manager.rs** - 聚合管理器（实时处理）

**位置**: `/root/project/md_processor/src/bar1min_aggregator.rs:214-325`

## 设计原则

### 1. 快速失败路径
最常用的字段优先检查，提高过滤效率

### 2. 与 on_batch 保持一致
确保流式处理和批量处理的过滤逻辑完全相同

### 3. 性能优化
- 字节级检查避免字符串转换
- 延迟昂贵操作（如时间解析）
- 使用 `#[inline]` 优化函数调用

## 启用的过滤规则

### 规则 1: 收盘价有效性检查 ⭐最重要

```rust
if tick.close <= 0.0 || !tick.close.is_finite() {
    return false;
}
```

**目的**: 收盘价是最关键的字段，必须有效且为正数

**过滤条件**:
- ❌ `close <= 0.0` - 价格必须大于0
- ❌ `close` 为 `NaN` 或 `Infinity` - 必须是有限数值

**示例**:
| close 值 | 是否通过 | 原因 |
|----------|---------|------|
| 100.5 | ✅ | 有效正数 |
| 0.0 | ❌ | 价格 <= 0 |
| -10.0 | ❌ | 负数价格 |
| NaN | ❌ | 非有限数值 |
| Infinity | ❌ | 非有限数值 |

---

### 规则 2: 字段非空检查（字节级快速检查）

```rust
if tick.comd[0] == 0 { return false; }       // 品种代码不能为空
if tick.contract[0] == 0 { return false; }   // 合约代码不能为空
if tick.trade_time[0] == 0 { return false; } // 交易时间不能为空
```

**目的**: 确保基础标识字段不为空

**技术细节**:
- 使用字节级检查 `[0] == 0`，避免完整字符串转换
- 检查固定长度数组的第一个字节是否为 `\0`（空字符）

**过滤条件**:
- ❌ `comd` 为空（品种代码）
- ❌ `contract` 为空（合约代码）
- ❌ `trade_time` 为空（交易时间）

**示例**:
| 字段 | 值 | 是否通过 | 原因 |
|------|-----|---------|------|
| comd | "rb" | ✅ | 有效品种 |
| comd | "" | ❌ | 空字符串 |
| contract | "rb2510" | ✅ | 有效合约 |
| contract | "" | ❌ | 空字符串 |

---

### 规则 3: 成交额非负检查

```rust
if tick.turnover < 0.0 { return false; }
```

**目的**: 成交额不能为负数（允许为 0）

**过滤条件**:
- ❌ `turnover < 0.0` - 成交额为负数

**示例**:
| turnover 值 | 是否通过 | 原因 |
|------------|---------|------|
| 1000000.0 | ✅ | 正常成交额 |
| 0.0 | ✅ | 无成交，允许为0 |
| -500.0 | ❌ | 负数成交额 |

---

### 规则 4: bid/ask 价格有效性检查

```rust
if tick.bid_price_1 < 0.0 || tick.bid_price_1.is_infinite() { return false; }
if tick.ask_price_1 < 0.0 || tick.ask_price_1.is_infinite() { return false; }
```

**目的**: 买卖价格必须非负且非无穷（允许 0 和 NaN）

**过滤条件**:
- ❌ `bid_price_1 < 0.0` - 买价为负数
- ❌ `bid_price_1` 为 `Infinity`
- ❌ `ask_price_1 < 0.0` - 卖价为负数
- ❌ `ask_price_1` 为 `Infinity`

**允许情况**:
- ✅ `bid_price_1 = 0.0` - 允许（无买盘）
- ✅ `ask_price_1 = 0.0` - 允许（无卖盘）
- ✅ `bid_price_1 = NaN` - 允许（原始数据中的无买盘）
- ✅ `ask_price_1 = NaN` - 允许（原始数据中的无卖盘）

**示例**:
| bid_price_1 | ask_price_1 | 是否通过 | 原因 |
|-------------|-------------|---------|------|
| 100.5 | 100.6 | ✅ | 正常买卖价 |
| 0.0 | 100.6 | ✅ | 无买盘，允许 |
| 100.5 | 0.0 | ✅ | 无卖盘，允许 |
| NaN | 100.6 | ✅ | 原始NaN，允许 |
| 100.5 | NaN | ✅ | 原始NaN，允许 |
| -1.0 | 100.6 | ❌ | 买价为负 |
| Infinity | 100.6 | ❌ | 买价为无穷 |
| 100.5 | Infinity | ❌ | 卖价为无穷 |

---

### 规则 5: bid <= ask 检查（条件性）

```rust
// bid <= ask 检查：只在bid和ask都>0时才要求bid<=ask
// 如果bid=0或ask=0，则跳过检查；只有当两者都>0时才要求bid<=ask
if tick.bid_price_1 > 0.0 && tick.ask_price_1 > 0.0 && tick.bid_price_1 > tick.ask_price_1 {
    return false;
}
```

**目的**: 当买卖价都有效时，买价不能高于卖价

**检查条件** (需要同时满足才触发检查):
1. `bid_price_1 > 0.0` - 买价有效
2. `ask_price_1 > 0.0` - 卖价有效
3. `bid_price_1 > ask_price_1` - 买价 > 卖价（异常）

**过滤条件**:
- ❌ bid > ask（当两者都 > 0 时）

**跳过检查的情况**:
- ✅ `bid = 0` 或 `ask = 0` - 不检查（缺少买/卖盘）

**示例**:
| bid_price_1 | ask_price_1 | 是否通过 | 原因 |
|-------------|-------------|---------|------|
| 100.5 | 100.6 | ✅ | bid < ask，正常 |
| 100.5 | 100.5 | ✅ | bid = ask，允许 |
| 100.6 | 100.5 | ❌ | bid > ask，异常 |
| 0.0 | 100.5 | ✅ | bid=0，跳过检查 |
| 100.5 | 0.0 | ✅ | ask=0，跳过检查 |
| 0.0 | 0.0 | ✅ | 两者都=0，跳过检查 |

---

### 规则 6: bid/ask 价格清理（0值或NaN转为NULL，volume清零）

```rust
// 清理bid/ask为0或NaN的情况：将价格设为NaN，volume设为0（Parquet会存储为NULL）
// 这样在读取时会正确识别为空值，统一表示"无买/卖盘"
if tick.bid_price_1 == 0.0 || tick.bid_price_1.is_nan() {
    tick.bid_price_1 = f64::NAN;
    tick.bid_volume_1 = 0;
}
if tick.ask_price_1 == 0.0 || tick.ask_price_1.is_nan() {
    tick.ask_price_1 = f64::NAN;
    tick.ask_volume_1 = 0;
}
```

**目的**: 统一处理0和NaN两种"无买/卖盘"表示，确保volume也为0

**转换逻辑**:
- `bid_price_1 = 0.0` 或 `NaN` → `NaN` (Parquet存储为NULL)
- 对应的 `bid_volume_1` 设为 `0`
- `ask_price_1 = 0.0` 或 `NaN` → `NaN` (Parquet存储为NULL)
- 对应的 `ask_volume_1` 设为 `0`

**重要**:
- NaN在Parquet中会被存储为NULL，读取时也会正确识别为NULL
- 原始数据中的NaN也需要清理对应的volume

**示例**:
| 原始bid_price_1 | 原始bid_volume_1 | 清理后bid_price_1 | 清理后bid_volume_1 | 说明 |
|----------------|-----------------|------------------|-------------------|------|
| 100.5 | 10 | 100.5 | 10 | 正常，不修改 |
| 0.0 | 5 | NaN (NULL) | 0 | 价格0，volume清零 |
| NaN | 5 | NaN (NULL) | 0 | 原始NaN，volume清零 |
| 100.5 | 0 | 100.5 | 0 | 价格有效，volume保持 |

**on_batch等价实现**:
```rust
let df = df.lazy()
    .with_columns([
        // 将bid_price_1=0或NaN转为NaN，并将对应的volume设为0
        when(col("bid_price_1").eq(lit(0.0)).or(col("bid_price_1").is_nan()))
            .then(lit(f64::NAN))
            .otherwise(col("bid_price_1"))
            .alias("bid_price_1_cleaned"),
        when(col("ask_price_1").eq(lit(0.0)).or(col("ask_price_1").is_nan()))
            .then(lit(f64::NAN))
            .otherwise(col("ask_price_1"))
            .alias("ask_price_1_cleaned"),
        // volume也同步设为0
        when(col("bid_price_1").eq(lit(0.0)).or(col("bid_price_1").is_nan()))
            .then(lit(0u32))
            .otherwise(col("bid_volume_1"))
            .alias("bid_volume_1_cleaned"),
        when(col("ask_price_1").eq(lit(0.0)).or(col("ask_price_1").is_nan()))
            .then(lit(0u32))
            .otherwise(col("ask_volume_1"))
            .alias("ask_volume_1_cleaned"),
    ])
```

**验证结果**:
- bb1710: 前10行中有多个ask_price_1被转为NULL
- y1701: 无0值，无转换
- 两个处理器的NULL位置完全一致 ✓

---

### 规则 7: mid_price 重新计算 ✨新启用

```rust
// 重新计算mid_price：只有bid和ask都有效时才计算，否则设为NaN
// 修正原始数据中当ask=0时mid_price=bid/2的错误逻辑
if tick.bid_price_1.is_nan() || tick.ask_price_1.is_nan() {
    tick.mid_price = f64::NAN;
} else {
    tick.mid_price = (tick.bid_price_1 + tick.ask_price_1) / 2.0;
}
```

**目的**: 修正原始tick数据中的错误mid_price计算（当ask=0时原数据有mid_price=bid/2的错误）

**计算逻辑**:
- 只有当 `bid_price_1` 和 `ask_price_1` **都不是NaN** 时，计算 `mid_price = (bid + ask) / 2`
- 否则，`mid_price` 设为 `NaN`（Parquet存储为NULL）

**重要说明**:
- 此步骤在规则6（bid/ask价格清理）**之后**执行
- 因此0价格已被转为NaN，mid_price会正确处理

**示例**:
| bid_price_1 | ask_price_1 | 原始mid_price | 重新计算后mid_price | 说明 |
|-------------|-------------|--------------|-------------------|------|
| 100.5 | 100.6 | 100.55 | 100.55 | 正常计算 |
| NaN (原0) | 100.6 | 50.25（错误） | NaN (NULL) | bid为空，设为NULL ✓ |
| 100.5 | NaN (原0) | 50.25（错误） | NaN (NULL) | ask为空，设为NULL ✓ |
| NaN | NaN | NaN | NaN (NULL) | 两者都为空 |

**on_batch等价实现**:
```rust
// 在bid/ask清理之后添加此步骤
let df = df.lazy()
    .with_columns([
        when(
            col("bid_price_1").is_not_nan().and(col("ask_price_1").is_not_nan())
        )
        .then((col("bid_price_1") + col("ask_price_1")) / lit(2.0))
        .otherwise(lit(f64::NAN))
        .alias("mid_price")
    ])
    .collect()?;
```

**验证结果** (bb1710):
- ask_price_1=NaN 的行数: 5,137
- 修复前: 5,137 行的 mid_price = bid_price_1 / 2 (错误)
- 修复后: 5,137 行的 mid_price = NaN ✓
- on_batch 和 on_tick 结果完全一致 ✓

---

## 已禁用的过滤规则（注释掉）

以下规则在代码中被注释掉，当前**不生效**：

### ❌ 规则 A: open/high/low 价格检查

```rust
// if tick.open <= 0.0 { return false; }
// if tick.high <= 0.0 { return false; }
// if tick.low <= 0.0 { return false; }
```

**禁用原因**: 这些字段在某些数据源中可能无效或缺失

---

### ❌ 规则 B: 昨日结算价检查

```rust
// if !tick.pre_settle.is_finite() || tick.pre_settle <= 0.0 { return false; }
```

**禁用原因**: `pre_settle` 字段在某些合约中为 NULL 或无效

---

### ❌ 规则 C: 价格区间逻辑检查

```rust
// if tick.open.is_finite() && tick.low.is_finite() && tick.high.is_finite() {
//     if tick.high < tick.low { return false; }              // high 必须 ≥ low
//     if tick.open < tick.low || tick.open > tick.high {     // open 必须在区间内
//         return false;
//     }
//     if tick.close < tick.low || tick.close > tick.high {   // close 必须在区间内
//         return false;
//     }
// }
```

**禁用原因**: open/high/low 字段不可靠，可能导致误过滤

---

### ❌ 规则 D: volume 和 turnover 一致性检查

```rust
// if !(tick.volume == 0 && (tick.turnover == 0.0 || tick.turnover.is_nan())
//     || (tick.volume > 0 && tick.turnover > 0.0)) {
//     return false;
// }
```

**禁用原因**:
- 数据源中存在 volume=0 但 turnover>0 的情况
- 过于严格会导致大量有效数据被过滤

---

### ❌ 规则 E: 成交均价区间检查

```rust
// if tick.volume > 0
//     && tick.turnover.is_finite()
//     && tick.low.is_finite()
//     && tick.high.is_finite()
// {
//     let avg_price = tick.turnover / tick.volume as f64;
//     if avg_price < tick.low - eps || avg_price > tick.high + eps {
//         return false;
//     }
// }
```

**禁用原因**: 依赖 high/low 字段，不可靠

---

### ❌ 规则 F: 交易时段检查

```rust
// let dt = match Self::parse_datetime_from_tick(tick) {
//     Some(t) => t,
//     None => return false,
// };
//
// let comd = to_string_field(&tick.comd);
// let trading_ranges = TRADE_SESSION_MAP.get_trading_ranges(&comd);
// if trading_ranges.is_empty() {
//     return true;  // 没有配置则不过滤
// }
// TRADE_SESSION_MAP.is_in_trading_session(&comd, dt.naive_utc())
```

**禁用原因**:
- 最昂贵的操作（时间解析 + 字符串分配 + 交易时段查询）
- 与 on_batch 保持一致（批量处理不做时段检查）
- 交易时段配置可能不完整

---

## on_batch 中的等价实现

`parquet_processor_on_batch.rs` 使用 Polars 的向量化过滤，逻辑与 `validate_tick` 完全一致：

```rust
let mut result_df = LazyFrame::scan_parquet(path_str, Default::default())?
    .select([...])
    .filter(
        // 规则 1: close 有效性
        col("close").is_not_null()
            .and(col("close").gt(lit(0.0)))
            .and(col("close").is_finite())

        // 规则 2: 字段非空（隐式：Parquet 读取时已确保）

        // 规则 3: turnover 非负
            .and(col("turnover").is_not_null())
            .and(col("turnover").gt_eq(lit(0.0)))

        // 规则 4: bid/ask 有效性
            .and(col("bid_price_1").is_not_null())
            .and(col("bid_price_1").gt_eq(lit(0.0)))  // bid >= 0（允许0和NaN）
            .and((col("bid_price_1").is_nan()).or(col("bid_price_1").is_finite()))  // 允许NaN，拒绝Infinity
            .and(col("ask_price_1").is_not_null())
            .and(col("ask_price_1").gt_eq(lit(0.0)))  // ask >= 0（允许0和NaN）
            .and((col("ask_price_1").is_nan()).or(col("ask_price_1").is_finite()))  // 允许NaN，拒绝Infinity

        // 规则 5: bid <= ask（条件性）
            .and(
                col("bid_price_1").eq(lit(0.0))  // bid=0, 通过
                .or(col("ask_price_1").eq(lit(0.0)))  // ask=0, 通过
                .or(col("bid_price_1").lt_eq(col("ask_price_1")))  // bid <= ask, 通过
            )
    )
```

## 过滤统计

### 典型数据集过滤情况

以 bb1710 合约为例（共 5,229 行）：

| 过滤规则 | 被过滤行数 | 占比 |
|---------|-----------|------|
| close 无效 | 0 | 0.00% |
| 字段为空 | 0 | 0.00% |
| turnover < 0 | 0 | 0.00% |
| bid/ask < 0 或 Infinity | 0 | 0.00% |
| bid > ask（都>0时） | 0 | 0.00% |
| **总计** | **0** | **0.00%** |

**结论**: 当前启用的过滤规则对高质量数据集几乎无影响

### 如果启用所有规则的预估影响

| 额外规则 | 预估被过滤行数 | 影响 |
|---------|--------------|------|
| open/high/low 检查 | ~100-500 | 中等 |
| pre_settle 检查 | ~120-236 | 中等 |
| volume/turnover 一致性 | ~50-200 | 低 |
| 价格区间逻辑 | ~200-800 | 高 |
| 交易时段检查 | ~1000-2000 | 极高 |

**注意**: 这些规则被禁用是因为会错误过滤大量有效数据

## 使用建议

### 1. 保持 on_tick 和 on_batch 一致

**重要**: 任何对 `validate_tick` 的修改都需要同步更新 `parquet_processor_on_batch.rs` 中的过滤逻辑

### 2. 谨慎启用已禁用规则

启用前应：
1. 在测试数据集上验证影响
2. 分析被过滤数据的合理性
3. 使用对比工具验证一致性

### 3. 性能考虑

当前启用的规则都是低成本检查：
- 字节级检查（无字符串分配）
- 简单数值比较（无复杂计算）
- 早期失败（最常失败的规则在前）

**禁用的昂贵操作**:
- ❌ 时间解析（~100ns per tick）
- ❌ 字符串分配（~50ns per tick）
- ❌ 交易时段查询（~200ns per tick）

### 4. 调试建议

如果发现数据被意外过滤，检查顺序：

1. **检查 close 价格**: 最常见的过滤原因
   ```rust
   if tick.close <= 0.0 || !tick.close.is_finite() { ... }
   ```

2. **检查 bid/ask 价格**: 第二常见的过滤原因
   ```rust
   if tick.bid_price_1 < 0.0 || tick.ask_price_1 < 0.0 { ... }
   ```

3. **检查 bid > ask**: 数据异常导致
   ```rust
   if bid > 0 && ask > 0 && bid > ask { ... }
   ```

## 版本历史

### v1.2 (当前版本) - 2025-01
- **规则4更新**: 将 `!is_finite()` 改为 `is_infinite()`，允许 NaN 通过
- **规则6更新**: 增加 NaN 处理，统一清理 0 和 NaN 价格对应的 volume
- **规则7新增**: mid_price 重新计算逻辑
- 启用 7 个过滤规则（新增2个）
- 禁用 7 个严格规则
- 与 on_batch 保持完全一致

### v1.1 - 2024-12
- **规则6新增**: bid/ask 价格清理（0值转为NULL）
- 启用 6 个基础过滤规则

### v1.0 - 2024-10
- 启用 5 个基础过滤规则
- 移除交易时段检查（性能优化）
- 移除 volume/turnover 一致性检查（避免误过滤）
- 移除 open/high/low 检查（字段不可靠）

## 相关文件

- **定义**: `/root/project/md_processor/src/bar1min_aggregator.rs:214-325`
- **on_tick 使用**: `/root/project/md_processor/src/parquet_processor_on_tick.rs:119`
- **on_batch 等价实现**: `/root/project/md_processor/src/parquet_processor_on_batch.rs:84-111`
- **对比工具**: `/root/project/md_toolkit/tools/validators/compare_bar_processors.py`

## 总结

当前 `validate_tick` 采用**最小化过滤 + 数据清理**策略：

✅ **启用规则** (7个):
1. close 价格有效性
2. 基础字段非空
3. turnover 非负
4. bid/ask 价格有效性（拒绝负数和Infinity，允许0和NaN）
5. bid <= ask（条件性）
6. bid/ask 价格清理（0值或NaN转为NULL，volume清零）
7. mid_price 重新计算

❌ **禁用规则** (7个):
- 避免误过滤有效数据
- 避免昂贵的性能开销
- 与 on_batch 保持一致

这种策略确保：
- ✅ 过滤掉明显无效的数据（负数、Infinity）
- ✅ 保留所有可能有效的数据（允许0和NaN）
- ✅ 正确表达NULL语义（0价格或NaN → NULL）
- ✅ 确保volume一致性（无效价格对应的volume清零）
- ✅ 修正原始数据的mid_price错误
- ✅ 流式和批量处理完全一致
- ✅ 高性能处理（无昂贵操作）
