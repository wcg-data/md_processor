# CSV输出格式修复说明

**修复日期**: 2025-11-01
**修改文件**: `src/memory_processor_on_tick.rs`
**修改函数**: `write_bar_to_csv` (第68-97行)

## 修复内容

### 问题描述
CSV输出与Parquet输出的NULL值处理不一致：
- **修复前**: `prev_close` 和 `pre_settle` 的NaN值会输出为 `"NaN"` 字符串
- **修复后**: NaN值输出为空字符串（与Parquet的NULL语义对应）

### 修改对比

```rust
// ❌ 修复前（第80-81行）
bar.prev_close,          // NaN → "NaN" 字符串
bar.pre_settle,          // NaN → "NaN" 字符串

// ✅ 修复后
if bar.prev_close.is_nan() { String::new() } else { bar.prev_close.to_string() },
if bar.pre_settle.is_nan() { String::new() } else { bar.pre_settle.to_string() },
```

### 输出格式统一规则

| 字段 | 特殊值 | CSV输出 | Parquet输出 | 语义一致性 |
|------|--------|---------|-------------|-----------|
| **prev_close** | NaN | 空字符串 | NULL | ✅ 一致 |
| **pre_settle** | NaN | 空字符串 | NULL | ✅ 一致 |
| **bid_price_1** | 0.0 | 空字符串 | 0.0 (f64) | ⚠️ 格式差异（符合bid清理规则） |
| **bid_volume_1** | 0 | 空字符串 | 0 (u32) | ⚠️ 格式差异（符合bid清理规则） |
| **ask_price_1** | 0.0 | 空字符串 | 0.0 (f64) | ⚠️ 格式差异（符合ask清理规则） |
| **ask_volume_1** | 0 | 空字符串 | 0 (u32) | ⚠️ 格式差异（符合ask清理规则） |
| **mid_price** | 0.0 | 空字符串 | 0.0 (f64) | ⚠️ 格式差异（符合mid_price清理规则） |
| **log_return** | NaN | 空字符串 | NULL | ✅ 一致 |

### 影响范围

**输出文件**：
- `output/md_snapshot_RTdongzheng_1min.csv.20251027`
- `output/md_snapshot_RTdongzheng_5min.csv.20251027`

**受影响字段**（修复后）：
1. `prev_close`: 第一个bar的值从 `"NaN"` 变为 `""`（空字符串）
2. `pre_settle`: 补全bar的值从 `"NaN"` 变为 `""`（空字符串）

**不受影响字段**：
- `bid_price_1`, `ask_price_1`, `mid_price`, `bid_volume_1`, `ask_volume_1`: 保持原有逻辑（0值→空字符串）
- `log_return`: 保持原有逻辑（NaN→空字符串）

## 验证方法

### 1. 编译验证
```bash
cargo check
# 输出：Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.29s
```

### 2. 运行验证
```bash
# 重新构建
cargo build --release

# 运行实时处理器（需要C++端配合）
./target/release/memory_processor_on_tick /md_shm

# 检查输出文件
head -5 output/md_snapshot_RTdongzheng_1min.csv.20251027
```

### 3. 对比验证
读取CSV和Parquet文件，对比相同合约的prev_close/pre_settle字段：
```python
import pandas as pd
import pyarrow.parquet as pq

# 读取CSV
csv_df = pd.read_csv('output/md_snapshot_RTdongzheng_1min.csv.20251027')

# 读取Parquet
parquet_df = pq.read_table('/data/future_data/dongzheng_data/full_bar_1min_on_tick/IF1706_1min.parquet').to_pandas()

# 对比prev_close字段
# CSV中空字符串应该对应Parquet中的NaN/None
csv_empty = csv_df[csv_df['prev_close'] == ''].shape[0]
parquet_null = parquet_df['prev_close'].isna().sum()
print(f"CSV空值数量: {csv_empty}, Parquet NULL数量: {parquet_null}")
```

## 向后兼容性

⚠️ **下游系统需要注意**：
- 如果下游代码使用 `value == "NaN"` 判断空值，需要改为 `value == ""`
- 推荐使用pandas读取时设置 `na_values=['', 'NaN']` 统一处理

## 参考文档
- `docs/validate_tick_过滤规则.md` - 规则6: bid/ask清理逻辑
- `集合竞价功能实现总结.md` - prev_close处理逻辑
- `paruqet 输出类型.md` - 输出类型对比分析
