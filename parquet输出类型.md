# Parquet与CSV输出格式完整对比

**最后更新**: 2025-11-01
**版本**: v2.0 (CSV格式已修复)

---

## 1️⃣ 字段列表（25个字段）

两种输出格式的字段列表**完全一致**，顺序相同：

| 序号 | 字段名 | 说明 | 类别 |
|------|--------|------|------|
| 1 | contract | 合约代码（如IF1706） | 标识 |
| 2 | contract_yymm | 合约年月（如1706） | 标识 |
| 3 | comd | 品种代码（如IF） | 标识 |
| 4 | exchange | 交易所（CFFEX/SHFE/DCE/ZCE/INE/GFEX） | 标识 |
| 5 | date | 交易日期（YYYY-MM-DD） | 时间 |
| 6 | trade_time | 交易时间（YYYY-MM-DD HH:MM:SS） | 时间 |
| 7 | open | 开盘价 | OHLC |
| 8 | high | 最高价 | OHLC |
| 9 | low | 最低价 | OHLC |
| 10 | close | 收盘价 | OHLC |
| 11 | prev_close | 上一bar收盘价 | 衍生价格 |
| 12 | pre_settle | 昨结算价 | 衍生价格 |
| 13 | volume | 成交量 | 成交 |
| 14 | turnover | 成交额 | 成交 |
| 15 | open_interest | 持仓量 | 持仓 |
| 16 | open_interest_diff | 持仓量变化 | 持仓 |
| 17 | bid_price_1 | 买一价 | 盘口 |
| 18 | bid_volume_1 | 买一量 | 盘口 |
| 19 | ask_price_1 | 卖一价 | 盘口 |
| 20 | ask_volume_1 | 卖一量 | 盘口 |
| 21 | mid_price | 中间价 (bid+ask)/2 | 衍生价格 |
| 22 | vwap | 成交均价 | 衍生价格 |
| 23 | log_return | 对数收益率 | 收益率 |
| 24 | maturity_month | 距到期月数 | 到期日 |
| 25 | maturity_day | 距到期天数 | 到期日 |

---

## 2️⃣ Parquet输出格式

### 生成代码
**文件**: `src/parquet_processor_on_tick.rs:17-50`

**函数**: `build_bars_dataframe(bars: &[BarData]) -> PolarsResult<DataFrame>`

### 输出路径
```
/data/future_data/dongzheng_data/full_bar_1min_on_tick/{contract}_1min.parquet
/data/future_data/dongzheng_data/full_bar_5min_on_tick/{contract}_5min.parquet
```

### 数据类型（Parquet Schema）

```
contract:             String
contract_yymm:        String
comd:                 String
exchange:             String
date:                 String
trade_time:           String

open:                 Float64
high:                 Float64
low:                  Float64
close:                Float64
prev_close:           Float64 (可为NULL)  ← 关键：支持NULL
pre_settle:           Float64 (可为NULL)  ← 关键：支持NULL

volume:               UInt32
turnover:             Float64
open_interest:        UInt32
open_interest_diff:   Int32

bid_price_1:          Float64
bid_volume_1:         UInt32
ask_price_1:          Float64
ask_volume_1:         UInt32

mid_price:            Float64
vwap:                 Float64
log_return:           Float64 (可为NULL)  ← 关键：支持NULL

maturity_month:       Int16
maturity_day:         Int16
```

### 特殊值处理逻辑

```rust
// 第30-31行：prev_close, pre_settle
Series::new("prev_close", bars.iter().map(|b|
    if b.prev_close.is_nan() { None } else { Some(b.prev_close) }
).collect::<Vec<_>>()),

Series::new("pre_settle", bars.iter().map(|b|
    if b.pre_settle.is_nan() { None } else { Some(b.pre_settle) }
).collect::<Vec<_>>()),

// 第38-43行：bid/ask/mid_price - 保留原始值（包括0.0）
Series::new("bid_price_1", bars.iter().map(|b| b.bid_price_1).collect::<Vec<_>>()),
Series::new("ask_price_1", bars.iter().map(|b| b.ask_price_1).collect::<Vec<_>>()),
Series::new("mid_price", bars.iter().map(|b| b.mid_price).collect::<Vec<_>>()),

// 第45行：log_return
Series::new("log_return", bars.iter().map(|b|
    if b.log_return.is_nan() { None } else { Some(b.log_return) }
).collect::<Vec<_>>()),

// 第182-189行：额外处理（补全bar的pre_settle）
df_out = df_out.lazy()
    .with_column(
        when(col("pre_settle").is_nan())
            .then(lit(NULL))
            .otherwise(col("pre_settle"))
            .alias("pre_settle")
    )
    .collect()?;
```

### Parquet特殊值映射表

| 场景 | 字段 | BarData值 | Parquet输出 |
|------|------|-----------|-------------|
| 第一个bar | prev_close | `NaN` | `NULL` |
| 第一个bar | log_return | `NaN` | `NULL` |
| 补全bar | pre_settle | `NaN` | `NULL` (两次转换确保) |
| bid/ask清理后 | bid_price_1 | `0.0` | `0.0` (保留) |
| bid/ask清理后 | ask_price_1 | `0.0` | `0.0` (保留) |
| bid/ask清理后 | mid_price | `0.0` | `0.0` (保留) |
| bid/ask清理后 | bid_volume_1 | `0` | `0` (保留) |
| bid/ask清理后 | ask_volume_1 | `0` | `0` (保留) |

---

## 3️⃣ CSV输出格式（✅ 已修复）

### 生成代码
**文件**: `src/memory_processor_on_tick.rs:67-108`

**函数**: `write_bar_to_csv(bar: &BarData, freq: &str)`

**修复日期**: 2025-11-01

### 输出路径
```
output/md_snapshot_RTdongzheng_1min.csv.20251027
output/md_snapshot_RTdongzheng_5min.csv.20251027
```

### CSV表头
```csv
contract,contract_yymm,comd,exchange,date,trade_time,open,high,low,close,prev_close,pre_settle,volume,turnover,open_interest,open_interest_diff,bid_price_1,bid_volume_1,ask_price_1,ask_volume_1,mid_price,vwap,log_return,maturity_month,maturity_day
```

### 数据类型（全部为字符串）

CSV格式没有类型系统，所有字段都是字符串。读取时需要手动转换：

```python
import pandas as pd

df = pd.read_csv('output/md_snapshot_RTdongzheng_1min.csv.20251027',
    dtype={
        'contract': str,
        'contract_yymm': str,
        'comd': str,
        'exchange': str,
        'date': str,
        'trade_time': str,
        'open': float,
        'high': float,
        'low': float,
        'close': float,
        'volume': 'UInt32',
        'turnover': float,
        'open_interest': 'UInt32',
        'open_interest_diff': 'Int32',
        'vwap': float,
        'maturity_month': 'Int16',
        'maturity_day': 'Int16',
    },
    na_values=['']  # 空字符串识别为缺失值
)

# 以下字段包含缺失值，自动转换为float
# prev_close, pre_settle, bid_price_1, bid_volume_1, ask_price_1, ask_volume_1, mid_price, log_return
```

### 特殊值处理逻辑（✅ 修复后）

```rust
// 第81-82行：prev_close, pre_settle（✅ 修复：NaN → 空字符串）
if bar.prev_close.is_nan() { String::new() } else { bar.prev_close.to_string() },
if bar.pre_settle.is_nan() { String::new() } else { bar.pre_settle.to_string() },

// 第88-92行：bid/ask/mid_price（0.0 → 空字符串）
if bar.bid_price_1 == 0.0 { String::new() } else { bar.bid_price_1.to_string() },
if bar.bid_volume_1 == 0 { String::new() } else { bar.bid_volume_1.to_string() },
if bar.ask_price_1 == 0.0 { String::new() } else { bar.ask_price_1.to_string() },
if bar.ask_volume_1 == 0 { String::new() } else { bar.ask_volume_1.to_string() },
if bar.mid_price == 0.0 { String::new() } else { bar.mid_price.to_string() },

// 第94行：log_return（NaN → 空字符串）
if bar.log_return.is_nan() { String::new() } else { bar.log_return.to_string() },

// 第76-79, 83-85, 91, 95-96行：其他字段（直接输出）
bar.open,
bar.high,
bar.low,
bar.close,
bar.volume,
bar.turnover,
bar.open_interest,
bar.open_interest_diff,
bar.vwap,
bar.maturity_month,
bar.maturity_day
```

### CSV特殊值映射表（✅ 修复后）

| 场景 | 字段 | BarData值 | CSV输出（修复前） | CSV输出（修复后） |
|------|------|-----------|------------------|------------------|
| 第一个bar | prev_close | `NaN` | `"NaN"` ❌ | `""` ✅ |
| 第一个bar | log_return | `NaN` | `""` ✅ | `""` ✅ |
| 补全bar | pre_settle | `NaN` | `"NaN"` ❌ | `""` ✅ |
| bid/ask清理后 | bid_price_1 | `0.0` | `""` | `""` |
| bid/ask清理后 | ask_price_1 | `0.0` | `""` | `""` |
| bid/ask清理后 | mid_price | `0.0` | `""` | `""` |
| bid/ask清理后 | bid_volume_1 | `0` | `""` | `""` |
| bid/ask清理后 | ask_volume_1 | `0` | `""` | `""` |
| 正常值 | open | `3505.5` | `"3505.5"` | `"3505.5"` |
| 正常值 | volume | `150` | `"150"` | `"150"` |

---

## 4️⃣ 完整对比表

### NULL值处理对比（✅ 修复后语义统一）

| 字段 | 场景 | Parquet | CSV（修复后） | 语义一致性 |
|------|------|---------|--------------|-----------|
| **prev_close** | 第一个bar | `NULL` | `""`（空字符串） | ✅ 一致 |
| **pre_settle** | 补全bar | `NULL` | `""`（空字符串） | ✅ 一致 |
| **log_return** | 第一个bar | `NULL` | `""`（空字符串） | ✅ 一致 |
| **log_return** | 延续价格（补全bar） | `NULL` | `"0.0"`（字符串） | ⚠️ 差异 |

### 零值处理对比（设计差异）

| 字段 | 场景 | Parquet | CSV | 说明 |
|------|------|---------|-----|------|
| **bid_price_1** | bid清理后 | `0.0` (f64) | `""` (空字符串) | CSV清理规则 |
| **bid_volume_1** | bid清理后 | `0` (u32) | `""` (空字符串) | CSV清理规则 |
| **ask_price_1** | ask清理后 | `0.0` (f64) | `""` (空字符串) | CSV清理规则 |
| **ask_volume_1** | ask清理后 | `0` (u32) | `""` (空字符串) | CSV清理规则 |
| **mid_price** | 清理后 | `0.0` (f64) | `""` (空字符串) | CSV清理规则 |
| **volume** | 补全bar | `0` (u32) | `"0"` (字符串) | 保留零值 |
| **turnover** | 补全bar | `0.0` (f64) | `"0"` (字符串) | 保留零值 |

**说明**: bid/ask/mid_price的零值差异是**设计上的考虑**：
- Parquet保留0.0，便于数值计算和类型安全
- CSV转为空字符串，符合交易数据清理习惯（0表示无盘口）

---

## 5️⃣ 实际输出示例

假设处理 `IF1706` 合约的3个bar：

### Parquet输出（查看方式）

```python
import pyarrow.parquet as pq

table = pq.read_table('/data/future_data/dongzheng_data/full_bar_1min_on_tick/IF1706_1min.parquet')
print(table.schema)
print(table.to_pandas().head(3))
```

**输出示例**:
```
contract  contract_yymm  comd  exchange  date        trade_time           open    high    low     close   prev_close  pre_settle  volume  turnover   open_interest  ...
IF1706    1706          IF    CFFEX     2017-01-03  2017-01-03 09:30:00  3505.5  3510.0  3505.0  3505.5  <NA>        3500.0      150     52500000   25000         ...
IF1706    1706          IF    CFFEX     2017-01-03  2017-01-03 09:31:00  3505.5  3506.0  3505.0  3506.0  3505.5      3500.0      80      28048000   25020         ...
IF1706    1706          IF    CFFEX     2017-01-03  2017-01-03 09:32:00  3506.0  3506.0  3506.0  3506.0  3506.0      <NA>        0       0          25020         ...
```

**类型信息**:
```
contract: string
prev_close: double (可为null)
volume: uint32
```

### CSV输出（✅ 修复后）

**文件内容**:
```csv
contract,contract_yymm,comd,exchange,date,trade_time,open,high,low,close,prev_close,pre_settle,volume,turnover,open_interest,open_interest_diff,bid_price_1,bid_volume_1,ask_price_1,ask_volume_1,mid_price,vwap,log_return,maturity_month,maturity_day
IF1706,1706,IF,CFFEX,2017-01-03,2017-01-03 09:30:00,3505.5,3510.0,3505.0,3505.5,,3500.0,150,52500000,25000,1000,3505.5,20,3506.0,15,3505.75,3505.5,,5,148
IF1706,1706,IF,CFFEX,2017-01-03,2017-01-03 09:31:00,3505.5,3506.0,3505.0,3506.0,3505.5,3500.0,80,28048000,25020,20,3506.0,18,3506.5,12,3506.25,3506.0,0.000285,5,148
IF1706,1706,IF,CFFEX,2017-01-03,2017-01-03 09:32:00,3506.0,3506.0,3506.0,3506.0,3506.0,,0,0,25020,0,,,,,,,0.0,5,148
```

**说明**:
- Row 1: `prev_close` 为空（第一个bar），`log_return` 为空
- Row 2: 正常bar，所有字段有值
- Row 3: 补全bar，`pre_settle` 为空，`bid/ask/mid_price` 为空（0.0被清理）

---

## 6️⃣ 读取建议

### Parquet读取（推荐）

```python
import pyarrow.parquet as pq
import pandas as pd

# 方法1：pyarrow直接读取（性能最优）
table = pq.read_table('/data/future_data/dongzheng_data/full_bar_1min_on_tick/IF1706_1min.parquet')
df = table.to_pandas()

# 方法2：pandas读取
df = pd.read_parquet('/data/future_data/dongzheng_data/full_bar_1min_on_tick/IF1706_1min.parquet')

# NULL值自动识别为NaN
print(df['prev_close'].isna().sum())  # 统计NULL数量
```

### CSV读取（需手动配置）

```python
import pandas as pd

df = pd.read_csv('output/md_snapshot_RTdongzheng_1min.csv.20251027',
    dtype={
        # 字符串字段
        'contract': str,
        'contract_yymm': str,
        'comd': str,
        'exchange': str,
        'date': str,
        'trade_time': str,
    },
    na_values=[''],  # ✅ 关键：空字符串识别为缺失值
    keep_default_na=True,  # 保留默认的NA识别（如'NaN'）
)

# 手动转换类型
df['volume'] = df['volume'].astype('UInt32')
df['open_interest'] = df['open_interest'].astype('UInt32')
df['open_interest_diff'] = df['open_interest_diff'].astype('Int32')
df['maturity_month'] = df['maturity_month'].astype('Int16')
df['maturity_day'] = df['maturity_day'].astype('Int16')

# 检查缺失值
print(df[['prev_close', 'pre_settle', 'bid_price_1', 'log_return']].isna().sum())
```

---

## 7️⃣ 性能对比

| 指标 | Parquet | CSV |
|------|---------|-----|
| **文件大小** | ~1/10 (压缩后) | 基准 |
| **读取速度** | 5-10x 更快 | 基准 |
| **写入速度** | 2-3x 更快 | 基准 |
| **类型安全** | ✅ 强类型 | ❌ 弱类型（需手动转换） |
| **NULL支持** | ✅ 原生支持 | ⚠️ 需配置na_values |
| **兼容性** | Python/Rust/C++/Java | 通用文本格式 |
| **推荐场景** | 生产环境、大规模数据 | 临时调试、小规模数据 |

---

## 8️⃣ 格式一致性检查清单

### ✅ 已保证一致的部分
- [x] 字段列表（25个字段，顺序相同）
- [x] 字段名称（完全相同）
- [x] NULL值语义（空字符串 ↔ NULL）
- [x] prev_close的NaN处理（✅ 修复后）
- [x] pre_settle的NaN处理（✅ 修复后）
- [x] log_return的NaN处理

### ⚠️ 设计上的差异（不影响语义）
- [ ] bid/ask/mid_price的零值处理（CSV清理，Parquet保留）
- [ ] 数据类型系统（Parquet强类型，CSV弱类型）
- [ ] 文件格式（二进制 vs 文本）

### 📌 下游使用建议
1. **生产环境**: 优先使用Parquet格式（性能、类型安全）
2. **快速验证**: 可使用CSV格式（人类可读）
3. **读取CSV时**: 必须配置 `na_values=['']`
4. **对比验证**: 使用 `pd.read_parquet()` 和 `pd.read_csv(..., na_values=[''])` 对比数据一致性

---

## 9️⃣ 版本历史

| 版本 | 日期 | 变更说明 |
|------|------|---------|
| v2.0 | 2025-11-01 | ✅ 修复CSV的prev_close和pre_settle输出（NaN→空字符串） |
| v1.0 | 2025-10-27 | 初始版本，存在CSV的NaN输出为"NaN"字符串的问题 |
