# CSV输出格式修复示例对比

## 示例数据
假设处理 `IF1706` 合约的第一个bar和补全bar：

```
Bar 1 (第一个有成交的bar):
  - prev_close: NaN (第一个bar无前一收盘价)
  - pre_settle: 3500.0 (昨结算价)
  - bid_price_1: 3505.5
  - log_return: NaN (第一个bar无收益率)

Bar 2 (正常bar):
  - prev_close: 3505.5
  - pre_settle: 3500.0
  - bid_price_1: 3506.0
  - log_return: 0.000285

Bar 3 (补全bar，无成交):
  - prev_close: 3506.0 (延续前一bar)
  - pre_settle: NaN (补全bar无昨结算)
  - bid_price_1: 0.0 (清理后的值)
  - log_return: 0.0 (延续价格，收益率为0)
```

## 修复前 CSV 输出

```csv
contract,contract_yymm,comd,exchange,date,trade_time,open,high,low,close,prev_close,pre_settle,volume,turnover,open_interest,open_interest_diff,bid_price_1,bid_volume_1,ask_price_1,ask_volume_1,mid_price,vwap,log_return,maturity_month,maturity_day
IF1706,1706,IF,CFFEX,2017-01-03,2017-01-03 09:30:00,3505.5,3510.0,3505.0,3505.5,NaN,3500.0,150,52500000,25000,1000,3505.5,20,3506.0,15,3505.75,3505.5,,5,148
IF1706,1706,IF,CFFEX,2017-01-03,2017-01-03 09:31:00,3505.5,3506.0,3505.0,3506.0,3505.5,3500.0,80,28048000,25020,20,3506.0,18,3506.5,12,3506.25,3506.0,0.000285,5,148
IF1706,1706,IF,CFFEX,2017-01-03,2017-01-03 09:32:00,3506.0,3506.0,3506.0,3506.0,3506.0,NaN,0,0,25020,0,,,,,,,0.0,5,148
```

### ❌ 问题：
- Bar 1 的 `prev_close`: `NaN` (字符串，不是空值)
- Bar 3 的 `pre_settle`: `NaN` (字符串，不是空值)

## 修复后 CSV 输出

```csv
contract,contract_yymm,comd,exchange,date,trade_time,open,high,low,close,prev_close,pre_settle,volume,turnover,open_interest,open_interest_diff,bid_price_1,bid_volume_1,ask_price_1,ask_volume_1,mid_price,vwap,log_return,maturity_month,maturity_day
IF1706,1706,IF,CFFEX,2017-01-03,2017-01-03 09:30:00,3505.5,3510.0,3505.0,3505.5,,3500.0,150,52500000,25000,1000,3505.5,20,3506.0,15,3505.75,3505.5,,5,148
IF1706,1706,IF,CFFEX,2017-01-03,2017-01-03 09:31:00,3505.5,3506.0,3505.0,3506.0,3505.5,3500.0,80,28048000,25020,20,3506.0,18,3506.5,12,3506.25,3506.0,0.000285,5,148
IF1706,1706,IF,CFFEX,2017-01-03,2017-01-03 09:32:00,3506.0,3506.0,3506.0,3506.0,3506.0,,0,0,25020,0,,,,,,,0.0,5,148
```

### ✅ 修复效果：
- Bar 1 的 `prev_close`: `` (空字符串，表示NULL)
- Bar 3 的 `pre_settle`: `` (空字符串，表示NULL)

## Pandas 读取对比

### 修复前
```python
import pandas as pd

df = pd.read_csv('output/md_snapshot_RTdongzheng_1min.csv.20251027')

# ❌ 问题：NaN被读取为字符串
print(df.loc[0, 'prev_close'])  # 输出: "NaN" (字符串)
print(type(df.loc[0, 'prev_close']))  # 输出: <class 'str'>

# 需要手动转换
df['prev_close'] = pd.to_numeric(df['prev_close'], errors='coerce')
```

### 修复后
```python
import pandas as pd

df = pd.read_csv('output/md_snapshot_RTdongzheng_1min.csv.20251027',
                 na_values=[''])  # 显式指定空字符串为缺失值

# ✅ 正确：空字符串被识别为NaN
print(df.loc[0, 'prev_close'])  # 输出: nan
print(type(df.loc[0, 'prev_close']))  # 输出: <class 'float'>
print(df['prev_close'].isna().sum())  # 统计缺失值数量
```

## Parquet 输出对比（参考）

使用 `parquet_processor_on_tick` 生成的Parquet文件：

```python
import pyarrow.parquet as pq

table = pq.read_table('/data/future_data/dongzheng_data/full_bar_1min_on_tick/IF1706_1min.parquet')
df = table.to_pandas()

# Parquet中的NULL值
print(df.loc[0, 'prev_close'])  # 输出: nan (真正的NaN)
print(df['prev_close'].dtype)   # 输出: float64
print(df['prev_close'].isna().sum())  # 统计NULL数量

# 类型信息
print(table.schema.field('prev_close'))
# 输出: pyarrow.Field<prev_close: double>
```

## 一致性验证

修复后，CSV和Parquet的NULL值语义完全一致：

| 字段 | 场景 | CSV修复前 | CSV修复后 | Parquet | 一致性 |
|------|------|-----------|----------|---------|--------|
| prev_close | 第一个bar | `"NaN"` | `""` | `NULL` | ✅ |
| pre_settle | 补全bar | `"NaN"` | `""` | `NULL` | ✅ |
| log_return | 第一个bar | `""` | `""` | `NULL` | ✅ |
| bid_price_1 | 清理后 | `""` | `""` | `0.0` | ⚠️ 格式差异 |

**注意**: `bid_price_1`, `ask_price_1`, `mid_price` 的0值处理差异是设计上的考虑（CSV清理规则），不影响数据语义。
