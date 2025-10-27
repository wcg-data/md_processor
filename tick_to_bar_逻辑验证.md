# Tick → Bar 逻辑验证文档

## 1. Tick数据特性

### 原始tick字段含义
- **volume**: 累计成交量（交易时段内累计，夜盘和日盘分别累计）
- **turnover**: 累计成交额（交易时段内累计，夜盘和日盘分别累计）
- **open_interest**: 持仓量（时点值，非累计）
- **close**: 最新成交价

### 关键特性
1. **Volume重置**：夜盘→日盘切换时，volume从大值跳到小值（如800→300）
2. **时段独立**：夜盘集合竞价和日盘集合竞价的volume独立累计

## 2. 集合竞价Bar生成（on_tick）

### 识别条件
- Tick时间在集合竞价时段内：
  - 股指期货：09:25-09:30
  - 商品期货日盘：08:55-09:00
  - 夜盘：20:55-21:00

### 集合竞价Bar字段
```
timestamp: 开盘时间（09:00/09:30/21:00）
open: 集合竞价第一个tick的close
high: 集合竞价期间最高价
low: 集合竞价期间最低价
close: 集合竞价最后一个tick的close
volume: last_tick.volume（累计值，直接使用）
turnover: last_tick.turnover（累计值，直接使用）
open_interest: last_tick.open_interest（时点值）
open_interest_diff: 0（无前置bar）
prev_close: NaN
log_return: NaN
```

### 关键逻辑
```rust
// 集合竞价bar使用累计值，不做差分
let volume = last_tick.volume;
let turnover = last_tick.turnover;

// 更新prev_last_tick为集合竞价的最后一个tick
self.prev_last_tick = Some(last_tick);
```

## 3. 1min连续交易Bar生成（on_tick）

### 第一个连续交易bar（例如09:31）

如果前面有集合竞价bar（09:30），则：
```
prev_last_tick = 集合竞价最后一个tick（09:29:59）
current_tick = 09:31:xx的某个tick

volume计算：
  if current_tick.volume < prev.volume:
    volume = current_tick.volume  // volume重置（夜盘→日盘）
  else:
    volume = current_tick.volume - prev.volume  // 正常差分

oi_diff = current_tick.oi - prev.oi
prev_close = prev.close
```

### 后续连续交易bar

正常差分逻辑：
```rust
let vol = if last_tick.volume < prev.volume {
    last_tick.volume  // 累计值重置
} else {
    last_tick.volume.saturating_sub(prev.volume)  // 正常差分
};
```

## 4. 5min Bar聚合

### 输入
5个1min bar（例如09:31-09:35）

### 聚合逻辑
```
timestamp: 右边界时间（09:35:00）
open: 第一个1min bar的open
high: max(5个1min bar的high)
low: min(5个1min bar的low)
close: 最后一个1min bar的close
volume: sum(5个1min bar的volume)
turnover: sum(5个1min bar的turnover)
open_interest: 最后一个1min bar的oi（时点值）
oi_diff: end_oi - start_oi
prev_close: 第一个1min bar的prev_close
```

### 关键代码
```rust
// 初始化窗口
self.volume_sum = bar.volume;
self.turnover_sum = bar.turnover;
self.start_open_interest = bar.open_interest - bar.open_interest_diff;

// 增量更新
self.volume_sum = self.volume_sum.saturating_add(bar.volume);
self.turnover_sum += bar.turnover;
self.end_open_interest = bar.open_interest;

// 输出
volume: self.volume_sum,  // 5个1min bar的累加
oi_diff: (end_oi - start_oi) as i32,
```

## 5. 关键场景验证

### 场景1：股指期货有集合竞价（IF1706）

**原始tick**：
```
09:29:00 - volume=2, oi=100  (集合竞价)
09:30:05 - volume=2, oi=101  (连续交易开始)
09:30:30 - volume=3, oi=102
```

**生成bar**：
```
09:30:00 bar (集合竞价):
  volume=2 (直接使用)
  oi_diff=0
  prev_close=NaN

09:31:00 bar (连续交易):
  volume=3-2=1 (差分)
  oi_diff=102-100=2
  prev_close=集合竞价close
```

### 场景2：夜盘→日盘volume重置（rb2412）

**原始tick**：
```
2024-05-06 20:59:30 - volume=800, oi=50000  (夜盘集合竞价)
2024-05-06 21:00:05 - volume=800, oi=50001  (夜盘连续交易)
...
2024-05-07 08:59:30 - volume=300, oi=50500  (日盘集合竞价，volume重置)
2024-05-07 09:00:05 - volume=305, oi=50502  (日盘连续交易)
```

**生成bar**：
```
21:00:00 bar (夜盘集合竞价):
  volume=800 (直接使用)
  oi_diff=0

21:01:00 bar (夜盘连续交易):
  volume=... (差分)

09:00:00 bar (日盘集合竞价):
  volume=300 (直接使用，新的累计起点)
  oi_diff=0

09:01:00 bar (日盘连续交易):
  检测到volume重置（305 < 前一tick的volume）
  volume=305 (直接使用当前值)
  oi_diff=50502-50500=2
```

### 场景3：无集合竞价tick（节假日停夜盘）

**原始tick**：
```
2024-04-30 08:59:30 - 无tick (节前无夜盘)
2024-04-30 09:00:05 - volume=100, oi=40000  (日盘开始)
```

**生成bar**：
```
不生成21:00:00 bar (无夜盘集合竞价tick)
不生成09:00:00 bar (无日盘集合竞价tick)
09:01:00 bar (第一个bar):
  volume=100 (第一个bar，直接使用累计值)
  oi_diff=0 (第一个bar)
  prev_close=NaN
```

## 6. 潜在问题检查

### ✅ 已修复的问题

1. **第一个bar的oi_diff**
   - ❌ 之前：使用last_tick.open_interest（错误）
   - ✅ 现在：使用0（正确）

2. **集合竞价tick被过滤**
   - ❌ 之前：validate_tick只检查连续交易时段，集合竞价tick被过滤
   - ✅ 现在：validate_tick同时检查连续交易时段和集合竞价时段

3. **Volume重置检测**
   - ✅ 已实现：`if last_tick.volume < prev.volume`

### ⚠️ 需要注意的边界情况

1. **集合竞价volume=0**
   - 当前处理：跳过生成bar（代码line 129-132）
   - ✅ 正确

2. **同一天多个集合竞价**
   - 夜盘21:00 + 日盘09:00
   - ✅ 已支持，因为volume重置检测会正常工作

3. **5min bar跨集合竞价**
   - 如果09:30是集合竞价bar，09:31-09:35聚合为09:35的5min bar
   - 09:30的集合竞价bar不参与5min聚合（因为09:30的bar已生成）
   - ✅ 正确（5min从09:31开始聚合）

## 7. 实际验证结果（IF1706，2016-10-24）

### on_tick处理器（完整支持集合竞价）

**生成的bar**：
```
09:30:00 (集合竞价bar):
  volume=2, oi_diff=0, prev_close=null ✅

09:31:00 (连续交易):
  volume=0, oi_diff=0, prev_close=3197.2 ✅

09:32:00:
  volume=1, oi_diff=1, prev_close=3197.2 ✅
```

### on_batch处理器（已修复oi_diff bug）

**生成的1min bar**：
```
09:31:00 (第一个bar，无集合竞价):
  volume=2, oi_diff=0, prev_close=null ✅ (已修复)

09:32:00:
  volume=1, oi_diff=1, prev_close=3197.2 ✅
```

### 5min聚合（使用on_batch的1min bar）

**输入**：09:31-09:35的5个1min bar
```
09:31: volume=2, oi=2, oi_diff=0
09:32: volume=1, oi=3, oi_diff=1
09:33: volume=1, oi=4, oi_diff=1
09:34: volume=3, oi=7, oi_diff=3
09:35: volume=0, oi=7, oi_diff=0
```

**输出**：09:35:00的5min bar
```
volume = 2+1+1+3+0 = 7 ✅
oi = 7 ✅
oi_diff = 0+1+1+3+0 = 5 ✅ (已修复)
```

## 8. 验证结论

当前逻辑：
- ✅ **集合竞价处理（on_tick）**：正确，数据驱动，自适应
- ✅ **Volume差分**：正确，支持重置检测
- ✅ **OI_diff计算**：✅ 已修复，第一个bar=0
- ✅ **5min聚合**：✅ 已修复，oi_diff=sum(1min bars的oi_diff)
- ✅ **边界情况**：正确处理节假日、volume重置

**整体评估**：✅ 逻辑完全正确，符合中国期货市场特性。

## 9. 关键修复

### 修复1：第一个bar的oi_diff（已完成，2025-10-27）
- **文件**：`bar1min_aggregator.rs` line 270, `parquet_processor_on_batch.rs` line 368
- **修复前**：`oi_diff = last_tick.open_interest`（错误）
- **修复后**：`oi_diff = 0`（正确）
- **验证**：IF1706第一个bar的oi_diff=0 ✅

### 修复2：集合竞价tick被过滤（已完成，2025-10-27）
- **文件**：`bar1min_aggregator.rs` line 432-446
- **修复前**：validate_tick只检查连续交易时段
- **修复后**：同时检查连续交易时段和集合竞价时段
- **验证**：IF1706成功生成09:30:00的集合竞价bar ✅
