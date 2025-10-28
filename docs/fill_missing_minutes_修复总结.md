# fill_missing_minutes 修复总结

**日期**: 2025-10-28
**问题**: 最后交易日停夜盘时错误补全 21:00-23:59 的bar
**影响合约**: cu1804, al1804 等最后交易日停夜盘的合约

---

## 问题描述

### 原始问题
在最后交易日（交割月合约），某些品种会停止夜盘交易（如铜、铝）。但原有的 `fill_missing_minutes` 逻辑会：

1. 基于 `date` 字段收集日期范围
2. 对每个日期，盲目补全所有配置的交易时段
3. 导致最后交易日（如 2018-04-16）被错误补全 21:00-23:59 的bar
4. 结果：最后bar从正确的 `15:00:00` 变成错误的 `23:59:00`

### 周末跨日问题
期货夜盘存在**交易日期**和**日历日期**不一致的情况：

- **周五 2018-04-13** 21:00 的夜盘
- **周六 2018-04-14** 00:00 的夜盘后半段
- 这些bar的 **trade_time** 是实际时钟时间（2018-04-13/14）
- 但 **date** 字段是交易日期（2018-04-16 周一）

原逻辑基于 `date` 字段，导致：
- 检测到 date=2018-04-16 有夜盘数据（周五的）
- 错误地补全 trade_time=2018-04-16 21:00 的夜盘（周一的）

---

## 修复方案

### 核心思路
**在补全之前，先识别哪些(日历日期, 交易时段)有原始数据，只补全有原始bar的时段。**

### 实现逻辑

```rust
// 1. 识别哪些(日历日期, 时段)有原始数据
let mut session_has_data: HashSet<(NaiveDate, usize)> = HashSet::new();

for time_str in existing_data.keys() {
    if let Ok(dt) = NaiveDateTime::parse_from_str(time_str, "%Y-%m-%d %H:%M:%S") {
        let time = dt.time();
        let date = dt.date();  // 使用 trade_time 的日历日期

        // 检查该bar属于哪个交易时段
        for (session_idx, (start_time, end_time)) in trading_ranges.iter().enumerate() {
            if bar_in_session(time, start_time, end_time) {
                // 标记该(日历日期, 时段)有数据
                session_has_data.insert((date, session_idx));

                // 跨日时段特殊处理
                if start_time > end_time && time <= *end_time {
                    let prev_date = date - chrono::Duration::days(1);
                    session_has_data.insert((prev_date, session_idx));
                }
                break;
            }
        }
    }
}

// 2. 只为有原始数据的时段生成 timeline
for date in &date_range {
    for (session_idx, (start_time, end_time)) in trading_ranges.iter().enumerate() {
        // 只补全有原始bar的时段
        if !session_has_data.contains(&(*date, session_idx)) {
            continue;  // 跳过没有原始数据的时段
        }

        // 生成该时段的完整时间线...
    }
}
```

### 关键改进

1. **基于日历日期判断**：使用 `trade_time.date()` 而非 `date` 字段
2. **时段级别检测**：不是简单检查"这天有没有数据"，而是检查"这天的这个时段有没有数据"
3. **跨日时段处理**：正确处理夜盘跨日的情况（00:00-01:00 的bar属于前一天的夜盘）

---

## 验证结果

### 修复前后对比

| 合约 | 修复前最后bar | 修复后最后bar | 状态 |
|------|--------------|--------------|------|
| cu1804 | 2018-04-16 **23:59:00** | 2018-04-16 **15:00:00** | ✅ 已修复 |
| al1804 | 2018-04-16 **23:59:00** | 2018-04-16 **15:00:00** | ✅ 已修复 |
| IF1802 | 2018-02-22 15:00:00 | 2018-02-22 15:00:00 | ✅ 不受影响 |
| m1803 | 2018-03-02 23:00:00 | 2018-03-02 23:00:00 | ✅ 不受影响 (周五夜盘正常) |

### 完整验证（8个合约）

运行 `verify_all_aspects.py` 验证 5 个指标：

1. ✅ oi_diff 累计正确性
2. ✅ 集合竞价处理
3. ✅ 交易时段开闭
4. ✅ volume=0 时的处理逻辑
5. ✅ 交易时段过滤

**结果**: 8/8 合约全部通过所有验证 🎉

---

## 修改文件

### src/common_utils.rs (lines 201-275)

- 添加 `session_has_data` 检测逻辑（33行）
- 修改 timeline 生成逻辑，添加条件检查（3行）

**编译**: `cargo build --release`
**测试**: `python3 verify_all_aspects.py`

---

## 影响分析

### 受益合约
所有在最后交易日停夜盘的合约（主要是SHFE的金属品种）：
- 铜(cu)、铝(al)、锌(zn)、铅(pb)、镍(ni)、锡(sn)
- 螺纹钢(rb)、热卷(hc)、线材(wr)
- 黄金(au)、白银(ag)

### 不受影响
- 股指期货（CFFEX）：本来就没有夜盘
- 农产品（DCE/CZCE）：大部分最后交易日也停夜盘，修复逻辑同样适用
- 正常交易日：完全不受影响，补全逻辑与之前一致

### 数据一致性
- ✅ 与原始tick数据一致
- ✅ 符合交易所规则（最后交易日停夜盘）
- ✅ on_batch 和 on_tick 结果一致

---

## 相关文档

- `docs/validate_tick_过滤规则.md` - 数据过滤规则
- `集合竞价功能实现总结.md` - 集合竞价处理
- `CLAUDE.md` - 项目设计标准

---

## 测试脚本

### verify_fix.py
验证修复效果，检查最后日历日是否还有错误补全的夜盘bar

```bash
python3 verify_fix.py
```

### verify_all_aspects.py
全面验证5个指标

```bash
python3 verify_all_aspects.py
```

### debug_last_day.py
调试最后交易日的详细数据，分析 date 和 trade_time 的关系

```bash
python3 debug_last_day.py
```

---

## 总结

**问题根源**: 混淆了交易日期（date）和日历日期（trade_time的日期），导致周末跨日的夜盘数据被误判。

**修复本质**: 改为基于日历日期的时段级别检测，只补全真正有原始数据的时段。

**验证结果**: 所有测试合约全部通过，修复成功且不影响其他合约。
