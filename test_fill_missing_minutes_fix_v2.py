#!/usr/bin/env python3
"""
测试 fill_missing_minutes 修复方案 V2

问题分析：
  - V1策略失败：因为 fill_missing_minutes 已经补全过了，所以 first_time 和 last_time 覆盖整天
  - 新策略：基于实际有成交（volume>0）或有原始数据的时段来判断

更好的策略：
  - 在 fill_missing_minutes 之前就要知道哪些交易时段有实际数据
  - 或者：根据每个交易时段是否有 volume>0 的bar来判断
"""

import polars as pl
from datetime import datetime

def analyze_actual_trading_sessions(contract: str):
    """分析合约在哪些交易时段实际有数据"""

    print("="*100)
    print(f"分析 {contract} 的实际交易时段")
    print("="*100)

    df = pl.read_parquet(f"/data/future_data/dongzheng_data/full_bar_1min/{contract}_1min_on_tick.parquet")
    df = df.filter(pl.col("contract") == contract).sort("trade_time")

    # 分析最后一个交易日
    last_bars = df.tail(500)
    last_date_str = str(last_bars[-1]["trade_time"][0]).split()[0]

    print(f"\n最后交易日: {last_date_str}")

    # 筛选最后一天的bar
    last_day = df.filter(
        pl.col("trade_time").str.contains(last_date_str)
    )

    print(f"最后一天总bar数: {len(last_day)}")

    # 统计每个小时的情况
    print(f"\n各小时统计（volume>0 / 总bar数）:")

    hours_stats = {}
    for i in range(len(last_day)):
        bar = last_day[i]
        time_str = str(bar["trade_time"][0])
        hour = time_str.split()[1][:2] if len(time_str.split()) > 1 else ""

        if hour not in hours_stats:
            hours_stats[hour] = {"total": 0, "volume_gt_zero": 0}

        hours_stats[hour]["total"] += 1
        if bar["volume"][0] > 0:
            hours_stats[hour]["volume_gt_zero"] += 1

    for hour in sorted(hours_stats.keys()):
        stats = hours_stats[hour]
        total = stats["total"]
        vol_gt_zero = stats["volume_gt_zero"]
        percentage = vol_gt_zero / total * 100 if total > 0 else 0

        if hour in ['00', '01', '02']:
            note = " 夜盘"
        elif hour in ['09', '10', '11', '13', '14', '15']:
            note = " 日盘"
        elif hour in ['21', '22', '23']:
            note = " 夜盘/异常"
        else:
            note = " 异常"

        has_trade = "✅" if vol_gt_zero > 0 else "❌"
        print(f"  {hour}:00 - {vol_gt_zero:>3}/{total:>3} bars ({percentage:>5.1f}%) {has_trade} {note}")

    # 关键发现
    print(f"\n{'='*100}")
    print("关键发现")
    print(f"{'='*100}")

    print("""
正确的修复策略：

1. 【检测策略】对每个交易时段，检查是否有任何 volume>0 的bar
   - 如果该时段没有任何成交，说明实际不交易（停夜盘等）
   - 即使没有成交，但有原始tick数据（oi变化等），也应保留

2. 【更简单的策略】检查原始数据（聚合前）
   - cu1804 的原始tick数据在哪个时段有数据
   - 但这需要访问原始tick文件

3. 【最佳策略】改进 fill_missing_minutes 的逻辑
   - 不再根据 date 字段盲目补全所有交易时段
   - 改为：只补全已有bar的交易时段内的缺失分钟
   - 实现方式：
     a. 对每个交易时段，检查是否存在至少一个原始bar（在补全前）
     b. 如果存在，才补全该时段的缺失分钟
     c. 如果不存在，跳过该时段
""")

    # 分析 21:00-23:59 时段
    print(f"\n分析 21:00-23:59 时段:")
    night_bars = last_day.filter(
        (pl.col("trade_time").str.contains(" 21:")) |
        (pl.col("trade_time").str.contains(" 22:")) |
        (pl.col("trade_time").str.contains(" 23:"))
    )

    if len(night_bars) > 0:
        vol_gt_zero = night_bars.filter(pl.col("volume") > 0)
        print(f"  总bar数: {len(night_bars)}")
        print(f"  volume>0: {len(vol_gt_zero)}")
        print(f"  volume=0: {len(night_bars) - len(vol_gt_zero)}")

        if len(vol_gt_zero) == 0:
            print(f"  ❌ 该时段所有bar的volume都是0 → 这些是错误补全的bar")
        else:
            print(f"  ✅ 该时段有成交数据")

            # 显示有成交的bar
            print(f"\n  有成交的bar:")
            for i in range(min(5, len(vol_gt_zero))):
                bar = vol_gt_zero[i]
                print(f"    {bar['trade_time'][0]} | vol={bar['volume'][0]:>6} | oi={bar['open_interest'][0]:>8}")
    else:
        print(f"  未找到该时段的bar")

def test_fix_strategy():
    """测试修复策略"""

    print("\n" + "="*100)
    print("测试修复策略")
    print("="*100)

    print("""
【最终修复方案】

核心思路：在补全之前，先识别哪些交易时段真正有原始数据

实现步骤：
  1. 遍历原始bars（补全前的数据）
  2. 对每个交易时段，标记是否有至少一个原始bar
  3. 只对有原始bar的交易时段进行缺失分钟补全

代码逻辑（伪代码）：
  ```
  // Step 1: 识别哪些交易时段有原始数据
  session_has_data = {}
  for bar in original_bars:
      for session in trading_sessions:
          if bar.time in session:
              session_has_data[session] = true

  // Step 2: 生成 complete_timeline
  for session in trading_sessions:
      if session_has_data[session]:  // 只补全有数据的时段
          generate_timeline_for_session(session)

  // Step 3: 补全缺失分钟
  ...
  ```

优点：
  ✅ 最后交易日停夜盘 → 21:00-23:59 没有原始bar → 不补全
  ✅ 正常交易日有夜盘 → 21:00-23:59 有原始bar → 正常补全
  ✅ 不依赖 date 字段，更可靠

测试验证：
  - cu1804 最后交易日：21:00-23:59 应该被跳过（因为没有原始bar）
  - cu1804 正常交易日：21:00-23:59 应该保留（因为有原始bar）
""")

def main():
    # 分析几个代表性合约
    contracts = [
        "cu1804",
        "al1804",
    ]

    for contract in contracts:
        analyze_actual_trading_sessions(contract)

    test_fix_strategy()

    print(f"\n\n{'='*100}")
    print("下一步")
    print(f"{'='*100}")

    print("""
1. 在 Rust 中实现修复逻辑：
   - 修改 fill_missing_minutes 函数
   - 添加：识别哪些交易时段有原始数据的逻辑
   - 只补全有原始数据的交易时段

2. 重新生成数据并验证

3. 检查修复效果：
   - cu1804/al1804 的最后bar应该从 23:59 改为 15:00
   - 其他合约不受影响
""")

if __name__ == "__main__":
    main()
