#!/usr/bin/env python3
"""
验证 fill_missing_minutes 修复效果
"""

import polars as pl

def verify_contract_fix(contract: str):
    """验证单个合约的修复效果"""
    print(f"\n{'='*80}")
    print(f"验证合约: {contract}")
    print(f"{'='*80}")

    df = pl.read_parquet(f"/data/future_data/dongzheng_data/full_bar_1min/{contract}_1min_on_tick.parquet")
    df = df.filter(pl.col("contract") == contract).sort("trade_time")

    if len(df) == 0:
        print("⚠️ 无数据")
        return

    # 获取第一天和最后一天
    first_bar = df[0]
    last_bar = df[-1]

    print(f"\n第一个bar: {first_bar['trade_time'][0]} | vol={first_bar['volume'][0]:>6} | oi={first_bar['open_interest'][0]:>8}")
    print(f"最后bar:   {last_bar['trade_time'][0]} | vol={last_bar['volume'][0]:>6} | oi={last_bar['open_interest'][0]:>8}")

    # 提取最后bar的时间
    last_time = str(last_bar['trade_time'][0])
    last_time_part = last_time.split()[1][:5] if len(last_time.split()) > 1 else ""

    print(f"\n最后bar时间: {last_time_part}")

    # 检查是否符合预期
    comd = ''.join([c for c in contract if c.isalpha()])

    if comd in ['cu', 'al']:
        # 这些品种的最后交易日应该在15:00结束（停夜盘）
        expected_end = "15:00"
        if last_time_part == expected_end:
            print(f"✅ 修复成功！最后bar时间为 {expected_end}（预期：停夜盘后的收盘时间）")
        else:
            print(f"❌ 修复失败？最后bar时间为 {last_time_part}（预期：{expected_end}）")
    else:
        print(f"ℹ️  该品种交易时段可能不同，需要人工检查")

    # 分析最后一天的时段分布（使用 trade_time 的日历日期）
    last_calendar_date = str(last_bar['trade_time'][0]).split()[0]
    last_day_df = df.filter(
        pl.col("trade_time").str.starts_with(last_calendar_date)
    )

    print(f"\n最后一个日历日 ({last_calendar_date}) 各时段bar数量:")

    # 统计各小时
    hour_counts = {}
    for i in range(len(last_day_df)):
        bar = last_day_df[i]
        time_str = str(bar['trade_time'][0])
        hour = time_str.split()[1][:2] if len(time_str.split()) > 1 else ""

        if hour not in hour_counts:
            hour_counts[hour] = 0
        hour_counts[hour] += 1

    for hour in sorted(hour_counts.keys()):
        count = hour_counts[hour]
        if hour in ['21', '22', '23']:
            print(f"  {hour}:00 - {count:>3} bars  ⚠️ 夜盘时段")
        elif hour in ['00', '01', '02']:
            print(f"  {hour}:00 - {count:>3} bars  ℹ️  夜盘后半段")
        else:
            print(f"  {hour}:00 - {count:>3} bars")

    # 检查最后日历日是否还有21:00-23:59的错误补全
    night_late_bars = last_day_df.filter(
        (pl.col("trade_time").str.contains(" 21:")) |
        (pl.col("trade_time").str.contains(" 22:")) |
        (pl.col("trade_time").str.contains(" 23:"))
    )

    if len(night_late_bars) > 0:
        print(f"\n❌ 最后日历日 {last_calendar_date} 仍然发现 {len(night_late_bars)} 个 21:00-23:59 的bar（可能是错误补全）")
    else:
        print(f"\n✅ 最后日历日 {last_calendar_date} 没有 21:00-23:59 的bar（修复成功）")

def main():
    print("="*80)
    print("验证 fill_missing_minutes 修复效果")
    print("="*80)

    # 测试问题合约
    problem_contracts = ['cu1804', 'al1804']

    for contract in problem_contracts:
        verify_contract_fix(contract)

    # 测试正常合约（确保不受影响）
    print("\n" + "="*80)
    print("验证正常合约（确保不受影响）")
    print("="*80)

    normal_contracts = ['IF1802', 'm1803']

    for contract in normal_contracts:
        verify_contract_fix(contract)

if __name__ == "__main__":
    main()
