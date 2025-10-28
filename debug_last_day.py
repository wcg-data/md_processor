#!/usr/bin/env python3
"""
深入分析最后交易日的数据问题
"""

import polars as pl

def debug_last_day(contract: str):
    """调试最后交易日的数据"""
    print(f"\n{'='*80}")
    print(f"调试合约: {contract}")
    print(f"{'='*80}")

    df = pl.read_parquet(f"/data/future_data/dongzheng_data/full_bar_1min/{contract}_1min_on_tick.parquet")
    df = df.filter(pl.col("contract") == contract).sort("trade_time")

    # 获取最后一个bar的date
    last_bar = df[-1]
    last_date_str = str(last_bar['date'][0])

    print(f"最后bar的 date 字段: {last_date_str}")
    print(f"最后bar的 trade_time: {last_bar['trade_time'][0]}")

    # 获取 date=最后交易日 的所有bar
    last_day_df = df.filter(pl.col("date").cast(str) == last_date_str)

    print(f"\ndate={last_date_str} 的总bar数: {len(last_day_df)}")

    # 分析 21:00-23:59 的bar
    night_bars = last_day_df.filter(
        (pl.col("trade_time").str.contains(" 21:")) |
        (pl.col("trade_time").str.contains(" 22:")) |
        (pl.col("trade_time").str.contains(" 23:"))
    )

    print(f"\n21:00-23:59 的bar数量: {len(night_bars)}")

    if len(night_bars) > 0:
        print("\n这些bar的详细信息（前5个和后5个）:")
        for i in [0, 1, 2, len(night_bars)-3, len(night_bars)-2, len(night_bars)-1]:
            if 0 <= i < len(night_bars):
                bar = night_bars[i]
                print(f"  {bar['trade_time'][0]} | date={bar['date'][0]} | vol={bar['volume'][0]:>6} | oi={bar['open_interest'][0]:>8}")

        # 检查这些bar的volume
        vol_gt_zero = night_bars.filter(pl.col("volume") > 0)
        print(f"\n这些bar中 volume>0 的数量: {len(vol_gt_zero)}")

        if len(vol_gt_zero) == 0:
            print("❌ 所有bar的volume都是0 → 这些是错误补全的bar")
        else:
            print("✅ 有真实成交数据")

    # 分析 00:00-02:30 的bar（夜盘后半段）
    print(f"\n{'='*80}")
    print("分析夜盘后半段 00:00-02:30")
    print(f"{'='*80}")

    early_morning = last_day_df.filter(
        (pl.col("trade_time").str.contains(" 00:")) |
        (pl.col("trade_time").str.contains(" 01:")) |
        (pl.col("trade_time").str.contains(" 02:"))
    )

    print(f"00:00-02:30 的bar数量: {len(early_morning)}")

    if len(early_morning) > 0:
        print("\n前5个:")
        for i in range(min(5, len(early_morning))):
            bar = early_morning[i]
            print(f"  {bar['trade_time'][0]} | date={bar['date'][0]} | vol={bar['volume'][0]:>6} | oi={bar['open_interest'][0]:>8}")

        vol_gt_zero = early_morning.filter(pl.col("volume") > 0)
        print(f"\n这些bar中 volume>0 的数量: {len(vol_gt_zero)}")

        if len(vol_gt_zero) == 0:
            print("❌ 所有bar的volume都是0 → 可能是错误补全")
        else:
            print("✅ 有真实成交数据")

    # 分析前一天的夜盘数据
    print(f"\n{'='*80}")
    print("检查前一天的数据")
    print(f"{'='*80}")

    # 找到倒数第二个date
    dates = df["date"].unique().sort()
    if len(dates) >= 2:
        prev_date_str = str(dates[-2])
        print(f"前一个交易日: {prev_date_str}")

        prev_day_df = df.filter(pl.col("date").cast(str) == prev_date_str)
        print(f"date={prev_date_str} 的总bar数: {len(prev_day_df)}")

        # 检查前一天是否有 21:00-23:59 的bar
        prev_night = prev_day_df.filter(
            (pl.col("trade_time").str.contains(" 21:")) |
            (pl.col("trade_time").str.contains(" 22:")) |
            (pl.col("trade_time").str.contains(" 23:"))
        )

        print(f"前一天 21:00-23:59 的bar数量: {len(prev_night)}")

        if len(prev_night) > 0:
            print("\n最后5个:")
            for i in range(max(0, len(prev_night)-5), len(prev_night)):
                bar = prev_night[i]
                print(f"  {bar['trade_time'][0]} | date={bar['date'][0]} | vol={bar['volume'][0]:>6} | oi={bar['open_interest'][0]:>8}")

def main():
    contracts = ['cu1804', 'al1804']

    for contract in contracts:
        debug_last_day(contract)

if __name__ == "__main__":
    main()
