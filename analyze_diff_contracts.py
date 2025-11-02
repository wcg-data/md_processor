#!/usr/bin/env python3
"""
analyze_diff_contracts.py - 深度分析au2409和bc2509的差异
"""

import polars as pl
from pathlib import Path

def analyze_contract(contract):
    """深度分析单个合约的差异"""
    batch_file = Path(f"/data/future_data/dongzheng_data/full_bar_1min/{contract}_1min.parquet")
    tick_file = Path(f"/data/future_data/dongzheng_data/full_bar_1min_on_tick/{contract}_1min.parquet")

    print("=" * 100)
    print(f"合约: {contract}")
    print("=" * 100)

    df_batch = pl.read_parquet(batch_file)
    df_tick = pl.read_parquet(tick_file)

    print(f"\n【基本统计】")
    print(f"  on_batch: {len(df_batch):,} bars")
    print(f"  on_tick:  {len(df_tick):,} bars")
    print(f"  差异:     {len(df_tick) - len(df_batch):+,} bars")

    # volume统计
    batch_vol0 = len(df_batch.filter(pl.col('volume') == 0))
    tick_vol0 = len(df_tick.filter(pl.col('volume') == 0))
    batch_real = len(df_batch.filter(pl.col('volume') > 0))
    tick_real = len(df_tick.filter(pl.col('volume') > 0))

    print(f"\n【volume统计】")
    print(f"  on_batch volume=0: {batch_vol0:,} bars")
    print(f"  on_tick  volume=0: {tick_vol0:,} bars")
    print(f"  差异:              {tick_vol0 - batch_vol0:+,} bars")
    print(f"\n  on_batch volume>0: {batch_real:,} bars")
    print(f"  on_tick  volume>0: {tick_real:,} bars")
    print(f"  差异:              {tick_real - batch_real:+,} bars")

    # 交易日期范围
    batch_dates = sorted(df_batch['date'].unique().to_list())
    tick_dates = sorted(df_tick['date'].unique().to_list())

    print(f"\n【交易日期】")
    print(f"  on_batch: {len(batch_dates)} 天 ({batch_dates[0]} ~ {batch_dates[-1]})")
    print(f"  on_tick:  {len(tick_dates)} 天 ({tick_dates[0]} ~ {tick_dates[-1]})")

    # 检查日期差异
    only_batch = set(batch_dates) - set(tick_dates)
    only_tick = set(tick_dates) - set(batch_dates)

    if only_batch:
        print(f"\n  仅on_batch有的日期: {sorted(only_batch)}")
    if only_tick:
        print(f"  仅on_tick有的日期:  {sorted(only_tick)}")

    # 按日期统计差异
    print(f"\n【逐日对比】")
    common_dates = sorted(set(batch_dates) & set(tick_dates))

    diff_days = []
    for date in common_dates:
        batch_count = len(df_batch.filter(pl.col('date') == date))
        tick_count = len(df_tick.filter(pl.col('date') == date))
        diff = tick_count - batch_count

        if diff != 0:
            diff_days.append({
                'date': date,
                'batch': batch_count,
                'tick': tick_count,
                'diff': diff
            })

    if diff_days:
        print(f"\n  存在差异的日期: {len(diff_days)} 天")
        print(f"\n  {'日期':<12} {'batch_bars':>10} {'tick_bars':>10} {'差异':>8}")
        print("-" * 50)
        for d in diff_days[:20]:  # 显示前20天
            print(f"  {d['date']!s:<12} {d['batch']:>10} {d['tick']:>10} {d['diff']:>+8}")

        if len(diff_days) > 20:
            print(f"  ... 还有 {len(diff_days) - 20} 天")

        # 分析差异最大的那一天
        max_diff_day = max(diff_days, key=lambda x: abs(x['diff']))
        print(f"\n【差异最大的日期详细分析】")
        print(f"  日期: {max_diff_day['date']}")
        print(f"  差异: {max_diff_day['diff']:+} bars")

        analyze_date(df_batch, df_tick, max_diff_day['date'])
    else:
        print(f"  所有日期的bar数量都一致")

    # 检查最后几天
    print(f"\n【最后5个交易日对比】")
    print(f"  {'日期':<12} {'batch_bars':>10} {'tick_bars':>10} {'差异':>8}")
    print("-" * 50)
    for date in common_dates[-5:]:
        batch_count = len(df_batch.filter(pl.col('date') == date))
        tick_count = len(df_tick.filter(pl.col('date') == date))
        diff = tick_count - batch_count
        print(f"  {date!s:<12} {batch_count:>10} {tick_count:>10} {diff:>+8}")

def analyze_date(df_batch, df_tick, date):
    """分析特定日期的差异"""
    df_batch_date = df_batch.filter(pl.col('date') == date).sort('trade_time')
    df_tick_date = df_tick.filter(pl.col('date') == date).sort('trade_time')

    print(f"\n  【{date} 详细对比】")

    # 时间范围
    if len(df_batch_date) > 0:
        print(f"\n  on_batch时间范围:")
        print(f"    第一个bar: {df_batch_date['trade_time'][0]}")
        print(f"    最后bar:   {df_batch_date['trade_time'][-1]}")

    if len(df_tick_date) > 0:
        print(f"\n  on_tick时间范围:")
        print(f"    第一个bar: {df_tick_date['trade_time'][0]}")
        print(f"    最后bar:   {df_tick_date['trade_time'][-1]}")

    # 获取所有时间点
    batch_times = set(df_batch_date['trade_time'].to_list())
    tick_times = set(df_tick_date['trade_time'].to_list())

    only_batch = sorted(batch_times - tick_times)
    only_tick = sorted(tick_times - batch_times)

    if only_batch:
        print(f"\n  仅on_batch有的时间点 ({len(only_batch)}个):")
        for t in only_batch[:20]:
            row = df_batch_date.filter(pl.col('trade_time') == t)[0]
            print(f"    {t}: volume={row['volume'][0]}, close={row['close'][0]:.2f}")

    if only_tick:
        print(f"\n  仅on_tick有的时间点 ({len(only_tick)}个):")
        for t in only_tick[:20]:
            row = df_tick_date.filter(pl.col('trade_time') == t)[0]
            print(f"    {t}: volume={row['volume'][0]}, close={row['close'][0]:.2f}")

    # 检查是否是集合竞价时间
    batch_21 = df_batch_date.filter(pl.col('trade_time').str.slice(11, 5) == '21:00')
    tick_21 = df_tick_date.filter(pl.col('trade_time').str.slice(11, 5) == '21:00')

    print(f"\n  21:00 bar检查:")
    print(f"    on_batch有21:00: {'是' if len(batch_21) > 0 else '否'}")
    print(f"    on_tick有21:00:  {'是' if len(tick_21) > 0 else '否'}")

    # 检查夜盘时段
    def get_night_bars(df):
        return df.filter(
            (pl.col('trade_time').str.slice(11, 2).cast(pl.Int32) >= 21) |
            (pl.col('trade_time').str.slice(11, 2).cast(pl.Int32) <= 2)
        )

    batch_night = get_night_bars(df_batch_date)
    tick_night = get_night_bars(df_tick_date)

    print(f"\n  夜盘时段bar数:")
    print(f"    on_batch: {len(batch_night)} bars")
    print(f"    on_tick:  {len(tick_night)} bars")
    print(f"    差异:     {len(tick_night) - len(batch_night):+} bars")

def main():
    print("=" * 100)
    print("深度分析存在差异的合约")
    print("=" * 100)

    contracts = ['au2409', 'bc2509']

    for contract in contracts:
        analyze_contract(contract)
        print("\n")

    # 总结
    print("=" * 100)
    print("【差异原因推断】")
    print("=" * 100)
    print("""
可能的原因：
1. 集合竞价时段处理差异（21:00/09:00/09:30）
2. 最后交易日停夜盘的处理差异
3. 跨日逻辑的微小差异
4. 特定交易日的异常情况（停盘、节假日等）

建议进一步检查：
- 原始tick数据在差异日期的情况
- 这两个合约的交易时段配置
- 集合竞价tick的分布
    """)
    print("=" * 100)

if __name__ == "__main__":
    main()
