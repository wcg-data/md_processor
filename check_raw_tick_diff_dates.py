#!/usr/bin/env python3
"""
check_raw_tick_diff_dates.py - 检查差异日期的原始tick数据
"""

import polars as pl
from pathlib import Path
from datetime import date

def check_contract_date(contract, check_date):
    """检查特定合约和日期的原始tick数据"""
    tick_file = Path(f"/data/future_data/dongzheng_data/full_tick/{contract}.parquet")

    if not tick_file.exists():
        print(f"✗ 文件不存在: {tick_file}")
        return

    print("=" * 80)
    print(f"合约: {contract}, 日期: {check_date}")
    print("=" * 80)

    df = pl.read_parquet(tick_file)

    # 添加日期列
    df = df.with_columns(
        pl.col('trade_time').dt.date().alias('date'),
        pl.col('trade_time').dt.hour().alias('hour'),
        pl.col('trade_time').dt.strftime('%H:%M:%S').alias('time')
    )

    # 筛选这一天的数据
    df_date = df.filter(pl.col('date') == check_date)

    print(f"\n【原始tick统计】")
    print(f"  总tick数: {len(df_date):,}")

    if len(df_date) == 0:
        print(f"  ⚠️  这一天没有任何tick数据！")
        return

    # 时间范围
    print(f"  第一个tick: {df_date.sort('trade_time')['trade_time'][0]}")
    print(f"  最后tick:   {df_date.sort('trade_time').tail(1)['trade_time'][0]}")

    # 按小时统计
    hour_counts = df_date.group_by('hour').agg(pl.count().alias('count')).sort('hour')

    print(f"\n【按小时分布】")
    for row in hour_counts.iter_rows(named=True):
        hour = row['hour']
        count = row['count']

        if hour == 9:
            session = "集合竞价+开盘"
        elif 10 <= hour <= 11 or 13 <= hour <= 14:
            session = "日盘"
        elif hour == 15:
            session = "收盘"
        elif hour == 20:
            session = "夜盘准备"
        elif hour == 21 or hour == 22:
            session = "夜盘"
        elif hour == 23:
            session = "夜盘收盘"
        else:
            session = "其他"

        print(f"  {hour:02d}:xx - {count:6,} ticks ({session})")

    # 检查夜盘集合竞价和连续交易
    print(f"\n【夜盘时段检查】")
    df_night = df_date.filter(
        (pl.col('hour') >= 20) & (pl.col('hour') <= 23)
    )

    if len(df_night) > 0:
        print(f"  夜盘tick数: {len(df_night):,}")

        # 检查21:00集合竞价
        df_21 = df_night.filter(
            (pl.col('time') >= '20:55:00') & (pl.col('time') <= '21:00:59')
        )

        if len(df_21) > 0:
            print(f"  ✓ 有夜盘集合竞价tick (20:55-21:00): {len(df_21)} 个")
            print(f"    {df_21.sort('trade_time')['trade_time'].to_list()}")
        else:
            print(f"  ✗ 没有夜盘集合竞价tick (20:55-21:00)")
    else:
        print(f"  ✗ 没有任何夜盘tick（20:00-23:59）")

    # 检查前后几天
    print(f"\n【前后日期对比】")
    all_dates = sorted(df['date'].unique().to_list())
    target_idx = all_dates.index(check_date) if check_date in all_dates else -1

    if target_idx >= 0:
        for offset in [-1, 0, 1]:
            idx = target_idx + offset
            if 0 <= idx < len(all_dates):
                check_date = all_dates[idx]
                df_check = df.filter(pl.col('date') == check_date)
                df_check_night = df_check.filter(
                    pl.col('trade_time').dt.hour().is_between(20, 23)
                )

                marker = "← 当天" if offset == 0 else ""
                print(f"  {check_date}: 总tick={len(df_check):,}, 夜盘tick={len(df_check_night):,} {marker}")

def main():
    print("=" * 80)
    print("检查差异日期的原始tick数据")
    print("=" * 80)

    # au2409: 2024-09-10
    check_contract_date('au2409', date(2024, 9, 10))

    print("\n")

    # bc2509: 2025-09-11
    check_contract_date('bc2509', date(2025, 9, 11))

    print("\n" + "=" * 80)
    print("【结论】")
    print("=" * 80)
    print("""
如果这两天的原始tick数据**没有夜盘tick**，则说明：
- ✅ on_tick（修复后）：正确地没有生成21:00 bar，没有填充夜盘
- ⚠️  on_batch：错误地生成了21:00 bar，导致fill_missing_minutes填充了整个夜盘

如果这两天的原始tick数据**有夜盘tick**，则说明：
- ⚠️  on_tick（修复后）：可能过度修复，丢失了真实的夜盘数据
- ✅ on_batch：正确保留了夜盘数据
    """)
    print("=" * 80)

if __name__ == "__main__":
    main()
