#!/usr/bin/env python3
"""
check_raw_tick_last_day.py - 检查SA2303最后交易日的原始tick数据
重点：2023-03-14夜盘是否有任何tick（包括集合竞价）
"""

import polars as pl
from pathlib import Path

def main():
    contract = "SA2303"

    # 读取原始tick数据
    tick_file = Path(f"/data/future_data/dongzheng_data/full_tick/{contract}.parquet")

    if not tick_file.exists():
        print(f"✗ 原始tick文件不存在: {tick_file}")
        return

    print("=" * 80)
    print(f"检查 {contract} 原始tick数据 - 最后交易日夜盘情况")
    print("=" * 80)

    df = pl.read_parquet(tick_file)

    # 获取所有交易日期（trade_time是datetime类型）
    df = df.with_columns(
        pl.col('trade_time').dt.date().alias('date')
    )

    all_dates = sorted(df['date'].unique().to_list())

    print(f"\n总交易日数: {len(all_dates)}")
    print(f"第一个交易日: {all_dates[0]}")
    print(f"最后交易日: {all_dates[-1]}")

    # 检查最后3个交易日
    print(f"\n{'=' * 80}")
    print(f"最后3个交易日的tick分布")
    print(f"{'=' * 80}")

    for date in all_dates[-3:]:
        df_date = df.filter(pl.col('date') == date)

        print(f"\n【{date}】")
        print(f"  总tick数: {len(df_date):,}")

        # 按小时统计
        df_date = df_date.with_columns(
            pl.col('trade_time').dt.hour().alias('hour')
        )

        hour_counts = df_date.group_by('hour').agg(
            pl.count().alias('count')
        ).sort('hour')

        print(f"  按小时分布:")
        for row in hour_counts.iter_rows(named=True):
            hour = row['hour']
            count = row['count']

            # 判断时段
            if hour == 9:
                session = "集合竞价+开盘"
            elif 10 <= hour <= 11 or 13 <= hour <= 14:
                session = "日盘"
            elif hour == 15:
                session = "收盘"
            elif hour == 20:
                session = "夜盘集合竞价准备"
            elif hour == 21 or hour == 22:
                session = "夜盘"
            elif hour == 23:
                session = "夜盘收盘"
            elif hour == 0 or hour == 1 or hour == 2:
                session = "夜盘后半段（跨日）"
            else:
                session = "其他"

            print(f"    {hour:02d}:xx - {count:6,} ticks ({session})")

        # 关键：检查夜盘集合竞价时段 (20:55-21:00)
        print(f"\n  【夜盘集合竞价时段详细分析】")
        df_auction = df_date.filter(
            (pl.col('hour') == 20) | (pl.col('hour') == 21)
        )

        if len(df_auction) > 0:
            print(f"  20:xx - 21:xx 总tick数: {len(df_auction):,}")

            # 精确到分钟
            df_auction = df_auction.with_columns(
                pl.col('trade_time').dt.strftime('%H:%M').alias('time')
            )

            time_counts = df_auction.group_by('time').agg(
                pl.count().alias('count')
            ).sort('time')

            print(f"  精确到分钟:")
            for row in time_counts.iter_rows(named=True):
                time_str = row['time']
                count = row['count']

                if '20:55' <= time_str <= '21:00':
                    marker = "← 集合竞价时段"
                elif time_str > '21:00':
                    marker = "← 连续交易"
                else:
                    marker = ""

                print(f"    {time_str}: {count:6,} ticks {marker}")
        else:
            print(f"  ✗ 没有20:xx - 21:xx的tick")

        # 检查夜盘连续交易时段 (21:01-23:00)
        print(f"\n  【夜盘连续交易时段】")
        df_night = df_date.filter(
            (pl.col('hour') >= 21) & (pl.col('hour') <= 23)
        )

        if len(df_night) > 0:
            print(f"  21:xx - 23:xx 总tick数: {len(df_night):,}")
            print(f"  第一个tick: {df_night.sort('trade_time')['trade_time'][0]}")
            print(f"  最后tick: {df_night.sort('trade_time').tail(1)['trade_time'][0]}")
        else:
            print(f"  ✗ 夜盘没有任何tick")

        # 显示这一天的第一个和最后一个tick
        print(f"\n  【当日tick时间范围】")
        print(f"  第一个tick: {df_date.sort('trade_time')['trade_time'][0]}")
        print(f"  最后tick: {df_date.sort('trade_time').tail(1)['trade_time'][0]}")

    # 关键结论
    print(f"\n{'=' * 80}")
    print(f"【关键结论】")
    print(f"{'=' * 80}")

    last_date = all_dates[-1]
    df_last = df.filter(pl.col('date') == last_date)

    # 检查夜盘是否有tick
    df_last_night = df_last.filter(
        pl.col('trade_time').dt.hour().is_between(20, 23)
    )

    if len(df_last_night) > 0:
        print(f"\n✓ 最后交易日 ({last_date}) **有夜盘tick**")
        print(f"  夜盘tick数: {len(df_last_night):,}")
        print(f"  时间范围: {df_last_night.sort('trade_time')['trade_time'][0]} ~ {df_last_night.sort('trade_time').tail(1)['trade_time'][0]}")

        # 检查是否有集合竞价tick (20:55-21:00)
        df_auction = df_last_night.with_columns(
            pl.col('trade_time').dt.strftime('%H:%M').alias('time_str')
        ).filter(
            (pl.col('time_str') >= '20:55') & (pl.col('time_str') <= '21:00')
        )

        if len(df_auction) > 0:
            print(f"  ✓ 有集合竞价tick (20:55-21:00): {len(df_auction):,}个")
            print(f"  → on_tick生成21:00的bar是**正确的**")
        else:
            print(f"  ✗ 没有集合竞价tick (20:55-21:00)")
            print(f"  → 需要检查on_tick为何生成21:00的bar")
    else:
        print(f"\n✗ 最后交易日 ({last_date}) **没有任何夜盘tick**")
        print(f"  → on_batch不生成21:00 bar是**正确的**")
        print(f"  → on_tick生成21:00 bar是**错误的**")

    print("=" * 80)

if __name__ == "__main__":
    main()
