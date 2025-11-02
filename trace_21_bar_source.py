#!/usr/bin/env python3
"""
trace_21_bar_source.py - 追踪21:00空bar的来源
检查倒数第二天是否也有这个问题
"""

import polars as pl
from pathlib import Path

def main():
    contract = "SA2303"

    tick_file = Path(f"/data/future_data/dongzheng_data/full_bar_1min_on_tick/{contract}_1min.parquet")
    df_tick = pl.read_parquet(tick_file)

    all_dates = sorted(set(df_tick['trade_time'].str.slice(0, 10).to_list()))

    print("=" * 80)
    print(f"追踪 {contract} - 21:00空bar的来源")
    print("=" * 80)

    # 检查最后5个交易日的21:00 bar
    print(f"\n【最后5个交易日的21:00 bar】")
    for date in all_dates[-5:]:
        df_date = df_tick.filter(pl.col('trade_time').str.starts_with(date))
        df_21 = df_date.filter(pl.col('trade_time').str.slice(11, 5) == '21:00')

        if len(df_21) > 0:
            vol = df_21['volume'][0]
            status = "实际" if vol > 0 else "空bar"
            print(f"  {date}: 21:00存在 ({status}, vol={vol})")
        else:
            print(f"  {date}: 21:00不存在")

    # 检查最后3个交易日的夜盘实际交易情况
    print(f"\n【最后3个交易日的夜盘实际交易】")
    for date in all_dates[-3:]:
        df_date = df_tick.filter(pl.col('trade_time').str.starts_with(date))

        # 夜盘时段：21:00-23:00
        df_night = df_date.filter(
            pl.col('trade_time').str.slice(11, 2).cast(pl.Int32).is_between(21, 23)
        )

        df_night_real = df_night.filter(pl.col('volume') > 0)

        print(f"\n  {date}:")
        print(f"    夜盘总bars: {len(df_night)}")
        print(f"    实际交易bars: {len(df_night_real)}")

        if len(df_night_real) > 0:
            print(f"    第一个实际bar: {df_night_real.sort('trade_time')['trade_time'][0]}")
            print(f"    最后实际bar: {df_night_real.sort('trade_time').tail(1)['trade_time'][0]}")

    # 关键：检查21:00集合竞价bar的生成逻辑
    print(f"\n【21:00集合竞价bar生成逻辑分析】")
    print(f"  21:00是夜盘集合竞价时间（实际是20:55-21:00）")
    print(f"  on_tick的Bar1MinAggregator应该：")
    print(f"    1. 在收到21:00的集合竞价tick时，生成21:00的bar")
    print(f"    2. 在没有21:00的tick时，不生成bar")

    # 检查最后交易日是否应该有21:00的bar
    last_date = all_dates[-1]
    second_last_date = all_dates[-2]

    df_last = df_tick.filter(pl.col('trade_time').str.starts_with(last_date))
    df_second_last = df_tick.filter(pl.col('trade_time').str.starts_with(second_last_date))

    print(f"\n【关键对比】")
    print(f"  倒数第二日 ({second_last_date}):")
    df_21_second = df_second_last.filter(pl.col('trade_time').str.slice(11, 5) == '21:00')
    if len(df_21_second) > 0:
        print(f"    有21:00 bar, volume={df_21_second['volume'][0]}")

    # 检查倒数第二日夜盘的实际交易
    df_night_second = df_second_last.filter(
        pl.col('trade_time').str.slice(11, 2).cast(pl.Int32).is_between(21, 23)
    )
    df_night_second_real = df_night_second.filter(pl.col('volume') > 0)
    if len(df_night_second_real) > 0:
        print(f"    夜盘有{len(df_night_second_real)}个实际交易bar")
        print(f"    第一个: {df_night_second_real.sort('trade_time')['trade_time'][0]}")

    print(f"\n  最后一日 ({last_date}):")
    df_21_last = df_last.filter(pl.col('trade_time').str.slice(11, 5) == '21:00')
    if len(df_21_last) > 0:
        print(f"    有21:00 bar, volume={df_21_last['volume'][0]} ⚠️")

    # 检查最后一日夜盘的实际交易
    df_night_last = df_last.filter(
        pl.col('trade_time').str.slice(11, 2).cast(pl.Int32).is_between(21, 23)
    )
    df_night_last_real = df_night_last.filter(pl.col('volume') > 0)
    print(f"    夜盘有{len(df_night_last_real)}个实际交易bar")

    if len(df_night_last_real) == 0 and len(df_21_last) > 0:
        print(f"\n  ⚠️⚠️⚠️ 矛盾！")
        print(f"    - 最后一日夜盘没有任何实际交易")
        print(f"    - 但却有21:00的空bar")
        print(f"    - 这个空bar是哪里来的？")

    print("\n" + "=" * 80)
    print("【推测】")
    print("  可能原因：")
    print("  1. on_tick的flush()在文件结束时，错误地生成了21:00的bar")
    print("  2. on_tick的跨日逻辑在处理最后一个tick后，生成了次日的集合竞价时间点")
    print("  3. on_tick的集合竞价处理逻辑有bug，即使没有tick也生成了21:00的bar")
    print("=" * 80)

if __name__ == "__main__":
    main()
