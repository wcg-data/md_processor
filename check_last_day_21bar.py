#!/usr/bin/env python3
"""
check_last_day_21bar.py - 详细检查最后交易日的21:00 bar
"""

import polars as pl
from pathlib import Path

def main():
    contract = "SA2303"

    on_batch_file = Path(f"/data/future_data/dongzheng_data/full_bar_1min/{contract}_1min.parquet")
    on_tick_file = Path(f"/data/future_data/dongzheng_data/full_bar_1min_on_tick/{contract}_1min.parquet")

    print("=" * 100)
    print(f"最后交易日21:00 bar详细检查 - {contract}")
    print("=" * 100)

    # 读取数据
    df_batch = pl.read_parquet(on_batch_file)
    df_tick = pl.read_parquet(on_tick_file)

    # 最后交易日
    last_date = df_batch['date'].max()

    print(f"\n最后交易日: {last_date}")

    # 获取最后交易日的所有bar
    df_batch_last = df_batch.filter(pl.col('date') == last_date).sort('trade_time')
    df_tick_last = df_tick.filter(pl.col('date') == last_date).sort('trade_time')

    print(f"\n【最后交易日bar总数】")
    print(f"  on_batch: {len(df_batch_last)} bars")
    print(f"  on_tick:  {len(df_tick_last)} bars")

    # 检查21:00 bar
    df_batch_21 = df_batch_last.filter(
        pl.col('trade_time').str.slice(11, 5) == '21:00'
    )
    df_tick_21 = df_tick_last.filter(
        pl.col('trade_time').str.slice(11, 5) == '21:00'
    )

    print(f"\n【21:00 bar检查】")

    if len(df_batch_21) > 0:
        print(f"\n  on_batch 21:00 bar:")
        row = df_batch_21[0]
        print(f"    trade_time: {row['trade_time'][0]}")
        print(f"    date: {row['date'][0]}")
        print(f"    volume: {row['volume'][0]}")
        print(f"    close: {row['close'][0]:.2f}")
    else:
        print(f"\n  on_batch: 没有21:00 bar ✓")

    if len(df_tick_21) > 0:
        print(f"\n  on_tick 21:00 bar:")
        row = df_tick_21[0]
        print(f"    trade_time: {row['trade_time'][0]}")
        print(f"    date: {row['date'][0]}")
        print(f"    volume: {row['volume'][0]}")
        print(f"    close: {row['close'][0]:.2f}")
    else:
        print(f"\n  on_tick: 没有21:00 bar ✓")

    # 检查最后交易日的时间范围
    print(f"\n【最后交易日时间范围】")
    print(f"\n  on_batch:")
    print(f"    第一个bar: {df_batch_last['trade_time'][0]}")
    print(f"    最后bar: {df_batch_last['trade_time'][-1]}")

    print(f"\n  on_tick:")
    print(f"    第一个bar: {df_tick_last['trade_time'][0]}")
    print(f"    最后bar: {df_tick_last['trade_time'][-1]}")

    # 检查夜盘bar数量
    df_batch_night = df_batch_last.filter(
        (pl.col('trade_time').str.slice(11, 2).cast(pl.Int32) >= 21) |
        (pl.col('trade_time').str.slice(11, 2).cast(pl.Int32) <= 2)
    )
    df_tick_night = df_tick_last.filter(
        (pl.col('trade_time').str.slice(11, 2).cast(pl.Int32) >= 21) |
        (pl.col('trade_time').str.slice(11, 2).cast(pl.Int32) <= 2)
    )

    print(f"\n【夜盘bar数量】")
    print(f"  on_batch: {len(df_batch_night)} bars")
    print(f"  on_tick:  {len(df_tick_night)} bars")

    if len(df_batch_night) == 0 and len(df_tick_night) == 0:
        print(f"  ✅ 两者都没有夜盘bar，修复成功！")
    elif len(df_batch_night) == len(df_tick_night):
        print(f"  ⚠️  两者都有夜盘bar，数量一致")
    else:
        print(f"  ✗ 夜盘bar数量不一致")

    print("=" * 100)

if __name__ == "__main__":
    main()
