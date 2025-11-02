#!/usr/bin/env python3
"""
analyze_fill_difference.py - 分析on_batch和on_tick在fill_missing_minutes前后的差异
检查是否在调用fill前就产生了不同的bars
"""

import polars as pl
from pathlib import Path

def main():
    contract = "SA2303"

    batch_file = Path(f"/data/future_data/dongzheng_data/full_bar_1min/{contract}_1min.parquet")
    tick_file = Path(f"/data/future_data/dongzheng_data/full_bar_1min_on_tick/{contract}_1min.parquet")

    print("=" * 80)
    print(f"分析 {contract} 的填充差异")
    print("=" * 80)

    df_batch = pl.read_parquet(batch_file)
    df_tick = pl.read_parquet(tick_file)

    # 过滤出非零volume的bar（这些是实际有tick的bar，不是填充的）
    df_batch_real = df_batch.filter(pl.col('volume') > 0)
    df_tick_real = df_tick.filter(pl.col('volume') > 0)

    print(f"\n【实际有tick的bar（volume > 0）】")
    print(f"  on_batch: {len(df_batch_real):,} bars")
    print(f"  on_tick:  {len(df_tick_real):,} bars")
    print(f"  差异:     {abs(len(df_batch_real) - len(df_tick_real)):,} bars")

    # 过滤出零volume的bar（这些是填充的空bar）
    df_batch_filled = df_batch.filter(pl.col('volume') == 0)
    df_tick_filled = df_tick.filter(pl.col('volume') == 0)

    print(f"\n【填充的空bar（volume = 0）】")
    print(f"  on_batch: {len(df_batch_filled):,} bars")
    print(f"  on_tick:  {len(df_tick_filled):,} bars")
    print(f"  差异:     {abs(len(df_batch_filled) - len(df_tick_filled)):,} bars")

    # 分析最后交易日
    print(f"\n【最后交易日分析】")

    batch_last_date = df_batch['trade_time'].max()[:10]
    tick_last_date = df_tick['trade_time'].max()[:10]

    print(f"  on_batch最后日期: {batch_last_date}")
    print(f"  on_tick最后日期:  {tick_last_date}")

    # 查看最后交易日的bar
    df_batch_last = df_batch.filter(pl.col('trade_time').str.starts_with(batch_last_date))
    df_tick_last = df_tick.filter(pl.col('trade_time').str.starts_with(tick_last_date))

    print(f"\n  最后交易日bars:")
    print(f"    on_batch: {len(df_batch_last)} bars")
    print(f"      - 实际: {len(df_batch_last.filter(pl.col('volume') > 0))} bars")
    print(f"      - 填充: {len(df_batch_last.filter(pl.col('volume') == 0))} bars")
    print(f"    on_tick:  {len(df_tick_last)} bars")
    print(f"      - 实际: {len(df_tick_last.filter(pl.col('volume') > 0))} bars")
    print(f"      - 填充: {len(df_tick_last.filter(pl.col('volume') == 0))} bars")

    # 查看最后交易日的时间范围
    print(f"\n  最后交易日时间范围:")
    print(f"    on_batch: {df_batch_last['trade_time'].min()} ~ {df_batch_last['trade_time'].max()}")
    print(f"    on_tick:  {df_tick_last['trade_time'].min()} ~ {df_tick_last['trade_time'].max()}")

    # 分析21:00-23:00的填充bar（夜盘）
    print(f"\n【夜盘时段(21:00-23:00)填充分析】")

    batch_night_filled = df_batch_filled.filter(
        pl.col('trade_time').str.slice(11, 5).is_in(['21:00', '21:01', '21:02', '21:30', '22:00', '22:30', '23:00'])
    )
    tick_night_filled = df_tick_filled.filter(
        pl.col('trade_time').str.slice(11, 5).is_in(['21:00', '21:01', '21:02', '21:30', '22:00', '22:30', '23:00'])
    )

    print(f"  on_batch夜盘填充: {len(batch_night_filled):,} bars")
    print(f"  on_tick夜盘填充:  {len(tick_night_filled):,} bars")

    # 按日期统计夜盘填充
    if len(tick_night_filled) > 0:
        tick_night_by_date = tick_night_filled.with_columns(
            pl.col('trade_time').str.slice(0, 10).alias('date')
        ).group_by('date').agg(pl.len().alias('count')).sort('date')

        print(f"\n  on_tick夜盘填充按日期统计（前10天）:")
        for row in tick_night_by_date.head(10).iter_rows(named=True):
            print(f"    {row['date']}: {row['count']} bars")

    print("\n" + "=" * 80)
    print("【结论】")

    if len(df_batch_real) == len(df_tick_real):
        print("  ✅ 两者实际tick生成的bar数量一致")
    else:
        print("  ⚠️ 两者实际tick生成的bar数量不一致")

    if len(df_batch_filled) != len(df_tick_filled):
        print(f"  ⚠️ 填充逻辑不同，差异 {abs(len(df_batch_filled) - len(df_tick_filled))} bars")
        print(f"  问题：on_tick可能在fill_missing_minutes之前就生成了额外的空bar")

    print("=" * 80)

if __name__ == "__main__":
    main()
