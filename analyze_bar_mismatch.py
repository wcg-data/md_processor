#!/usr/bin/env python3
"""
analyze_bar_mismatch.py - 分析on_batch和on_tick的bar数量差异
选择一个具体合约进行详细对比
"""

import polars as pl
from pathlib import Path

def main():
    # 选择 SA2303 进行详细分析（差异较大：963个bar）
    contract = "SA2303"

    batch_file = Path(f"/data/future_data/dongzheng_data/full_bar_1min/{contract}_1min.parquet")
    tick_file = Path(f"/data/future_data/dongzheng_data/full_bar_1min_on_tick/{contract}_1min.parquet")

    print("=" * 80)
    print(f"详细分析: {contract}")
    print("=" * 80)

    df_batch = pl.read_parquet(batch_file)
    df_tick = pl.read_parquet(tick_file)

    print(f"\n【基本信息】")
    print(f"  on_batch: {len(df_batch):,} bars")
    print(f"  on_tick:  {len(df_tick):,} bars")
    print(f"  差异:     {abs(len(df_batch) - len(df_tick)):,} bars ({abs(len(df_batch) - len(df_tick))*100/len(df_tick):.1f}%)")

    # 检查时间范围
    print(f"\n【时间范围】")
    print(f"  on_batch: {df_batch['trade_time'].min()} ~ {df_batch['trade_time'].max()}")
    print(f"  on_tick:  {df_tick['trade_time'].min()} ~ {df_tick['trade_time'].max()}")

    # 找出只在on_tick中存在的时间点
    batch_times = set(df_batch['trade_time'].to_list())
    tick_times = set(df_tick['trade_time'].to_list())

    only_in_batch = sorted(batch_times - tick_times)
    only_in_tick = sorted(tick_times - batch_times)

    print(f"\n【差异时间点】")
    print(f"  仅在on_batch: {len(only_in_batch)} 个")
    print(f"  仅在on_tick:  {len(only_in_tick)} 个")

    if only_in_batch:
        print(f"\n  仅在on_batch的前20个时间点:")
        for t in only_in_batch[:20]:
            bar = df_batch.filter(pl.col('trade_time') == t)
            print(f"    {t}: vol={bar['volume'][0]:,}, open={bar['open'][0]:.2f}")

    if only_in_tick:
        print(f"\n  仅在on_tick的前20个时间点:")
        for t in only_in_tick[:20]:
            bar = df_tick.filter(pl.col('trade_time') == t)
            print(f"    {t}: vol={bar['volume'][0]:,}, open={bar['open'][0]:.2f}")

    # 按日期分组统计差异
    print(f"\n【按日期统计差异】")

    # 提取日期
    df_batch = df_batch.with_columns(
        pl.col('trade_time').str.slice(0, 10).alias('date')
    )
    df_tick = df_tick.with_columns(
        pl.col('trade_time').str.slice(0, 10).alias('date')
    )

    # 统计每天的bar数
    batch_by_date = df_batch.group_by('date').agg(pl.count().alias('count_batch')).sort('date')
    tick_by_date = df_tick.group_by('date').agg(pl.count().alias('count_tick')).sort('date')

    # 合并
    merged = batch_by_date.join(tick_by_date, on='date', how='outer').fill_null(0)
    merged = merged.with_columns(
        (pl.col('count_tick') - pl.col('count_batch')).alias('diff')
    )

    # 只显示有差异的日期
    diff_dates = merged.filter(pl.col('diff') != 0).sort(pl.col('diff').abs(), descending=True)

    print(f"\n  差异最大的前20天:")
    print(f"  {'日期':<12} {'on_batch':>10} {'on_tick':>10} {'差异':>8}")
    print("  " + "-" * 45)
    for row in diff_dates.head(20).iter_rows(named=True):
        print(f"  {row['date']:<12} {row['count_batch']:>10,} {row['count_tick']:>10,} {row['diff']:>+8,}")

    print("\n" + "=" * 80)
    print("【可能原因分析】")
    print("  1. 缺失分钟补全(fill_missing_minutes)逻辑不同")
    print("  2. 集合竞价bar处理方式不同")
    print("  3. 时间窗口flush条件不同")
    print("  4. 空bar过滤条件不同")
    print("=" * 80)

if __name__ == "__main__":
    main()
