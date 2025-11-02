#!/usr/bin/env python3
"""
find_root_cause.py - 找出为什么fill_missing_minutes会填充最后交易日的夜盘
关键：检查是否有21:00的集合竞价bar导致session_has_data误判
"""

import polars as pl
from pathlib import Path

def main():
    contract = "SA2303"
    last_date = "2023-03-14"

    tick_file = Path(f"/data/future_data/dongzheng_data/full_bar_1min_on_tick/{contract}_1min.parquet")
    df_tick = pl.read_parquet(tick_file)

    print("=" * 80)
    print(f"调查 {contract} - 最后交易日 {last_date}")
    print("=" * 80)

    df_last = df_tick.filter(pl.col('trade_time').str.starts_with(last_date))

    # 检查是否有21:00的bar（这是夜盘集合竞价时间）
    print(f"\n【检查21:00集合竞价bar】")
    df_21 = df_last.filter(pl.col('trade_time').str.slice(11, 5) == '21:00')

    if len(df_21) > 0:
        print(f"  ✓ 发现21:00的bar: {len(df_21)}个")
        for row in df_21.iter_rows(named=True):
            print(f"    时间: {row['trade_time']}")
            print(f"    open={row['open']:.2f}, close={row['close']:.2f}, volume={row['volume']}")

        print(f"\n  【根本原因分析】")
        if df_21['volume'][0] == 0:
            print(f"  ⚠️ 这个21:00的bar是volume=0的空bar")
            print(f"  问题：这个空bar是谁生成的？")
            print(f"    1. 如果是fill_missing_minutes生成的，那是循环依赖")
            print(f"    2. 如果是on_tick的flush逻辑生成的，那就是bug")
        else:
            print(f"  ✓ 这个21:00的bar有实际交易")
            print(f"  fill_missing_minutes识别到这个bar，认为该日夜盘'有数据'，从而填充整个夜盘")
    else:
        print(f"  ✗ 没有发现21:00的bar")

    # 检查所有非零volume的bar
    print(f"\n【所有实际交易bar（volume > 0）】")
    df_real = df_last.filter(pl.col('volume') > 0)
    print(f"  共{len(df_real)}个实际bar:")
    for row in df_real.iter_rows(named=True):
        print(f"    {row['trade_time']}: vol={row['volume']:,}")

    # 检查时间范围
    print(f"\n【时间范围分析】")
    print(f"  最早bar: {df_last['trade_time'].min()}")
    print(f"  最晚bar: {df_last['trade_time'].max()}")
    print(f"  最早实际bar: {df_real['trade_time'].min()}")
    print(f"  最晚实际bar: {df_real['trade_time'].max()}")

    # 关键：检查是否有跨日时段的误判
    print(f"\n【跨日时段误判分析】")
    print(f"  fill_missing_minutes的逻辑（common_utils.rs:224-228）:")
    print(f"    if start_time > end_time && time <= *end_time:")
    print(f"        // 跨日时段的后半段，标记前一天")
    print(f"        session_has_data.insert((prev_date, session_idx))")

    print(f"\n  SA品种的交易时段（郑商所）:")
    print(f"    夜盘: 21:00-23:00 (跨日时段)")
    print(f"    日盘: 09:00 (集合竞价)")
    print(f"    日盘: 09:30-11:30")
    print(f"    日盘: 13:30-15:00")

    # 检查是否有凌晨的bar（如00:00-02:30）
    df_early_morning = df_last.filter(
        pl.col('trade_time').str.slice(11, 2).cast(pl.Int32).is_between(0, 2)
    )

    if len(df_early_morning) > 0:
        print(f"\n  ⚠️ 发现凌晨bar: {len(df_early_morning)}个")
        print(f"  这些bar会被识别为'前一天夜盘的后半段'")
        print(f"  导致fill_missing_minutes认为前一天夜盘'有数据'")
        for row in df_early_morning.head(5).iter_rows(named=True):
            print(f"    {row['trade_time']}: vol={row['volume']}")
    else:
        print(f"\n  ✓ 没有凌晨bar")

    # 终极检查：对比倒数第二天
    print(f"\n【对比倒数第二个交易日】")
    all_dates = sorted(set(df_tick['trade_time'].str.slice(0, 10).to_list()))
    if len(all_dates) >= 2:
        second_last_date = all_dates[-2]
        df_second_last = df_tick.filter(pl.col('trade_time').str.starts_with(second_last_date))

        df_21_second = df_second_last.filter(pl.col('trade_time').str.slice(11, 5) == '21:00')

        print(f"  倒数第二日 ({second_last_date}):")
        print(f"    有21:00的bar: {'是' if len(df_21_second) > 0 else '否'}")
        if len(df_21_second) > 0:
            print(f"    21:00的volume: {df_21_second['volume'][0]}")

        print(f"\n  最后一日 ({last_date}):")
        print(f"    有21:00的bar: {'是' if len(df_21) > 0 else '否'}")
        if len(df_21) > 0:
            print(f"    21:00的volume: {df_21['volume'][0]}")

        print(f"\n  【差异分析】")
        if len(df_21_second) > 0 and len(df_21) > 0:
            if df_21_second['volume'][0] > 0 and df_21['volume'][0] == 0:
                print(f"  ⚠️ 关键差异：")
                print(f"    - 倒数第二天21:00有实际交易（vol={df_21_second['volume'][0]}）")
                print(f"    - 最后一天21:00是空bar（vol=0）")
                print(f"    - 这个空21:00 bar导致fill_missing_minutes误判")

    print("\n" + "=" * 80)
    print("【结论】")
    if len(df_21) > 0 and df_21['volume'][0] == 0:
        print("  ✓✓✓ 找到根本原因！")
        print("  1. 最后交易日有一个21:00的空bar（volume=0）")
        print("  2. 这个空bar被传入fill_missing_minutes")
        print("  3. fill_missing_minutes的session_has_data识别逻辑认为:")
        print("     '最后交易日的夜盘时段有数据'")
        print("  4. 因此填充了整个夜盘（21:00-23:00）的121个bar")
        print(f"\n  问题根源：on_tick在某处生成了这个21:00的空bar")
    else:
        print("  需要进一步调查")
    print("=" * 80)

if __name__ == "__main__":
    main()
