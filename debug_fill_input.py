#!/usr/bin/env python3
"""
debug_fill_input.py - 检查on_tick在最后交易日生成了哪些bar
分析是否在fill_missing_minutes之前就产生了额外的时间点
"""

import polars as pl
from pathlib import Path

def main():
    contract = "SA2303"

    batch_file = Path(f"/data/future_data/dongzheng_data/full_bar_1min/{contract}_1min.parquet")
    tick_file = Path(f"/data/future_data/dongzheng_data/full_bar_1min_on_tick/{contract}_1min.parquet")

    print("=" * 80)
    print(f"调查 {contract} - on_tick为何生成更多填充bar")
    print("=" * 80)

    df_batch = pl.read_parquet(batch_file)
    df_tick = pl.read_parquet(tick_file)

    # 焦点：最后交易日 2023-03-14
    last_date = "2023-03-14"

    df_batch_last = df_batch.filter(pl.col('trade_time').str.starts_with(last_date))
    df_tick_last = df_tick.filter(pl.col('trade_time').str.starts_with(last_date))

    print(f"\n【最后交易日 {last_date} 详细分析】")
    print(f"  on_batch: {len(df_batch_last)} bars")
    print(f"  on_tick:  {len(df_tick_last)} bars")

    # 按时段分组分析
    print(f"\n【按时段分析】")

    # 集合竞价 (09:00)
    auction_batch = df_batch_last.filter(pl.col('trade_time').str.slice(11, 5) == '09:00')
    auction_tick = df_tick_last.filter(pl.col('trade_time').str.slice(11, 5) == '09:00')
    print(f"\n  集合竞价 (09:00):")
    print(f"    on_batch: {len(auction_batch)} bars")
    print(f"    on_tick:  {len(auction_tick)} bars")

    # 日盘 (09:30-11:30, 13:30-15:00)
    def is_day_session(time_str):
        hour_min = time_str[11:16]
        return ('09:30' <= hour_min <= '11:30') or ('13:30' <= hour_min <= '15:00')

    day_batch = df_batch_last.filter(pl.col('trade_time').map_elements(is_day_session, return_dtype=pl.Boolean))
    day_tick = df_tick_last.filter(pl.col('trade_time').map_elements(is_day_session, return_dtype=pl.Boolean))

    print(f"\n  日盘 (09:30-11:30, 13:30-15:00):")
    print(f"    on_batch: {len(day_batch)} bars (实际:{len(day_batch.filter(pl.col('volume')>0))}, 填充:{len(day_batch.filter(pl.col('volume')==0))})")
    print(f"    on_tick:  {len(day_tick)} bars (实际:{len(day_tick.filter(pl.col('volume')>0))}, 填充:{len(day_tick.filter(pl.col('volume')==0))})")

    # 夜盘 (21:00-23:00)
    def is_night_session(time_str):
        hour_min = time_str[11:16]
        return '21:00' <= hour_min <= '23:00'

    night_batch = df_batch_last.filter(pl.col('trade_time').map_elements(is_night_session, return_dtype=pl.Boolean))
    night_tick = df_tick_last.filter(pl.col('trade_time').map_elements(is_night_session, return_dtype=pl.Boolean))

    print(f"\n  夜盘 (21:00-23:00):")
    print(f"    on_batch: {len(night_batch)} bars (实际:{len(night_batch.filter(pl.col('volume')>0))}, 填充:{len(night_batch.filter(pl.col('volume')==0))})")
    print(f"    on_tick:  {len(night_tick)} bars (实际:{len(night_tick.filter(pl.col('volume')>0))}, 填充:{len(night_tick.filter(pl.col('volume')==0))})")

    # 关键：检查夜盘是否有实际tick
    if len(night_tick.filter(pl.col('volume') > 0)) > 0:
        print(f"\n  ⚠️ 发现：on_tick在最后交易日夜盘有{len(night_tick.filter(pl.col('volume')>0))}个实际tick!")
        print(f"     这会导致fill_missing_minutes认为该日夜盘'有数据'，从而填充整个夜盘")
    else:
        print(f"\n  ✓ on_tick在最后交易日夜盘没有实际tick")
        print(f"  但填充了 {len(night_tick)} 个空bar")
        print(f"  问题：为什么fill_missing_minutes会填充这些空bar？")

    # 检查倒数第二个交易日
    print(f"\n【倒数第二个交易日分析】")

    # 获取所有日期
    all_dates_batch = sorted(set(df_batch['trade_time'].str.slice(0, 10).to_list()))
    all_dates_tick = sorted(set(df_tick['trade_time'].str.slice(0, 10).to_list()))

    print(f"  on_batch有{len(all_dates_batch)}个交易日")
    print(f"  on_tick有{len(all_dates_tick)}个交易日")

    if len(all_dates_batch) >= 2:
        second_last_date_batch = all_dates_batch[-2]
        df_batch_second_last = df_batch.filter(pl.col('trade_time').str.starts_with(second_last_date_batch))

        print(f"\n  倒数第二日 ({second_last_date_batch}) on_batch:")
        night_batch_2nd = df_batch_second_last.filter(
            pl.col('trade_time').map_elements(is_night_session, return_dtype=pl.Boolean)
        )
        print(f"    夜盘: {len(night_batch_2nd)} bars (实际:{len(night_batch_2nd.filter(pl.col('volume')>0))}, 填充:{len(night_batch_2nd.filter(pl.col('volume')==0))})")

    if len(all_dates_tick) >= 2:
        second_last_date_tick = all_dates_tick[-2]
        df_tick_second_last = df_tick.filter(pl.col('trade_time').str.starts_with(second_last_date_tick))

        print(f"\n  倒数第二日 ({second_last_date_tick}) on_tick:")
        night_tick_2nd = df_tick_second_last.filter(
            pl.col('trade_time').map_elements(is_night_session, return_dtype=pl.Boolean)
        )
        print(f"    夜盘: {len(night_tick_2nd)} bars (实际:{len(night_tick_2nd.filter(pl.col('volume')>0))}, 填充:{len(night_tick_2nd.filter(pl.col('volume')==0))})")

    # 检查最后交易日夜盘时间点的open价格来源
    print(f"\n【最后交易日夜盘空bar的来源分析】")
    if len(night_tick) > 0:
        print(f"\n  on_tick夜盘空bar样本（前5个）:")
        for row in night_tick.filter(pl.col('volume') == 0).head(5).iter_rows(named=True):
            print(f"    {row['trade_time']}: open={row['open']:.2f}, close={row['close']:.2f}, volume={row['volume']}")

        print(f"\n  这些空bar的open价格 = {night_tick.filter(pl.col('volume')==0)['open'][0]:.2f}")
        print(f"  最后一个实际bar的close价格 = {df_tick_last.filter(pl.col('volume')>0).sort('trade_time').tail(1)['close'][0]:.2f}")

        if len(night_tick.filter(pl.col('volume') == 0)) > 0:
            first_night_open = night_tick.filter(pl.col('volume') == 0).sort('trade_time')['open'][0]
            last_real_close = df_tick_last.filter(pl.col('volume')>0).sort('trade_time').tail(1)['close'][0]

            if abs(first_night_open - last_real_close) < 0.01:
                print(f"  ✓ 夜盘空bar使用了最后实际bar的close价格（正常）")
            else:
                print(f"  ⚠️ 夜盘空bar的价格来源异常")

    print("\n" + "=" * 80)
    print("【推测】")
    print("  最可能的原因：")
    print("  1. on_tick在处理时，某个逻辑生成了volume=0的bar")
    print("  2. 这些bar被传入fill_missing_minutes")
    print("  3. fill_missing_minutes认为该时段'有数据'，填充了整个夜盘")
    print("=" * 80)

if __name__ == "__main__":
    main()
