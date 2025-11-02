#!/usr/bin/env python3
"""
verify_fix_sa2303.py - 验证on_tick修复效果（SA2303合约）
"""

import polars as pl
from pathlib import Path

def main():
    contract = "SA2303"

    on_batch_file = Path(f"/data/future_data/dongzheng_data/full_bar_1min/{contract}_1min.parquet")
    on_tick_file = Path(f"/data/future_data/dongzheng_data/full_bar_1min_on_tick/{contract}_1min.parquet")

    print("=" * 100)
    print(f"on_tick修复效果验证 - {contract}")
    print("=" * 100)

    # 读取数据
    df_batch = pl.read_parquet(on_batch_file)
    df_tick = pl.read_parquet(on_tick_file)

    print(f"\n【bar总数对比】")
    print(f"  on_batch: {len(df_batch):,} bars")
    print(f"  on_tick:  {len(df_tick):,} bars")

    diff = len(df_tick) - len(df_batch)
    if diff == 0:
        print(f"  ✅ 完全一致！")
    else:
        print(f"  ⚠️  差异: {diff:,} bars ({diff/len(df_batch)*100:.2f}%)")

    # 检查最后交易日的21:00 bar
    print(f"\n【最后交易日检查】")

    # 获取最后交易日
    last_date_batch = df_batch['date'].max()
    last_date_tick = df_tick['date'].max()

    print(f"  on_batch最后交易日: {last_date_batch}")
    print(f"  on_tick最后交易日:  {last_date_tick}")

    # 检查最后交易日的21:00 bar
    df_batch_last = df_batch.filter(pl.col('date') == last_date_batch)
    df_tick_last = df_tick.filter(pl.col('date') == last_date_tick)

    # 检查21:00 bar
    batch_has_21 = any('21:00' in str(t) for t in df_batch_last['trade_time'].to_list())
    tick_has_21 = any('21:00' in str(t) for t in df_tick_last['trade_time'].to_list())

    print(f"\n  最后交易日21:00 bar:")
    print(f"    on_batch: {'存在' if batch_has_21 else '不存在'} {'✓' if not batch_has_21 else '✗'}")
    print(f"    on_tick:  {'存在' if tick_has_21 else '不存在'} {'✓' if not tick_has_21 else '✗'}")

    if not batch_has_21 and not tick_has_21:
        print(f"\n  ✅ 修复成功！on_tick不再生成错误的21:00空bar")
    elif batch_has_21 and tick_has_21:
        print(f"\n  ✅ 两者一致（都有21:00 bar，可能是正常情况）")
    else:
        print(f"\n  ⚠️  两者不一致")

    # 统计所有交易日的21:00 bar数量
    print(f"\n【21:00 bar统计】")

    batch_21_count = sum(1 for t in df_batch['trade_time'].to_list() if '21:00' in str(t))
    tick_21_count = sum(1 for t in df_tick['trade_time'].to_list() if '21:00' in str(t))

    print(f"  on_batch 21:00 bar总数: {batch_21_count}")
    print(f"  on_tick  21:00 bar总数: {tick_21_count}")

    if batch_21_count == tick_21_count:
        print(f"  ✅ 完全一致！")
    else:
        print(f"  ⚠️  差异: {tick_21_count - batch_21_count} 个")

    # 对比volume=0的bar数量
    print(f"\n【volume=0 bar统计】")

    batch_zero_vol = len(df_batch.filter(pl.col('volume') == 0))
    tick_zero_vol = len(df_tick.filter(pl.col('volume') == 0))

    print(f"  on_batch volume=0: {batch_zero_vol:,} bars")
    print(f"  on_tick  volume=0: {tick_zero_vol:,} bars")

    if batch_zero_vol == tick_zero_vol:
        print(f"  ✅ 完全一致！")
    else:
        print(f"  ⚠️  差异: {tick_zero_vol - batch_zero_vol:,} bars")

    # 对比volume>0的bar（实际交易数据）
    print(f"\n【实际交易数据对比】")

    batch_real = len(df_batch.filter(pl.col('volume') > 0))
    tick_real = len(df_tick.filter(pl.col('volume') > 0))

    print(f"  on_batch volume>0: {batch_real:,} bars")
    print(f"  on_tick  volume>0: {tick_real:,} bars")

    if batch_real == tick_real:
        print(f"  ✅ 完全一致！")
    else:
        print(f"  ⚠️  差异: {tick_real - batch_real:,} bars")

    print("=" * 100)

    # 最终结论
    print(f"\n【修复效果总结】")
    if diff == 0 and batch_21_count == tick_21_count:
        print(f"  ✅✅✅ 修复完全成功！on_tick与on_batch完全一致！")
    elif abs(diff) <= 10:
        print(f"  ✅ 修复效果良好，差异在可接受范围内")
    else:
        print(f"  ⚠️  还存在一定差异，需要进一步检查")

    print("=" * 100)

if __name__ == "__main__":
    main()
