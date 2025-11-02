#!/usr/bin/env python3
"""
compare_batch_tick_full.py - 全面对比on_batch和on_tick修复后的结果
"""

import polars as pl
from pathlib import Path
import sys

def compare_contract(contract):
    """对比单个合约的bar数据"""
    batch_file = Path(f"/data/future_data/dongzheng_data/full_bar_1min/{contract}_1min.parquet")
    tick_file = Path(f"/data/future_data/dongzheng_data/full_bar_1min_on_tick/{contract}_1min.parquet")

    if not batch_file.exists() or not tick_file.exists():
        return None

    try:
        df_batch = pl.read_parquet(batch_file)
        df_tick = pl.read_parquet(tick_file)

        result = {
            'contract': contract,
            'batch_bars': len(df_batch),
            'tick_bars': len(df_tick),
            'diff': len(df_tick) - len(df_batch),
            'batch_vol0': len(df_batch.filter(pl.col('volume') == 0)),
            'tick_vol0': len(df_tick.filter(pl.col('volume') == 0)),
            'batch_real': len(df_batch.filter(pl.col('volume') > 0)),
            'tick_real': len(df_tick.filter(pl.col('volume') > 0)),
        }

        return result
    except Exception as e:
        print(f"✗ {contract}: {e}", file=sys.stderr)
        return None

def main():
    print("=" * 100)
    print("on_batch vs on_tick 全面对比分析（修复后）")
    print("=" * 100)

    # 获取共同处理的合约列表
    batch_dir = Path("/data/future_data/dongzheng_data/full_bar_1min")
    tick_dir = Path("/data/future_data/dongzheng_data/full_bar_1min_on_tick")

    batch_contracts = {f.stem.replace('_1min', '') for f in batch_dir.glob("*_1min.parquet")}
    tick_contracts = {f.stem.replace('_1min', '') for f in tick_dir.glob("*_1min.parquet")}

    common_contracts = sorted(batch_contracts & tick_contracts)
    only_batch = sorted(batch_contracts - tick_contracts)
    only_tick = sorted(tick_contracts - batch_contracts)

    print(f"\n【文件统计】")
    print(f"  on_batch文件数: {len(batch_contracts)}")
    print(f"  on_tick文件数:  {len(tick_contracts)}")
    print(f"  共同处理:      {len(common_contracts)}")
    print(f"  仅batch:       {len(only_batch)}")
    print(f"  仅tick:        {len(only_tick)}")

    if only_batch:
        print(f"\n  仅on_batch处理的前20个: {only_batch[:20]}")
    if only_tick:
        print(f"\n  仅on_tick处理的前20个: {only_tick[:20]}")

    # 对比共同处理的合约
    print(f"\n{'=' * 100}")
    print(f"对比共同处理的 {len(common_contracts)} 个合约...")
    print(f"{'=' * 100}")

    results = []
    identical_count = 0
    diff_count = 0

    for i, contract in enumerate(common_contracts):
        if (i + 1) % 1000 == 0:
            print(f"  进度: {i+1} / {len(common_contracts)}")

        result = compare_contract(contract)
        if result:
            results.append(result)

            if result['diff'] == 0:
                identical_count += 1
            else:
                diff_count += 1

    print(f"✓ 对比完成")

    # 统计结果
    print(f"\n{'=' * 100}")
    print(f"【整体统计】")
    print(f"{'=' * 100}")

    print(f"\n完全一致: {identical_count} / {len(results)} ({identical_count/len(results)*100:.1f}%)")
    print(f"存在差异: {diff_count} / {len(results)} ({diff_count/len(results)*100:.1f}%)")

    # bar总数统计
    total_batch_bars = sum(r['batch_bars'] for r in results)
    total_tick_bars = sum(r['tick_bars'] for r in results)
    total_diff = total_tick_bars - total_batch_bars

    print(f"\n【bar总数】")
    print(f"  on_batch: {total_batch_bars:,} bars")
    print(f"  on_tick:  {total_tick_bars:,} bars")
    print(f"  差异:     {total_diff:,} bars ({abs(total_diff)/total_batch_bars*100:.2f}%)")

    # volume=0 bar统计
    total_batch_vol0 = sum(r['batch_vol0'] for r in results)
    total_tick_vol0 = sum(r['tick_vol0'] for r in results)

    print(f"\n【volume=0 bar】")
    print(f"  on_batch: {total_batch_vol0:,} bars")
    print(f"  on_tick:  {total_tick_vol0:,} bars")
    print(f"  差异:     {total_tick_vol0 - total_batch_vol0:,} bars")

    # 实际交易数据统计
    total_batch_real = sum(r['batch_real'] for r in results)
    total_tick_real = sum(r['tick_real'] for r in results)

    print(f"\n【实际交易数据 (volume>0)】")
    print(f"  on_batch: {total_batch_real:,} bars")
    print(f"  on_tick:  {total_tick_real:,} bars")

    if total_batch_real == total_tick_real:
        print(f"  ✅ 完全一致！")
    else:
        print(f"  ⚠️  差异: {total_tick_real - total_batch_real:,} bars")

    # 存在差异的合约详情
    if diff_count > 0:
        print(f"\n{'=' * 100}")
        print(f"【存在差异的合约】({diff_count}个)")
        print(f"{'=' * 100}")

        diff_results = [r for r in results if r['diff'] != 0]
        diff_results.sort(key=lambda x: abs(x['diff']), reverse=True)

        print(f"\n前20个差异最大的合约:")
        print(f"{'合约':<12} {'batch_bars':>10} {'tick_bars':>10} {'差异':>8} {'batch_vol0':>12} {'tick_vol0':>12}")
        print("-" * 100)

        for r in diff_results[:20]:
            print(f"{r['contract']:<12} {r['batch_bars']:>10,} {r['tick_bars']:>10,} "
                  f"{r['diff']:>+8,} {r['batch_vol0']:>12,} {r['tick_vol0']:>12,}")

        # 按差异类型分类
        more_bars = [r for r in diff_results if r['diff'] > 0]
        less_bars = [r for r in diff_results if r['diff'] < 0]

        print(f"\n差异分布:")
        print(f"  on_tick多bar: {len(more_bars)} 个合约")
        print(f"  on_tick少bar: {len(less_bars)} 个合约")

    # 最终结论
    print(f"\n{'=' * 100}")
    print(f"【修复效果评估】")
    print(f"{'=' * 100}")

    if diff_count == 0:
        print(f"\n✅✅✅ 修复完全成功！on_tick与on_batch在所有合约上完全一致！")
    elif diff_count / len(results) < 0.01:  # 小于1%
        print(f"\n✅ 修复效果优秀！99%以上的合约完全一致")
    elif diff_count / len(results) < 0.05:  # 小于5%
        print(f"\n✅ 修复效果良好！95%以上的合约完全一致")
    else:
        print(f"\n⚠️  还有{diff_count/len(results)*100:.1f}%的合约存在差异，需要进一步检查")

    print(f"\n实际交易数据一致性: ", end="")
    if total_batch_real == total_tick_real:
        print(f"✅ 100%一致")
    else:
        diff_pct = abs(total_tick_real - total_batch_real) / total_batch_real * 100
        print(f"⚠️  差异{diff_pct:.2f}%")

    print("=" * 100)

if __name__ == "__main__":
    main()
