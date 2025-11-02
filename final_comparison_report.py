#!/usr/bin/env python3
"""
final_comparison_report.py - on_batch修复后的最终对比报告
对比修复后的on_batch和on_tick输出结果
"""

from pathlib import Path
import polars as pl

def main():
    batch_dir = Path("/data/future_data/dongzheng_data/full_bar_1min")
    tick_dir = Path("/data/future_data/dongzheng_data/full_bar_1min_on_tick")

    print("=" * 80)
    print("on_batch vs on_tick 最终一致性报告 (修复后)")
    print("=" * 80)

    # 1. 文件数量对比
    batch_files = set(f.name for f in batch_dir.glob("*.parquet"))
    tick_files = set(f.name for f in tick_dir.glob("*.parquet"))

    print(f"\n【文件数量对比】")
    print(f"  on_batch: {len(batch_files):,} 个文件")
    print(f"  on_tick:  {len(tick_files):,} 个文件")
    print(f"  共同文件: {len(batch_files & tick_files):,} 个")

    only_in_batch = sorted(batch_files - tick_files)
    only_in_tick = sorted(tick_files - batch_files)

    if only_in_batch:
        print(f"\n  ⚠ 仅在on_batch中: {len(only_in_batch)} 个")
        for f in only_in_batch[:10]:
            print(f"    - {f}")

    if only_in_tick:
        print(f"\n  ⚠ 仅在on_tick中: {len(only_in_tick)} 个")
        for f in only_in_tick[:10]:
            print(f"    - {f}")

    # 2. 数据一致性验证（采样）
    common_files = sorted(batch_files & tick_files)

    if not common_files:
        print("\n❌ 没有共同文件可以对比！")
        return

    print(f"\n【数据一致性验证】采样 50 个共同文件")

    import random
    random.seed(42)
    sample_files = random.sample(common_files, min(50, len(common_files)))

    bar_count_match = 0
    data_content_match = 0
    mismatches = []

    for filename in sample_files:
        batch_file = batch_dir / filename
        tick_file = tick_dir / filename

        try:
            df_batch = pl.read_parquet(batch_file)
            df_tick = pl.read_parquet(tick_file)

            # 检查bar数量
            if len(df_batch) == len(df_tick):
                bar_count_match += 1

                # 检查关键字段是否一致
                key_cols = ['trade_time', 'open', 'high', 'low', 'close', 'volume']
                cols_to_check = [c for c in key_cols if c in df_batch.columns and c in df_tick.columns]

                if df_batch.select(cols_to_check).equals(df_tick.select(cols_to_check)):
                    data_content_match += 1
                else:
                    mismatches.append({
                        'file': filename,
                        'batch_bars': len(df_batch),
                        'tick_bars': len(df_tick),
                        'issue': '数据内容不一致'
                    })
            else:
                mismatches.append({
                    'file': filename,
                    'batch_bars': len(df_batch),
                    'tick_bars': len(df_tick),
                    'issue': f'bar数量不一致 ({len(df_batch)} vs {len(df_tick)})'
                })
        except Exception as e:
            mismatches.append({
                'file': filename,
                'batch_bars': '?',
                'tick_bars': '?',
                'issue': f'读取错误: {e}'
            })

    print(f"\n  Bar数量一致: {bar_count_match}/{len(sample_files)} ({bar_count_match*100/len(sample_files):.1f}%)")
    print(f"  数据内容一致: {data_content_match}/{len(sample_files)} ({data_content_match*100/len(sample_files):.1f}%)")

    if mismatches:
        print(f"\n  ⚠ 发现 {len(mismatches)} 个不一致:")
        for m in mismatches[:10]:
            print(f"    - {m['file']}: {m['issue']}")

    # 3. 修复效果总结
    print(f"\n{'=' * 80}")
    print("【修复效果总结】")

    if len(batch_files) == len(tick_files):
        print(f"  ✓ 文件数量完全一致: {len(batch_files):,} 个")
    else:
        diff = abs(len(batch_files) - len(tick_files))
        print(f"  ⚠ 文件数量差异: {diff} 个")

    if bar_count_match == len(sample_files):
        print(f"  ✓ 采样文件bar数量 100% 一致")
    else:
        print(f"  ⚠ 采样文件bar数量一致率: {bar_count_match*100/len(sample_files):.1f}%")

    if data_content_match == len(sample_files):
        print(f"  ✓ 采样文件数据内容 100% 一致")
    else:
        print(f"  ⚠ 采样文件数据内容一致率: {data_content_match*100/len(sample_files):.1f}%")

    print(f"\n【结论】")
    if (len(batch_files) == len(tick_files) and
        bar_count_match == len(sample_files) and
        data_content_match == len(sample_files)):
        print("  ✓✓✓ 修复成功！on_batch和on_tick输出完全一致！")
    else:
        print("  ⚠ 仍存在差异，需要进一步分析")

    print("=" * 80)

if __name__ == "__main__":
    main()
