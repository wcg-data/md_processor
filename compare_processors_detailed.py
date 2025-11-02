#!/usr/bin/env python3
"""
compare_processors_detailed.py - on_batch与on_tick处理结果详细对比
功能：
1. 识别55个文件差异（哪些在on_tick有但on_batch没有）
2. 对匹配文件进行数据一致性检查（bar数量、OHLCV值）
3. 生成完整的一致性报告
"""

import polars as pl
from pathlib import Path
import sys

def main():
    batch_dir = Path("/data/future_data/dongzheng_data/full_bar_1min")
    tick_dir = Path("/data/future_data/dongzheng_data/full_bar_1min_on_tick")

    # 获取文件列表
    batch_files = {f.name for f in batch_dir.glob("*.parquet")}
    tick_files = {f.name for f in tick_dir.glob("*.parquet")}

    print("=" * 80)
    print("on_batch vs on_tick 详细对比分析")
    print("=" * 80)
    print(f"\n【1. 文件数量统计】")
    print(f"  on_batch:  {len(batch_files):,} 个文件")
    print(f"  on_tick:   {len(tick_files):,} 个文件")
    print(f"  差异:      {len(tick_files) - len(batch_files):+,} 个文件")

    # 找出差异文件
    only_in_tick = sorted(tick_files - batch_files)
    only_in_batch = sorted(batch_files - tick_files)
    common_files = sorted(batch_files & tick_files)

    print(f"\n【2. 文件差异分析】")
    if only_in_tick:
        print(f"  仅在 on_tick 中存在: {len(only_in_tick)} 个文件")
        print(f"  前10个: {only_in_tick[:10]}")
        if len(only_in_tick) > 10:
            print(f"  后10个: {only_in_tick[-10:]}")

    if only_in_batch:
        print(f"  仅在 on_batch 中存在: {len(only_in_batch)} 个文件")
        print(f"  列表: {only_in_batch}")

    print(f"\n  共同文件数: {len(common_files):,} 个")

    # 数据一致性检查（全量）
    print(f"\n【3. 数据一致性检查】(全量分析 {len(common_files):,} 个文件)")
    print(f"  开始对比所有共同文件...")

    bar_count_diff = []
    data_diff = []
    read_errors = []
    total = len(common_files)

    for i, filename in enumerate(common_files):
        if (i + 1) % 1000 == 0 or i == 0:
            print(f"  进度: {i+1:,}/{total:,} ({100*(i+1)/total:.1f}%)")

        batch_file = batch_dir / filename
        tick_file = tick_dir / filename

        try:
            df_batch = pl.read_parquet(batch_file)
            df_tick = pl.read_parquet(tick_file)

            # 检查bar数量
            if len(df_batch) != len(df_tick):
                bar_count_diff.append({
                    'file': filename,
                    'batch_bars': len(df_batch),
                    'tick_bars': len(df_tick),
                    'diff': len(df_tick) - len(df_batch)
                })

            # 检查数据内容（如果行数相同）
            if len(df_batch) == len(df_tick):
                # 比较关键字段
                has_diff = False
                for col in ['open', 'high', 'low', 'close', 'volume', 'turnover', 'open_interest']:
                    if col in df_batch.columns and col in df_tick.columns:
                        if not df_batch[col].equals(df_tick[col]):
                            max_diff = (df_batch[col] - df_tick[col]).abs().max()
                            data_diff.append({
                                'file': filename,
                                'column': col,
                                'max_diff': max_diff
                            })
                            has_diff = True
                            break

        except Exception as e:
            read_errors.append({'file': filename, 'error': str(e)})
            continue

    print(f"  完成: {total:,}/{total:,} (100.0%)")

    print(f"\n【4. 一致性检查结果】(全量 {len(common_files):,} 个文件)")
    print(f"\n  读取错误: {len(read_errors)} 个文件")
    if read_errors:
        for item in read_errors[:5]:
            print(f"    - {item['file']}: {item['error']}")

    print(f"\n  bar数量不一致: {len(bar_count_diff)} 个文件")
    if bar_count_diff:
        print(f"    显示前20个:")
        for item in bar_count_diff[:20]:
            print(f"      {item['file']}: batch={item['batch_bars']}, tick={item['tick_bars']}, diff={item['diff']:+d}")
        if len(bar_count_diff) > 20:
            print(f"    ... 还有 {len(bar_count_diff)-20} 个文件")

    print(f"\n  数据内容不一致: {len(data_diff)} 个文件")
    if data_diff:
        print(f"    显示前20个:")
        for item in data_diff[:20]:
            print(f"      {item['file']}, 字段={item['column']}, 最大差异={item['max_diff']}")
        if len(data_diff) > 20:
            print(f"    ... 还有 {len(data_diff)-20} 个文件")

    # 总结
    print(f"\n{'=' * 80}")
    print("【总结】")
    if len(only_in_tick) == 55 and len(only_in_batch) == 0:
        print(f"  • on_tick 多处理了 55 个合约（原因：on_batch过滤后产生空dataframe崩溃）")

    perfect_match = total - len(bar_count_diff) - len(data_diff) - len(read_errors)
    print(f"\n  完全一致: {perfect_match:,}/{total:,} ({100*perfect_match/total:.2f}%)")
    print(f"  bar数量差异: {len(bar_count_diff):,} ({100*len(bar_count_diff)/total:.2f}%)")
    print(f"  数据内容差异: {len(data_diff):,} ({100*len(data_diff)/total:.2f}%)")
    print(f"  读取错误: {len(read_errors):,} ({100*len(read_errors)/total:.2f}%)")

    if perfect_match == total:
        print(f"\n  ✓✓✓ 所有共同文件数据完全一致！")
    elif len(bar_count_diff) + len(data_diff) == 0:
        print(f"\n  ✓✓ 除读取错误外，所有文件数据一致")
    else:
        print(f"\n  ⚠ 发现 {len(bar_count_diff) + len(data_diff)} 个文件存在差异")
    print("=" * 80)

if __name__ == "__main__":
    main()
