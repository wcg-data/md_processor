#!/usr/bin/env python3
# compare_batch_vs_tick.py - 并行比较 on_batch 和 on_tick 结果
# 功能：快速对比所有合约的 on_batch 和 on_tick 结果
# 使用方法：
#   python3 script/compare_batch_vs_tick.py                    # 比较所有合约
#   python3 script/compare_batch_vs_tick.py --threads 64       # 自定义线程数
#   python3 script/compare_batch_vs_tick.py --save-details     # 保存详细差异到文件

import polars as pl
from pathlib import Path
from datetime import datetime
import argparse
import csv
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict
import sys

DATA_ROOT = Path("/data/future_data/dongzheng_data")
BATCH_DIR = DATA_ROOT / "full_bar_1min"
TICK_DIR = DATA_ROOT / "full_bar_1min_on_tick"
TRADE_SESSION_CSV = Path("/root/project/md_processor/trade_session.csv")


def load_trade_sessions():
    """加载交易时段配置"""
    sessions = defaultdict(list)
    try:
        with open(TRADE_SESSION_CSV, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 4:
                    comd = row[0]
                    sessions[comd].append({
                        'auction': row[1],
                        'start': row[2],
                        'end': row[3]
                    })
    except Exception as e:
        print(f"警告: 无法加载交易时段配置: {e}")
    return sessions


def compare_single_contract(contract, save_details=False):
    """
    比较单个合约的 on_batch 和 on_tick 结果

    Returns:
        dict: 比较结果，包含 contract, batch_count, tick_count, bars_match 等字段
              如果文件不存在或比较失败，返回 None
    """
    batch_file = BATCH_DIR / f"{contract}_1min.parquet"
    tick_file = TICK_DIR / f"{contract}_1min.parquet"

    # 检查文件是否存在
    if not batch_file.exists():
        return {'contract': contract, 'status': 'batch_missing'}
    if not tick_file.exists():
        return {'contract': contract, 'status': 'tick_missing'}

    try:
        # 读取数据
        df_batch = pl.read_parquet(batch_file)
        df_tick = pl.read_parquet(tick_file)

        # 基础比较
        result = {
            'contract': contract,
            'status': 'ok',
            'batch_count': len(df_batch),
            'tick_count': len(df_tick),
            'bars_match': len(df_batch) == len(df_tick),
            'bar_diff': abs(len(df_batch) - len(df_tick)),
        }

        # 如果数量不匹配，记录详细差异
        if not result['bars_match'] and save_details:
            # 找出差异的时间点
            batch_times = set(df_batch['trade_time'].to_list())
            tick_times = set(df_tick['trade_time'].to_list())

            only_in_batch = batch_times - tick_times
            only_in_tick = tick_times - batch_times

            result['only_in_batch'] = sorted(list(only_in_batch))[:10]  # 只保存前10个
            result['only_in_tick'] = sorted(list(only_in_tick))[:10]

        # 提取品种代码
        comd_match = re.match(r'([A-Za-z]+)\d+', contract)
        if not comd_match:
            return result

        comd = comd_match.group(1)
        result['comd'] = comd

        # 检查数据值差异（如果数量一致）
        if result['bars_match']:
            # 排序后比较
            df_batch_sorted = df_batch.sort('trade_time')
            df_tick_sorted = df_tick.sort('trade_time')

            # 比较 trade_time
            times_match = (df_batch_sorted['trade_time'] == df_tick_sorted['trade_time']).all()
            result['times_match'] = times_match

            if times_match:
                # 比较关键字段
                for col in ['open', 'high', 'low', 'close', 'volume', 'turnover', 'open_interest']:
                    if col in df_batch_sorted.columns and col in df_tick_sorted.columns:
                        # 对于浮点数，使用相对容差比较
                        if col in ['open', 'high', 'low', 'close', 'turnover']:
                            diff = (df_batch_sorted[col] - df_tick_sorted[col]).abs()
                            max_diff = diff.max()
                            result[f'{col}_max_diff'] = float(max_diff) if max_diff is not None else 0.0
                            result[f'{col}_match'] = (max_diff < 1e-6)
                        else:
                            # 整数字段精确比较
                            match = (df_batch_sorted[col] == df_tick_sorted[col]).all()
                            result[f'{col}_match'] = match

        return result

    except Exception as e:
        return {
            'contract': contract,
            'status': 'error',
            'error_msg': str(e)[:200]
        }


def main():
    parser = argparse.ArgumentParser(description='并行比较 on_batch 和 on_tick 结果')
    parser.add_argument('--threads', type=int, default=64,
                        help='并行线程数（默认: 64）')
    parser.add_argument('--save-details', action='store_true',
                        help='保存详细差异到文件')
    parser.add_argument('--patterns', nargs='+',
                        help='只比较匹配指定模式的合约（可选）')
    args = parser.parse_args()

    print("=" * 120)
    print("并行比较 on_batch vs on_tick")
    print("=" * 120)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"并行线程: {args.threads}")

    # 扫描合约
    print("\n[步骤1] 扫描合约...")

    if not BATCH_DIR.exists():
        print(f"❌ on_batch 目录不存在: {BATCH_DIR}")
        return

    if not TICK_DIR.exists():
        print(f"❌ on_tick 目录不存在: {TICK_DIR}")
        return

    # 获取 batch 目录下的所有合约
    batch_files = list(BATCH_DIR.glob("*_1min.parquet"))
    contracts = [f.stem.replace('_1min', '') for f in batch_files]

    # 过滤合约
    if args.patterns:
        contracts = [c for c in contracts if any(p in c for p in args.patterns)]

    contracts = sorted(contracts)
    print(f"找到 {len(contracts)} 个合约")

    if len(contracts) == 0:
        print("❌ 未找到任何合约")
        return

    # 按品种分组显示
    commodity_groups = defaultdict(list)
    for contract in contracts:
        match = re.match(r'([A-Za-z]+)\d+', contract)
        if match:
            comd = match.group(1)
            commodity_groups[comd].append(contract)

    print(f"\n覆盖品种: {len(commodity_groups)} 个")
    for comd in sorted(commodity_groups.keys())[:20]:
        count = len(commodity_groups[comd])
        sample = ', '.join(sorted(commodity_groups[comd])[:3])
        if count > 3:
            sample += f", ... (+{count-3}个)"
        print(f"  {comd}: {count} 个合约 ({sample})")
    if len(commodity_groups) > 20:
        print(f"  ... 还有 {len(commodity_groups)-20} 个品种")

    # 并行比较
    print(f"\n[步骤2] 并行比较 ({args.threads} 线程)...")
    results = []
    completed = 0

    with ProcessPoolExecutor(max_workers=args.threads) as executor:
        futures = {
            executor.submit(compare_single_contract, contract, args.save_details): contract
            for contract in contracts
        }

        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

            completed += 1
            if completed % 100 == 0:
                print(f"进度: {completed}/{len(contracts)} ({completed*100//len(contracts)}%)")

    print(f"完成: {len(results)}/{len(contracts)} 个合约")

    # 分类统计
    print("\n" + "=" * 120)
    print("[步骤3] 分类统计")
    print("=" * 120)

    status_counts = defaultdict(int)
    for r in results:
        status_counts[r.get('status', 'unknown')] += 1

    print(f"\n文件状态:")
    print(f"  正常对比: {status_counts['ok']}")
    if status_counts['batch_missing'] > 0:
        print(f"  on_batch缺失: {status_counts['batch_missing']}")
    if status_counts['tick_missing'] > 0:
        print(f"  on_tick缺失: {status_counts['tick_missing']}")
    if status_counts['error'] > 0:
        print(f"  比较错误: {status_counts['error']}")

    # 只统计正常对比的结果
    ok_results = [r for r in results if r.get('status') == 'ok']

    if len(ok_results) == 0:
        print("\n❌ 没有成功对比的合约")
        return

    # Bar数量统计
    bars_match_count = sum(1 for r in ok_results if r.get('bars_match'))
    bars_mismatch = [r for r in ok_results if not r.get('bars_match')]

    print(f"\nBar数量对比:")
    print(f"  完全一致: {bars_match_count}/{len(ok_results)} ({bars_match_count*100//len(ok_results)}%)")

    if bars_mismatch:
        print(f"  数量不一致: {len(bars_mismatch)} 个合约")

        # 统计差异分布
        diff_distribution = defaultdict(int)
        for r in bars_mismatch:
            diff = r.get('bar_diff', 0)
            if diff == 0:
                continue
            elif diff <= 5:
                diff_distribution['1-5'] += 1
            elif diff <= 10:
                diff_distribution['6-10'] += 1
            elif diff <= 50:
                diff_distribution['11-50'] += 1
            elif diff <= 100:
                diff_distribution['51-100'] += 1
            else:
                diff_distribution['>100'] += 1

        print(f"\n  差异分布:")
        for range_name in ['1-5', '6-10', '11-50', '51-100', '>100']:
            if diff_distribution[range_name] > 0:
                print(f"    差异 {range_name} 个bar: {diff_distribution[range_name]} 个合约")

        # 显示差异最大的10个合约
        top_diff = sorted(bars_mismatch, key=lambda x: x.get('bar_diff', 0), reverse=True)[:10]
        print(f"\n  差异最大的10个合约:")
        print(f"  {'合约':<15} {'on_batch':>12} {'on_tick':>12} {'差异':>10}")
        print("  " + "-" * 55)
        for r in top_diff:
            print(f"  {r['contract']:<15} {r['batch_count']:>12,} {r['tick_count']:>12,} {r['bar_diff']:>10,}")

    # 数据值对比（只针对数量一致的合约）
    value_match_results = [r for r in ok_results if r.get('bars_match') and r.get('times_match')]

    if value_match_results:
        print(f"\n数据值对比（{len(value_match_results)} 个数量一致的合约）:")

        # 统计各字段的匹配情况
        for field in ['volume', 'turnover', 'open_interest', 'open', 'high', 'low', 'close']:
            match_key = f'{field}_match'
            match_count = sum(1 for r in value_match_results if r.get(match_key))
            if match_count < len(value_match_results):
                mismatch_count = len(value_match_results) - match_count
                print(f"  {field}: {match_count}/{len(value_match_results)} 一致, {mismatch_count} 个有差异")

                # 显示最大差异
                if field in ['open', 'high', 'low', 'close', 'turnover']:
                    max_diff_contracts = [
                        (r['contract'], r.get(f'{field}_max_diff', 0))
                        for r in value_match_results
                        if not r.get(match_key)
                    ]
                    if max_diff_contracts:
                        top_5 = sorted(max_diff_contracts, key=lambda x: x[1], reverse=True)[:5]
                        print(f"    最大差异的5个合约:")
                        for contract, diff in top_5:
                            print(f"      {contract}: {diff:.2e}")

    # 整体评估
    print("\n" + "=" * 120)
    print("[整体评估]")
    print("=" * 120)

    if bars_match_count == len(ok_results):
        print("\n✅ 所有合约的bar数量完全一致")
        if value_match_results:
            all_values_match = all(
                r.get(f'{field}_match', False)
                for r in value_match_results
                for field in ['volume', 'turnover', 'open_interest', 'open', 'high', 'low', 'close']
            )
            if all_values_match:
                print("✅ 所有数据值完全一致")
                print("✅ on_batch 和 on_tick 处理结果完美一致！")
            else:
                print("⚠️ 部分合约的数据值有差异")
    else:
        print(f"\n⚠️ {len(bars_mismatch)} 个合约的bar数量不一致")
        print(f"⚠️ 一致率: {bars_match_count*100//len(ok_results)}%")

    # 保存详细结果
    if args.save_details:
        output_file = Path("comparison_report.csv")
        print(f"\n保存详细结果到: {output_file}")

        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'contract', 'comd', 'status', 'batch_count', 'tick_count',
                'bars_match', 'bar_diff', 'times_match',
                'volume_match', 'turnover_match', 'open_interest_match'
            ])

            for r in results:
                writer.writerow([
                    r.get('contract', ''),
                    r.get('comd', ''),
                    r.get('status', ''),
                    r.get('batch_count', ''),
                    r.get('tick_count', ''),
                    r.get('bars_match', ''),
                    r.get('bar_diff', ''),
                    r.get('times_match', ''),
                    r.get('volume_match', ''),
                    r.get('turnover_match', ''),
                    r.get('open_interest_match', '')
                ])

    print(f"\n结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
