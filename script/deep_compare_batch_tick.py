#!/usr/bin/env python3
# deep_compare_batch_tick.py - 深度对比 on_batch 和 on_tick 结果
# 功能：逐行、逐字段精确比较，包括NaN位置、数值差异等
# 使用方法：
#   python3 script/deep_compare_batch_tick.py                     # 比较所有合约
#   python3 script/deep_compare_batch_tick.py --contracts au2502  # 比较指定合约
#   python3 script/deep_compare_batch_tick.py --sample 100        # 随机抽样100个合约

import polars as pl
import numpy as np
from pathlib import Path
from datetime import datetime
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict
import random

DATA_ROOT = Path("/data/future_data/dongzheng_data")
BATCH_DIR = DATA_ROOT / "full_bar_1min"
TICK_DIR = DATA_ROOT / "full_bar_1min_on_tick"

# 需要比较的数值字段
NUMERIC_FIELDS = [
    'open', 'high', 'low', 'close', 'prev_close', 'pre_settle',
    'volume', 'turnover', 'open_interest', 'open_interest_diff',
    'bid_price_1', 'bid_volume_1', 'ask_price_1', 'ask_volume_1', 'mid_price',
    'vwap', 'log_return', 'maturity_month', 'maturity_day'
]


def deep_compare_contract(contract):
    """
    深度比较单个合约的 on_batch 和 on_tick 结果

    Returns:
        dict: 详细比较结果
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

        result = {
            'contract': contract,
            'status': 'ok',
            'row_count_batch': len(df_batch),
            'row_count_tick': len(df_tick),
            'row_count_match': len(df_batch) == len(df_tick),
            'field_diffs': {}
        }

        # 如果行数不匹配，记录差异
        if not result['row_count_match']:
            result['status'] = 'row_count_mismatch'
            return result

        # 排序确保顺序一致
        df_batch = df_batch.sort('trade_time')
        df_tick = df_tick.sort('trade_time')

        # 比较 trade_time
        times_match = (df_batch['trade_time'] == df_tick['trade_time']).all()
        result['times_match'] = times_match

        if not times_match:
            result['status'] = 'time_mismatch'
            # 找出不匹配的时间点
            time_diffs = []
            for i in range(min(10, len(df_batch))):  # 只显示前10个
                if df_batch['trade_time'][i] != df_tick['trade_time'][i]:
                    time_diffs.append({
                        'index': i,
                        'batch': df_batch['trade_time'][i],
                        'tick': df_tick['trade_time'][i]
                    })
            result['time_diffs'] = time_diffs
            return result

        # 逐字段详细比较
        total_rows = len(df_batch)
        all_match = True

        for field in NUMERIC_FIELDS:
            if field not in df_batch.columns or field not in df_tick.columns:
                continue

            batch_col = df_batch[field]
            tick_col = df_tick[field]

            field_result = {
                'null_count_batch': batch_col.null_count(),
                'null_count_tick': tick_col.null_count(),
                'null_match': batch_col.null_count() == tick_col.null_count(),
            }

            # 检查 null 位置是否一致
            if field_result['null_count_batch'] > 0 or field_result['null_count_tick'] > 0:
                batch_nulls = batch_col.is_null()
                tick_nulls = tick_col.is_null()
                null_positions_match = (batch_nulls == tick_nulls).all()
                field_result['null_positions_match'] = null_positions_match

                if not null_positions_match:
                    all_match = False
                    field_result['status'] = 'null_position_mismatch'
                    # 找出不匹配的位置（最多10个）
                    mismatch_indices = []
                    for i in range(min(len(batch_nulls), 10)):
                        if batch_nulls[i] != tick_nulls[i]:
                            mismatch_indices.append({
                                'index': i,
                                'time': df_batch['trade_time'][i],
                                'batch_null': batch_nulls[i],
                                'tick_null': tick_nulls[i],
                                'batch_val': batch_col[i],
                                'tick_val': tick_col[i]
                            })
                    field_result['null_mismatches'] = mismatch_indices

            # 对于非null的值，检查数值差异
            # 使用 polars 的方式计算差异
            batch_vals = batch_col.fill_null(0.0).to_numpy()
            tick_vals = tick_col.fill_null(0.0).to_numpy()

            # 只在非null位置比较
            non_null_mask = (~batch_col.is_null()).to_numpy() & (~tick_col.is_null()).to_numpy()

            if non_null_mask.sum() > 0:
                diff = np.abs(batch_vals[non_null_mask] - tick_vals[non_null_mask])

                field_result['max_diff'] = float(diff.max())
                field_result['mean_diff'] = float(diff.mean())
                field_result['nonzero_diff_count'] = int((diff > 1e-10).sum())
                field_result['nonzero_diff_ratio'] = float(field_result['nonzero_diff_count'] / non_null_mask.sum())

                # 判断是否一致（考虑浮点精度）
                if field in ['open', 'high', 'low', 'close', 'prev_close', 'pre_settle',
                             'turnover', 'bid_price_1', 'ask_price_1', 'mid_price', 'vwap']:
                    # 浮点字段，使用相对容差
                    tolerance = 1e-6
                    values_match = field_result['max_diff'] < tolerance
                else:
                    # 整数字段，精确比较
                    values_match = field_result['max_diff'] < 1e-10

                field_result['values_match'] = values_match

                if not values_match:
                    all_match = False
                    field_result['status'] = 'value_mismatch'

                    # 找出差异最大的位置（最多10个）
                    diff_full = np.abs(batch_vals - tick_vals)
                    top_diff_indices = np.argsort(diff_full)[-10:][::-1]

                    top_diffs = []
                    for idx in top_diff_indices:
                        if diff_full[idx] > 1e-10:
                            top_diffs.append({
                                'index': int(idx),
                                'time': df_batch['trade_time'][idx],
                                'batch_val': float(batch_vals[idx]) if not batch_col.is_null()[idx] else None,
                                'tick_val': float(tick_vals[idx]) if not tick_col.is_null()[idx] else None,
                                'diff': float(diff_full[idx])
                            })
                    field_result['top_diffs'] = top_diffs
            else:
                field_result['values_match'] = True

            # 只保存有差异的字段结果
            if not field_result.get('null_positions_match', True) or not field_result.get('values_match', True):
                result['field_diffs'][field] = field_result

        # 设置最终状态
        if all_match:
            result['status'] = 'perfect_match'
        else:
            result['status'] = 'field_mismatch'

        return result

    except Exception as e:
        return {
            'contract': contract,
            'status': 'error',
            'error_msg': str(e)[:500]
        }


def print_contract_report(result):
    """打印单个合约的详细报告"""
    print("\n" + "=" * 100)
    print(f"合约: {result['contract']}")
    print("=" * 100)
    print(f"状态: {result['status']}")
    print(f"行数: batch={result.get('row_count_batch', '?')}, tick={result.get('row_count_tick', '?')}")

    if result['status'] == 'time_mismatch':
        print(f"\n❌ 时间不匹配！")
        if 'time_diffs' in result:
            print("前10个不匹配的时间点:")
            for td in result['time_diffs']:
                print(f"  第{td['index']}行: batch={td['batch']}, tick={td['tick']}")

    if result['status'] == 'field_mismatch':
        print(f"\n⚠️ 字段值有差异:")
        for field, field_result in result['field_diffs'].items():
            print(f"\n  【{field}】")
            print(f"    null数量: batch={field_result['null_count_batch']}, tick={field_result['null_count_tick']}")

            if not field_result.get('null_positions_match', True):
                print(f"    ❌ null位置不匹配！")
                if 'null_mismatches' in field_result:
                    print(f"    前{len(field_result['null_mismatches'])}个不匹配位置:")
                    for nm in field_result['null_mismatches']:
                        print(f"      第{nm['index']}行 ({nm['time']}): batch={nm['batch_val']}, tick={nm['tick_val']}")

            if not field_result.get('values_match', True):
                print(f"    ❌ 数值不匹配！")
                print(f"    最大差异: {field_result.get('max_diff', 0):.2e}")
                print(f"    平均差异: {field_result.get('mean_diff', 0):.2e}")
                print(f"    差异数量: {field_result.get('nonzero_diff_count', 0)} ({field_result.get('nonzero_diff_ratio', 0)*100:.2f}%)")

                if 'top_diffs' in field_result and len(field_result['top_diffs']) > 0:
                    print(f"    差异最大的位置:")
                    for td in field_result['top_diffs'][:5]:
                        print(f"      第{td['index']}行 ({td['time']}): batch={td['batch_val']}, tick={td['tick_val']}, diff={td['diff']:.2e}")

    if result['status'] == 'perfect_match':
        print("\n✅ 完美一致！所有字段、所有行的值和null位置都完全相同。")


def main():
    parser = argparse.ArgumentParser(description='深度对比 on_batch 和 on_tick 结果')
    parser.add_argument('--threads', type=int, default=64, help='并行线程数')
    parser.add_argument('--contracts', nargs='+', help='指定要比较的合约')
    parser.add_argument('--sample', type=int, help='随机抽样N个合约')
    parser.add_argument('--show-details', action='store_true', help='显示每个合约的详细差异')
    args = parser.parse_args()

    print("=" * 100)
    print("深度对比 on_batch vs on_tick")
    print("=" * 100)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 扫描合约
    if args.contracts:
        contracts = args.contracts
    else:
        batch_files = list(BATCH_DIR.glob("*_1min.parquet"))
        contracts = [f.stem.replace('_1min', '') for f in batch_files]
        contracts = sorted(contracts)

        if args.sample and args.sample < len(contracts):
            contracts = random.sample(contracts, args.sample)
            print(f"随机抽样: {args.sample} 个合约")

    print(f"待比较合约数: {len(contracts)}")

    # 并行比较
    print(f"\n开始深度比较 ({args.threads} 线程)...")
    results = []
    completed = 0

    with ProcessPoolExecutor(max_workers=args.threads) as executor:
        futures = {executor.submit(deep_compare_contract, contract): contract for contract in contracts}

        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

            completed += 1
            if completed % 100 == 0:
                print(f"进度: {completed}/{len(contracts)} ({completed*100//len(contracts)}%)")

    print(f"完成: {len(results)}/{len(contracts)} 个合约")

    # 统计结果
    print("\n" + "=" * 100)
    print("汇总统计")
    print("=" * 100)

    status_counts = defaultdict(int)
    for r in results:
        status_counts[r['status']] += 1

    print(f"\n状态分布:")
    print(f"  完美一致: {status_counts['perfect_match']} ({status_counts['perfect_match']*100//len(results)}%)")
    print(f"  字段差异: {status_counts['field_mismatch']}")
    print(f"  行数不匹配: {status_counts['row_count_mismatch']}")
    print(f"  时间不匹配: {status_counts['time_mismatch']}")
    print(f"  批处理缺失: {status_counts['batch_missing']}")
    print(f"  tick处理缺失: {status_counts['tick_missing']}")
    print(f"  比较错误: {status_counts['error']}")

    # 字段差异统计
    field_mismatch_results = [r for r in results if r['status'] == 'field_mismatch']
    if field_mismatch_results:
        print(f"\n字段差异详情:")
        field_diff_counts = defaultdict(int)

        for r in field_mismatch_results:
            for field in r['field_diffs'].keys():
                field_diff_counts[field] += 1

        for field, count in sorted(field_diff_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {field}: {count} 个合约有差异")

    # 显示详细差异
    if args.show_details:
        print("\n" + "=" * 100)
        print("详细差异报告")
        print("=" * 100)

        for r in results:
            if r['status'] in ['field_mismatch', 'time_mismatch', 'row_count_mismatch']:
                print_contract_report(r)

    # 显示有差异的合约列表
    if field_mismatch_results:
        print(f"\n有差异的合约列表:")
        for r in field_mismatch_results[:20]:
            print(f"  {r['contract']}: {len(r['field_diffs'])} 个字段有差异")
        if len(field_mismatch_results) > 20:
            print(f"  ... 还有 {len(field_mismatch_results) - 20} 个")

    # 整体结论
    print("\n" + "=" * 100)
    print("整体结论")
    print("=" * 100)

    if status_counts['perfect_match'] == len(results):
        print("\n✅ 所有合约完美一致！")
        print("   - 所有行数一致")
        print("   - 所有时间点一致")
        print("   - 所有字段值一致")
        print("   - 所有null位置一致")
    else:
        print(f"\n⚠️ 发现 {len(results) - status_counts['perfect_match']} 个合约有差异")
        if field_mismatch_results:
            print(f"   - 字段差异: {len(field_mismatch_results)} 个合约")
        if status_counts['row_count_mismatch'] > 0:
            print(f"   - 行数不匹配: {status_counts['row_count_mismatch']} 个合约")
        if status_counts['time_mismatch'] > 0:
            print(f"   - 时间不匹配: {status_counts['time_mismatch']} 个合约")

    print(f"\n结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
