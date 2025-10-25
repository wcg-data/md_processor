#!/usr/bin/env python3
# compare_memory_vs_parquet.py - 对比 memory_processor 和 parquet_processor 的1min bar结果
# 功能：
# 1. 读取 memory_processor 输出的1min bar CSV (实时处理)
# 2. 读取对应的 parquet 1min bar 数据 (离线批量处理)
# 3. 按合约和时间对比关键字段的一致性

import pandas as pd
import numpy as np
from pathlib import Path
import sys

def load_memory_csv(csv_path):
    """加载 memory_processor 输出的CSV文件"""
    # 格式: contract,comd,exchange,date,trade_time,open,high,low,close,pre_settle,volume,turnover,open_interest,vwap,open_interest_diff,bid_price_1,bid_volume_1,ask_price_1,ask_volume_1,mid_price

    columns = [
        'contract', 'comd', 'exchange', 'date', 'trade_time',
        'open', 'high', 'low', 'close', 'pre_settle',
        'volume', 'turnover', 'open_interest', 'vwap', 'open_interest_diff',
        'bid_price_1', 'bid_volume_1', 'ask_price_1', 'ask_volume_1', 'mid_price'
    ]

    df = pd.read_csv(csv_path, names=columns, header=None)

    # 转换数据类型
    df['trade_time'] = pd.to_datetime(df['trade_time'])

    numeric_cols = ['open', 'high', 'low', 'close', 'pre_settle', 'volume',
                    'turnover', 'open_interest', 'vwap', 'open_interest_diff',
                    'bid_price_1', 'bid_volume_1', 'ask_price_1', 'ask_volume_1', 'mid_price']

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    return df

def load_parquet_bars(data_dir, contract, freq='1min'):
    """加载 parquet_processor 输出的1min bar"""
    parquet_path = Path(data_dir) / f"full_bar_{freq}" / f"{contract}_{freq}.parquet"

    if not parquet_path.exists():
        print(f"警告: parquet文件不存在: {parquet_path}")
        return None

    df = pd.read_parquet(parquet_path)
    df['trade_time'] = pd.to_datetime(df['trade_time'])

    return df

def compare_contracts(memory_df, parquet_df, contract):
    """对比单个合约的数据"""
    print(f"\n{'='*80}")
    print(f"对比合约: {contract}")
    print(f"{'='*80}")

    # 筛选合约数据
    mem_data = memory_df[memory_df['contract'] == contract].copy()
    par_data = parquet_df[parquet_df['contract'] == contract].copy()

    print(f"\nmemory_processor 行数: {len(mem_data)}")
    print(f"parquet_processor 行数: {len(par_data)}")

    if len(mem_data) == 0 or len(par_data) == 0:
        print("警告: 数据为空，跳过对比")
        return

    # 按时间合并
    merged = pd.merge(
        mem_data, par_data,
        on='trade_time',
        suffixes=('_mem', '_par'),
        how='outer',
        indicator=True
    )

    both_count = (merged['_merge'] == 'both').sum()
    left_only = (merged['_merge'] == 'left_only').sum()
    right_only = (merged['_merge'] == 'right_only').sum()

    print(f"\n时间匹配情况:")
    print(f"  两者都有: {both_count}")
    print(f"  只在memory中: {left_only}")
    print(f"  只在parquet中: {right_only}")

    if both_count == 0:
        print("警告: 没有匹配的时间点")
        return

    # 只对比共同的时间点
    common = merged[merged['_merge'] == 'both'].copy()

    print(f"\n时间范围:")
    print(f"  起始: {common['trade_time'].min()}")
    print(f"  结束: {common['trade_time'].max()}")

    # 对比关键字段
    compare_fields = ['open', 'high', 'low', 'close', 'volume', 'turnover',
                      'open_interest', 'bid_price_1', 'ask_price_1', 'mid_price']

    print(f"\n{'='*80}")
    print("字段对比:")
    print(f"{'='*80}")

    mismatch_summary = {}

    for field in compare_fields:
        mem_col = f"{field}_mem"
        par_col = f"{field}_par"

        if mem_col not in common.columns or par_col not in common.columns:
            print(f"✗ {field:15s}: 字段缺失")
            continue

        # 计算差异
        diff = common[mem_col] - common[par_col]

        # 处理NaN的情况
        both_nan = common[mem_col].isna() & common[par_col].isna()
        one_nan = common[mem_col].isna() != common[par_col].isna()

        # 非NaN的差异
        valid_diff = diff[~both_nan & ~one_nan]

        exact_match = (valid_diff.abs() < 1e-6).sum()
        total_valid = len(valid_diff)

        nan_diff_count = one_nan.sum()

        if total_valid == exact_match and nan_diff_count == 0:
            print(f"✓ {field:15s}: 完全一致 ({total_valid} 行)")
        else:
            mismatch_count = total_valid - exact_match
            mismatch_summary[field] = mismatch_count + nan_diff_count

            print(f"✗ {field:15s}: {mismatch_count} 行数值不一致, {nan_diff_count} 行NaN差异")

            if mismatch_count > 0:
                max_diff = valid_diff.abs().max()
                mean_diff = valid_diff.abs().mean()
                print(f"                   最大差异: {max_diff:.6f}, 平均差异: {mean_diff:.6f}")

                # 显示前3个不一致的例子
                mismatches = common[valid_diff.abs() >= 1e-6].head(3)
                for idx, row in mismatches.iterrows():
                    print(f"                   {row['trade_time']}: mem={row[mem_col]:.4f}, par={row[par_col]:.4f}, diff={row[mem_col]-row[par_col]:.4f}")

    # 总结
    print(f"\n{'='*80}")
    print("对比总结:")
    print(f"{'='*80}")

    total_fields = len(compare_fields)
    match_fields = total_fields - len(mismatch_summary)

    print(f"完全匹配字段: {match_fields}/{total_fields}")

    if mismatch_summary:
        print(f"\n不匹配字段:")
        for field, count in sorted(mismatch_summary.items(), key=lambda x: x[1], reverse=True):
            print(f"  - {field}: {count} 行")

    return common

def main():
    if len(sys.argv) < 3:
        print("用法: python compare_memory_vs_parquet.py <memory_csv> <contract1> [contract2] ...")
        print("示例: python compare_memory_vs_parquet.py md_snapshot_RTdongzheng.csv.20251020 MA512 zn2511")
        return

    memory_csv = sys.argv[1]
    contracts = sys.argv[2:]

    data_dir = "/data/future_data/dongzheng_data"

    # 加载 memory_processor 数据
    print(f"加载 memory_processor 数据: {memory_csv}")
    memory_df = load_memory_csv(memory_csv)
    print(f"总行数: {len(memory_df)}")
    print(f"合约数: {memory_df['contract'].nunique()}")

    # 对比每个合约
    for contract in contracts:
        # 加载 parquet_processor 数据
        parquet_df = load_parquet_bars(data_dir, contract)

        if parquet_df is None:
            print(f"跳过合约: {contract}")
            continue

        compare_contracts(memory_df, parquet_df, contract)

if __name__ == '__main__':
    main()
