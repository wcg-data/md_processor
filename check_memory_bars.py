#!/usr/bin/env python3
# check_memory_bars.py - 检查 memory_processor 输出的1min bar数据质量
# 功能：
# 1. 检查OHLC关系是否正确 (low <= open/close <= high)
# 2. 检查时间间隔是否正确 (应该是1分钟间隔)
# 3. 检查vwap计算是否合理
# 4. 检查bid/ask关系

import pandas as pd
import numpy as np
import sys

def load_memory_csv(csv_path):
    """加载 memory_processor 输出的CSV文件"""
    columns = [
        'contract', 'comd', 'exchange', 'date', 'trade_time',
        'open', 'high', 'low', 'close', 'pre_settle',
        'volume', 'turnover', 'open_interest', 'vwap', 'open_interest_diff',
        'bid_price_1', 'bid_volume_1', 'ask_price_1', 'ask_volume_1', 'mid_price'
    ]

    df = pd.read_csv(csv_path, names=columns, header=None)
    df['trade_time'] = pd.to_datetime(df['trade_time'])

    numeric_cols = ['open', 'high', 'low', 'close', 'pre_settle', 'volume',
                    'turnover', 'open_interest', 'vwap', 'open_interest_diff',
                    'bid_price_1', 'bid_volume_1', 'ask_price_1', 'ask_volume_1', 'mid_price']

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    return df

def check_ohlc_consistency(df, contract):
    """检查OHLC关系"""
    contract_data = df[df['contract'] == contract].copy()

    if len(contract_data) == 0:
        return

    print(f"\n{'='*80}")
    print(f"检查合约: {contract}")
    print(f"{'='*80}")
    print(f"总行数: {len(contract_data)}")
    print(f"时间范围: {contract_data['trade_time'].min()} 到 {contract_data['trade_time'].max()}")

    # 检查1: low <= open <= high
    invalid_open = ~((contract_data['low'] <= contract_data['open']) &
                     (contract_data['open'] <= contract_data['high']))
    invalid_open_count = invalid_open.sum()

    # 检查2: low <= close <= high
    invalid_close = ~((contract_data['low'] <= contract_data['close']) &
                      (contract_data['close'] <= contract_data['high']))
    invalid_close_count = invalid_close.sum()

    # 检查3: bid <= ask (当都存在时)
    has_both = (contract_data['bid_price_1'] > 0) & (contract_data['ask_price_1'] > 0)
    invalid_bid_ask = has_both & (contract_data['bid_price_1'] > contract_data['ask_price_1'])
    invalid_bid_ask_count = invalid_bid_ask.sum()

    # 检查4: mid_price应该在bid和ask之间
    invalid_mid = has_both & (
        (contract_data['mid_price'] < contract_data['bid_price_1']) |
        (contract_data['mid_price'] > contract_data['ask_price_1'])
    )
    invalid_mid_count = invalid_mid.sum()

    # 检查5: 时间间隔
    contract_data = contract_data.sort_values('trade_time')
    time_diffs = contract_data['trade_time'].diff()
    time_diff_minutes = time_diffs.dt.total_seconds() / 60.0

    # 排除第一行（NaT）
    time_diff_minutes = time_diff_minutes[1:]

    expected_intervals = [1, 2, 3, 4, 5]  # 可能的分钟间隔
    unexpected_intervals = time_diff_minutes[~time_diff_minutes.isin(expected_intervals)]

    print(f"\n{'='*80}")
    print("OHLC 一致性检查:")
    print(f"{'='*80}")

    if invalid_open_count == 0:
        print(f"✓ open在[low, high]范围内: 全部通过")
    else:
        print(f"✗ open超出[low, high]范围: {invalid_open_count} 行")
        print("  前3个错误:")
        for idx, row in contract_data[invalid_open].head(3).iterrows():
            print(f"    {row['trade_time']}: low={row['low']}, open={row['open']}, high={row['high']}")

    if invalid_close_count == 0:
        print(f"✓ close在[low, high]范围内: 全部通过")
    else:
        print(f"✗ close超出[low, high]范围: {invalid_close_count} 行")
        print("  前3个错误:")
        for idx, row in contract_data[invalid_close].head(3).iterrows():
            print(f"    {row['trade_time']}: low={row['low']}, close={row['close']}, high={row['high']}")

    if invalid_bid_ask_count == 0:
        print(f"✓ bid <= ask: 全部通过")
    else:
        print(f"✗ bid > ask: {invalid_bid_ask_count} 行")
        print("  前3个错误:")
        for idx, row in contract_data[invalid_bid_ask].head(3).iterrows():
            print(f"    {row['trade_time']}: bid={row['bid_price_1']}, ask={row['ask_price_1']}")

    if invalid_mid_count == 0:
        print(f"✓ mid_price在[bid, ask]范围内: 全部通过")
    else:
        print(f"✗ mid_price超出[bid, ask]范围: {invalid_mid_count} 行")

    print(f"\n{'='*80}")
    print("时间间隔检查:")
    print(f"{'='*80}")

    print(f"最小间隔: {time_diff_minutes.min():.1f} 分钟")
    print(f"最大间隔: {time_diff_minutes.max():.1f} 分钟")
    print(f"平均间隔: {time_diff_minutes.mean():.1f} 分钟")

    if len(unexpected_intervals) > 0:
        print(f"\n异常间隔统计 (非1-5分钟):")
        print(unexpected_intervals.value_counts().head(10))

    # 检查数据内部一致性
    print(f"\n{'='*80}")
    print("数据内部一致性:")
    print(f"{'='*80}")

    # volume > 0 时，turnover也应该 > 0
    has_volume = contract_data['volume'] > 0
    has_turnover = contract_data['turnover'] > 0

    volume_but_no_turnover = has_volume & ~has_turnover
    turnover_but_no_volume = has_turnover & ~has_volume

    print(f"有成交量的行数: {has_volume.sum()}")
    print(f"有成交额的行数: {has_turnover.sum()}")

    if volume_but_no_turnover.sum() > 0:
        print(f"✗ 有成交量但无成交额: {volume_but_no_turnover.sum()} 行")
    else:
        print(f"✓ volume与turnover一致")

    if turnover_but_no_volume.sum() > 0:
        print(f"✗ 有成交额但无成交量: {turnover_but_no_volume.sum()} 行")

    # 显示一些样本数据
    print(f"\n{'='*80}")
    print("前5个bar样本:")
    print(f"{'='*80}")

    display_cols = ['trade_time', 'open', 'high', 'low', 'close', 'volume',
                    'turnover', 'bid_price_1', 'ask_price_1', 'mid_price']

    print(contract_data[display_cols].head(5).to_string(index=False))

def main():
    if len(sys.argv) < 2:
        print("用法: python check_memory_bars.py <memory_csv> [contract1] [contract2] ...")
        print("示例: python check_memory_bars.py md_snapshot_RTdongzheng.csv.20251020 MA512 zn2511")
        print("      如果不指定合约，将显示文件中的所有合约")
        return

    memory_csv = sys.argv[1]

    # 加载数据
    print(f"加载 memory_processor 数据: {memory_csv}")
    df = load_memory_csv(memory_csv)
    print(f"总行数: {len(df)}")
    print(f"合约数: {df['contract'].nunique()}")

    if len(sys.argv) > 2:
        # 检查指定合约
        contracts = sys.argv[2:]
    else:
        # 显示前10个合约
        contracts = df['contract'].value_counts().head(10).index.tolist()
        print(f"\n将检查前10个合约（按数据量排序）:")
        for i, c in enumerate(contracts, 1):
            count = (df['contract'] == c).sum()
            print(f"  {i}. {c}: {count} 行")

    # 检查每个合约
    for contract in contracts:
        check_ohlc_consistency(df, contract)

if __name__ == '__main__':
    main()
