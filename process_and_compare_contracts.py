#!/usr/bin/env python3
# process_and_compare_contracts.py
# 扫描指定合约，处理并对比on_batch和on_tick结果
#
# 用法:
#   python3 process_and_compare_contracts.py                    # 默认处理2501-2504合约
#   python3 process_and_compare_contracts.py --patterns 2505    # 只处理2505合约
#   python3 process_and_compare_contracts.py --patterns 2501 2502  # 处理2501和2502合约

import subprocess
import sys
from pathlib import Path
from datetime import datetime
import argparse

def find_contracts_in_tick(patterns):
    """从full_tick目录扫描符合条件的合约"""
    tick_dir = Path("/data/future_data/dongzheng_data/full_tick")

    if not tick_dir.exists():
        print(f"❌ tick数据目录不存在: {tick_dir}")
        return []

    # 扫描所有tick文件
    tick_files = list(tick_dir.glob("*.parquet"))

    contracts = []
    for f in tick_files:
        contract = f.stem

        # 检查是否匹配指定模式
        if any(pattern in contract for pattern in patterns):
            contracts.append(contract)

    return sorted(contracts)

def process_contract(contract, mode):
    """
    处理单个合约

    Args:
        contract: 合约代码
        mode: 'batch' 或 'tick'

    Returns:
        True if success, False otherwise
    """
    if mode == 'batch':
        cmd = f"./target/release/parquet_processor_on_batch dongzheng_data 1min {contract}"
    elif mode == 'tick':
        cmd = f"./target/release/parquet_processor_on_tick dongzheng_data 1min {contract}"
    else:
        print(f"❌ 未知模式: {mode}")
        return False

    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=600  # 10分钟超时
        )

        if result.returncode == 0:
            return True
        else:
            print(f"    ❌ 失败: {result.stderr[:200]}")
            return False
    except subprocess.TimeoutExpired:
        print(f"    ❌ 超时（>10分钟）")
        return False
    except Exception as e:
        print(f"    ❌ 异常: {e}")
        return False

def quick_compare_contract(contract):
    """快速对比单个合约的结果"""
    import polars as pl
    import csv

    batch_file = f"/data/future_data/dongzheng_data/full_bar_1min/{contract}_1min.parquet"
    tick_file = f"/data/future_data/dongzheng_data/full_bar_1min_on_tick/{contract}_1min.parquet"

    if not Path(batch_file).exists() or not Path(tick_file).exists():
        return None

    try:
        df_batch = pl.read_parquet(batch_file)
        df_tick = pl.read_parquet(tick_file)
    except:
        return None

    result = {
        'contract': contract,
        'batch_count': len(df_batch),
        'tick_count': len(df_tick),
        'bars_match': len(df_batch) == len(df_tick)
    }

    # 检查第一个收盘时间点
    import re
    comd_match = re.match(r'([A-Za-z]+)\d+', contract)
    if not comd_match:
        return result

    comd = comd_match.group(1)

    # 获取交易时段配置
    sessions = []
    try:
        with open('trade_session.csv', 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 4 and row[0] == comd:
                    sessions.append({
                        'auction': row[1],
                        'start': row[2],
                        'end': row[3]
                    })
    except:
        pass

    # 检查收盘时间点
    closing_match = None
    if sessions:
        end_time = sessions[0]['end']
        end_hour = end_time[:2]
        end_minute = end_time[3:5]

        batch_closing = df_batch.filter(
            pl.col("trade_time").str.contains(f" {end_hour}:{end_minute}:00")
        )
        tick_closing = df_tick.filter(
            pl.col("trade_time").str.contains(f" {end_hour}:{end_minute}:00")
        )

        if len(batch_closing) > 0 and len(tick_closing) > 0:
            batch_vol = batch_closing.tail(1)["volume"][0]
            tick_vol = tick_closing.tail(1)["volume"][0]
            closing_match = (batch_vol == tick_vol)

    result['closing_match'] = closing_match

    # 检查集合竞价
    auction_match = None
    if sessions and sessions[0]['auction']:
        opening_time = sessions[0]['start']
        hour = opening_time[:2]
        minute = opening_time[3:5]

        batch_auction = df_batch.filter(pl.col("trade_time").str.contains(f" {hour}:{minute}:00"))
        tick_auction = df_tick.filter(pl.col("trade_time").str.contains(f" {hour}:{minute}:00"))

        if len(batch_auction) > 0 and len(tick_auction) > 0:
            batch_vol = batch_auction.head(1)["volume"][0]
            tick_vol = tick_auction.head(1)["volume"][0]
            auction_match = (batch_vol == tick_vol)

    result['auction_match'] = auction_match

    return result

def main():
    parser = argparse.ArgumentParser(description='处理并对比on_batch和on_tick结果')
    parser.add_argument('--patterns', nargs='+', default=['2501'],
                        help='合约模式列表（默认: 2501）')
    parser.add_argument('--skip-processing', action='store_true',
                        help='跳过处理，直接对比（假设已处理）')
    args = parser.parse_args()

    print("=" * 120)
    print("处理并对比合约 - on_batch vs on_tick")
    print("=" * 120)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"合约模式: {', '.join(args.patterns)}")

    # 1. 扫描合约
    print("\n" + "=" * 120)
    print("【步骤1】扫描合约")
    print("=" * 120)

    contracts = find_contracts_in_tick(args.patterns)
    print(f"\n找到 {len(contracts)} 个合约")

    if len(contracts) == 0:
        print("❌ 未找到任何合约")
        return

    # 按品种分组显示
    from collections import defaultdict
    import re

    commodity_groups = defaultdict(list)
    for contract in contracts:
        match = re.match(r'([A-Za-z]+)\d+', contract)
        if match:
            comd = match.group(1)
            commodity_groups[comd].append(contract)

    print(f"\n覆盖品种: {len(commodity_groups)} 个")
    for comd in sorted(commodity_groups.keys()):
        contracts_str = ', '.join(sorted(commodity_groups[comd])[:5])
        if len(commodity_groups[comd]) > 5:
            contracts_str += f", ... (+{len(commodity_groups[comd])-5}个)"
        print(f"  {comd}: {len(commodity_groups[comd])} 个合约 ({contracts_str})")

    if not args.skip_processing:
        # 2. 处理合约（on_batch）
        print("\n" + "=" * 120)
        print("【步骤2】处理合约 - on_batch")
        print("=" * 120)

        batch_success = []
        batch_failed = []

        for idx, contract in enumerate(contracts, 1):
            print(f"\n[{idx}/{len(contracts)}] 处理 {contract} (on_batch)...", end=" ")
            if process_contract(contract, 'batch'):
                print("✅")
                batch_success.append(contract)
            else:
                print("❌")
                batch_failed.append(contract)

            # 每10个显示进度
            if idx % 10 == 0:
                print(f"\n进度: {idx}/{len(contracts)} ({idx*100//len(contracts)}%), 成功:{len(batch_success)}, 失败:{len(batch_failed)}")

        print(f"\n\non_batch处理完成: 成功 {len(batch_success)}/{len(contracts)}")
        if batch_failed:
            print(f"失败的合约: {', '.join(batch_failed)}")

        # 3. 处理合约（on_tick）
        print("\n" + "=" * 120)
        print("【步骤3】处理合约 - on_tick")
        print("=" * 120)

        tick_success = []
        tick_failed = []

        # 只处理on_batch成功的合约
        for idx, contract in enumerate(batch_success, 1):
            print(f"\n[{idx}/{len(batch_success)}] 处理 {contract} (on_tick)...", end=" ")
            if process_contract(contract, 'tick'):
                print("✅")
                tick_success.append(contract)
            else:
                print("❌")
                tick_failed.append(contract)

            # 每10个显示进度
            if idx % 10 == 0:
                print(f"\n进度: {idx}/{len(batch_success)} ({idx*100//len(batch_success)}%), 成功:{len(tick_success)}, 失败:{len(tick_failed)}")

        print(f"\n\non_tick处理完成: 成功 {len(tick_success)}/{len(batch_success)}")
        if tick_failed:
            print(f"失败的合约: {', '.join(tick_failed)}")

        # 只对比两者都成功的合约
        contracts_to_compare = tick_success
    else:
        print("\n⚠️ 跳过处理步骤")
        contracts_to_compare = contracts

    # 4. 对比结果
    print("\n" + "=" * 120)
    print("【步骤4】对比结果")
    print("=" * 120)

    print(f"\n对比 {len(contracts_to_compare)} 个合约...")

    results = []
    for idx, contract in enumerate(contracts_to_compare, 1):
        result = quick_compare_contract(contract)
        if result:
            results.append(result)

    # 显示结果表格
    print(f"\n\n{'合约':<15} {'batch_bars':>12} {'tick_bars':>12} {'bars一致':<10} {'收盘一致':<10} {'竞价一致':<10}")
    print("-" * 120)

    for r in results:
        bars_status = "✅" if r['bars_match'] else f"❌"
        closing_status = "✅" if r.get('closing_match') else ("❌" if r.get('closing_match') is False else "N/A")
        auction_status = "✅" if r.get('auction_match') else ("❌" if r.get('auction_match') is False else "N/A")

        print(f"{r['contract']:<15} {r['batch_count']:>12,} {r['tick_count']:>12,} "
              f"{bars_status:<10} {closing_status:<10} {auction_status:<10}")

    # 5. 统计汇总
    print("\n" + "=" * 120)
    print("【汇总】")
    print("=" * 120)

    bars_match_count = sum(1 for r in results if r['bars_match'])
    closing_results = [r for r in results if r.get('closing_match') is not None]
    closing_match_count = sum(1 for r in closing_results if r['closing_match'])
    auction_results = [r for r in results if r.get('auction_match') is not None]
    auction_match_count = sum(1 for r in auction_results if r['auction_match'])

    print(f"\n对比合约数: {len(results)}")
    print(f"\nBars数量一致: {bars_match_count}/{len(results)}", end=" ")
    if bars_match_count == len(results):
        print("✅ 全部一致")
    else:
        print(f"⚠️ {len(results) - bars_match_count}个不一致")
        for r in results:
            if not r['bars_match']:
                print(f"  - {r['contract']}: batch={r['batch_count']:,}, tick={r['tick_count']:,}, 差异={abs(r['batch_count']-r['tick_count']):,}")

    print(f"\n收盘时间点一致: {closing_match_count}/{len(closing_results)}", end=" ")
    if closing_match_count == len(closing_results):
        print("✅ 全部一致")
    else:
        print(f"⚠️ {len(closing_results) - closing_match_count}个不一致")

    print(f"\n集合竞价一致: {auction_match_count}/{len(auction_results)}", end=" ")
    if auction_match_count == len(auction_results):
        print("✅ 全部一致")
    else:
        print(f"⚠️ {len(auction_results) - auction_match_count}个不一致")

    # 整体评估
    print("\n" + "=" * 120)
    print("【整体评估】")
    print("=" * 120)

    if bars_match_count == len(results) and closing_match_count == len(closing_results) and auction_match_count == len(auction_results):
        print("\n✅ 所有合约的bars数量、收盘时间点和集合竞价完全一致")
        print("✅ on_batch和on_tick处理结果符合预期")
    else:
        print("\n⚠️ 部分合约存在差异")
        print("\n建议: 使用 comprehensive_compare_all_contracts.py 进行详细分析")

    print(f"\n结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
