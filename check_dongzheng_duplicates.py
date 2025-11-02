#!/usr/bin/env python3
"""
check_dongzheng_duplicates.py - 检查dongzheng/full_tick数据中各交易所的重复记录情况

功能：
1. 读取所有tick数据文件
2. 按交易所统计重复记录
3. 生成重复率对比报告
"""

import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path
from collections import defaultdict
import re

def get_product_from_contract(contract):
    """从合约代码提取品种代码"""
    match = re.match(r'([A-Za-z]+)', contract)
    return match.group(1).upper() if match else ""

def load_product_exchange_map():
    """加载品种到交易所的映射"""
    # dongzheng数据中每个文件都有exchange列，无需单独加载映射
    # 直接从数据文件中读取exchange信息
    return {}

def check_duplicates_by_exchange():
    """检查各交易所的重复数据"""
    tick_dir = Path('/data/future_data/dongzheng_data/full_tick')

    if not tick_dir.exists():
        print(f"❌ 目录不存在: {tick_dir}")
        return

    # 加载品种-交易所映射
    product_exchange = load_product_exchange_map()

    # 统计结果
    stats = defaultdict(lambda: {'total': 0, 'duplicates': 0, 'contracts': []})

    # 获取所有文件
    files = sorted(tick_dir.glob('*.parquet'))
    total_files = len(files)

    print(f"📊 开始检查 {total_files} 个合约的重复数据...")
    print("=" * 80)

    processed = 0
    for file in files:
        contract = file.stem
        product = get_product_from_contract(contract)

        # 从 parquet 文件中读取 exchange
        try:
            table = pq.read_table(file)
            if len(table) == 0:
                continue

            df = table.to_pandas()

            # 从数据中获取交易所
            if 'exchange' in df.columns and len(df) > 0:
                exchange = df['exchange'].iloc[0]
            else:
                # 如果没有exchange列，从映射表获取
                exchange = product_exchange.get(product, 'UNKNOWN')

            # 统计总记录数
            total_records = len(df)

            # 检查重复记录（所有列完全相同的记录）
            duplicates = df.duplicated(keep=False)
            duplicate_count = duplicates.sum()

            # 更新统计
            stats[exchange]['total'] += total_records
            stats[exchange]['duplicates'] += duplicate_count

            if duplicate_count > 0:
                stats[exchange]['contracts'].append({
                    'contract': contract,
                    'total': total_records,
                    'duplicates': duplicate_count,
                    'rate': duplicate_count / total_records * 100
                })

            processed += 1
            if processed % 500 == 0:
                print(f"进度: {processed}/{total_files} ({processed/total_files*100:.1f}%)")

        except Exception as e:
            print(f"⚠️ 处理 {contract} 时出错: {e}")
            continue

    return stats

def print_report(stats):
    """打印重复数据报告"""
    print("\n" + "=" * 80)
    print("📊 各交易所重复数据统计报告")
    print("=" * 80)

    # 计算每个交易所的重复率并排序
    exchange_summary = []
    for exchange, data in stats.items():
        if data['total'] > 0:
            rate = data['duplicates'] / data['total'] * 100
            exchange_summary.append({
                'exchange': exchange,
                'total': data['total'],
                'duplicates': data['duplicates'],
                'rate': rate,
                'affected_contracts': len(data['contracts'])
            })

    # 按重复率降序排序
    exchange_summary.sort(key=lambda x: x['rate'], reverse=True)

    # 打印总表
    print(f"\n{'排名':<6} {'交易所':<8} {'总记录数':<15} {'重复记录':<15} {'重复率':<10} {'特征':<10}")
    print("-" * 80)

    for i, summary in enumerate(exchange_summary, 1):
        emoji = "1️⃣" if i == 1 else str(i)
        rate = summary['rate']

        # 判断严重程度
        if rate > 50:
            feature = "⚠️ 极其严重"
        elif rate > 10:
            feature = "⚠️ 严重"
        elif rate > 1:
            feature = "⚠️ 较多"
        elif rate > 0.1:
            feature = "轻微"
        elif rate > 0:
            feature = "几乎没有"
        else:
            feature = "✅ 完美"

        print(f"{emoji:<6} {summary['exchange']:<8} "
              f"{summary['total']:>14,} {summary['duplicates']:>14,} "
              f"{rate:>9.2f}% {feature:<10}")

    # 打印详细信息
    print("\n" + "=" * 80)
    print("📝 重复数据详细分析")
    print("=" * 80)

    for summary in exchange_summary:
        exchange = summary['exchange']
        data = stats[exchange]

        print(f"\n【{exchange}】")
        print(f"  总记录数: {summary['total']:,}")
        print(f"  重复记录: {summary['duplicates']:,}")
        print(f"  重复率: {summary['rate']:.2f}%")
        print(f"  受影响合约数: {summary['affected_contracts']}")

        # 如果有重复，显示前10个最严重的合约
        if data['contracts']:
            print(f"\n  重复最严重的前10个合约:")
            sorted_contracts = sorted(data['contracts'],
                                    key=lambda x: x['rate'],
                                    reverse=True)[:10]
            for contract_info in sorted_contracts:
                print(f"    - {contract_info['contract']}: "
                      f"{contract_info['duplicates']:,}/{contract_info['total']:,} "
                      f"({contract_info['rate']:.2f}%)")

def main():
    """主函数"""
    print("🔍 检查 dongzheng/full_tick 数据重复情况\n")

    stats = check_duplicates_by_exchange()

    if stats:
        print_report(stats)

        # 保存详细报告
        print("\n💾 保存详细报告...")

        # 转换为DataFrame保存
        rows = []
        for exchange, data in stats.items():
            for contract_info in data['contracts']:
                rows.append({
                    'exchange': exchange,
                    'contract': contract_info['contract'],
                    'total_records': contract_info['total'],
                    'duplicate_records': contract_info['duplicates'],
                    'duplicate_rate': contract_info['rate']
                })

        if rows:
            df = pd.DataFrame(rows)
            output_file = 'dongzheng_duplicates_detail.csv'
            df.to_csv(output_file, index=False)
            print(f"✅ 详细报告已保存到: {output_file}")
    else:
        print("❌ 没有统计到任何数据")

if __name__ == '__main__':
    main()
