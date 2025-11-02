#!/usr/bin/env python3
"""
analyze_missing_files.py - 分析on_batch缺失的55个文件
检查：
1. 这些文件是否存在于原始tick数据目录
2. 文件大小是否异常（可能触发跳过逻辑）
3. 是否有特定的命名模式
"""

from pathlib import Path
import polars as pl

def main():
    tick_source = Path("/data/future_data/dongzheng_data/full_tick")
    batch_dir = Path("/data/future_data/dongzheng_data/full_bar_1min")
    tick_dir = Path("/data/future_data/dongzheng_data/full_bar_1min_on_tick")

    # 获取差异文件列表
    batch_files = {f.name for f in batch_dir.glob("*.parquet")}
    tick_files = {f.name for f in tick_dir.glob("*.parquet")}
    only_in_tick = sorted(tick_files - batch_files)

    print("=" * 80)
    print(f"分析 on_batch 缺失的 {len(only_in_tick)} 个文件")
    print("=" * 80)

    # 分析这些文件的原始tick数据
    print(f"\n【1. 检查原始tick文件是否存在】")

    missing_in_source = []
    existing_in_source = []

    for filename in only_in_tick:
        # 从 CF1001_1min.parquet 提取 CF1001
        contract = filename.replace("_1min.parquet", "")
        source_file = tick_source / f"{contract}.parquet"

        if source_file.exists():
            try:
                df = pl.read_parquet(source_file)
                size_mb = source_file.stat().st_size / 1024 / 1024
                existing_in_source.append({
                    'contract': contract,
                    'filename': filename,
                    'tick_count': len(df),
                    'size_mb': size_mb
                })
            except Exception as e:
                print(f"  ⚠ {contract}: 读取失败 - {e}")
        else:
            missing_in_source.append(contract)

    print(f"\n  原始tick目录中存在: {len(existing_in_source)} 个")
    print(f"  原始tick目录中不存在: {len(missing_in_source)} 个")

    if missing_in_source:
        print(f"\n  不存在的合约（可能已被删除）:")
        for contract in missing_in_source[:20]:
            print(f"    - {contract}")

    # 分析存在的文件特征
    print(f"\n【2. 分析原始tick文件特征】")
    if existing_in_source:
        print(f"\n  {'合约':<12} {'Tick数':<10} {'文件大小(MB)':<15} {'特征'}")
        print("  " + "-" * 60)

        for item in existing_in_source[:20]:
            feature = ""
            if item['tick_count'] < 1000:
                feature = "⚠ tick数极少"
            elif item['size_mb'] < 0.1:
                feature = "⚠ 文件极小"

            print(f"  {item['contract']:<12} {item['tick_count']:<10,} {item['size_mb']:<15.2f} {feature}")

        # 统计
        small_files = [x for x in existing_in_source if x['tick_count'] < 1000]
        print(f"\n  统计:")
        print(f"    - Tick数 < 1000: {len(small_files)} 个文件")
        print(f"    - 平均tick数: {sum(x['tick_count'] for x in existing_in_source) / len(existing_in_source):,.0f}")
        print(f"    - 最小tick数: {min(x['tick_count'] for x in existing_in_source):,}")
        print(f"    - 最大tick数: {max(x['tick_count'] for x in existing_in_source):,}")

    # 检查命名模式
    print(f"\n【3. 命名模式分析】")

    # 提取品种代码
    products = {}
    for filename in only_in_tick:
        contract = filename.replace("_1min.parquet", "")
        # 提取字母部分作为品种
        product = ''.join(c for c in contract if c.isalpha())
        if product not in products:
            products[product] = []
        products[product].append(contract)

    print(f"\n  按品种分组:")
    for product in sorted(products.keys()):
        contracts = sorted(products[product])
        print(f"    {product}: {len(contracts)} 个 - {contracts[:5]}")

    print(f"\n{'=' * 80}")
    print("【结论推测】")
    print("  可能原因：")
    print("  1. 这些文件数据量极小，on_batch可能因某种条件跳过")
    print("  2. 文件读取或解析时on_batch遇到错误但未输出")
    print("  3. on_batch的跳过逻辑与on_tick不同")
    print("=" * 80)

if __name__ == "__main__":
    main()
