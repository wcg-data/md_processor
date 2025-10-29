#!/usr/bin/env python3
# update_trade_session.py - 从OpenCTP API更新交易时段配置
# 功能：
# 1. 从OpenCTP API获取所有期货品种的交易时段
# 2. 根据交易所规则推断集合竞价时间
# 3. 生成符合现有格式的trade_session.csv文件

import requests
import csv
from datetime import datetime, timedelta
from collections import defaultdict
from typing import List, Dict, Tuple, Optional

# API URL
API_URL = "http://dict.openctp.cn/times?markets=SHFE,CFFEX,DCE,CZCE,INE,GFEX"

# 集合竞价时间规则（根据交易所和交易时段推断）
# 格式：{交易所: {时段开始时间: (集合竞价开始, 集合竞价结束)}}
AUCTION_RULES = {
    # 上期所(SHFE)、上海国际能源交易中心(INE)：日盘和夜盘都有集合竞价
    'SHFE': {
        '09:00:00': ('08:55:00', '09:00:00'),  # 日盘集合竞价
        '21:00:00': ('20:55:00', '21:00:00'),  # 夜盘集合竞价
    },
    'INE': {
        '09:00:00': ('08:55:00', '09:00:00'),
        '21:00:00': ('20:55:00', '21:00:00'),
    },
    # 大商所(DCE)：日盘和夜盘都有集合竞价
    'DCE': {
        '09:00:00': ('08:55:00', '09:00:00'),
        '21:00:00': ('20:55:00', '21:00:00'),
    },
    # 郑商所(ZCE)：日盘和夜盘都有集合竞价
    'CZCE': {
        '09:00:00': ('08:55:00', '09:00:00'),
        '21:00:00': ('20:55:00', '21:00:00'),
    },
    # 中金所(CFFEX)：只有日盘，有集合竞价
    'CFFEX': {
        '09:30:00': ('09:25:00', '09:30:00'),  # 股指期货
        '09:15:00': ('09:10:00', '09:15:00'),  # 国债期货
    },
    # 广期所(GFEX)：有集合竞价
    'GFEX': {
        '09:00:00': ('08:55:00', '09:00:00'),
        '21:00:00': ('20:55:00', '21:00:00'),
    },
}


def fetch_trading_sessions() -> List[Dict]:
    """从OpenCTP API获取交易时段数据"""
    try:
        print(f"正在从 {API_URL} 获取数据...")
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()

        data = response.json()

        if data['rsp_code'] != 0:
            raise Exception(f"API返回错误: {data['rsp_message']}")

        sessions = data['data']
        print(f"✓ 成功获取 {len(sessions)} 条交易时段记录")
        return sessions

    except requests.RequestException as e:
        print(f"✗ 网络请求失败: {e}")
        raise
    except Exception as e:
        print(f"✗ 数据解析失败: {e}")
        raise


def get_auction_time(exchange: str, time_begin: str) -> Optional[str]:
    """
    根据交易所和交易时段开始时间推断集合竞价时间

    Args:
        exchange: 交易所代码
        time_begin: 交易时段开始时间

    Returns:
        集合竞价开始时间，如果没有集合竞价则返回None
    """
    if exchange not in AUCTION_RULES:
        return None

    rules = AUCTION_RULES[exchange]
    if time_begin in rules:
        return rules[time_begin][0]

    return None


def normalize_product_case(product: str, exchange: str) -> str:
    """
    根据交易所规则标准化品种代码的大小写

    规则：
    - CZCE（郑州商品交易所）+ CFFEX（中国金融期货交易所）= 大写
    - SHFE（上海期货交易所）+ DCE（大连商品交易所）+ INE（上海国际能源）+ GFEX（广州期货）= 小写

    Args:
        product: 原始品种代码
        exchange: 交易所代码

    Returns:
        标准化后的品种代码
    """
    # 大写交易所
    UPPERCASE_EXCHANGES = {'CZCE', 'CFFEX'}
    # 小写交易所
    LOWERCASE_EXCHANGES = {'SHFE', 'DCE', 'INE', 'GFEX'}

    if exchange in UPPERCASE_EXCHANGES:
        return product.upper()
    elif exchange in LOWERCASE_EXCHANGES:
        return product.lower()
    else:
        # 未知交易所，保持原样
        return product


def organize_sessions_by_product(sessions: List[Dict]) -> Dict[str, List[Tuple]]:
    """
    按品种组织交易时段数据

    Args:
        sessions: API返回的交易时段列表

    Returns:
        {品种代码: [(交易所, 时段号, 集合竞价时间, 开始时间, 结束时间), ...]}
    """
    product_sessions = defaultdict(list)

    for session in sessions:
        exchange = session['ExchangeID']
        product_raw = session['ProductID']
        segment_no = session['SegmentNo']
        time_begin = session['TimeBegin']
        time_end = session['TimeEnd']

        # 根据交易所规则标准化品种代码大小写
        product = normalize_product_case(product_raw, exchange)

        # 获取集合竞价时间（只有第一个时段才可能有集合竞价）
        auction_time = get_auction_time(exchange, time_begin) if segment_no == 1 else None

        product_sessions[product].append((
            exchange,
            segment_no,
            auction_time,
            time_begin,
            time_end
        ))

    # 按时段号排序
    for product in product_sessions:
        product_sessions[product].sort(key=lambda x: x[1])

    return product_sessions


def write_trade_session_csv(product_sessions: Dict[str, List[Tuple]], output_file: str):
    """
    写入trade_session.csv文件

    Args:
        product_sessions: 按品种组织的交易时段数据
        output_file: 输出文件路径
    """
    print(f"\n正在写入 {output_file}...")

    # 统计信息
    total_products = len(product_sessions)
    total_sessions = sum(len(sessions) for sessions in product_sessions.values())

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)

        # 写入表头
        writer.writerow(['comd', 'auction_time', 'opening_time', 'closing_time'])

        # 按品种代码排序
        for product in sorted(product_sessions.keys()):
            sessions = product_sessions[product]

            for exchange, segment_no, auction_time, time_begin, time_end in sessions:
                # 格式：
                # - 第一行（第一个时段）：包含集合竞价时间（如果有）
                # - 后续行：集合竞价时间为空
                writer.writerow([
                    product,
                    auction_time if auction_time else '',
                    time_begin,
                    time_end
                ])

    print(f"✓ 成功写入 {total_products} 个品种，共 {total_sessions} 个交易时段")


def compare_with_existing(new_file: str, old_file: str):
    """
    对比新旧文件的差异

    Args:
        new_file: 新生成的文件
        old_file: 旧文件
    """
    try:
        print(f"\n对比新旧文件...")

        # 读取新文件
        with open(new_file, 'r', encoding='utf-8') as f:
            new_lines = set(f.readlines()[1:])  # 跳过表头

        # 读取旧文件
        with open(old_file, 'r', encoding='utf-8') as f:
            old_lines = set(f.readlines()[1:])  # 跳过表头

        # 统计
        new_products = {line.split(',')[0] for line in new_lines}
        old_products = {line.split(',')[0] for line in old_lines}

        added_products = new_products - old_products
        removed_products = old_products - new_products
        common_products = new_products & old_products

        print(f"\n统计信息:")
        print(f"  新文件品种数: {len(new_products)}")
        print(f"  旧文件品种数: {len(old_products)}")
        print(f"  新增品种数: {len(added_products)}")
        print(f"  移除品种数: {len(removed_products)}")
        print(f"  共同品种数: {len(common_products)}")

        if added_products:
            print(f"\n新增品种: {sorted(added_products)}")

        if removed_products:
            print(f"\n移除品种: {sorted(removed_products)}")

    except FileNotFoundError:
        print(f"✗ 旧文件 {old_file} 不存在，跳过对比")
    except Exception as e:
        print(f"✗ 对比失败: {e}")


def backup_file(file_path: str) -> str:
    """
    备份现有文件

    Args:
        file_path: 文件路径

    Returns:
        备份文件路径
    """
    import os
    import shutil

    if not os.path.exists(file_path):
        return None

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = f"{file_path}.backup_{timestamp}"

    shutil.copy2(file_path, backup_path)
    print(f"✓ 已备份现有文件至: {backup_path}")

    return backup_path


def main():
    """主函数"""
    print("=" * 80)
    print("交易时段配置更新工具")
    print("=" * 80)

    output_file = '/root/project/md_processor/trade_session.csv'

    try:
        # 1. 备份现有文件
        backup_path = backup_file(output_file)

        # 2. 从API获取数据
        sessions = fetch_trading_sessions()

        # 3. 按品种组织数据
        print("\n正在组织交易时段数据...")
        product_sessions = organize_sessions_by_product(sessions)
        print(f"✓ 成功组织 {len(product_sessions)} 个品种的交易时段")

        # 4. 写入CSV文件
        write_trade_session_csv(product_sessions, output_file)

        # 5. 对比新旧文件
        if backup_path:
            compare_with_existing(output_file, backup_path)

        print("\n" + "=" * 80)
        print("✓ 交易时段配置更新完成!")
        print(f"✓ 新文件: {output_file}")
        if backup_path:
            print(f"✓ 备份文件: {backup_path}")
        print("=" * 80)

    except Exception as e:
        print("\n" + "=" * 80)
        print(f"✗ 更新失败: {e}")
        print("=" * 80)
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == '__main__':
    exit(main())
