# fix_tick_date.py - 修正tick数据的date字段为交易日
#
# 核心逻辑：
# 1. 完整交易日 = 夜盘(前一交易日的晚上) + 日盘(当日)
# 2. 夜盘时段（hour >= 21）：date = 下一个交易日
# 3. 凌晨时段（hour < 3）：date = 当日（如非交易日则为下一交易日）
# 4. 日盘时段：date = trade_time.date()

import polars as pl
import sys
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

DATA_ROOT = "/data/future_data/dongzheng_data"

def load_trading_calendar():
    """从1分钟bar数据中提取交易日历"""
    print("正在构建交易日历...")

    bar_dir = Path(f"{DATA_ROOT}/full_bar_1min")
    bar_files = list(bar_dir.glob("*.parquet"))

    if not bar_files:
        raise RuntimeError("未找到1分钟bar文件")

    # 收集所有交易日（只采样前10个合约以加快速度）
    all_dates = set()

    for bar_file in bar_files[:10]:
        try:
            # 只读取date列，减少内存占用
            df = pl.scan_parquet(bar_file).select('date').unique().collect()
            dates = df['date'].to_list()
            all_dates.update(dates)
        except:
            continue

    # 转换为排序列表
    trading_dates = sorted(list(all_dates))

    print(f"  提取到 {len(trading_dates)} 个交易日")
    print(f"  起始日期: {trading_dates[0]}")
    print(f"  结束日期: {trading_dates[-1]}")

    return set(trading_dates), trading_dates

def next_trading_day(date_str, trading_dates_list):
    """查找下一个交易日"""
    current_date = datetime.strptime(date_str, '%Y-%m-%d')

    # 最多向后查找14天（考虑春节等长假）
    for i in range(1, 15):
        next_date = current_date + timedelta(days=i)
        next_date_str = next_date.strftime('%Y-%m-%d')

        if next_date_str in trading_dates_list:
            return next_date_str

    # 如果14天内没有交易日，返回None（数据可能有问题）
    return None

def load_trade_sessions():
    """加载交易时段配置"""
    df = pl.read_csv('trade_session.csv')

    sessions = {}
    for comd in df['comd'].unique():
        comd_sessions = df.filter(pl.col('comd') == comd).select(['opening_time', 'closing_time']).to_dicts()
        sessions[comd] = comd_sessions

    return sessions

def has_night_session(comd, sessions):
    """判断品种是否有夜盘"""
    if comd not in sessions:
        return False

    for session in sessions[comd]:
        opening = session['opening_time']
        if opening and opening.startswith('2'):  # 20:xx 或 21:xx
            return True

    return False

def fix_date_field(df, comd, sessions, trading_calendar_set, trading_dates_list):
    """
    修正date字段为交易日（向量化优化版本）

    逻辑（基于交易所规则：节假日前不会有夜盘）：
    - 无夜盘品种：date保持不变
    - 有夜盘品种：
      - hour >= 21：date = 次日（如果次日是周六/日，则顺延到周一）
      - hour < 3：date = 当日（如果当日是周六/日，则顺延到周一）
      - 其他：date = trade_time.date()
    """
    # 检查是否有夜盘
    if not has_night_session(comd, sessions):
        # 无夜盘，不需要修正
        return df

    # 提取时间信息
    df = df.with_columns([
        pl.col('trade_time').dt.hour().alias('hour'),
        pl.col('trade_time').dt.date().alias('natural_date_obj'),
        pl.col('trade_time').dt.date().cast(pl.Utf8).alias('natural_date')
    ])

    # 向量化修正逻辑：处理夜盘和周末
    df = df.with_columns([
        pl.when(pl.col('hour') >= 21)
        # 夜盘：date = 次日
        .then(pl.col('natural_date_obj') + pl.duration(days=1))
        .when(pl.col('hour') < 3)
        # 凌晨：保持当日
        .then(pl.col('natural_date_obj'))
        .otherwise(pl.col('natural_date_obj'))
        .alias('temp_date_obj')
    ])

    # 处理周末：周六+2天，周日+1天
    # 注意：Polars的weekday定义：1=周一, 2=周二, ..., 6=周六, 7=周日
    df = df.with_columns([
        pl.when(pl.col('temp_date_obj').dt.weekday() == 6)  # 周六
        .then(pl.col('temp_date_obj') + pl.duration(days=2))
        .when(pl.col('temp_date_obj').dt.weekday() == 7)  # 周日
        .then(pl.col('temp_date_obj') + pl.duration(days=1))
        .otherwise(pl.col('temp_date_obj'))
        .cast(pl.Utf8)
        .alias('date')
    ])

    # 删除临时列
    df = df.drop(['hour', 'natural_date', 'natural_date_obj', 'temp_date_obj'])

    return df

def fix_tick_file(tick_path, sessions, trading_calendar_set, trading_dates_list):
    """修正单个tick文件的date字段（覆盖原文件）"""
    try:
        # 读取数据
        df = pl.read_parquet(tick_path)

        # 提取品种代码
        comd = df['comd'][0]

        # 检查是否有夜盘
        has_night = has_night_session(comd, sessions)

        if not has_night:
            # 无夜盘，跳过
            return {'status': 'skipped', 'file': tick_path.name, 'reason': 'no_night_session'}

        # 记录原始date数量
        original_dates = df.select('date').unique()

        # 修正date字段
        df_fixed = fix_date_field(df, comd, sessions, trading_calendar_set, trading_dates_list)

        # 记录修正后date数量
        fixed_dates = df_fixed.select('date').unique()

        # 保存（覆盖原文件）
        df_fixed.write_parquet(tick_path, compression='snappy')

        return {
            'status': 'success',
            'file': tick_path.name,
            'tick_count': len(df),
            'original_date_count': len(original_dates),
            'fixed_date_count': len(fixed_dates)
        }

    except Exception as e:
        return {'status': 'failed', 'file': tick_path.name, 'error': str(e)}

def main():
    if len(sys.argv) < 2:
        print("用法:")
        print(f"  {sys.argv[0]} <contract>           # 处理单个合约")
        print(f"  {sys.argv[0]} --all [num_threads]  # 批量处理所有合约")
        print(f"  {sys.argv[0]} --test               # 测试模式（cu1708和AP001）")
        return

    # 加载交易日历
    trading_calendar_set, trading_dates_list = load_trading_calendar()
    print()

    # 加载交易时段配置
    print("加载交易时段配置...")
    sessions = load_trade_sessions()
    print(f"  已加载 {len(sessions)} 个品种的配置")
    print()

    if sys.argv[1] == '--test':
        # 测试模式
        test_contracts = ['cu1708', 'AP001']
        print("测试模式：处理cu1708（有夜盘）和AP001（无夜盘）")
        print()

        for contract in test_contracts:
            tick_path = Path(f"{DATA_ROOT}/full_tick/{contract}.parquet")
            if not tick_path.exists():
                print(f"✗ {contract}: 文件不存在")
                continue

            print(f"处理: {contract}")
            result = fix_tick_file(tick_path, sessions, trading_calendar_set, trading_dates_list)

            if result['status'] == 'success':
                print(f"  ✓ 成功")
                print(f"    tick数: {result['tick_count']}")
                print(f"    原始date数: {result['original_date_count']}")
                print(f"    修正后date数: {result['fixed_date_count']}")
            elif result['status'] == 'skipped':
                print(f"  - 跳过 ({result['reason']})")
            else:
                print(f"  ✗ 失败: {result['error']}")
            print()

    elif sys.argv[1] == '--all':
        # 批量处理
        num_threads = int(sys.argv[2]) if len(sys.argv) > 2 else 64

        tick_dir = Path(f"{DATA_ROOT}/full_tick")
        tick_files = sorted(list(tick_dir.glob("*.parquet")))

        print(f"批量处理模式")
        print(f"  找到 {len(tick_files)} 个tick文件")
        print(f"  使用 {num_threads} 个线程")
        print()

        success_count = 0
        skipped_count = 0
        failed_count = 0

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # 提交所有任务
            futures = {
                executor.submit(fix_tick_file, f, sessions, trading_calendar_set, trading_dates_list): f
                for f in tick_files
            }

            # 处理结果
            for i, future in enumerate(as_completed(futures), 1):
                result = future.result()

                if result['status'] == 'success':
                    success_count += 1
                    print(f"[{i}/{len(tick_files)}] ✓ {result['file']}: {result['tick_count']} ticks, "
                          f"{result['original_date_count']}→{result['fixed_date_count']} dates")
                elif result['status'] == 'skipped':
                    skipped_count += 1
                    if i % 100 == 0:  # 只显示部分跳过信息
                        print(f"[{i}/{len(tick_files)}] - {result['file']}: {result['reason']}")
                else:
                    failed_count += 1
                    print(f"[{i}/{len(tick_files)}] ✗ {result['file']}: {result['error']}")

        print()
        print("=" * 60)
        print("批量处理完成")
        print(f"  成功修正: {success_count}")
        print(f"  跳过（无夜盘）: {skipped_count}")
        print(f"  失败: {failed_count}")
        print("=" * 60)

    else:
        # 单个合约处理
        contract = sys.argv[1]
        tick_path = Path(f"{DATA_ROOT}/full_tick/{contract}.parquet")

        if not tick_path.exists():
            print(f"错误: 文件不存在: {tick_path}")
            return

        print(f"处理单个合约: {contract}")
        print()

        result = fix_tick_file(tick_path, sessions, trading_calendar_set, trading_dates_list)

        if result['status'] == 'success':
            print(f"✓ 成功修正")
            print(f"  tick数: {result['tick_count']}")
            print(f"  原始date数: {result['original_date_count']}")
            print(f"  修正后date数: {result['fixed_date_count']}")
        elif result['status'] == 'skipped':
            print(f"- 跳过: {result['reason']}")
        else:
            print(f"✗ 失败: {result['error']}")

if __name__ == '__main__':
    main()
