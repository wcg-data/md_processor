#!/usr/bin/env python3
"""
详细分析收盘时间异常问题
"""

import polars as pl
from collections import defaultdict

def analyze_daily_closing_times(contract: str):
    """分析合约每天的收盘时间"""

    comd = ''.join([c for c in contract if c.isalpha()])

    print(f"\n{'='*100}")
    print(f"合约: {contract} (品种: {comd})")
    print(f"{'='*100}")

    try:
        df = pl.read_parquet(f"/data/future_data/dongzheng_data/full_bar_1min/{contract}_1min_on_tick.parquet")
        df = df.filter(pl.col("contract") == contract).sort("trade_time")

        if len(df) == 0:
            print("⚠️ 无数据")
            return

        # 按日期分组
        df_with_date = df.with_columns(
            pl.col("trade_time").str.slice(0, 10).alias("date_only")
        )

        dates = df_with_date["date_only"].unique().sort()

        print(f"\n总交易日数: {len(dates)}")
        print(f"日期范围: {dates[0]} - {dates[-1]}")

        # 分析每天的最后一个bar
        print(f"\n{'='*100}")
        print("每天最后一个bar的时间分析")
        print(f"{'='*100}")

        closing_time_stats = defaultdict(int)

        # 只显示前10天和后10天
        dates_to_show = []
        if len(dates) <= 20:
            dates_to_show = dates.to_list()
        else:
            dates_to_show = dates[:10].to_list() + ["..."] + dates[-10:].to_list()

        for date in dates.to_list():
            day_df = df_with_date.filter(pl.col("date_only") == date)
            if len(day_df) > 0:
                last_bar = day_df[-1]
                last_time = str(last_bar["trade_time"][0])
                time_part = last_time.split()[1] if len(last_time.split()) > 1 else ""

                closing_time_stats[time_part] += 1

                # 只显示前10天和后10天
                if date in dates_to_show:
                    print(f"  {date}: 最后bar={time_part} | vol={last_bar['volume'][0]:>6} | oi={last_bar['open_interest'][0]:>8}")

        if "..." in dates_to_show:
            print(f"  ... (省略 {len(dates) - 20} 天)")

        # 统计
        print(f"\n{'='*100}")
        print("收盘时间统计")
        print(f"{'='*100}")

        sorted_times = sorted(closing_time_stats.items(), key=lambda x: x[1], reverse=True)
        for time_str, count in sorted_times:
            percentage = count / len(dates) * 100
            print(f"  {time_str}: {count:>4} 天 ({percentage:>5.1f}%)")

        # 分析是否有规律
        print(f"\n{'='*100}")
        print("规律分析")
        print(f"{'='*100}")

        if len(closing_time_stats) == 1:
            print(f"  ✅ 所有交易日的收盘时间都一致")
        else:
            print(f"  ⚠️ 发现 {len(closing_time_stats)} 个不同的收盘时间")

            # 检查是否只有最后几天不同
            last_10_days = dates[-10:].to_list()
            last_10_times = []
            for date in last_10_days:
                day_df = df_with_date.filter(pl.col("date_only") == date)
                if len(day_df) > 0:
                    last_bar = day_df[-1]
                    last_time = str(last_bar["trade_time"][0])
                    time_part = last_time.split()[1] if len(last_time.split()) > 1 else ""
                    last_10_times.append(time_part)

            if len(set(last_10_times)) < len(closing_time_stats):
                print(f"  💡 最后10天的收盘时间更统一，可能是交割月临近的正常调整")

        # 如果有夜盘，分析夜盘收盘情况
        if comd in ['cu', 'al', 'rb', 'm', 'y', 'p']:
            print(f"\n{'='*100}")
            print("夜盘收盘时间分析")
            print(f"{'='*100}")

            night_closing_stats = defaultdict(int)

            for date in dates.to_list():
                # 查找该日期夜盘的最后一个bar（时间在21:00-23:59或00:00-02:30）
                day_df = df_with_date.filter(pl.col("date_only") == date)

                # 筛选夜盘时段的bar
                night_bars = []
                for i in range(len(day_df)):
                    bar = day_df[i]
                    time_str = str(bar["trade_time"][0])
                    time_part = time_str.split()[1][:5] if len(time_str.split()) > 1 else ""

                    # 夜盘时段：21:00-23:59
                    if time_part >= "21:00" and time_part <= "23:59":
                        night_bars.append((i, time_part, bar))

                # 查找次日00:00-02:30的bar
                try:
                    date_idx = dates.to_list().index(date)
                    if date_idx < len(dates) - 1:
                        next_date = dates[date_idx + 1]
                        next_day_df = df_with_date.filter(pl.col("date_only") == next_date)

                        for i in range(len(next_day_df)):
                            bar = next_day_df[i]
                            time_str = str(bar["trade_time"][0])
                            time_part = time_str.split()[1][:5] if len(time_str.split()) > 1 else ""

                            if time_part >= "00:00" and time_part <= "02:30":
                                night_bars.append((i, time_part, bar))
                            else:
                                break  # 已经到日盘了
                except:
                    pass

                if night_bars:
                    last_night_bar = night_bars[-1]
                    night_closing_stats[last_night_bar[1]] += 1

            if night_closing_stats:
                print(f"\n夜盘收盘时间统计:")
                sorted_night_times = sorted(night_closing_stats.items(), key=lambda x: x[1], reverse=True)
                for time_str, count in sorted_night_times:
                    percentage = count / len(dates) * 100
                    print(f"  {time_str}: {count:>4} 天 ({percentage:>5.1f}%)")

    except Exception as e:
        print(f"❌ 分析失败: {e}")
        import traceback
        traceback.print_exc()

def find_contracts_with_suffix(suffix: str):
    """查找指定后缀的合约"""
    import os
    import re

    tick_dir = "/data/future_data/dongzheng_data/full_bar_1min"

    contracts = []
    for filename in os.listdir(tick_dir):
        if filename.endswith("_1min_on_tick.parquet"):
            contract = filename.replace("_1min_on_tick.parquet", "")
            # 提取年月
            match = re.search(r'(\d{4})$', contract)
            if match and match.group(1) == suffix:
                contracts.append(contract)

    return sorted(contracts)

def main():
    print("="*100)
    print("详细分析：收盘时间异常问题")
    print("="*100)

    # 1. 分析问题合约
    print("\n" + "="*100)
    print("第一部分：问题合约详细分析")
    print("="*100)

    problem_contracts = ['IC2008', 'cu1804', 'al1804']

    for contract in problem_contracts:
        analyze_daily_closing_times(contract)

    # 2. 查找并分析2024-2025年的合约
    print("\n" + "="*100)
    print("第二部分：2024-2025年合约分析")
    print("="*100)

    recent_suffixes = ['2412', '2501', '2502', '2503']

    for suffix in recent_suffixes:
        print(f"\n{'='*100}")
        print(f"查找 {suffix} 合约...")
        print(f"{'='*100}")

        contracts = find_contracts_with_suffix(suffix)

        if contracts:
            print(f"找到 {len(contracts)} 个合约: {', '.join(contracts[:10])}")
            if len(contracts) > 10:
                print(f"... (共 {len(contracts)} 个)")

            # 选择几个代表性的合约分析
            selected = []
            for comd in ['IF', 'IC', 'IH', 'cu', 'al', 'rb', 'm', 'y']:
                for contract in contracts:
                    if contract.startswith(comd):
                        selected.append(contract)
                        break

            for contract in selected[:5]:  # 最多分析5个
                analyze_daily_closing_times(contract)
        else:
            print(f"未找到 {suffix} 合约")

if __name__ == "__main__":
    main()
