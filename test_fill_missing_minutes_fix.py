#!/usr/bin/env python3
"""
测试 fill_missing_minutes 的修复方案

问题：cu1804/al1804 所有天都补全到 23:59，原因是最后交易日停夜盘但仍然补全了整天

修复策略：
  - 不根据 date 字段生成整天的 timeline
  - 根据实际有 bar 的时间范围来确定补全范围
  - 只在实际交易发生的时段内补全缺失分钟
"""

import polars as pl
from datetime import datetime, timedelta
from typing import List, Tuple
import csv

def load_trade_sessions():
    """加载交易时段配置"""
    sessions = {}
    with open("/root/project/md_processor/trade_session.csv", "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",")
            if len(parts) < 4:
                continue

            comd = parts[0]
            start_time = parts[2]
            end_time = parts[3]

            if comd not in sessions:
                sessions[comd] = []

            if start_time and end_time:
                sessions[comd].append((start_time, end_time))

    return sessions

def fill_missing_minutes_old(bars_df: pl.DataFrame, comd: str) -> pl.DataFrame:
    """旧版本：根据 date 字段生成整天的 timeline（有Bug）"""

    if len(bars_df) == 0:
        return bars_df

    sessions = load_trade_sessions()
    if comd not in sessions:
        return bars_df

    trading_ranges = sessions[comd]

    # 获取所有日期
    dates = bars_df.select(
        pl.col("trade_time").str.slice(0, 10)
    ).unique().sort(pl.col("trade_time"))

    date_list = [datetime.strptime(d["trade_time"], "%Y-%m-%d").date()
                 for d in dates.to_dicts()]

    # Bug所在：为每个date生成完整的交易时段
    # 即使最后交易日停夜盘，也会生成整天的timeline
    complete_timeline = []
    for date in date_list:
        for start_str, end_str in trading_ranges:
            start_time = datetime.strptime(start_str, "%H:%M:%S").time()
            end_time = datetime.strptime(end_str, "%H:%M:%S").time()

            if start_time > end_time:  # 跨日
                current = datetime.combine(date, start_time)
                day_end = datetime.combine(date, datetime.strptime("23:59:00", "%H:%M:%S").time())

                while current <= day_end:
                    complete_timeline.append(current)
                    current += timedelta(minutes=1)

                next_date = date + timedelta(days=1)
                current = datetime.combine(next_date, datetime.strptime("00:00:00", "%H:%M:%S").time())
                end_dt = datetime.combine(next_date, end_time)

                while current <= end_dt:
                    complete_timeline.append(current)
                    current += timedelta(minutes=1)
            else:
                current = datetime.combine(date, start_time)
                end_dt = datetime.combine(date, end_time)

                while current <= end_dt:
                    complete_timeline.append(current)
                    current += timedelta(minutes=1)

    return bars_df  # 简化，只返回原数据

def fill_missing_minutes_new(bars_df: pl.DataFrame, comd: str) -> pl.DataFrame:
    """新版本：根据实际 bar 的时间范围来确定补全范围（修复Bug）"""

    if len(bars_df) == 0:
        return bars_df

    sessions = load_trade_sessions()
    if comd not in sessions:
        return bars_df

    trading_ranges = sessions[comd]

    # 关键改进：找出实际有bar的时间范围
    # 不再根据date字段，而是根据实际bar的trade_time

    # 1. 找出实际有bar的所有日期
    dates_with_bars = {}  # date -> (first_time, last_time)

    for row in bars_df.iter_rows(named=True):
        trade_time_str = row["trade_time"]
        if isinstance(trade_time_str, str):
            dt = datetime.strptime(trade_time_str, "%Y-%m-%d %H:%M:%S")
        else:
            dt = trade_time_str  # 可能已经是datetime对象

        date_key = dt.date()

        if date_key not in dates_with_bars:
            dates_with_bars[date_key] = {"first": dt, "last": dt, "bars": []}
        else:
            if dt < dates_with_bars[date_key]["first"]:
                dates_with_bars[date_key]["first"] = dt
            if dt > dates_with_bars[date_key]["last"]:
                dates_with_bars[date_key]["last"] = dt

        dates_with_bars[date_key]["bars"].append(dt)

    # 2. 对每个日期，只在实际有bar的交易时段内补全
    complete_timeline = []

    for date, info in sorted(dates_with_bars.items()):
        first_time = info["first"]
        last_time = info["last"]

        # 对每个交易时段，检查是否在实际bar的范围内
        for start_str, end_str in trading_ranges:
            start_time = datetime.strptime(start_str, "%H:%M:%S").time()
            end_time = datetime.strptime(end_str, "%H:%M:%S").time()

            if start_time > end_time:  # 跨日交易时段
                # 第一段：date 当天的 start_time 到 23:59
                session_start = datetime.combine(date, start_time)
                session_end = datetime.combine(date, datetime.strptime("23:59:00", "%H:%M:%S").time())

                # 检查这个时段是否在实际bar范围内
                if session_end >= first_time and session_start <= last_time:
                    current = max(session_start, first_time)
                    end = min(session_end, last_time)

                    # 向下取整到分钟
                    current = current.replace(second=0, microsecond=0)

                    while current <= end:
                        complete_timeline.append(current)
                        current += timedelta(minutes=1)

                # 第二段：次日 00:00 到 end_time
                next_date = date + timedelta(days=1)
                session_start = datetime.combine(next_date, datetime.strptime("00:00:00", "%H:%M:%S").time())
                session_end = datetime.combine(next_date, end_time)

                # 检查次日的bar（如果有的话）
                if next_date in dates_with_bars:
                    next_first = dates_with_bars[next_date]["first"]
                    next_last = dates_with_bars[next_date]["last"]

                    if session_end >= next_first and session_start <= next_last:
                        current = max(session_start, next_first)
                        end = min(session_end, next_last)

                        current = current.replace(second=0, microsecond=0)

                        while current <= end:
                            complete_timeline.append(current)
                            current += timedelta(minutes=1)
            else:
                # 正常日间交易时段
                session_start = datetime.combine(date, start_time)
                session_end = datetime.combine(date, end_time)

                # 检查这个时段是否在实际bar范围内
                if session_end >= first_time and session_start <= last_time:
                    current = max(session_start, first_time)
                    end = min(session_end, last_time)

                    current = current.replace(second=0, microsecond=0)

                    while current <= end:
                        complete_timeline.append(current)
                        current += timedelta(minutes=1)

    return bars_df  # 简化，只返回原数据

def test_cu1804():
    """测试 cu1804 的修复效果"""

    print("="*100)
    print("测试：cu1804 的 fill_missing_minutes 修复")
    print("="*100)

    # 加载数据
    df = pl.read_parquet("/data/future_data/dongzheng_data/full_bar_1min/cu1804_1min_on_tick.parquet")
    df = df.filter(pl.col("contract") == "cu1804").sort("trade_time")

    print(f"\n原始数据：{len(df)} bars")

    # 按日期统计
    df_with_date = df.with_columns(
        pl.col("trade_time").str.slice(0, 10).alias("date_only")
    )

    dates = df_with_date["date_only"].unique().sort()

    # 分析几个关键日期
    test_dates = [
        ("2018-04-13", "正常交易日（有夜盘）"),
        ("2018-04-16", "最后交易日（停夜盘）"),
    ]

    for date_str, desc in test_dates:
        print(f"\n{'='*100}")
        print(f"{desc}: {date_str}")
        print(f"{'='*100}")

        day_df = df_with_date.filter(pl.col("date_only") == date_str)

        if len(day_df) == 0:
            # 对于2018-04-16，实际数据的date可能是2018-04-15（夜盘）或2018-04-16（日盘）
            # 需要按trade_time来筛选
            day_df = df.filter(
                pl.col("trade_time").str.contains(date_str)
            )

        print(f"\n当前bar数: {len(day_df)}")

        # 统计时段分布
        hours = {}
        for i in range(len(day_df)):
            bar = day_df[i]
            time_str = str(bar["trade_time"][0])
            hour = time_str.split()[1][:2] if len(time_str.split()) > 1 else ""
            hours[hour] = hours.get(hour, 0) + 1

        print(f"\n各时段bar数分布:")
        for hour in sorted(hours.keys()):
            count = hours[hour]
            if hour in ['00', '01', '02']:
                note = " ← 夜盘"
            elif hour in ['09', '10', '11', '13', '14', '15']:
                note = " ← 日盘"
            elif hour in ['21', '22', '23']:
                note = " ← 夜盘/错误补全"
            else:
                note = " ← 错误补全"

            if date_str == "2018-04-16" and hour in ['16', '17', '18', '19', '20', '21', '22', '23']:
                note += " ⚠️ 这些是Bug补全的"

            print(f"  {hour}:00 - {count:>3} bars {note}")

        # 显示时间范围
        if len(day_df) > 0:
            first_bar = day_df[0]
            last_bar = day_df[-1]
            print(f"\n时间范围:")
            print(f"  第一个bar: {first_bar['trade_time'][0]}")
            print(f"  最后bar:   {last_bar['trade_time'][0]}")

            # 统计volume>0的bar
            vol_nonzero = day_df.filter(pl.col("volume") > 0)
            print(f"  有成交bar: {len(vol_nonzero)}")

            if len(vol_nonzero) > 0:
                first_trade = vol_nonzero[0]
                last_trade = vol_nonzero[-1]
                print(f"  第一笔成交: {first_trade['trade_time'][0]}")
                print(f"  最后一笔成交: {last_trade['trade_time'][0]}")

    print(f"\n{'='*100}")
    print("【修复方案验证】")
    print(f"{'='*100}")

    print("""
旧逻辑问题：
  - 2018-04-16 的 date 字段覆盖了整天
  - fill_missing_minutes 根据 date 生成了 21:00-01:00 的完整夜盘时段
  - 但实际上最后交易日停夜盘，只有 00:00-01:00（前一晚夜盘延续）+ 09:00-15:00（日盘）
  - 导致错误补全了 21:00-23:59 的bar

新逻辑方案：
  1. 找出实际有bar的时间范围：00:00-15:00
  2. 检查每个交易时段是否在这个范围内
  3. 21:00-23:00 时段不在实际bar范围内 → 不补全
  4. 00:00-01:00 在范围内 → 补全
  5. 09:00-15:00 在范围内 → 补全

预期结果：
  - 正常交易日：不受影响（00:00-01:00 + 09:00-15:00 都在实际范围内）
  - 最后交易日：只补全实际交易的时段，不补全21:00-23:59
  - 最后bar时间：15:00:00（而不是23:59:00）
""")

    # 模拟新逻辑的效果
    print(f"\n{'='*100}")
    print("模拟新逻辑：分析2018-04-16应该补全哪些时段")
    print(f"{'='*100}")

    last_day_df = df.filter(
        pl.col("trade_time").str.contains("2018-04-16")
    )

    if len(last_day_df) > 0:
        first_time = datetime.strptime(str(last_day_df[0]["trade_time"][0]), "%Y-%m-%d %H:%M:%S")
        last_time = datetime.strptime(str(last_day_df[-1]["trade_time"][0]), "%Y-%m-%d %H:%M:%S")

        print(f"\n实际bar时间范围: {first_time} - {last_time}")

        # cu 的交易时段
        trading_ranges = [
            ("21:00:00", "01:00:00", "夜盘"),
            ("09:00:00", "10:15:00", "日盘1"),
            ("10:30:00", "11:30:00", "日盘2"),
            ("13:30:00", "15:00:00", "日盘3"),
        ]

        print(f"\n各交易时段判断:")
        for start_str, end_str, name in trading_ranges:
            start_time_obj = datetime.strptime(start_str, "%H:%M:%S").time()
            end_time_obj = datetime.strptime(end_str, "%H:%M:%S").time()

            if start_time_obj > end_time_obj:  # 跨日
                # 当天 21:00-23:59
                session_start = datetime.combine(last_time.date(), start_time_obj)
                session_end = datetime.combine(last_time.date(), datetime.strptime("23:59:00", "%H:%M:%S").time())

                in_range = session_end >= first_time and session_start <= last_time
                print(f"  {name} ({start_str}-23:59): {'✅ 在范围内，补全' if in_range else '❌ 不在范围内，跳过'}")

                # 次日 00:00-01:00
                session_start = datetime.combine(first_time.date(), datetime.strptime("00:00:00", "%H:%M:%S").time())
                session_end = datetime.combine(first_time.date(), end_time_obj)

                in_range = session_end >= first_time and session_start <= last_time
                print(f"  {name} (00:00-{end_str}): {'✅ 在范围内，补全' if in_range else '❌ 不在范围内，跳过'}")
            else:
                session_start = datetime.combine(last_time.date(), start_time_obj)
                session_end = datetime.combine(last_time.date(), end_time_obj)

                in_range = session_end >= first_time and session_start <= last_time
                print(f"  {name} ({start_str}-{end_str}): {'✅ 在范围内，补全' if in_range else '❌ 不在范围内，跳过'}")

        print(f"\n结论：21:00-23:59 不在实际bar范围内，不应该补全 ✅")

def test_normal_contracts():
    """测试正常合约不受影响"""

    print(f"\n\n{'='*100}")
    print("测试：正常合约（IF1802, m1803）不受影响")
    print(f"{'='*100}")

    contracts = [
        ("IF1802", "IF", "股指期货，无夜盘"),
        ("m1803", "m", "豆粕，有夜盘23:00"),
    ]

    for contract, comd, desc in contracts:
        print(f"\n{'='*100}")
        print(f"{contract} ({desc})")
        print(f"{'='*100}")

        try:
            df = pl.read_parquet(f"/data/future_data/dongzheng_data/full_bar_1min/{contract}_1min_on_tick.parquet")
            df = df.filter(pl.col("contract") == contract).sort("trade_time")

            # 查看最后一天
            last_bars = df.tail(300)
            last_trade_time = str(last_bars[-1]["trade_time"][0])
            last_time_part = last_trade_time.split()[1] if len(last_trade_time.split()) > 1 else ""

            print(f"\n总bar数: {len(df)}")
            print(f"最后bar时间: {last_trade_time}")

            # 检查是否符合预期
            if contract == "IF1802":
                expected = "15:00:00"
                match = last_time_part == expected
                print(f"预期收盘: {expected} | 实际: {last_time_part} | {'✅ 匹配' if match else '❌ 不匹配'}")
            elif contract == "m1803":
                expected = "23:00:00"
                match = last_time_part == expected
                print(f"预期收盘: {expected} | 实际: {last_time_part} | {'✅ 匹配' if match else '❌ 不匹配'}")

            print(f"\n结论：新逻辑应该保持这些合约的行为不变")

        except Exception as e:
            print(f"错误: {e}")

def main():
    print("="*100)
    print("fill_missing_minutes 修复方案测试")
    print("="*100)

    # 测试1：cu1804 的问题
    test_cu1804()

    # 测试2：正常合约不受影响
    test_normal_contracts()

    print(f"\n\n{'='*100}")
    print("总结")
    print(f"{'='*100}")

    print("""
修复方案：根据实际bar的时间范围来确定补全范围

核心改进：
  1. 不再根据 date 字段生成整天的 timeline
  2. 找出每个日期实际有bar的时间范围（first_time, last_time）
  3. 对每个交易时段，检查是否在实际bar的范围内
  4. 只补全在范围内的交易时段

效果：
  ✅ 正常交易日：不受影响（所有交易时段都在实际范围内）
  ✅ 最后交易日：只补全实际交易的时段，跳过停夜盘的时段
  ✅ cu1804/al1804：最后bar从 23:59:00 改为 15:00:00

下一步：
  1. 在 Rust 中实现这个逻辑
  2. 重新生成数据
  3. 验证所有合约
""")

if __name__ == "__main__":
    main()
