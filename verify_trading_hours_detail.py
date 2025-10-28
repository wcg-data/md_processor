#!/usr/bin/env python3
"""
详细验证交易时段开闭和集合竞价时间
"""

import polars as pl

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
            auction_time = parts[1]
            start_time = parts[2]
            end_time = parts[3]

            if comd not in sessions:
                sessions[comd] = {
                    'auction_times': [],
                    'trading_ranges': []
                }

            if auction_time:
                sessions[comd]['auction_times'].append(auction_time)

            sessions[comd]['trading_ranges'].append((start_time, end_time))

    return sessions

def verify_contract_detail(contract: str):
    """详细验证单个合约"""

    # 提取品种代码
    comd = ''.join([c for c in contract if c.isalpha()])

    print(f"\n{'='*100}")
    print(f"合约: {contract} (品种: {comd})")
    print(f"{'='*100}")

    # 加载配置
    sessions = load_trade_sessions()

    if comd not in sessions:
        print(f"⚠️ 未找到品种 {comd} 的交易时段配置")
        return

    config = sessions[comd]

    print(f"\n配置的交易时段:")
    for auction in config['auction_times']:
        print(f"  集合竞价: {auction}")
    for start, end in config['trading_ranges']:
        print(f"  交易时段: {start} - {end}")

    # 加载数据
    try:
        df = pl.read_parquet(f"/data/future_data/dongzheng_data/full_bar_1min/{contract}_1min_on_tick.parquet")
        df = df.filter(pl.col("contract") == contract).sort("trade_time")

        if len(df) == 0:
            print("⚠️ 无数据")
            return

        print(f"\n总bar数: {len(df)}")

        # ===== 1. 检查第一天和最后一天的开闭 =====
        print(f"\n{'='*80}")
        print("1. 交易日开闭时间验证")
        print(f"{'='*80}")

        # 获取第一天和最后一天
        first_date = str(df[0]["date"][0])
        last_date = str(df[-1]["date"][0])

        # 第一天
        first_day_df = df.filter(pl.col("date").cast(str) == first_date)
        if len(first_day_df) > 0:
            first_bar = first_day_df[0]
            last_bar_of_first_day = first_day_df[-1]

            print(f"\n第一天 ({first_date}):")
            print(f"  第一个bar: {first_bar['trade_time'][0]} | vol={first_bar['volume'][0]:>6} | oi={first_bar['open_interest'][0]:>6}")
            print(f"  最后bar:   {last_bar_of_first_day['trade_time'][0]} | vol={last_bar_of_first_day['volume'][0]:>6} | oi={last_bar_of_first_day['open_interest'][0]:>6}")

        # 最后一天
        last_day_df = df.filter(pl.col("date").cast(str) == last_date)
        if len(last_day_df) > 0:
            first_bar_of_last_day = last_day_df[0]
            last_bar = last_day_df[-1]

            print(f"\n最后一天 ({last_date}):")
            print(f"  第一个bar: {first_bar_of_last_day['trade_time'][0]} | vol={first_bar_of_last_day['volume'][0]:>6} | oi={first_bar_of_last_day['open_interest'][0]:>6}")
            print(f"  最后bar:   {last_bar['trade_time'][0]} | vol={last_bar['volume'][0]:>6} | oi={last_bar['open_interest'][0]:>6}")

            # 检查最后一个bar的时间是否匹配配置
            last_time = str(last_bar['trade_time'][0]).split()[1][:5]  # HH:MM

            # 检查是否匹配任何配置的结束时间
            matches_end_time = False
            for start, end in config['trading_ranges']:
                if last_time == end[:5]:
                    matches_end_time = True
                    print(f"  ✅ 最后bar时间 {last_time} 匹配配置的结束时间 {end}")
                    break

            if not matches_end_time:
                print(f"  ⚠️ 最后bar时间 {last_time} 不匹配任何配置的结束时间")
                print(f"     配置的结束时间: {[end for _, end in config['trading_ranges']]}")

        # ===== 2. 集合竞价bar详细检查 =====
        print(f"\n{'='*80}")
        print("2. 集合竞价bar详细检查")
        print(f"{'='*80}")

        if config['auction_times']:
            print(f"\n配置的集合竞价时间: {config['auction_times']}")

            # 根据集合竞价时间推断开盘时间
            auction_to_opening = {
                "20:55:00": "21:00:00",
                "08:55:00": "09:00:00",
                "09:25:00": "09:30:00",
            }

            for auction_time in config['auction_times']:
                opening_time = auction_to_opening.get(auction_time, "未知")
                print(f"\n集合竞价时段: {auction_time} → 开盘时间: {opening_time}")

                # 查找该时间点的bar
                auction_bars = df.filter(
                    pl.col("trade_time").str.ends_with(opening_time)
                )

                if len(auction_bars) > 0:
                    print(f"  找到 {len(auction_bars)} 个 {opening_time} 的bar")

                    # 显示前5个
                    for i in range(min(5, len(auction_bars))):
                        bar = auction_bars[i]
                        print(f"    {bar['trade_time'][0]} | vol={bar['volume'][0]:>6} | oi={bar['open_interest'][0]:>6} | oi_diff={bar['open_interest_diff'][0]:>6} | close={bar['close'][0]:>8.2f}")
                else:
                    print(f"  未找到 {opening_time} 的bar")
        else:
            print("该品种无集合竞价配置")

        # ===== 3. 每个时段的最后一分钟检查 =====
        print(f"\n{'='*80}")
        print("3. 各交易时段的收盘bar")
        print(f"{'='*80}")

        for start, end in config['trading_ranges']:
            # 查找该时段的最后一分钟
            end_time_short = end[:5]  # HH:MM
            end_bars = df.filter(
                pl.col("trade_time").str.contains(f" {end_time_short}:00")
            )

            if len(end_bars) > 0:
                print(f"\n时段 {start} - {end} 的收盘bar:")
                # 显示最后3个
                for i in range(max(0, len(end_bars)-3), len(end_bars)):
                    bar = end_bars[i]
                    print(f"  {bar['trade_time'][0]} | vol={bar['volume'][0]:>6} | oi={bar['open_interest'][0]:>6}")
            else:
                print(f"\n时段 {start} - {end}: 未找到 {end_time_short} 的bar ⚠️")

        # ===== 4. 检查是否有跨时段的缺失 =====
        print(f"\n{'='*80}")
        print("4. 时段连续性检查")
        print(f"{'='*80}")

        # 检查是否有非交易时段的bar
        df_with_time = df.with_columns(
            pl.col("trade_time").str.slice(11, 5).alias("time_hhmm")
        )

        # 检查几个典型的非交易时段
        non_trading_periods = [
            ("00:01", "08:54", "凌晨-早盘前"),
            ("03:00", "08:54", "凌晨"),
        ]

        for start_t, end_t, desc in non_trading_periods:
            suspicious = df_with_time.filter(
                (pl.col("time_hhmm") >= start_t) & (pl.col("time_hhmm") <= end_t)
            )

            if len(suspicious) > 0:
                print(f"  ⚠️ 发现 {len(suspicious)} 个bar在 {desc} ({start_t}-{end_t})")
                # 显示前3个
                for i in range(min(3, len(suspicious))):
                    bar = suspicious[i]
                    print(f"    {bar['trade_time'][0]}")
            else:
                print(f"  ✅ {desc} ({start_t}-{end_t}): 无数据")

    except Exception as e:
        print(f"❌ 验证失败: {e}")
        import traceback
        traceback.print_exc()

def main():
    contracts = [
        ('IF1802', 'IF'),  # 股指期货 CFFEX
        ('IC2008', 'IC'),  # 股指期货 CFFEX
        ('m1803', 'm'),    # 豆粕 DCE 夜盘23:00
        ('cu1804', 'cu'),  # 铜 SHFE 夜盘01:00
        ('al1804', 'al'),  # 铝 SHFE 夜盘01:00
        ('rb1804', 'rb'),  # 螺纹钢 SHFE 夜盘23:00
    ]

    print("="*100)
    print("详细验证：交易时段开闭和集合竞价时间")
    print("="*100)

    for contract, comd in contracts:
        verify_contract_detail(contract)

if __name__ == "__main__":
    main()
