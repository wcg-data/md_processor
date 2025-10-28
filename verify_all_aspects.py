#!/usr/bin/env python3
"""
全面验证 on_tick 处理结果的各个方面
"""

import polars as pl
import sys

def verify_contract(contract: str):
    """验证单个合约的所有方面"""
    print(f"\n{'='*80}")
    print(f"验证合约: {contract}")
    print(f"{'='*80}")

    try:
        df = pl.read_parquet(f"/data/future_data/dongzheng_data/full_bar_1min/{contract}_1min_on_tick.parquet")
        df = df.filter(pl.col("contract") == contract).sort("trade_time")

        if len(df) == 0:
            print(f"⚠️ 无数据")
            return False

        all_passed = True

        # ===== 1. OI_DIFF 累计正确性 =====
        print("\n[1] OI_DIFF 累计正确性:")
        first_bar = df[0]
        last_bar = df[-1]
        sum_oi_diff = df["open_interest_diff"].sum()
        total_change = last_bar["open_interest"][0] - first_bar["open_interest"][0]
        match = sum_oi_diff == total_change

        print(f"  sum(oi_diff) = {sum_oi_diff}")
        print(f"  total_change = {total_change}")
        print(f"  结果: {'✅ 通过' if match else '❌ 失败'}")

        if not match:
            all_passed = False

        # ===== 2. 集合竞价处理 =====
        print("\n[2] 集合竞价处理:")
        # 查找可能的集合竞价bar（时间结尾是 :00, :30, 21:00）
        auction_times = ["09:00:00", "09:30:00", "21:00:00"]
        auction_bars = df.filter(
            pl.col("trade_time").str.ends_with("09:00:00") |
            pl.col("trade_time").str.ends_with("09:30:00") |
            pl.col("trade_time").str.ends_with("21:00:00")
        )

        if len(auction_bars) > 0:
            print(f"  找到 {len(auction_bars)} 个可能的集合竞价bar")
            for i in range(min(3, len(auction_bars))):
                bar = auction_bars[i]
                print(f"    - {bar['trade_time'][0]}: vol={bar['volume'][0]}, oi={bar['open_interest'][0]}, oi_diff={bar['open_interest_diff'][0]}")
        else:
            print(f"  未找到集合竞价bar（可能该合约无集合竞价数据）")

        # ===== 3. 交易时段开闭 =====
        print("\n[3] 交易时段开闭:")
        print(f"  第一个bar: {first_bar['trade_time'][0]}, oi={first_bar['open_interest'][0]}, vol={first_bar['volume'][0]}")
        print(f"  最后bar:   {last_bar['trade_time'][0]}, oi={last_bar['open_interest'][0]}, vol={last_bar['volume'][0]}")

        # 检查第一个bar的时间是否合理
        first_time_str = str(first_bar['trade_time'][0])
        first_time_part = first_time_str.split()[1] if len(first_time_str.split()) > 1 else ""

        # 常见的交易时段开始时间
        valid_start_times = ["09:00", "09:15", "09:30", "21:00", "20:55", "20:56", "10:30", "13:30"]
        is_valid_start = any(first_time_part.startswith(t) for t in valid_start_times)

        if is_valid_start:
            print(f"  第一个bar时间: ✅ 合理")
        else:
            print(f"  第一个bar时间: ⚠️ 需要检查 ({first_time_part})")

        # ===== 4. VOLUME=0 时的处理 =====
        print("\n[4] VOLUME=0 时的处理:")
        vol_zero_df = df.filter(pl.col("volume") == 0)
        vol_zero_count = len(vol_zero_df)

        print(f"  volume=0 的bar数量: {vol_zero_count} ({vol_zero_count/len(df)*100:.1f}%)")

        if vol_zero_count > 0:
            # 检查前几个 volume=0 的bar
            print(f"  检查前3个 volume=0 的bar:")

            for i in range(min(3, len(vol_zero_df))):
                bar = vol_zero_df[i]
                bar_idx = df.filter(pl.col("trade_time") == bar["trade_time"][0]).select(pl.first()).to_dicts()[0]

                # 找到这个bar在原始df中的位置
                idx = None
                for j in range(len(df)):
                    if df[j]["trade_time"][0] == bar["trade_time"][0]:
                        idx = j
                        break

                if idx is not None and idx > 0:
                    prev_bar = df[idx - 1]
                    curr_bar = df[idx]

                    print(f"\n    Bar {idx}: {curr_bar['trade_time'][0]}")
                    print(f"      volume={curr_bar['volume'][0]}, turnover={curr_bar['turnover'][0]}")
                    print(f"      oi={curr_bar['open_interest'][0]}, oi_diff={curr_bar['open_interest_diff'][0]}")
                    print(f"      close={curr_bar['close'][0]}")
                    print(f"      prev_bar close={prev_bar['close'][0]}, oi={prev_bar['open_interest'][0]}")

                    # 验证: volume=0 时 turnover 应该为 0
                    if curr_bar['turnover'][0] != 0:
                        print(f"      ❌ turnover 应该为0")
                        all_passed = False
                    else:
                        print(f"      ✅ turnover=0")

                    # 验证: volume=0 时 close 应该等于 prev_bar 的 close
                    if curr_bar['close'][0] == prev_bar['close'][0]:
                        print(f"      ✅ close 沿用了上一个bar")
                    else:
                        print(f"      ⚠️ close 与上一个bar不同 (可能是交易时段切换)")

                    # 验证: volume=0 时 oi 应该等于 prev_bar 的 oi
                    if curr_bar['open_interest'][0] == prev_bar['open_interest'][0]:
                        print(f"      ✅ oi 沿用了上一个bar")
                    else:
                        print(f"      ⚠️ oi 与上一个bar不同 (oi_diff={curr_bar['open_interest_diff'][0]})")
        else:
            print(f"  该合约无 volume=0 的bar")

        # ===== 5. 交易时段过滤 =====
        print("\n[5] 交易时段过滤:")
        # 检查是否有明显不在交易时段的bar（如凌晨3-8点）

        # 统计各小时的bar数量
        df_with_hour = df.with_columns(
            pl.col("trade_time").str.slice(11, 2).alias("hour")
        )

        hour_counts = df_with_hour.group_by("hour").agg(pl.count()).sort("hour")

        print(f"  各小时bar数量分布:")
        suspicious_hours = []
        for row in hour_counts.iter_rows(named=True):
            hour = row['hour']
            count = row['count']

            # 检查可疑的时间段（大多数品种不交易的时间）
            if hour in ['03', '04', '05', '06', '07', '08']:
                suspicious_hours.append((hour, count))
                print(f"    {hour}:00 - {count} bars ⚠️ 可疑时段")
            else:
                print(f"    {hour}:00 - {count} bars")

        if len(suspicious_hours) > 0:
            print(f"  ⚠️ 发现 {len(suspicious_hours)} 个可疑时段，可能包含非交易时段数据")
            all_passed = False
        else:
            print(f"  ✅ 未发现明显的非交易时段数据")

        # ===== 总结 =====
        print(f"\n{'='*80}")
        if all_passed:
            print(f"✅ {contract} 所有验证通过")
        else:
            print(f"⚠️ {contract} 部分验证未通过，需要检查")
        print(f"{'='*80}")

        return all_passed

    except Exception as e:
        print(f"❌ 验证失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    contracts = ['IF1802', 'IC2008', 'm1803', 'cu1804', 'al1804', 'i2005', 'y1803', 'rb1804']

    print("="*80)
    print("全面验证 on_tick 处理结果")
    print("="*80)
    print("\n验证指标:")
    print("  1. oi_diff 累计正确性")
    print("  2. 集合竞价处理")
    print("  3. 交易时段开闭")
    print("  4. volume=0 时的处理逻辑")
    print("  5. 交易时段过滤")

    results = {}
    for contract in contracts:
        results[contract] = verify_contract(contract)

    # 总结
    print("\n" + "="*80)
    print("总结")
    print("="*80)

    passed_count = sum(1 for v in results.values() if v)
    total_count = len(results)

    print(f"\n通过: {passed_count}/{total_count}")

    for contract, passed in results.items():
        status = "✅" if passed else "⚠️"
        print(f"  {contract}: {status}")

    if passed_count == total_count:
        print("\n🎉 所有合约的所有指标都通过验证！")
        return 0
    else:
        print(f"\n⚠️ {total_count - passed_count} 个合约需要进一步检查")
        return 1

if __name__ == "__main__":
    sys.exit(main())
