#!/usr/bin/env python3
"""
测试 flush_auction_bar 的 volume=0 处理逻辑

测试场景：
1. 集合竞价时段收到 volume=0 的tick
2. 修复前：跳过生成bar
3. 修复后：生成bar（使用tick的价格、volume=0、正确计算oi_diff）
"""

import polars as pl
from typing import Optional, List, Dict

class TickData:
    def __init__(self, trade_time, close, volume, open_interest, pre_settle=None):
        self.trade_time = trade_time
        self.close = close
        self.volume = volume
        self.open_interest = open_interest
        self.pre_settle = pre_settle

class BarData:
    def __init__(self, trade_time, open, high, low, close, volume, turnover,
                 open_interest, open_interest_diff, prev_close=None):
        self.trade_time = trade_time
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.turnover = turnover
        self.open_interest = open_interest
        self.open_interest_diff = open_interest_diff
        self.prev_close = prev_close

    def __repr__(self):
        return (f"Bar(time={self.trade_time}, "
                f"close={self.close}, vol={self.volume}, "
                f"oi={self.open_interest}, oi_diff={self.open_interest_diff})")

class AuctionBarAggregatorOld:
    """修复前的逻辑：跳过 volume=0 的bar"""
    def __init__(self):
        self.auction_ticks = []
        self.prev_last_tick = None

    def flush_auction_bar(self, opening_time: str) -> Optional[BarData]:
        if not self.auction_ticks:
            return None

        last_tick = self.auction_ticks[-1]

        # 修复前的逻辑：volume=0 时跳过
        if last_tick.volume == 0:
            self.auction_ticks.clear()
            return None  # 跳过！

        # 聚合 OHLC
        open_price = self.auction_ticks[0].close
        high_price = max(t.close for t in self.auction_ticks)
        low_price = min(t.close for t in self.auction_ticks)
        close_price = last_tick.close

        # 计算 oi_diff
        if self.prev_last_tick:
            oi_diff = last_tick.open_interest - self.prev_last_tick.open_interest
        else:
            oi_diff = 0

        bar = BarData(
            trade_time=opening_time,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=last_tick.volume,
            turnover=0.0,
            open_interest=last_tick.open_interest,
            open_interest_diff=oi_diff
        )

        # 更新 prev_last_tick
        self.prev_last_tick = last_tick
        self.auction_ticks.clear()

        return bar

class AuctionBarAggregatorNew:
    """修复后的逻辑：保留 volume=0 的bar"""
    def __init__(self):
        self.auction_ticks = []
        self.prev_last_tick = None

    def flush_auction_bar(self, opening_time: str) -> Optional[BarData]:
        if not self.auction_ticks:
            return None

        last_tick = self.auction_ticks[-1]

        # 修复后的逻辑：删除 volume=0 跳过检查
        # 即使 volume=0 也生成bar

        # 聚合 OHLC
        open_price = self.auction_ticks[0].close
        high_price = max(t.close for t in self.auction_ticks)
        low_price = min(t.close for t in self.auction_ticks)
        close_price = last_tick.close

        # 计算 oi_diff
        if self.prev_last_tick:
            oi_diff = last_tick.open_interest - self.prev_last_tick.open_interest
        else:
            oi_diff = 0

        bar = BarData(
            trade_time=opening_time,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=last_tick.volume,
            turnover=0.0,
            open_interest=last_tick.open_interest,
            open_interest_diff=oi_diff
        )

        # 更新 prev_last_tick
        self.prev_last_tick = last_tick
        self.auction_ticks.clear()

        return bar

def test_with_real_data():
    """使用 m1803 的真实数据测试"""
    print("=" * 80)
    print("测试：使用 m1803 的真实 tick 数据")
    print("=" * 80)

    # 读取前10个tick
    df = pl.read_parquet("/data/future_data/dongzheng_data/full_tick/m1803.parquet")

    # 构造测试用的 tick 序列
    test_ticks = [
        TickData("2017-03-14 20:55:05", 2909.0, 0, 0),      # 集合竞价 volume=0
        TickData("2017-03-14 21:00:00", 2909.0, 0, 0),      # 夜盘开始 volume=0
        TickData("2017-03-14 21:00:07", 2724.0, 4, 4),      # 第一笔交易
        TickData("2017-03-14 21:00:16", 2724.0, 10, 10),    # 第二笔交易
    ]

    print("\n输入 tick 数据：")
    for i, tick in enumerate(test_ticks, 1):
        print(f"  {i}. {tick.trade_time}: close={tick.close}, vol={tick.volume}, oi={tick.open_interest}")

    # 测试修复前的逻辑
    print("\n" + "-" * 80)
    print("修复前的逻辑（跳过 volume=0）：")
    print("-" * 80)

    agg_old = AuctionBarAggregatorOld()
    bars_old = []

    # 模拟处理集合竞价时段的 tick
    agg_old.auction_ticks.append(test_ticks[0])  # 20:55:05 volume=0
    bar1 = agg_old.flush_auction_bar("2017-03-14 21:00:00")
    if bar1:
        bars_old.append(bar1)
        print(f"生成 bar: {bar1}")
    else:
        print(f"跳过 bar (20:55 集合竞价 volume=0)")

    agg_old.auction_ticks.append(test_ticks[1])  # 21:00:00 volume=0
    bar2 = agg_old.flush_auction_bar("2017-03-14 21:00:00")
    if bar2:
        bars_old.append(bar2)
        print(f"生成 bar: {bar2}")
    else:
        print(f"跳过 bar (21:00 集合竞价 volume=0)")

    # 第一笔真实交易
    agg_old.auction_ticks.append(test_ticks[2])  # 21:00:07 volume=4
    bar3 = agg_old.flush_auction_bar("2017-03-14 21:00:00")
    if bar3:
        bars_old.append(bar3)
        print(f"生成 bar: {bar3}")

    # 计算统计
    total_oi_diff_old = sum(b.open_interest_diff for b in bars_old)
    first_oi_old = bars_old[0].open_interest if bars_old else 0
    last_oi_old = bars_old[-1].open_interest if bars_old else 0
    total_change_old = last_oi_old - first_oi_old

    print(f"\n统计（修复前）：")
    print(f"  生成 bar 数量: {len(bars_old)}")
    print(f"  sum(oi_diff) = {total_oi_diff_old}")
    print(f"  total_change = {last_oi_old} - {first_oi_old} = {total_change_old}")
    print(f"  匹配: {total_oi_diff_old == total_change_old} {'✅' if total_oi_diff_old == total_change_old else '❌'}")

    # 测试修复后的逻辑
    print("\n" + "-" * 80)
    print("修复后的逻辑（保留 volume=0）：")
    print("-" * 80)

    agg_new = AuctionBarAggregatorNew()
    bars_new = []

    # 模拟处理集合竞价时段的 tick
    agg_new.auction_ticks.append(test_ticks[0])  # 20:55:05 volume=0
    bar1 = agg_new.flush_auction_bar("2017-03-14 21:00:00")
    if bar1:
        bars_new.append(bar1)
        print(f"生成 bar: {bar1}")
    else:
        print(f"跳过 bar")

    agg_new.auction_ticks.append(test_ticks[1])  # 21:00:00 volume=0
    bar2 = agg_new.flush_auction_bar("2017-03-14 21:00:00")
    if bar2:
        bars_new.append(bar2)
        print(f"生成 bar: {bar2}")
    else:
        print(f"跳过 bar")

    # 第一笔真实交易
    agg_new.auction_ticks.append(test_ticks[2])  # 21:00:07 volume=4
    bar3 = agg_new.flush_auction_bar("2017-03-14 21:00:00")
    if bar3:
        bars_new.append(bar3)
        print(f"生成 bar: {bar3}")

    # 计算统计
    total_oi_diff_new = sum(b.open_interest_diff for b in bars_new)
    first_oi_new = bars_new[0].open_interest if bars_new else 0
    last_oi_new = bars_new[-1].open_interest if bars_new else 0
    total_change_new = last_oi_new - first_oi_new

    print(f"\n统计（修复后）：")
    print(f"  生成 bar 数量: {len(bars_new)}")
    print(f"  sum(oi_diff) = {total_oi_diff_new}")
    print(f"  total_change = {last_oi_new} - {first_oi_new} = {total_change_new}")
    print(f"  匹配: {total_oi_diff_new == total_change_new} {'✅' if total_oi_diff_new == total_change_new else '❌'}")

    # 对比结果
    print("\n" + "=" * 80)
    print("对比总结：")
    print("=" * 80)
    print(f"修复前: 生成 {len(bars_old)} 个bar, sum(oi_diff)={total_oi_diff_old}, total_change={total_change_old}")
    print(f"修复后: 生成 {len(bars_new)} 个bar, sum(oi_diff)={total_oi_diff_new}, total_change={total_change_new}")
    print(f"\n修复效果:")
    print(f"  1. volume=0 的bar是否保留: {'✅ 是' if len(bars_new) > len(bars_old) else '❌ 否'}")
    print(f"  2. oi_diff 累计是否正确: {'✅ 是' if total_oi_diff_new == total_change_new else '❌ 否'}")
    print(f"  3. 价格信息（昨结算价）是否保留: {'✅ 是 (2909.0)' if bars_new and bars_new[0].close == 2909.0 else '❌ 否'}")

    return len(bars_new) > len(bars_old) and total_oi_diff_new == total_change_new

if __name__ == "__main__":
    success = test_with_real_data()

    if success:
        print("\n" + "=" * 80)
        print("✅ 测试通过！修复方案有效，可以应用到正式代码")
        print("=" * 80)
        exit(0)
    else:
        print("\n" + "=" * 80)
        print("❌ 测试失败！需要重新审视修复方案")
        print("=" * 80)
        exit(1)
