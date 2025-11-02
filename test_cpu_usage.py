#!/usr/bin/env python3
import polars as pl
import time
import sys

# 模拟处理 3 个合约
contracts = ['IF1802', 'IC2008', 'IH2008']

print(f"开始处理 {len(contracts)} 个合约...")
start = time.time()

for i, contract in enumerate(contracts):
    print(f"[{i+1}/{len(contracts)}] 正在处理 {contract}...")
    # 模拟读取和处理 parquet
    df = pl.read_parquet(f"/data/future_data/dongzheng_data/full_tick/{contract}.parquet")
    # 一些计算操作
    result = df.select([
        pl.col("close").mean(),
        pl.col("volume").sum()
    ])
    print(f"    {contract} 处理完成")

elapsed = time.time() - start
print(f"\n总耗时: {elapsed:.2f} 秒")
