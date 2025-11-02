#!/bin/bash
# 使用230核心并行处理所有合约的on_tick

LOG_FILE="on_tick_230_processing.log"
CORES=230

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 开始on_tick处理 (${CORES}核心)..." | tee $LOG_FILE

# 获取所有合约列表（排除测试合约和无效数据）
find /data/future_data/dongzheng_data/full_tick -name "*.parquet" -type f | 
  grep -v "88\.parquet$" | 
  grep -v "88A2\.parquet$" | 
  grep -v "888\.parquet$" |
  sed 's|.*/\(.*\)\.parquet|\1|' | 
  parallel -j $CORES --bar "
    ./target/release/parquet_processor_on_tick dongzheng_data 1min {} 2>&1 | 
    grep -E '(✓|✗|警告|错误)' || echo '处理: {}'
  " >> $LOG_FILE 2>&1

echo "[$(date '+%Y-%m-%d %H:%M:%S')] on_tick处理完成" | tee -a $LOG_FILE
