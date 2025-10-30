#!/bin/bash

while true; do
  FILE_COUNT=$(ls /data/future_data/dongzheng_data/full_bar_1min/*.parquet 2>/dev/null | wc -l)
  PROC_COUNT=$(ps aux | grep "parquet_processor_on_batch.*1min.*230" | grep -v grep | wc -l)

  clear
  echo "╔══════════════════════════════════════════════════════════════════════════╗"
  echo "║  修复后的 on_batch 处理进度监控                                          ║"
  echo "╚══════════════════════════════════════════════════════════════════════════╝"
  echo ""
  date "+当前时间: %Y-%m-%d %H:%M:%S"
  echo ""
  echo "【文件进度】"
  PCT=$(echo "scale=1; $FILE_COUNT*100/8901" | bc)
  echo "  已处理: $FILE_COUNT / 8,901 文件 ($PCT%)"
  echo ""
  echo "【进程状态】"
  if [ "$PROC_COUNT" -gt "0" ]; then
    echo "  状态: ✓ 运行中"
    CPU=$(ps aux | grep "parquet_processor_on_batch.*1min.*230" | grep -v grep | awk '{print $3}' | head -1)
    echo "  CPU: ${CPU}%"
  else
    echo "  状态: ✓ 已完成"
    echo ""
    echo "╔══════════════════════════════════════════════════════════════════════════╗"
    echo "║  ✓ 处理完成！最终文件数: $FILE_COUNT                                    ║"
    echo "╚══════════════════════════════════════════════════════════════════════════╝"
    break
  fi

  echo ""
  sleep 10
done
