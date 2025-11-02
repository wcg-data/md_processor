#!/bin/bash
while true; do
    clear
    echo "=== on_tick 处理进度监控 ==="
    echo "时间: $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""
    
    # 进程数
    PROC_COUNT=$(ps aux | grep "parquet_processor_on_tick" | grep -v grep | wc -l)
    echo "运行中的进程数: $PROC_COUNT"
    
    # 已生成文件数
    FILE_COUNT=$(ls /data/future_data/dongzheng_data/full_bar_1min_on_tick/*.parquet 2>/dev/null | wc -l)
    echo "已生成文件数: $FILE_COUNT / 8656"
    
    # 计算进度百分比
    PROGRESS=$(echo "scale=2; $FILE_COUNT * 100 / 8656" | bc)
    echo "进度: ${PROGRESS}%"
    
    # 最新日志
    echo ""
    echo "=== 最新处理记录 ==="
    tail -10 on_tick_230_processing.log | grep -E "(处理:|✓|✗)"
    
    # 如果进程数为0，退出监控
    if [ $PROC_COUNT -eq 0 ]; then
        echo ""
        echo "所有进程已完成"
        break
    fi
    
    sleep 30
done
