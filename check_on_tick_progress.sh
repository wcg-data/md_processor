#!/bin/bash
# 每30秒检查一次进度

echo "=== 开始监控 on_tick 处理进度 ==="
echo "开始时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

LAST_COUNT=0
while true; do
    # 进程数
    PROC_COUNT=$(ps aux | grep "parquet_processor_on_tick" | grep -v grep | wc -l)
    
    # 已生成文件数
    FILE_COUNT=$(ls /data/future_data/dongzheng_data/full_bar_1min_on_tick/*.parquet 2>/dev/null | wc -l)
    
    # 计算进度百分比
    PROGRESS=$(echo "scale=2; $FILE_COUNT * 100 / 8656" | bc)
    
    # 计算速度（文件/分钟）
    if [ $LAST_COUNT -gt 0 ]; then
        SPEED=$(echo "scale=0; ($FILE_COUNT - $LAST_COUNT) * 2" | bc)
        echo "[$(date '+%H:%M:%S')] 进程: $PROC_COUNT | 文件: $FILE_COUNT/8656 ($PROGRESS%) | 速度: ${SPEED}文件/分钟"
    else
        echo "[$(date '+%H:%M:%S')] 进程: $PROC_COUNT | 文件: $FILE_COUNT/8656 ($PROGRESS%)"
    fi
    
    LAST_COUNT=$FILE_COUNT
    
    # 如果进程数为0，退出
    if [ $PROC_COUNT -eq 0 ]; then
        echo ""
        echo "所有进程已完成！"
        echo "结束时间: $(date '+%Y-%m-%d %H:%M:%S')"
        echo "最终文件数: $FILE_COUNT / 8656"
        break
    fi
    
    sleep 30
done
