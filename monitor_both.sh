#!/bin/bash
while true; do
    clear
    echo "╔══════════════════════════════════════════════════════════════════════════╗"
    echo "║  双处理器实时监控 (230线程 vs 230线程)                                  ║"
    echo "╚══════════════════════════════════════════════════════════════════════════╝"
    echo ""
    date "+当前时间: %Y-%m-%d %H:%M:%S"
    echo ""
    
    # on_batch 进度
    batch_count=$(ls /data/future_data/dongzheng_data/full_bar_1min/*.parquet 2>/dev/null | wc -l)
    batch_pct=$(echo "scale=1; $batch_count*100/8901" | bc)
    echo "┌─ on_batch (向量化) ──────────────────────────────────────┐"
    echo "│  已处理: $batch_count / 8,901 文件 ($batch_pct%)"
    echo "│  输出: /data/future_data/dongzheng_data/full_bar_1min/"
    batch_running=$(ps aux | grep "parquet_processor_on_batch.*1min.*230" | grep -v grep | wc -l)
    if [ $batch_running -gt 0 ]; then
        echo "│  状态: ✓ 运行中"
        batch_cpu=$(ps aux | grep "parquet_processor_on_batch.*1min.*230" | grep -v grep | awk '{print $3}' | head -1)
        echo "│  CPU: ${batch_cpu}%"
    else
        echo "│  状态: ✓ 已完成"
    fi
    echo "└──────────────────────────────────────────────────────────┘"
    echo ""
    
    # on_tick 进度
    tick_count=$(ls /data/future_data/dongzheng_data/full_bar_1min_on_tick/*.parquet 2>/dev/null | wc -l)
    tick_pct=$(echo "scale=1; $tick_count*100/8901" | bc)
    echo "┌─ on_tick (流式) ─────────────────────────────────────────┐"
    echo "│  已处理: $tick_count / 8,901 文件 ($tick_pct%)"
    echo "│  输出: /data/future_data/dongzheng_data/full_bar_1min_on_tick/"
    tick_running=$(ps aux | grep "parquet_processor_on_tick.*1min.*230" | grep -v grep | wc -l)
    if [ $tick_running -gt 0 ]; then
        echo "│  状态: ✓ 运行中"
        tick_cpu=$(ps aux | grep "parquet_processor_on_tick.*1min.*230" | grep -v grep | awk '{print $3}' | head -1)
        echo "│  CPU: ${tick_cpu}%"
    else
        echo "│  状态: ✓ 已完成"
    fi
    echo "└──────────────────────────────────────────────────────────┘"
    echo ""
    
    # 系统资源
    echo "┌─ 系统资源 ───────────────────────────────────────────────┐"
    total_cpu=$(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1)
    echo "│  总CPU使用: ${total_cpu}%"
    echo "│  总核心数: 256"
    echo "└──────────────────────────────────────────────────────────┘"
    echo ""
    
    # 检查是否都完成
    if [ $batch_running -eq 0 ] && [ $tick_running -eq 0 ]; then
        echo "╔══════════════════════════════════════════════════════════════════════════╗"
        echo "║  ✓ 两个处理器都已完成！                                                  ║"
        echo "╚══════════════════════════════════════════════════════════════════════════╝"
        break
    fi
    
    sleep 10
done
