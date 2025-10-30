#!/bin/bash
# monitor_on_tick.sh - 实时监控on_tick处理进度

TOTAL=8901

while true; do
    clear
    echo "╔══════════════════════════════════════════════════════════════════════════╗"
    echo "║  on_tick 处理进度实时监控 (修复后版本)                                   ║"
    echo "╚══════════════════════════════════════════════════════════════════════════╝"
    echo ""
    date "+当前时间: %Y-%m-%d %H:%M:%S"
    echo ""

    # on_tick 进度
    tick_count=$(ls /data/future_data/dongzheng_data/full_bar_1min_on_tick/*.parquet 2>/dev/null | wc -l)
    tick_pct=$(echo "scale=2; $tick_count*100/$TOTAL" | bc)

    echo "┌─ on_tick (流式) ─────────────────────────────────────────┐"
    echo "│  已处理: $tick_count / $TOTAL 文件 ($tick_pct%)"
    echo "│  输出: /data/future_data/dongzheng_data/full_bar_1min_on_tick/"

    # 检查进程状态
    running=$(ps aux | grep "parquet_processor_on_tick" | grep -v grep | wc -l)
    if [ $running -gt 0 ]; then
        echo "│  状态: ✓ 运行中 ($running 个进程)"
        echo "│  进度: ["
        # 进度条
        filled=$((tick_count * 50 / TOTAL))
        for ((i=0; i<filled; i++)); do echo -n "█"; done
        for ((i=filled; i<50; i++)); do echo -n "░"; done
        echo "]"
    else
        if [ $tick_count -eq $TOTAL ]; then
            echo "│  状态: ✅ 已完成"
        else
            echo "│  状态: ⚠️  进程已停止（可能遇到错误）"
        fi
    fi
    echo "└──────────────────────────────────────────────────────────┘"
    echo ""

    # on_batch 对比
    batch_count=$(ls /data/future_data/dongzheng_data/full_bar_1min/*.parquet 2>/dev/null | wc -l)
    echo "┌─ on_batch (参考) ────────────────────────────────────────┐"
    echo "│  已完成: $batch_count / $TOTAL 文件"
    echo "└──────────────────────────────────────────────────────────┘"
    echo ""

    # 预估剩余时间
    if [ $tick_count -gt 0 ] && [ $running -gt 0 ]; then
        if [ -f /root/project/md_processor/on_tick_start_time ]; then
            start_time=$(cat /root/project/md_processor/on_tick_start_time)
        else
            date +%s > /root/project/md_processor/on_tick_start_time
            start_time=$(date +%s)
        fi

        current_time=$(date +%s)
        elapsed=$((current_time - start_time))

        if [ $elapsed -gt 30 ]; then  # 至少运行30秒后才预估
            speed=$(echo "scale=2; $tick_count / $elapsed" | bc)
            remaining=$((TOTAL - tick_count))
            eta_seconds=$(echo "scale=0; $remaining / $speed" | bc)
            eta_minutes=$((eta_seconds / 60))

            echo "统计信息:"
            echo "  已运行: $((elapsed / 60))分$((elapsed % 60))秒"
            echo "  处理速度: $(echo "scale=1; $speed * 60" | bc) 文件/分钟"
            echo "  预计剩余: ${eta_minutes}分钟"
        fi
    fi

    echo ""
    echo "按 Ctrl+C 退出监控"
    echo ""

    # 检查是否完成
    if [ $running -eq 0 ] && [ $tick_count -eq $TOTAL ]; then
        echo "╔══════════════════════════════════════════════════════════════════════════╗"
        echo "║  ✅ 处理完成！                                                            ║"
        echo "╚══════════════════════════════════════════════════════════════════════════╝"
        rm -f /root/project/md_processor/on_tick_start_time
        break
    fi

    sleep 10
done
