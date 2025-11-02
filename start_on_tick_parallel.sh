#!/bin/bash
# 后台并行处理on_tick

THREADS=230
OUTPUT_DIR="/data/future_data/dongzheng_data/full_bar_1min_on_tick"

echo "╔══════════════════════════════════════════════════════════════════════════╗"
echo "║  启动 on_tick 并行处理 (230线程)                                         ║"
echo "╚══════════════════════════════════════════════════════════════════════════╝"
echo ""
echo "输入: 8,901 个tick文件"
echo "输出: $OUTPUT_DIR"
echo "线程: $THREADS"
echo "预计时间: 约40-60分钟（on_tick比on_batch慢）"
echo ""

# 创建输出目录
mkdir -p "$OUTPUT_DIR"

# 后台启动
echo "启动中..."
nohup bash -c '
START_TIME=$(date +%s)
ls /data/future_data/dongzheng_data/full_tick/*.parquet | \
sed "s|.*/||; s|\.parquet||" | \
xargs -P 230 -I {} sh -c "./target/release/parquet_processor_on_tick dongzheng_data 1min {} > /dev/null 2>&1"
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
echo "处理完成，耗时: $((ELAPSED / 60))分$((ELAPSED % 60))秒" > /root/project/md_processor/on_tick_done.log
' > /root/project/md_processor/on_tick_processing.log 2>&1 &

PROC_PID=$!
echo "✓ 进程已启动，PID: $PROC_PID"
echo ""
echo "监控命令:"
echo "  watch -n 5 'ls $OUTPUT_DIR/*.parquet | wc -l'"
echo "  tail -f /root/project/md_processor/on_tick_processing.log"
