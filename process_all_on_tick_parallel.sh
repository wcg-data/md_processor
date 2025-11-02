#!/bin/bash
# process_all_on_tick_parallel.sh - 并行处理所有合约（on_tick模式）

set -e

# 配置参数
TICK_DIR="/data/future_data/dongzheng_data/full_tick"
OUTPUT_DIR="/data/future_data/dongzheng_data/full_bar_1min_on_tick"
THREADS=${1:-230}  # 默认230线程

echo "╔══════════════════════════════════════════════════════════════════════════╗"
echo "║  on_tick 全量并行处理                                                    ║"
echo "╚══════════════════════════════════════════════════════════════════════════╝"
echo ""

# 创建输出目录
mkdir -p "$OUTPUT_DIR"

# 统计输入文件
TOTAL_FILES=$(ls "$TICK_DIR"/*.parquet 2>/dev/null | wc -l)
echo "输入目录: $TICK_DIR"
echo "输出目录: $OUTPUT_DIR"
echo "总文件数: $TOTAL_FILES"
echo "并行线程: $THREADS"
echo ""

# 确认
read -p "确认开始处理？(y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "已取消"
    exit 1
fi

echo ""
echo "开始处理..."
echo "开始时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# 使用GNU parallel并行处理
START_TIME=$(date +%s)

ls "$TICK_DIR"/*.parquet | sed 's|.*/||; s|\.parquet||' | \
parallel -j "$THREADS" --bar --eta \
    "./target/release/parquet_processor_on_tick dongzheng_data 1min {} > /dev/null 2>&1"

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo ""
echo "╔══════════════════════════════════════════════════════════════════════════╗"
echo "║  处理完成                                                                ║"
echo "╚══════════════════════════════════════════════════════════════════════════╝"
echo ""
echo "结束时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "耗时: $((ELAPSED / 60))分$((ELAPSED % 60))秒"
echo ""

# 统计输出
OUTPUT_COUNT=$(ls "$OUTPUT_DIR"/*.parquet 2>/dev/null | wc -l)
echo "输出文件数: $OUTPUT_COUNT / $TOTAL_FILES"

if [ "$OUTPUT_COUNT" -eq "$TOTAL_FILES" ]; then
    echo "✅ 所有文件处理成功"
else
    echo "⚠️  有 $((TOTAL_FILES - OUTPUT_COUNT)) 个文件处理失败"
fi

echo ""
echo "下一步: 运行 compare_batch_tick_all.py 对比结果"
