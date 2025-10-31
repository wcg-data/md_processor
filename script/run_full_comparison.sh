#!/bin/bash
# run_full_comparison.sh - 完整执行 on_batch、on_tick 和比较
# 功能：依次运行 on_batch、on_tick 处理，然后比较结果
# 使用方法：
#   ./script/run_full_comparison.sh                  # 使用默认线程数（batch:230, tick:230）
#   ./script/run_full_comparison.sh --skip-batch     # 跳过 on_batch（假设已处理）
#   ./script/run_full_comparison.sh --skip-tick      # 跳过 on_tick（假设已处理）
#   ./script/run_full_comparison.sh --skip-clean     # 增量模式（不删除旧数据）

set -e  # 遇到错误立即退出

# ==================== 配置参数 ====================
PROJECT_ROOT="/root/project/md_processor"
BATCH_THREADS=230  # on_batch 线程数
TICK_THREADS=230   # on_tick 线程数
COMPARE_THREADS=64 # 比较线程数

SKIP_BATCH=false
SKIP_TICK=false
SKIP_CLEAN=false

# ==================== 解析命令行参数 ====================
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-batch)
            SKIP_BATCH=true
            shift
            ;;
        --skip-tick)
            SKIP_TICK=true
            shift
            ;;
        --skip-clean)
            SKIP_CLEAN=true
            shift
            ;;
        --batch-threads)
            BATCH_THREADS="$2"
            shift 2
            ;;
        --tick-threads)
            TICK_THREADS="$2"
            shift 2
            ;;
        --compare-threads)
            COMPARE_THREADS="$2"
            shift 2
            ;;
        *)
            echo "未知参数: $1"
            echo "使用方法: $0 [--skip-batch] [--skip-tick] [--skip-clean] [--batch-threads N] [--tick-threads N] [--compare-threads N]"
            exit 1
            ;;
    esac
done

# ==================== 日志函数 ====================
log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] $*"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2
}

log_step() {
    echo ""
    echo "========================================"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    echo "========================================"
}

# ==================== 开始处理 ====================
start_time=$(date +%s)

log_step "完整对比流程开始"
log_info "on_batch 线程数: $BATCH_THREADS"
log_info "on_tick 线程数: $TICK_THREADS"
log_info "比较线程数: $COMPARE_THREADS"
log_info "增量模式: $SKIP_CLEAN"

cd "$PROJECT_ROOT"

# ==================== 步骤1: on_batch 处理 ====================
if [ "$SKIP_BATCH" = false ]; then
    log_step "步骤1: 处理 on_batch (full_bar_1min)"
    step1_start=$(date +%s)

    if [ "$SKIP_CLEAN" = true ]; then
        ./script/process_all_contracts.sh --skip-clean --threads "$BATCH_THREADS"
    else
        ./script/process_all_contracts.sh --threads "$BATCH_THREADS"
    fi

    step1_end=$(date +%s)
    step1_duration=$((step1_end - step1_start))
    log_info "✓ on_batch 处理完成，用时: ${step1_duration}秒"
else
    log_step "步骤1: 跳过 on_batch 处理"
fi

# ==================== 步骤2: on_tick 处理 ====================
if [ "$SKIP_TICK" = false ]; then
    log_step "步骤2: 处理 on_tick (full_bar_1min_on_tick)"
    step2_start=$(date +%s)

    if [ "$SKIP_CLEAN" = true ]; then
        ./script/process_all_on_tick.sh --skip-clean --threads "$TICK_THREADS"
    else
        ./script/process_all_on_tick.sh --threads "$TICK_THREADS"
    fi

    step2_end=$(date +%s)
    step2_duration=$((step2_end - step2_start))
    log_info "✓ on_tick 处理完成，用时: ${step2_duration}秒"
else
    log_step "步骤2: 跳过 on_tick 处理"
fi

# ==================== 步骤3: 比较结果 ====================
log_step "步骤3: 比较 on_batch vs on_tick"
step3_start=$(date +%s)

python3 ./script/compare_batch_vs_tick.py --threads "$COMPARE_THREADS" --save-details

step3_end=$(date +%s)
step3_duration=$((step3_end - step3_start))
log_info "✓ 比较完成，用时: ${step3_duration}秒"

# ==================== 完成统计 ====================
end_time=$(date +%s)
total_duration=$((end_time - start_time))
hours=$((total_duration / 3600))
minutes=$(((total_duration % 3600) / 60))
seconds=$((total_duration % 60))

log_step "完整对比流程完成"
log_info "总用时: ${hours}小时${minutes}分钟${seconds}秒"

if [ "$SKIP_BATCH" = false ]; then
    log_info "  on_batch 用时: ${step1_duration}秒"
fi
if [ "$SKIP_TICK" = false ]; then
    log_info "  on_tick 用时: ${step2_duration}秒"
fi
log_info "  比较用时: ${step3_duration}秒"

echo ""
echo "=========================================="
echo "所有流程执行完毕！"
echo "=========================================="
echo ""
echo "下一步："
echo "  1. 查看比较报告: cat comparison_report.csv"
echo "  2. 查看 on_tick 日志: ls -lh logs/on_tick/"
echo ""
