#!/bin/bash
# run_1min_comparison.sh - 1min数据深度对比流程（使用deep_compare）
# 功能：
#   1. 生成 on_batch 1min 数据（可选）
#   2. 生成 on_tick 1min 数据（可选）
#   3. 深度对比所有18个字段（使用 deep_compare_batch_tick.py）
#
# 使用方法：
#   ./script/run_1min_comparison.sh                    # 完整流程（生成+对比）
#   ./script/run_1min_comparison.sh --skip-batch       # 跳过on_batch生成
#   ./script/run_1min_comparison.sh --skip-tick        # 跳过on_tick生成
#   ./script/run_1min_comparison.sh --skip-generate    # 只对比，不生成
#   ./script/run_1min_comparison.sh --skip-clean       # 增量模式
#   ./script/run_1min_comparison.sh --sample 100       # 只对比100个随机样本

set -e

# ============ 配置参数 ============
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DATA_ROOT="/data/future_data/dongzheng_data"

BATCH_1MIN_DIR="$DATA_ROOT/full_bar_1min"
TICK_1MIN_DIR="$DATA_ROOT/full_bar_1min_on_tick"

# 默认参数
BATCH_THREADS=230
TICK_THREADS=230
COMPARE_THREADS=64
SKIP_BATCH=false
SKIP_TICK=false
SKIP_CLEAN=false
SKIP_GENERATE=false
SAMPLE_SIZE=""
SHOW_DETAILS=false

# ============ 解析命令行参数 ============
while [[ $# -gt 0 ]]; do
    case $1 in
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
        --skip-generate)
            SKIP_GENERATE=true
            SKIP_BATCH=true
            SKIP_TICK=true
            shift
            ;;
        --sample)
            SAMPLE_SIZE="$2"
            shift 2
            ;;
        --show-details)
            SHOW_DETAILS=true
            shift
            ;;
        *)
            echo "未知参数: $1"
            echo "使用方法: $0 [选项]"
            echo ""
            echo "选项:"
            echo "  --batch-threads N       on_batch线程数 (默认:230)"
            echo "  --tick-threads N        on_tick线程数 (默认:230)"
            echo "  --compare-threads N     对比线程数 (默认:64)"
            echo "  --skip-batch           跳过on_batch生成"
            echo "  --skip-tick            跳过on_tick生成"
            echo "  --skip-generate        跳过数据生成，只对比"
            echo "  --skip-clean           增量模式（不删除旧数据）"
            echo "  --sample N             随机抽样N个合约对比"
            echo "  --show-details         显示详细差异信息"
            exit 1
            ;;
    esac
done

# ============ 辅助函数 ============
log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] $*"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2
}

log_separator() {
    echo ""
    echo "========================================"
    echo "$*"
    echo "========================================"
}

# ============ 主流程 ============
cd "$PROJECT_ROOT"

log_separator "1min 数据深度对比流程"
log_info "on_batch 线程数: $BATCH_THREADS"
log_info "on_tick 线程数: $TICK_THREADS"
log_info "对比线程数: $COMPARE_THREADS"
log_info "跳过on_batch: $SKIP_BATCH"
log_info "跳过on_tick: $SKIP_TICK"
log_info "增量模式: $SKIP_CLEAN"
if [ -n "$SAMPLE_SIZE" ]; then
    log_info "抽样模式: $SAMPLE_SIZE 个合约"
fi

START_TOTAL=$(date +%s)

# ============ 步骤1: 编译 Rust 程序 ============
if [ "$SKIP_GENERATE" = false ]; then
    log_separator "步骤1: 编译 Rust 程序"
    if cargo build --release 2>&1 | grep -E "(Finished|error)" | head -5; then
        log_info "✓ Rust 编译成功"
    else
        log_error "Rust 编译失败"
        exit 1
    fi
fi

# ============ 步骤2: on_batch (tick → 1min) ============
if [ "$SKIP_BATCH" = false ]; then
    log_separator "步骤2: on_batch (tick → 1min, ${BATCH_THREADS}线程)"

    if [ "$SKIP_CLEAN" = false ]; then
        log_info "清理旧的 on_batch 1min 数据..."
        rm -rf "$BATCH_1MIN_DIR"
    fi

    START_TIME=$(date +%s)
    ./target/release/parquet_processor_on_batch dongzheng_data 1min "$BATCH_THREADS"
    END_TIME=$(date +%s)
    ELAPSED=$((END_TIME - START_TIME))

    BATCH_COUNT=$(find "$BATCH_1MIN_DIR" -name "*.parquet" 2>/dev/null | wc -l)
    log_info "✓ on_batch 1min 完成，用时: ${ELAPSED}秒，生成 ${BATCH_COUNT} 个合约"
else
    log_info "跳过 on_batch 1min 生成"
fi

# ============ 步骤3: on_tick (tick → 1min) ============
if [ "$SKIP_TICK" = false ]; then
    log_separator "步骤3: on_tick (tick → 1min, ${TICK_THREADS}线程)"

    if [ "$SKIP_CLEAN" = false ]; then
        log_info "清理旧的 on_tick 1min 数据..."
        rm -rf "$TICK_1MIN_DIR"
        mkdir -p "$TICK_1MIN_DIR"
    fi

    START_TIME=$(date +%s)
    ./target/release/parquet_processor_on_tick dongzheng_data 1min "$TICK_THREADS"
    END_TIME=$(date +%s)
    ELAPSED=$((END_TIME - START_TIME))

    TICK_COUNT=$(find "$TICK_1MIN_DIR" -name "*.parquet" 2>/dev/null | wc -l)
    log_info "✓ on_tick 1min 完成，用时: ${ELAPSED}秒，生成 ${TICK_COUNT} 个合约"
else
    log_info "跳过 on_tick 1min 生成"
fi

# ============ 步骤4: 深度对比 1min 数据（18个字段）============
log_separator "步骤4: 深度对比 1min 数据（18个字段）"

# 检查数据是否存在
if [ ! -d "$BATCH_1MIN_DIR" ] || [ -z "$(ls -A "$BATCH_1MIN_DIR" 2>/dev/null)" ]; then
    log_error "错误：on_batch 1min数据不存在: $BATCH_1MIN_DIR"
    exit 1
fi

if [ ! -d "$TICK_1MIN_DIR" ] || [ -z "$(ls -A "$TICK_1MIN_DIR" 2>/dev/null)" ]; then
    log_error "错误：on_tick 1min数据不存在: $TICK_1MIN_DIR"
    exit 1
fi

START_TIME=$(date +%s)

# 构建对比命令
COMPARE_CMD="python3 ./script/deep_compare_batch_tick.py --threads $COMPARE_THREADS"

if [ -n "$SAMPLE_SIZE" ]; then
    COMPARE_CMD="$COMPARE_CMD --sample $SAMPLE_SIZE"
fi

if [ "$SHOW_DETAILS" = true ]; then
    COMPARE_CMD="$COMPARE_CMD --show-details"
fi

log_info "执行命令: $COMPARE_CMD"

# 运行深度对比
$COMPARE_CMD

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

log_info "✓ 深度对比完成，用时: ${ELAPSED}秒"

# ============ 完成统计 ============
END_TOTAL=$(date +%s)
TOTAL_ELAPSED=$((END_TOTAL - START_TOTAL))

log_separator "1min 深度对比流程完成"

MINUTES=$((TOTAL_ELAPSED / 60))
SECONDS=$((TOTAL_ELAPSED % 60))

log_info "总用时: ${MINUTES}分${SECONDS}秒"

echo ""
echo "=========================================="
echo "对比完成！"
echo "=========================================="
echo ""
echo "对比说明："
echo "  - 使用深度对比脚本: deep_compare_batch_tick.py"
echo "  - 对比字段数: 18个完整字段"
echo "  - 包括: OHLC, volume, turnover, open_interest, bid/ask, vwap, log_return 等"
echo "  - null值位置检测: ✓"
echo "  - 数值差异统计: ✓"
echo ""
echo "下一步："
echo "  1. 查看上方的对比统计报告"
if [ "$SHOW_DETAILS" = false ]; then
    echo "  2. 如需查看详细差异，加上 --show-details 参数重新运行"
fi
if [ -z "$SAMPLE_SIZE" ]; then
    echo "  3. 如需快速抽样验证，使用 --sample 100 参数"
fi
echo ""
