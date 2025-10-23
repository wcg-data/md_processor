#!/bin/bash
# process_all_contracts.sh - 批量处理所有合约数据流程
# 功能：tick -> 1min -> 5min -> 生成main_bar_1min和main_bar_5min
# 使用方法：
#   ./script/process_all_contracts.sh          # 删除旧数据并重新生成
#   ./script/process_all_contracts.sh --skip-clean  # 增量处理，跳过已存在文件

set -e  # 遇到错误立即退出

# ==================== 配置参数 ====================
DATA_ROOT="/data/future_data/dongzheng_data"
PROJECT_ROOT="/root/project/md_processor"
THREADS=64  # 并行线程数
SKIP_CLEAN=false  # 是否跳过清理

# ==================== 解析命令行参数 ====================
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-clean)
            SKIP_CLEAN=true
            shift
            ;;
        --threads)
            THREADS="$2"
            shift 2
            ;;
        *)
            echo "未知参数: $1"
            echo "使用方法: $0 [--skip-clean] [--threads N]"
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

# ==================== 计时函数 ====================
start_time=$(date +%s)

# ==================== 步骤0: 清理旧数据（可选） ====================
if [ "$SKIP_CLEAN" = false ]; then
    log_step "步骤0: 清理旧数据"

    for dir in "full_bar_1min" "full_bar_5min" "main_bar_1min" "main_bar_5min"; do
        dir_path="${DATA_ROOT}/${dir}"
        if [ -d "$dir_path" ]; then
            log_info "删除目录: $dir_path"
            rm -rf "$dir_path"
        fi
        log_info "创建目录: $dir_path"
        mkdir -p "$dir_path"
    done
else
    log_step "步骤0: 跳过清理，增量处理模式"

    # 确保输出目录存在
    for dir in "full_bar_1min" "full_bar_5min" "main_bar_1min" "main_bar_5min"; do
        mkdir -p "${DATA_ROOT}/${dir}"
    done
fi

# ==================== 步骤1: 编译 Rust 程序 ====================
log_step "步骤1: 编译 Rust 程序"
cd "$PROJECT_ROOT"

if cargo build --release; then
    log_info "✓ Rust 编译成功"
else
    log_error "✗ Rust 编译失败"
    exit 1
fi

# ==================== 步骤2: tick -> 1min bar ====================
log_step "步骤2: tick -> 1min bar (${THREADS}线程)"
step2_start=$(date +%s)

if ./target/release/parquet_processor_on_batch dongzheng_data 1min $THREADS; then
    step2_end=$(date +%s)
    step2_duration=$((step2_end - step2_start))
    log_info "✓ tick -> 1min 处理完成，用时: ${step2_duration}秒"
else
    log_error "✗ tick -> 1min 处理失败"
    exit 1
fi

# 统计生成的文件数
bar1min_count=$(find "${DATA_ROOT}/full_bar_1min" -name "*.parquet" | wc -l)
log_info "生成1min bar文件数: $bar1min_count"

# ==================== 步骤3: 1min -> 5min bar ====================
log_step "步骤3: 1min -> 5min bar (${THREADS}线程)"
step3_start=$(date +%s)

if ./target/release/parquet_processor_on_batch dongzheng_data 5min $THREADS; then
    step3_end=$(date +%s)
    step3_duration=$((step3_end - step3_start))
    log_info "✓ 1min -> 5min 处理完成，用时: ${step3_duration}秒"
else
    log_error "✗ 1min -> 5min 处理失败"
    exit 1
fi

# 统计生成的文件数
bar5min_count=$(find "${DATA_ROOT}/full_bar_5min" -name "*.parquet" | wc -l)
log_info "生成5min bar文件数: $bar5min_count"

# ==================== 步骤4: 生成主力合约数据 ====================
log_step "步骤4: 生成主力合约数据 (main_bar_1min, main_bar_5min)"
step4_start=$(date +%s)

if python3 /root/project/md_toolkit/processors/gen_main_bars.py --freqs 1min 5min; then
    step4_end=$(date +%s)
    step4_duration=$((step4_end - step4_start))
    log_info "✓ 主力合约数据生成完成，用时: ${step4_duration}秒"
else
    log_error "✗ 主力合约数据生成失败"
    exit 1
fi

# 统计生成的主力合约文件数
main1min_count=$(find "${DATA_ROOT}/main_bar_1min" -name "*.parquet" 2>/dev/null | wc -l)
main5min_count=$(find "${DATA_ROOT}/main_bar_5min" -name "*.parquet" 2>/dev/null | wc -l)
log_info "生成main_bar_1min文件数: $main1min_count"
log_info "生成main_bar_5min文件数: $main5min_count"

# ==================== 完成统计 ====================
end_time=$(date +%s)
total_duration=$((end_time - start_time))
hours=$((total_duration / 3600))
minutes=$(((total_duration % 3600) / 60))
seconds=$((total_duration % 60))

log_step "处理完成"
log_info "总用时: ${hours}小时${minutes}分钟${seconds}秒"
log_info "数据输出目录: $DATA_ROOT"
log_info "  - full_bar_1min/   : $bar1min_count 个文件"
log_info "  - full_bar_5min/   : $bar5min_count 个文件"
log_info "  - main_bar_1min/   : $main1min_count 个文件"
log_info "  - main_bar_5min/   : $main5min_count 个文件"

echo ""
echo "=========================================="
echo "所有数据处理流程执行完毕！"
echo "=========================================="
