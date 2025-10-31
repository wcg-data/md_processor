#!/bin/bash
# process_all_on_tick.sh - 并行处理所有合约的on_tick数据
# 功能：使用 parquet_processor_on_tick 并行处理所有合约
# 使用方法：
#   ./script/process_all_on_tick.sh                # 删除旧数据并重新生成（230线程）
#   ./script/process_all_on_tick.sh --skip-clean   # 增量处理，跳过已存在文件
#   ./script/process_all_on_tick.sh --threads 64   # 自定义线程数

set -e  # 遇到错误立即退出

# ==================== 配置参数 ====================
DATA_ROOT="/data/future_data/dongzheng_data"
PROJECT_ROOT="/root/project/md_processor"
THREADS=230  # 并行线程数
SKIP_CLEAN=false  # 是否跳过清理
OUTPUT_DIR="${DATA_ROOT}/full_bar_1min_on_tick"

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

    if [ -d "$OUTPUT_DIR" ]; then
        log_info "删除目录: $OUTPUT_DIR"
        rm -rf "$OUTPUT_DIR"
    fi
    log_info "创建目录: $OUTPUT_DIR"
    mkdir -p "$OUTPUT_DIR"
else
    log_step "步骤0: 跳过清理，增量处理模式"
    mkdir -p "$OUTPUT_DIR"
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

# ==================== 步骤2: 获取所有合约列表 ====================
log_step "步骤2: 扫描合约列表"

TICK_DIR="${DATA_ROOT}/full_tick"
if [ ! -d "$TICK_DIR" ]; then
    log_error "tick数据目录不存在: $TICK_DIR"
    exit 1
fi

# 生成合约列表（去掉 .parquet 后缀）
CONTRACT_LIST=$(mktemp)
find "$TICK_DIR" -name "*.parquet" -type f | \
    sed 's|.*/||' | \
    sed 's|\.parquet$||' | \
    sort > "$CONTRACT_LIST"

total_contracts=$(wc -l < "$CONTRACT_LIST")
log_info "找到 $total_contracts 个合约"

if [ "$total_contracts" -eq 0 ]; then
    log_error "未找到任何合约文件"
    exit 1
fi

# ==================== 步骤3: 并行处理所有合约 ====================
log_step "步骤3: 并行处理 on_tick (${THREADS}线程)"
step3_start=$(date +%s)

# 创建日志目录
LOG_DIR="${PROJECT_ROOT}/logs/on_tick"
mkdir -p "$LOG_DIR"

# 清理旧日志
rm -f "${LOG_DIR}"/*.log

log_info "开始并行处理..."
log_info "日志目录: $LOG_DIR"

# 定义处理函数
process_contract() {
    local contract="$1"
    local project_root="$2"
    local output_file="${DATA_ROOT}/full_bar_1min_on_tick/${contract}_1min.parquet"

    # 如果是增量模式且文件已存在，跳过
    if [ "$SKIP_CLEAN" = true ] && [ -f "$output_file" ]; then
        echo "[SKIP] $contract (已存在)"
        return 0
    fi

    # 执行处理
    if "${project_root}/target/release/parquet_processor_on_tick" dongzheng_data 1min "$contract" > /dev/null 2>&1; then
        echo "[OK] $contract"
        return 0
    else
        echo "[FAIL] $contract"
        return 1
    fi
}

export -f process_contract
export DATA_ROOT
export PROJECT_ROOT
export SKIP_CLEAN

# 使用 GNU parallel 并行处理
if command -v parallel &> /dev/null; then
    log_info "使用 GNU parallel 进行并行处理"

    cat "$CONTRACT_LIST" | \
        parallel --jobs "$THREADS" \
                 --bar \
                 --joblog "${LOG_DIR}/parallel.log" \
                 process_contract {} "$PROJECT_ROOT"
else
    # 备用方案：使用 xargs（功能较少但更通用）
    log_info "使用 xargs 进行并行处理（建议安装 GNU parallel 以获得更好体验）"

    cat "$CONTRACT_LIST" | \
        xargs -P "$THREADS" -I {} bash -c 'process_contract "$@"' _ {} "$PROJECT_ROOT"
fi

step3_end=$(date +%s)
step3_duration=$((step3_end - step3_start))

# ==================== 统计结果 ====================
log_step "统计结果"

# 统计生成的文件数
generated_count=$(find "$OUTPUT_DIR" -name "*.parquet" -type f | wc -l)
log_info "成功生成: $generated_count 个文件"

# 计算成功率
success_rate=$(awk "BEGIN {printf \"%.2f\", ($generated_count / $total_contracts) * 100}")
log_info "成功率: ${success_rate}%"

if [ "$generated_count" -lt "$total_contracts" ]; then
    failed_count=$((total_contracts - generated_count))
    log_error "失败: $failed_count 个合约"

    # 找出失败的合约
    log_info "查找失败的合约..."
    FAILED_LIST=$(mktemp)

    while read -r contract; do
        output_file="${OUTPUT_DIR}/${contract}_1min.parquet"
        if [ ! -f "$output_file" ]; then
            echo "$contract" >> "$FAILED_LIST"
        fi
    done < "$CONTRACT_LIST"

    failed_actual=$(wc -l < "$FAILED_LIST")
    if [ "$failed_actual" -gt 0 ]; then
        log_info "失败的合约列表已保存到: ${LOG_DIR}/failed_contracts.txt"
        cp "$FAILED_LIST" "${LOG_DIR}/failed_contracts.txt"

        # 显示前10个失败的合约
        log_info "前10个失败的合约:"
        head -10 "$FAILED_LIST"
        if [ "$failed_actual" -gt 10 ]; then
            log_info "... 还有 $((failed_actual - 10)) 个"
        fi
    fi
    rm -f "$FAILED_LIST"
fi

# 清理临时文件
rm -f "$CONTRACT_LIST"

# ==================== 完成统计 ====================
end_time=$(date +%s)
total_duration=$((end_time - start_time))
hours=$((total_duration / 3600))
minutes=$(((total_duration % 3600) / 60))
seconds=$((total_duration % 60))

log_step "处理完成"
log_info "总用时: ${hours}小时${minutes}分钟${seconds}秒"
log_info "处理用时: ${step3_duration}秒"
log_info "数据输出目录: $OUTPUT_DIR"
log_info "成功生成: $generated_count / $total_contracts 个文件"

echo ""
echo "=========================================="
echo "on_tick 数据处理完毕！"
echo "=========================================="
