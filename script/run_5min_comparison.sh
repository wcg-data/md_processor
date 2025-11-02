#!/bin/bash
# run_5min_comparison.sh - 完整执行 5min 数据生成和对比流程
# 功能：
#   1. 从1min聚合生成5min数据（on_batch方式）
#   2. 从1min聚合生成5min数据（on_tick方式）
#   3. 深度对比两种方式的5min结果
#
# 前置条件：1min数据已生成（full_bar_1min 和 full_bar_1min_on_tick）
#
# 使用方法：
#   ./script/run_5min_comparison.sh                    # 完整流程
#   ./script/run_5min_comparison.sh --skip-batch       # 跳过on_batch
#   ./script/run_5min_comparison.sh --skip-tick        # 跳过on_tick
#   ./script/run_5min_comparison.sh --skip-clean       # 增量模式
#   ./script/run_5min_comparison.sh --threads 64       # 自定义线程数

set -e

# ============ 配置参数 ============
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DATA_ROOT="/data/future_data/dongzheng_data"

BATCH_1MIN_DIR="$DATA_ROOT/full_bar_1min"
TICK_1MIN_DIR="$DATA_ROOT/full_bar_1min_on_tick"
BATCH_5MIN_DIR="$DATA_ROOT/full_bar_5min"
TICK_5MIN_DIR="$DATA_ROOT/full_bar_5min_on_tick"

# 默认参数
THREADS=230
SKIP_BATCH=false
SKIP_TICK=false
SKIP_CLEAN=false

# ============ 解析命令行参数 ============
while [[ $# -gt 0 ]]; do
    case $1 in
        --threads)
            THREADS="$2"
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
        *)
            echo "未知参数: $1"
            echo "使用方法: $0 [--threads N] [--skip-batch] [--skip-tick] [--skip-clean]"
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

log_separator "5min 数据生成和对比流程"
log_info "线程数: $THREADS"
log_info "跳过on_batch: $SKIP_BATCH"
log_info "跳过on_tick: $SKIP_TICK"
log_info "增量模式: $SKIP_CLEAN"

# 检查前置条件
if [ ! -d "$BATCH_1MIN_DIR" ] || [ -z "$(ls -A "$BATCH_1MIN_DIR" 2>/dev/null)" ]; then
    log_error "错误：on_batch 1min数据不存在: $BATCH_1MIN_DIR"
    log_error "请先运行: ./script/run_full_comparison.sh"
    exit 1
fi

if [ ! -d "$TICK_1MIN_DIR" ] || [ -z "$(ls -A "$TICK_1MIN_DIR" 2>/dev/null)" ]; then
    log_error "错误：on_tick 1min数据不存在: $TICK_1MIN_DIR"
    log_error "请先运行: ./script/run_full_comparison.sh"
    exit 1
fi

log_info "✓ 前置条件检查通过（1min数据已存在）"

# ============ 步骤1: 编译 Rust 程序 ============
log_separator "步骤1: 编译 Rust 程序"
if cargo build --release 2>&1 | grep -E "(Finished|error)" | head -5; then
    log_info "✓ Rust 编译成功"
else
    log_error "Rust 编译失败"
    exit 1
fi

# ============ 步骤2: on_batch (1min → 5min) ============
if [ "$SKIP_BATCH" = false ]; then
    log_separator "步骤2: on_batch (1min → 5min, ${THREADS}线程)"

    if [ "$SKIP_CLEAN" = false ]; then
        log_info "清理旧的 on_batch 5min 数据..."
        rm -rf "$BATCH_5MIN_DIR"
    fi

    START_TIME=$(date +%s)
    ./target/release/parquet_processor_on_batch dongzheng_data 5min "$THREADS"
    END_TIME=$(date +%s)
    ELAPSED=$((END_TIME - START_TIME))

    BATCH_COUNT=$(find "$BATCH_5MIN_DIR" -name "*.parquet" 2>/dev/null | wc -l)
    log_info "✓ on_batch 5min 完成，用时: ${ELAPSED}秒，生成 ${BATCH_COUNT} 个合约"
else
    log_info "跳过 on_batch 5min 生成"
fi

# ============ 步骤3: on_tick (1min → 5min) ============
if [ "$SKIP_TICK" = false ]; then
    log_separator "步骤3: on_tick (1min → 5min, ${THREADS}线程)"

    if [ "$SKIP_CLEAN" = false ]; then
        log_info "清理旧的 on_tick 5min 数据..."
        rm -rf "$TICK_5MIN_DIR"
        mkdir -p "$TICK_5MIN_DIR"
    fi

    START_TIME=$(date +%s)

    # 使用 parquet_processor_on_tick 的 5min 模式（从 1min 聚合到 5min）
    ./target/release/parquet_processor_on_tick dongzheng_data 5min "$THREADS"

    END_TIME=$(date +%s)
    ELAPSED=$((END_TIME - START_TIME))

    TICK_COUNT=$(find "$TICK_5MIN_DIR" -name "*.parquet" 2>/dev/null | wc -l)
    log_info "✓ on_tick 5min 完成，用时: ${ELAPSED}秒，生成 ${TICK_COUNT} 个合约"
else
    log_info "跳过 on_tick 5min 生成"
fi

# ============ 步骤4: 深度对比 5min 数据 ============
log_separator "步骤4: 深度对比 5min 数据"

START_TIME=$(date +%s)

python3 << 'PYTHON_SCRIPT'
import polars as pl
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import sys

DATA_ROOT = Path("/data/future_data/dongzheng_data")
BATCH_DIR = DATA_ROOT / "full_bar_5min"
TICK_DIR = DATA_ROOT / "full_bar_5min_on_tick"

def compare_contract(batch_file):
    """比较单个合约的5min数据"""
    contract = batch_file.stem.replace("_5min", "")
    tick_file = TICK_DIR / f"{contract}_5min.parquet"

    if not tick_file.exists():
        return {
            'contract': contract,
            'status': 'missing_tick',
            'batch_count': 0,
            'tick_count': 0,
            'bars_match': False
        }

    try:
        batch_df = pl.read_parquet(batch_file)
        tick_df = pl.read_parquet(tick_file)

        batch_count = len(batch_df)
        tick_count = len(tick_df)
        bars_match = (batch_count == tick_count)

        # 如果数量一致，进一步比较数据值
        times_match = volume_match = turnover_match = oi_match = False

        if bars_match:
            # 按时间排序
            batch_df = batch_df.sort("trade_time")
            tick_df = tick_df.sort("trade_time")

            # 比较时间戳
            times_match = batch_df["trade_time"].equals(tick_df["trade_time"])

            # 比较数值字段
            volume_match = batch_df["volume"].equals(tick_df["volume"])
            turnover_match = batch_df["turnover"].equals(tick_df["turnover"])
            oi_match = batch_df["open_interest"].equals(tick_df["open_interest"])

        return {
            'contract': contract,
            'comd': batch_df["comd"][0] if len(batch_df) > 0 else "",
            'status': 'ok',
            'batch_count': batch_count,
            'tick_count': tick_count,
            'bars_match': bars_match,
            'bar_diff': batch_count - tick_count,
            'times_match': times_match,
            'volume_match': volume_match,
            'turnover_match': turnover_match,
            'open_interest_match': oi_match
        }

    except Exception as e:
        return {
            'contract': contract,
            'status': f'error: {str(e)}',
            'batch_count': 0,
            'tick_count': 0,
            'bars_match': False
        }

# 扫描所有batch文件
batch_files = list(BATCH_DIR.glob("*_5min.parquet"))
total = len(batch_files)
print(f"\n发现 {total} 个合约需要对比")

# 并行对比
results = []
completed = 0

with ProcessPoolExecutor(max_workers=64) as executor:
    futures = {executor.submit(compare_contract, f): f for f in batch_files}

    for future in as_completed(futures):
        result = future.result()
        results.append(result)
        completed += 1

        if completed % 100 == 0 or completed == total:
            print(f"\r进度: {completed}/{total}", end='', flush=True)

print(f"\n完成: {completed}/{total} 个合约")

# 统计结果
bars_match_count = sum(1 for r in results if r.get('bars_match', False))
bars_mismatch_count = total - bars_match_count

print(f"\n[步骤1] Bar数量对比")
print(f"  ✅ 数量一致: {bars_match_count} ({bars_match_count*100/total:.1f}%)")
print(f"  ❌ 数量不一致: {bars_mismatch_count} ({bars_mismatch_count*100/total:.1f}%)")

if bars_mismatch_count > 0:
    print(f"\n  差异最大的前10个合约:")
    mismatches = [r for r in results if not r.get('bars_match', False)]
    mismatches.sort(key=lambda x: abs(x.get('bar_diff', 0)), reverse=True)
    for r in mismatches[:10]:
        print(f"    {r['contract']:10s}: batch={r['batch_count']:6,}, tick={r['tick_count']:6,}, 差异={r['bar_diff']:6,}")

# 数据值一致性
if bars_match_count > 0:
    matches = [r for r in results if r.get('bars_match', False)]
    times_match = sum(1 for r in matches if r.get('times_match', False))
    volume_match = sum(1 for r in matches if r.get('volume_match', False))
    turnover_match = sum(1 for r in matches if r.get('turnover_match', False))
    oi_match = sum(1 for r in matches if r.get('open_interest_match', False))

    all_match = sum(1 for r in matches if
        r.get('times_match', False) and
        r.get('volume_match', False) and
        r.get('turnover_match', False) and
        r.get('open_interest_match', False))

    print(f"\n[步骤2] 数据值一致性（{bars_match_count} 个数量一致的合约）")
    print(f"  时间戳:   ✅ {times_match} ({times_match*100/bars_match_count:.1f}%)")
    print(f"  成交量:   ✅ {volume_match} ({volume_match*100/bars_match_count:.1f}%)")
    print(f"  成交额:   ✅ {turnover_match} ({turnover_match*100/bars_match_count:.1f}%)")
    print(f"  持仓量:   ✅ {oi_match} ({oi_match*100/bars_match_count:.1f}%)")

    print(f"\n[步骤3] 完全一致性")
    print(f"  🎉 完全一致: {all_match} / {total} ({all_match*100/total:.1f}%)")

# 保存详细结果
import csv
with open('comparison_report_5min.csv', 'w', newline='') as f:
    if results:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

print(f"\n保存详细结果到: comparison_report_5min.csv")

# 返回状态码
sys.exit(0 if bars_mismatch_count == 0 else 1)

PYTHON_SCRIPT

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

log_info "✓ 比较完成，用时: ${ELAPSED}秒"

# ============ 完成 ============
log_separator "5min 完整对比流程完成"
echo "  1. 查看比较报告: cat comparison_report_5min.csv"
echo "  2. 查看on_batch数据: ls -lh $BATCH_5MIN_DIR | head"
echo "  3. 查看on_tick数据: ls -lh $TICK_5MIN_DIR | head"
