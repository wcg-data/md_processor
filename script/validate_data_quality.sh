#!/bin/bash
# validate_data_quality.sh - 数据质量综合检查脚本
# 功能：快速检查生成的bar数据质量
# 使用方法：
#   ./script/validate_data_quality.sh              # 检查所有数据
#   ./script/validate_data_quality.sh --quick      # 快速检查（仅统计）
#   ./script/validate_data_quality.sh --detailed   # 详细检查（含主力合约验证）

set -e

# ==================== 配置参数 ====================
DATA_ROOT="/data/future_data/dongzheng_data"
QUICK_MODE=false
DETAILED_MODE=false

# ==================== 解析命令行参数 ====================
while [[ $# -gt 0 ]]; do
    case $1 in
        --quick)
            QUICK_MODE=true
            shift
            ;;
        --detailed)
            DETAILED_MODE=true
            shift
            ;;
        *)
            echo "未知参数: $1"
            echo "使用方法: $0 [--quick|--detailed]"
            exit 1
            ;;
    esac
done

# ==================== 日志函数 ====================
log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] $*"
}

log_section() {
    echo ""
    echo "========================================"
    echo "$*"
    echo "========================================"
}

# ==================== 开始检查 ====================
log_section "数据质量检查报告"
log_info "数据目录: $DATA_ROOT"
log_info "开始时间: $(date '+%Y-%m-%d %H:%M:%S')"

# ==================== 1. 文件数量统计 ====================
log_section "1. 文件数量统计"

bar1min_count=$(find "$DATA_ROOT/full_bar_1min" -name "*.parquet" 2>/dev/null | wc -l)
bar5min_count=$(find "$DATA_ROOT/full_bar_5min" -name "*.parquet" 2>/dev/null | wc -l)
main1min_count=$(find "$DATA_ROOT/main_bar_1min" -name "*.parquet" 2>/dev/null | wc -l)
main5min_count=$(find "$DATA_ROOT/main_bar_5min" -name "*.parquet" 2>/dev/null | wc -l)
tick_count=$(find "$DATA_ROOT/full_tick" -name "*.parquet" 2>/dev/null | wc -l)

echo "原始数据:"
echo "  full_tick:       $tick_count 个合约"
echo ""
echo "生成数据:"
echo "  full_bar_1min:   $bar1min_count 个合约"
echo "  full_bar_5min:   $bar5min_count 个合约"
echo "  main_bar_1min:   $main1min_count 个品种"
echo "  main_bar_5min:   $main5min_count 个品种"

# 计算覆盖率
if [ $tick_count -gt 0 ]; then
    coverage1min=$(echo "scale=1; $bar1min_count * 100 / $tick_count" | bc)
    coverage5min=$(echo "scale=1; $bar5min_count * 100 / $tick_count" | bc)
    echo ""
    echo "覆盖率:"
    echo "  1min: ${coverage1min}%"
    echo "  5min: ${coverage5min}%"
fi

# ==================== 2. 磁盘占用 ====================
log_section "2. 磁盘占用"

echo "$(du -sh $DATA_ROOT/full_bar_1min | awk '{print "full_bar_1min:  ", $1}')"
echo "$(du -sh $DATA_ROOT/full_bar_5min | awk '{print "full_bar_5min:  ", $1}')"
echo "$(du -sh $DATA_ROOT/main_bar_1min | awk '{print "main_bar_1min:  ", $1}')"
echo "$(du -sh $DATA_ROOT/main_bar_5min | awk '{print "main_bar_5min:  ", $1}')"

total_size=$(du -sh $DATA_ROOT | awk '{print $1}')
echo ""
echo "总计: $total_size"

# 快速模式到此结束
if [ "$QUICK_MODE" = true ]; then
    log_info "快速模式完成"
    exit 0
fi

# ==================== 3. 数据完整性抽样检查 ====================
log_section "3. 数据完整性抽样检查（Python）"

python3 << 'PYTHON_EOF'
import pandas as pd
import random
from pathlib import Path

data_root = Path("/data/future_data/dongzheng_data")

# 随机选择3个1min文件检查
bar1min_files = list(data_root.glob("full_bar_1min/*_1min.parquet"))
if bar1min_files:
    sample_files = random.sample(bar1min_files, min(3, len(bar1min_files)))

    print("抽样检查 1min bar 数据:")
    for file in sample_files:
        try:
            df = pd.read_parquet(file)
            contract = file.stem.replace('_1min', '')

            # 检查必要字段
            required_cols = ['contract', 'trade_time', 'open', 'high', 'low', 'close', 'volume']
            missing_cols = [col for col in required_cols if col not in df.columns]

            if missing_cols:
                print(f"  ❌ {contract}: 缺少字段 {missing_cols}")
            else:
                # 检查数据质量
                null_counts = df[required_cols].isnull().sum()
                has_nulls = null_counts[null_counts > 0]

                if len(has_nulls) > 0:
                    print(f"  ⚠️  {contract}: 包含空值 {dict(has_nulls)}")
                else:
                    time_range = f"{df['trade_time'].min()} ~ {df['trade_time'].max()}"
                    print(f"  ✓ {contract}: {len(df):,}行, {time_range}")
        except Exception as e:
            print(f"  ❌ {contract}: 读取失败 - {e}")

# 检查主力合约数据
print("\n抽样检查 main_bar 数据:")
main_files = list(data_root.glob("main_bar_1min/*.parquet"))
if main_files:
    sample_files = random.sample(main_files, min(3, len(main_files)))

    for file in sample_files:
        try:
            df = pd.read_parquet(file)
            comd = file.stem.replace('_1min', '')

            # 检查新增字段
            if 'history_factor' not in df.columns or 'factor_multiply' not in df.columns:
                print(f"  ❌ {comd}: 缺少history_factor或factor_multiply字段")
            else:
                # 统计换月次数
                switches = (df['history_factor'] != 1.0).sum()
                contracts = df['contract'].nunique()
                time_range = f"{df['trade_time'].min()} ~ {df['trade_time'].max()}"
                print(f"  ✓ {comd}: {len(df):,}行, {contracts}个合约, {switches}次换月, {time_range}")
        except Exception as e:
            print(f"  ❌ {comd}: 读取失败 - {e}")

PYTHON_EOF

# ==================== 4. 数据时间范围统计 ====================
log_section "4. 数据时间范围统计"

python3 << 'PYTHON_EOF'
import pandas as pd
from pathlib import Path
from datetime import datetime

data_root = Path("/data/future_data/dongzheng_data")

# 统计1min数据时间范围
bar1min_files = list(data_root.glob("full_bar_1min/*_1min.parquet"))[:100]  # 抽样100个
if bar1min_files:
    min_time = None
    max_time = None

    for file in bar1min_files:
        try:
            df = pd.read_parquet(file, columns=['trade_time'])
            file_min = pd.to_datetime(df['trade_time']).min()
            file_max = pd.to_datetime(df['trade_time']).max()

            if min_time is None or file_min < min_time:
                min_time = file_min
            if max_time is None or file_max > max_time:
                max_time = file_max
        except:
            pass

    if min_time and max_time:
        print(f"1min bar 时间跨度: {min_time} ~ {max_time}")
        days = (max_time - min_time).days
        print(f"覆盖天数: {days:,} 天 ({days/365:.1f} 年)")

# 统计主力合约时间范围
main_files = list(data_root.glob("main_bar_1min/*.parquet"))[:20]  # 抽样20个品种
if main_files:
    print("\n主力合约时间跨度示例:")
    for file in main_files[:5]:
        try:
            df = pd.read_parquet(file, columns=['trade_time', 'contract'])
            comd = file.stem.replace('_1min', '')
            file_min = pd.to_datetime(df['trade_time']).min()
            file_max = pd.to_datetime(df['trade_time']).max()
            contracts = df['contract'].nunique()
            days = (file_max - file_min).days
            print(f"  {comd:6s}: {file_min.date()} ~ {file_max.date()} ({days:,}天, {contracts}个合约)")
        except:
            pass

PYTHON_EOF

# ==================== 5. 详细模式：主力合约验证 ====================
if [ "$DETAILED_MODE" = true ]; then
    log_section "5. 主力合约验证（详细模式）"

    echo "正在验证主力合约数据..."
    echo "（这可能需要几分钟）"
    echo ""

    cd /root/project/md_toolkit

    # 验证几个重要品种
    python3 tools/validators/main_contract_validator.py \
        --comds rb cu al ag au \
        --freq 1min \
        2>&1 | grep -E "验证品种|验证总结|验证通过|错误数|警告数|✅|❌" | head -50

    cd - > /dev/null
fi

# ==================== 总结 ====================
log_section "数据质量检查完成"

echo "统计摘要:"
echo "  ✓ full_bar_1min:  $bar1min_count 个文件"
echo "  ✓ full_bar_5min:  $bar5min_count 个文件"
echo "  ✓ main_bar_1min:  $main1min_count 个品种"
echo "  ✓ main_bar_5min:  $main5min_count 个品种"
echo ""
echo "完成时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""
echo "提示："
echo "  - 使用 --quick 参数可以跳过详细检查"
echo "  - 使用 --detailed 参数可以运行完整的主力合约验证"
