#!/bin/bash
# build.sh - md_processor 项目编译脚本
# 功能：编译 release 版本的所有可执行文件（memory_processor_on_tick, parquet_processor_on_batch, parquet_processor_on_tick）

set -e  # 遇到错误立即退出

# 切换到项目根目录
cd "$(dirname "$0")/.."

# 颜色输出
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== md_processor 编译开始 ===${NC}"

# 参数解析
CLEAN=false
BUILD_MODE="release"

while [[ $# -gt 0 ]]; do
    case $1 in
        --clean)
            CLEAN=true
            shift
            ;;
        --debug)
            BUILD_MODE="debug"
            shift
            ;;
        *)
            echo -e "${RED}未知参数: $1${NC}"
            echo "用法: $0 [--clean] [--debug]"
            echo "  --clean  清理旧的编译文件"
            echo "  --debug  编译 debug 版本（默认 release）"
            exit 1
            ;;
    esac
done

# 清理旧文件
if [ "$CLEAN" = true ]; then
    echo -e "${YELLOW}清理旧编译文件...${NC}"
    cargo clean
fi

# 编译
if [ "$BUILD_MODE" = "release" ]; then
    echo -e "${YELLOW}编译 release 版本...${NC}"
    cargo build --release
    TARGET_DIR="./target/release"
else
    echo -e "${YELLOW}编译 debug 版本...${NC}"
    cargo build
    TARGET_DIR="./target/debug"
fi

# 显示编译结果
echo ""
echo -e "${GREEN}=== 编译成功 ===${NC}"
echo "可执行文件位置："
for bin in memory_processor_on_tick parquet_processor_on_batch parquet_processor_on_tick; do
    if [ -f "$TARGET_DIR/$bin" ]; then
        SIZE=$(du -h "$TARGET_DIR/$bin" | cut -f1)
        echo -e "  ${GREEN}✓${NC} $TARGET_DIR/$bin ($SIZE)"
    fi
done

echo ""
echo -e "${GREEN}编译完成！${NC}"
echo "启动服务: ./script/start.sh"
echo "停止服务: ./script/stop.sh"
