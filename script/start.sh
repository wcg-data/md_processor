#!/bin/bash
# start.sh - 启动 md_processor 服务
# 功能：后台启动服务，输出日志到 logs/ 目录
# 用法：./script/start.sh <可执行文件名> <共享内存名称>

set -e

# 切换到项目根目录
cd "$(dirname "$0")/.."

# 颜色输出
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# 检查参数
if [ $# -lt 2 ]; then
    echo -e "${RED}错误: 参数不足${NC}"
    echo "用法: $0 <可执行文件名> <共享内存名称>"
    echo "示例: $0 memory_processor_on_tick MD_SNAPSHOT_DONGZHENG"
    exit 1
fi

EXE_NAME="$1"
SHM_NAME="$2"

# 配置
BINARY="./target/release/${EXE_NAME}"
LOG_DIR="./logs"
PID_FILE="$LOG_DIR/${EXE_NAME}_${SHM_NAME}.pid"
LOG_FILE="$LOG_DIR/${EXE_NAME}_${SHM_NAME}.log.$(date +%Y%m%d)"

# 创建日志目录
mkdir -p "$LOG_DIR"

# 检查可执行文件
if [ ! -f "$BINARY" ]; then
    echo -e "${RED}错误: 可执行文件不存在: $BINARY${NC}"
    echo "请先运行: ./script/build.sh"
    exit 1
fi

# 检查是否已在运行
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if ps -p "$OLD_PID" > /dev/null 2>&1; then
        echo -e "${YELLOW}服务已在运行 (PID: $OLD_PID)${NC}"
        echo "查看日志: tail -f $LOG_FILE"
        echo "停止服务: ./script/stop.sh $EXE_NAME $SHM_NAME"
        exit 0
    else
        echo -e "${YELLOW}清理旧的 PID 文件...${NC}"
        rm -f "$PID_FILE"
    fi
fi

# 检查共享内存
if [ ! -f "/dev/shm/$SHM_NAME" ]; then
    echo -e "${RED}警告: 共享内存文件不存在: /dev/shm/$SHM_NAME${NC}"
    read -p "是否继续启动? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# 启动服务
echo -e "${YELLOW}启动 ${EXE_NAME}...${NC}"
nohup "$BINARY" "$SHM_NAME" >> "$LOG_FILE" 2>&1 &
PID=$!

# 保存 PID
echo "$PID" > "$PID_FILE"

# 等待确认启动
sleep 1
if ps -p "$PID" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ 服务启动成功 (PID: $PID)${NC}"
    echo "日志文件: $LOG_FILE"
    echo "查看日志: tail -f $LOG_FILE"
    echo "停止服务: ./script/stop.sh $EXE_NAME $SHM_NAME"
else
    echo -e "${RED}✗ 服务启动失败${NC}"
    echo "请查看日志: cat $LOG_FILE"
    rm -f "$PID_FILE"
    exit 1
fi
