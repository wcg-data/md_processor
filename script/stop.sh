#!/bin/bash
# stop.sh - 停止 md_processor 服务
# 功能：优雅地停止服务，清理 PID 文件
# 用法：./script/stop.sh <可执行文件名> <共享内存名称>

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
LOG_DIR="./logs"
PID_FILE="$LOG_DIR/${EXE_NAME}_${SHM_NAME}.pid"

echo -e "${YELLOW}停止 ${EXE_NAME} (${SHM_NAME})...${NC}"

# 检查 PID 文件
if [ ! -f "$PID_FILE" ]; then
    echo -e "${YELLOW}服务未在运行 (PID 文件不存在)${NC}"
    # 尝试通过进程名查找
    PIDS=$(pgrep -f "${EXE_NAME}.*${SHM_NAME}" || true)
    if [ -n "$PIDS" ]; then
        echo -e "${YELLOW}发现运行中的进程: $PIDS${NC}"
        read -p "是否强制停止? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            kill $PIDS
            echo -e "${GREEN}✓ 进程已停止${NC}"
        fi
    fi
    exit 0
fi

# 读取 PID
PID=$(cat "$PID_FILE")

# 检查进程是否存在
if ! ps -p "$PID" > /dev/null 2>&1; then
    echo -e "${YELLOW}进程 $PID 不存在，清理 PID 文件...${NC}"
    rm -f "$PID_FILE"
    exit 0
fi

# 优雅停止（发送 SIGTERM，允许 Ctrl+C 处理器执行）
echo -e "${YELLOW}发送停止信号到进程 $PID...${NC}"
kill -TERM "$PID"

# 等待进程退出（最多10秒）
for i in {1..10}; do
    if ! ps -p "$PID" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ 服务已停止 (PID: $PID)${NC}"
        rm -f "$PID_FILE"
        exit 0
    fi
    sleep 1
done

# 强制停止
echo -e "${RED}进程未响应，强制停止...${NC}"
kill -9 "$PID" 2>/dev/null || true
rm -f "$PID_FILE"

if ps -p "$PID" > /dev/null 2>&1; then
    echo -e "${RED}✗ 停止失败${NC}"
    exit 1
else
    echo -e "${GREEN}✓ 服务已强制停止${NC}"
fi
