#!/bin/bash
# show.sh - 查看当前运行的 md_processor 服务
# 功能：显示所有运行中的服务状态、PID、日志文件等信息

# 切换到项目根目录
cd "$(dirname "$0")/.."

# 颜色输出
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

LOG_DIR="./logs"

echo -e "${BOLD}${CYAN}=== md_processor 服务状态 ===${NC}\n"

# 查找所有 PID 文件
PID_FILES=$(find "$LOG_DIR" -name "*.pid" 2>/dev/null)

if [ -z "$PID_FILES" ]; then
    echo -e "${YELLOW}没有找到运行中的服务（无 PID 文件）${NC}\n"
else
    echo -e "${BOLD}运行中的服务：${NC}\n"

    for PID_FILE in $PID_FILES; do
        # 从文件名解析信息
        BASENAME=$(basename "$PID_FILE" .pid)
        PID=$(cat "$PID_FILE" 2>/dev/null)

        # 检查进程是否存在
        if ps -p "$PID" > /dev/null 2>&1; then
            # 获取进程信息
            PROCESS_INFO=$(ps -p "$PID" -o pid,etime,rss,cmd --no-headers)
            RSS=$(echo "$PROCESS_INFO" | awk '{print $3}')
            MEM_MB=$((RSS / 1024))
            ETIME=$(echo "$PROCESS_INFO" | awk '{print $2}')
            CMD=$(echo "$PROCESS_INFO" | awk '{for(i=4;i<=NF;i++) printf $i" "; print ""}')

            # 查找对应的日志文件
            LOG_PATTERN="${BASENAME}.log.*"
            RECENT_LOG=$(find "$LOG_DIR" -name "$LOG_PATTERN" 2>/dev/null | sort -r | head -1)

            if [ -n "$RECENT_LOG" ]; then
                LOG_SIZE=$(du -h "$RECENT_LOG" 2>/dev/null | cut -f1)
                LOG_LINES=$(wc -l < "$RECENT_LOG" 2>/dev/null)
            else
                LOG_SIZE="N/A"
                LOG_LINES="0"
            fi

            echo -e "${GREEN}●${NC} ${BOLD}${BASENAME}${NC}"
            echo -e "  PID:      ${PID}"
            echo -e "  运行时间: ${ETIME}"
            echo -e "  内存:     ${MEM_MB} MB"
            echo -e "  日志:     ${RECENT_LOG} (${LOG_SIZE}, ${LOG_LINES} 行)"
            echo -e "  命令:     ${CMD}"
            echo -e "  停止:     ./script/stop.sh $(echo $BASENAME | sed 's/_/ /')"
            echo ""
        else
            echo -e "${RED}●${NC} ${BOLD}${BASENAME}${NC} ${RED}(进程不存在)${NC}"
            echo -e "  PID 文件: ${PID_FILE}"
            echo -e "  记录 PID: ${PID}"
            echo -e "  ${YELLOW}建议清理: rm ${PID_FILE}${NC}"
            echo ""
        fi
    done
fi

# 查找可能的孤立进程（运行中但无 PID 文件）
echo -e "${BOLD}检查孤立进程：${NC}\n"

ORPHAN_PROCESSES=$(pgrep -f "memory_processor_on_tick|parquet_processor" || true)

if [ -z "$ORPHAN_PROCESSES" ]; then
    echo -e "${GREEN}✓ 没有孤立进程${NC}\n"
else
    HAS_ORPHAN=false
    for PID in $ORPHAN_PROCESSES; do
        # 检查是否有对应的 PID 文件
        FOUND_IN_PIDFILE=false
        for PID_FILE in $PID_FILES; do
            if [ "$(cat "$PID_FILE" 2>/dev/null)" = "$PID" ]; then
                FOUND_IN_PIDFILE=true
                break
            fi
        done

        if [ "$FOUND_IN_PIDFILE" = false ]; then
            if [ "$HAS_ORPHAN" = false ]; then
                echo -e "${YELLOW}发现孤立进程：${NC}\n"
                HAS_ORPHAN=true
            fi

            PROCESS_INFO=$(ps -p "$PID" -o pid,etime,cmd --no-headers)
            echo -e "${YELLOW}⚠${NC}  PID: $PID"
            echo -e "  命令: $(echo "$PROCESS_INFO" | awk '{for(i=3;i<=NF;i++) printf $i" "; print ""}')"
            echo -e "  运行时间: $(echo "$PROCESS_INFO" | awk '{print $2}')"
            echo -e "  停止: kill $PID"
            echo ""
        fi
    done

    if [ "$HAS_ORPHAN" = false ]; then
        echo -e "${GREEN}✓ 没有孤立进程${NC}\n"
    fi
fi

# 显示最近的日志文件
echo -e "${BOLD}最近的日志文件 (前5个)：${NC}\n"

RECENT_LOGS=$(find "$LOG_DIR" -name "*.log.*" -type f 2>/dev/null | sort -r | head -5)

if [ -z "$RECENT_LOGS" ]; then
    echo -e "${YELLOW}没有找到日志文件${NC}\n"
else
    for LOG_FILE in $RECENT_LOGS; do
        LOG_SIZE=$(du -h "$LOG_FILE" | cut -f1)
        LOG_TIME=$(stat -c %y "$LOG_FILE" | cut -d'.' -f1)
        LOG_LINES=$(wc -l < "$LOG_FILE")

        echo -e "  ${CYAN}$(basename "$LOG_FILE")${NC}"
        echo -e "    大小: ${LOG_SIZE}  行数: ${LOG_LINES}  修改时间: ${LOG_TIME}"
    done
    echo ""
fi

# 快捷命令提示
echo -e "${BOLD}${CYAN}快捷命令：${NC}"
echo -e "  启动服务: ${GREEN}./script/start.sh <exe_name> <shm_name>${NC}"
echo -e "  停止服务: ${RED}./script/stop.sh <exe_name> <shm_name>${NC}"
echo -e "  查看日志: ${YELLOW}tail -f logs/<log_file>${NC}"
echo -e "  编译项目: ${CYAN}./script/build.sh${NC}"
