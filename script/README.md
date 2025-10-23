# 脚本使用说明

## 快速开始

```bash
# 1. 编译项目
./script/build.sh

# 2. 启动服务
./script/start.sh

# 3. 查看日志
tail -f logs/memory_processor_$(date +%Y%m%d).log

# 4. 停止服务
./script/stop.sh
```

## 脚本详解

### build.sh - 编译脚本

编译 release 版本的所有可执行文件。

**基本用法**：
```bash
./script/build.sh                # 编译 release 版本
./script/build.sh --debug        # 编译 debug 版本
./script/build.sh --clean        # 清理后编译
```

**输出**：
- `./target/release/memory_processor_on_tick` - 实时共享内存处理
- `./target/release/parquet_processor_on_batch` - 离线批量处理
- `./target/release/parquet_processor_on_tick` - 离线逐tick处理

---

### start.sh - 启动服务

后台启动 `memory_processor_on_tick` 服务。

**功能**：
- ✓ 检查共享内存是否存在 (`/dev/shm/MD_SNAPSHOT_DONGZHENG`)
- ✓ 防止重复启动（PID 检查）
- ✓ 自动创建日志目录
- ✓ 后台运行，日志输出到文件

**日志位置**：
```
logs/memory_processor_YYYYMMDD.log  # 按日期分隔的日志
logs/memory_processor.pid           # 进程 PID 文件
```

**查看实时日志**：
```bash
tail -f logs/memory_processor_$(date +%Y%m%d).log
```

---

### stop.sh - 停止服务

优雅地停止 `memory_processor_on_tick` 服务。

**停止流程**：
1. 读取 PID 文件
2. 发送 SIGTERM 信号（允许优雅退出）
3. 等待最多 10 秒
4. 如未响应，强制 kill -9

**处理孤立进程**：
```bash
# 如果 PID 文件丢失，脚本会自动搜索进程名并提示是否停止
./script/stop.sh
```

---

## 服务管理

### 检查服务状态

```bash
# 方法 1: 查看 PID 文件
cat logs/memory_processor.pid

# 方法 2: 查看进程
ps aux | grep memory_processor_on_tick

# 方法 3: 查看最新日志
tail -20 logs/memory_processor_$(date +%Y%m%d).log
```

### 重启服务

```bash
./script/stop.sh && ./script/start.sh
```

### 日志管理

```bash
# 查看今日日志
cat logs/memory_processor_$(date +%Y%m%d).log

# 搜索错误
grep -i error logs/memory_processor_*.log

# 清理旧日志（保留最近7天）
find logs/ -name "memory_processor_*.log" -mtime +7 -delete
```

---

## 配置说明

### start.sh 配置项

修改 `script/start.sh` 中的以下变量：

```bash
BINARY="./target/release/memory_processor_on_tick"  # 可执行文件路径
SHM_NAME="MD_SNAPSHOT_DONGZHENG"                   # 共享内存名称
LOG_DIR="./logs"                                    # 日志目录
```

---

## 常见问题

### Q1: 启动失败，提示共享内存不存在

**检查**：
```bash
ls -lh /dev/shm/MD_SNAPSHOT_DONGZHENG
```

**解决**：确保数据源程序（写入共享内存的进程）已启动。

### Q2: 服务无法停止

**手动强制停止**：
```bash
pkill -9 memory_processor_on_tick
rm -f logs/memory_processor.pid
```

### Q3: 日志文件过大

**轮转日志**：脚本自动按日期分隔日志，定期清理旧日志即可：
```bash
# 添加到 crontab
0 3 * * * find /root/project/md_processor/logs/ -name "*.log" -mtime +7 -delete
```

---

## 生产环境建议

### 使用 systemd 管理服务

创建 `/etc/systemd/system/memory-processor.service`：

```ini
[Unit]
Description=MD Processor - Memory Tick Consumer
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/project/md_processor
ExecStart=/root/project/md_processor/target/release/memory_processor_on_tick MD_SNAPSHOT_DONGZHENG
Restart=on-failure
RestartSec=5s
StandardOutput=append:/root/project/md_processor/logs/memory_processor.log
StandardError=append:/root/project/md_processor/logs/memory_processor.log

[Install]
WantedBy=multi-user.target
```

**管理命令**：
```bash
sudo systemctl start memory-processor
sudo systemctl stop memory-processor
sudo systemctl status memory-processor
sudo systemctl enable memory-processor  # 开机自启
```

---

## 批量数据处理

### process_all_contracts.sh - 批量处理所有合约

对所有合约执行完整的数据处理流程：tick → 1min → 5min → 主力合约

**数据流程**：
```
full_tick/         →  full_bar_1min/   (tick → 1min聚合)
                   ↓
full_bar_1min/     →  full_bar_5min/   (1min → 5min聚合)
                   ↓
full_bar_1min/5min →  main_bar_1min/   (生成主力合约)
                      main_bar_5min/
```

**基本用法**：
```bash
# 删除旧数据并重新生成所有数据（使用64线程）
./script/process_all_contracts.sh

# 增量处理，跳过已存在的文件
./script/process_all_contracts.sh --skip-clean

# 自定义线程数
./script/process_all_contracts.sh --threads 32
```

**命令行参数**：
- `--skip-clean`：跳过清理步骤，保留已生成的文件（增量处理）
- `--threads N`：指定并行线程数（默认：64）

**处理步骤**：
1. **清理旧数据**（可选）：删除 `full_bar_1min/`、`full_bar_5min/`、`main_bar_1min/`、`main_bar_5min/`
2. **编译程序**：`cargo build --release`
3. **tick → 1min**：`parquet_processor_on_batch dongzheng_data 1min 64`
4. **1min → 5min**：`parquet_processor_on_batch dongzheng_data 5min 64`
5. **生成主力合约**：调用 `gen_main_bars.py`

**日志输出**：
- 实时显示处理进度和时间统计
- 每个步骤的执行时间
- 生成文件数量统计
- 总用时（时/分/秒）

**示例输出**：
```
========================================
[2025-10-20 10:00:00] 步骤2: tick -> 1min bar (64线程)
========================================
✓ 线程1: bb1710
✓ 线程2: y1701
...
[2025-10-20 10:15:30] [INFO] ✓ tick -> 1min 处理完成，用时: 930秒
[2025-10-20 10:15:30] [INFO] 生成1min bar文件数: 8712
```

**注意事项**：
- 处理 8712 个合约可能需要数小时
- 确保磁盘空间充足（每个频率约需数十GB）
- 自动跳过损坏或空文件
- 如遇错误会立即退出并显示错误信息

**常见使用场景**：

```bash
# 场景1: 首次处理或需要重新生成所有数据
./script/process_all_contracts.sh

# 场景2: 增量更新，只处理新增合约
./script/process_all_contracts.sh --skip-clean

# 场景3: 资源受限服务器，降低线程数
./script/process_all_contracts.sh --threads 16

# 场景4: 高性能服务器，最大化处理速度
./script/process_all_contracts.sh --threads 128
```
