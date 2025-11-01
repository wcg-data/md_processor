# on_batch vs on_tick 完整对比流程

## 脚本说明

### 1. process_all_on_tick.sh
并行处理所有合约的 on_tick 数据（使用 parquet_processor_on_tick）

**功能**：
- 扫描 full_tick 目录下的所有合约
- 使用 GNU parallel 或 xargs 并行处理
- 输出到 `full_bar_1min_on_tick/` 目录

**使用方法**：
```bash
# 清理旧数据并重新生成（230线程）
./script/process_all_on_tick.sh

# 增量处理，跳过已存在文件
./script/process_all_on_tick.sh --skip-clean

# 自定义线程数
./script/process_all_on_tick.sh --threads 64
```

**输出**：
- 数据文件：`/data/future_data/dongzheng_data/full_bar_1min_on_tick/*.parquet`
- 日志文件：`logs/on_tick/parallel.log`
- 失败列表：`logs/on_tick/failed_contracts.txt`（如有失败）

---

### 2. compare_batch_vs_tick.py
并行比较 on_batch 和 on_tick 的结果

**功能**：
- 并行比较所有合约的 bar 数量
- 比较数据值（open, high, low, close, volume, turnover, open_interest）
- 统计差异分布和成功率

**使用方法**：
```bash
# 比较所有合约（64线程）
python3 script/compare_batch_vs_tick.py

# 自定义线程数
python3 script/compare_batch_vs_tick.py --threads 128

# 保存详细差异到 CSV
python3 script/compare_batch_vs_tick.py --save-details

# 只比较特定合约
python3 script/compare_batch_vs_tick.py --patterns 2501 2502
```

**输出**：
- 控制台：汇总统计和差异报告
- 文件：`comparison_report.csv`（使用 --save-details）

---

### 3. run_full_comparison.sh
完整执行 on_batch、on_tick 和比较（主控脚本）

**功能**：
- 依次执行 on_batch 处理、on_tick 处理、结果比较
- 自动统计耗时

**使用方法**：
```bash
# 完整流程（on_batch 230线程 + on_tick 230线程）
./script/run_full_comparison.sh

# 跳过 on_batch（假设已处理）
./script/run_full_comparison.sh --skip-batch

# 跳过 on_tick（假设已处理）
./script/run_full_comparison.sh --skip-tick

# 增量模式（不删除旧数据）
./script/run_full_comparison.sh --skip-clean

# 自定义线程数
./script/run_full_comparison.sh --batch-threads 230 --tick-threads 230 --compare-threads 128
```

**执行步骤**：
1. 处理 on_batch → `full_bar_1min/`
2. 处理 on_tick → `full_bar_1min_on_tick/`
3. 比较结果 → `comparison_report.csv`

---

## 快速开始

### 场景1：全新对比（推荐）
```bash
# 清理旧数据，完整重新生成并对比（230核心）
./script/run_full_comparison.sh
```

**预计耗时**（8700+合约）：
- on_batch：~30-60分钟（230核心，向量化处理）
- on_tick：~2-4小时（230核心，逐tick处理）
- 比较：~5-10分钟（64核心）

---

### 场景2：只重新生成 on_tick
```bash
# 假设 on_batch 已经处理好
./script/process_all_on_tick.sh --threads 230
```

---

### 场景3：只比较现有数据
```bash
# 假设两个目录都已经有数据
python3 script/compare_batch_vs_tick.py --threads 128 --save-details
```

---

## 监控进度

### 查看 on_tick 处理进度
```bash
# 查看并行日志（如果安装了 GNU parallel）
tail -f logs/on_tick/parallel.log

# 查看已生成的文件数
watch -n 5 'ls /data/future_data/dongzheng_data/full_bar_1min_on_tick/*.parquet | wc -l'

# 查看失败的合约
cat logs/on_tick/failed_contracts.txt
```

### 查看系统负载
```bash
# CPU 使用率
htop

# 磁盘 I/O
iostat -x 2
```

---

## 目录结构

```
/data/future_data/dongzheng_data/
├── full_tick/              # 原始 tick 数据（输入）
├── full_bar_1min/          # on_batch 输出（230核心处理）
├── full_bar_1min_on_tick/  # on_tick 输出（230核心处理）
└── full_bar_5min/          # 5分钟 bar（可选）

/root/project/md_processor/
├── logs/on_tick/           # on_tick 处理日志
└── comparison_report.csv   # 对比结果（使用 --save-details）
```

---

## 常见问题

### Q1: 如何安装 GNU parallel？
```bash
# Ubuntu/Debian
sudo apt-get install parallel

# CentOS/RHEL
sudo yum install parallel

# 或使用 xargs（脚本会自动降级）
```

### Q2: 如何处理失败的合约？
```bash
# 查看失败列表
cat logs/on_tick/failed_contracts.txt

# 重新处理单个合约
./target/release/parquet_processor_on_tick dongzheng_data 1min <contract_name>

# 批量重新处理失败的合约
cat logs/on_tick/failed_contracts.txt | \
    xargs -P 64 -I {} ./target/release/parquet_processor_on_tick dongzheng_data 1min {}
```

### Q3: 对比发现差异怎么办？
```bash
# 使用详细对比工具
python3 comprehensive_compare_all_contracts.py --contracts <差异合约>

# 查看具体差异
python3 << 'EOF'
import polars as pl
batch = pl.read_parquet('/data/future_data/dongzheng_data/full_bar_1min/<contract>_1min.parquet')
tick = pl.read_parquet('/data/future_data/dongzheng_data/full_bar_1min_on_tick/<contract>_1min.parquet')
print(f"Batch: {len(batch)} bars")
print(f"Tick: {len(tick)} bars")
# 进一步分析...
EOF
```

### Q4: 如何暂停和恢复？
```bash
# 暂停：Ctrl+C（会中断当前任务）

# 恢复：使用 --skip-clean 增量模式
./script/run_full_comparison.sh --skip-clean
```

---

## 性能优化建议

1. **线程数设置**：
   - on_batch：可以用满所有核心（230），I/O密集
   - on_tick：建议230核心，单核处理速度较慢
   - 比较：建议64-128核心，内存密集

2. **磁盘优化**：
   - 确保 `/data` 使用高速 SSD
   - 考虑使用 tmpfs 存储临时数据（如果内存充足）

3. **内存管理**：
   - 监控内存使用：`free -h`
   - 如果内存不足，降低线程数

---

## 预期结果

如果一切正常，应该看到：

```
[整体评估]
========================================================

✅ 所有合约的bar数量完全一致
✅ 所有数据值完全一致
✅ on_batch 和 on_tick 处理结果完美一致！
```

如果有差异，脚本会详细列出：
- 数量不一致的合约
- 差异分布统计
- 数据值差异的字段和最大误差
