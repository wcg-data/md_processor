# CLAUDE.md

## 项目概述

md_processor - 高性能期货市场数据处理系统（Rust）
你应该总是用最新的代码生成的结果进行分析 
**核心功能**：
- tick数据聚合为1分钟K线
- 支持批量处理（on_batch）和流式处理（on_tick）
- 交易时段过滤和缺失分钟补全
- 集合竞价独立bar生成（on_tick支持，符合行业标准）

## 数据处理工作流

### 批量处理所有合约
```bash
# 完整流程：tick→1min→5min→主力合约
./script/process_all_contracts.sh                  # 重新生成所有数据（64线程）
./script/process_all_contracts.sh --skip-clean     # 增量处理
./script/process_all_contracts.sh --threads 32     # 自定义线程数
```

### 处理器选择
```bash
# on_batch：批量并行处理（推荐，性能高）
./target/release/parquet_processor_on_batch dongzheng_data 1min 8    # 8线程
./target/release/parquet_processor_on_batch dongzheng_data 5min 8    # 1min→5min

# on_tick：逐tick流式处理（支持集合竞价，用于验证一致性）
./target/release/parquet_processor_on_tick dongzheng_data 1min IF1706  # 测试集合竞价
```

## 核心模块

**可执行程序**（统一命名：数据源_processor_on_模式）：
- `memory_processor_on_tick.rs` - 实时共享内存逐tick处理
- `parquet_processor_on_batch.rs` - 离线批量并行处理（支持1min和5min）
- `parquet_processor_on_tick.rs` - 离线逐tick增量处理（支持集合竞价）

**数据处理库**：
- `bar1min_aggregator.rs` - 1分钟聚合器，包含 `validate_tick` 过滤函数，支持集合竞价
- `bar5min_aggregator.rs` - 5分钟聚合器，从1min bar聚合到5min bar
- `aggregator_manager.rs` - 多合约聚合管理器
- `common_utils.rs` - 共享工具（`fill_missing_minutes` 等）

**配置**：
- `trade_session_loader.rs` - 交易时段加载器，支持集合竞价时段配置
- `trade_session.csv` - 交易时段配置（168品种，6交易所，包含auction_time）

**数据结构**：
- `md_structures.rs` - TickData、BarData（C++兼容）
- `ring_buffer.rs` - 共享内存环形缓冲区

## 数据过滤规则

**9个启用规则**（详见 `docs/validate_tick_过滤规则.md`）：
1. close价格有效性
2. 基础字段非空
3. turnover非负
4. bid/ask有效性
5. bid≤ask（条件性）
6. bid/ask清理（0/NaN→NULL）
7. mid_price重算
8. volume/turnover一致性
9. 时段过滤（交易时段+集合竞价时段）

**重要**：修改过滤规则需同步更新 `bar1min_aggregator.rs` 和 `parquet_processor_on_batch.rs`

## 开发工作流

### 修改过滤规则
1. 同步修改 `validate_tick` 和 on_batch 向量化逻辑
2. `cargo build --release`
3. 测试并对比两种处理器结果
4. 更新文档版本号

### 更新交易时段
1. `python3 update_trade_session.py`
2. `cargo clean && cargo build --release`
3. 验证关键合约

## 数据处理流程

```
数据输入: /data/future_data/dongzheng_data/full_tick/ (8712个合约)
    ↓ [parquet_processor_on_batch dongzheng_data 1min]
中间输出: full_bar_1min/
    ↓ [parquet_processor_on_batch dongzheng_data 5min]
中间输出: full_bar_5min/
    ↓ [gen_main_bars.py]
最终输出: main_bar_1min/ + main_bar_5min/ (主力合约)
```

**一键执行**：`./script/process_all_contracts.sh`

## 重要文档

- `集合竞价功能实现总结.md` - 集合竞价功能详解（2025-10-27）
- `docs/fill_missing_minutes_修复总结.md` - 最后交易日停夜盘修复（2025-10-28）
- `script/README.md` - 脚本使用说明
- `docs/validate_tick_过滤规则.md` - 过滤规则详解（v1.4）
- `docs/trade_session_更新工具使用指南.md` - 交易时段配置
- `docs/README.md` - 文档索引

## 技术特点

- **高性能**：无锁数据结构、零拷贝、向量化处理
- **一致性**：on_batch和on_tick结果验证一致（16/17字段完全一致）
- **C++兼容**：`#[repr(C)]` 确保跨进程通信
- **容错性**：自动跳过损坏文件和空数据
- **集合竞价**：on_tick完整支持（详见下文）

## 集合竞价功能

on_tick处理器完整支持集合竞价独立bar生成（09:00/09:30/21:00），符合专业平台标准。

**核心特性**：
- 数据驱动，自动适配节假日（停夜盘/恢复夜盘）
- 自动推断auction_time（09:00→08:55, 09:30→09:25, 21:00→20:55）
- 自动检测volume重置（夜盘→日盘）

**详见**：`集合竞价功能实现总结.md`
