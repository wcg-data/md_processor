# CLAUDE.md

## 项目概述

md_processor - 高性能期货市场数据处理系统（Rust）

**核心功能**：
- tick数据聚合为1分钟K线
- 支持批量处理（on_batch）和流式处理（on_tick）
- 交易时段过滤和缺失分钟补全

## 常用命令

```bash
# 编译
cargo build --release
./script/build.sh  # 或使用脚本

# 批量处理所有合约（完整流程：tick→1min→5min→主力合约）
./script/process_all_contracts.sh                  # 重新生成所有数据（64线程）
./script/process_all_contracts.sh --skip-clean     # 增量处理
./script/process_all_contracts.sh --threads 32     # 自定义线程数

# on_batch处理（批量并行，推荐）
./target/release/parquet_processor_on_batch dongzheng_data 1min      # tick→1min, 4线程
./target/release/parquet_processor_on_batch dongzheng_data 1min 8    # tick→1min, 8线程
./target/release/parquet_processor_on_batch dongzheng_data 5min 8    # 1min→5min, 8线程
./target/release/parquet_processor_on_batch dongzheng_data 1min bb1710  # 单合约

# on_tick处理（单合约）
cargo run --bin parquet_processor_on_tick dongzheng_data 1min bb1710

# 生成主力合约数据
python3 /root/project/md_toolkit/processors/gen_main_bars.py --freqs 1min 5min

# 对比验证
python3 /root/project/md_toolkit/tools/validators/compare_bar_processors.py bb1710

# 更新交易时段配置
python3 update_trade_session.py
```

## 核心模块

**可执行程序**（统一命名：数据源_processor_on_模式）：
- `memory_processor_on_tick.rs` - 实时共享内存逐tick处理（177行，自包含）
- `parquet_processor_on_batch.rs` - 离线批量并行处理（970行，自包含，支持1min和5min）
- `parquet_processor_on_tick.rs` - 离线逐tick增量处理（224行，自包含）

**数据处理库**：
- `bar1min_aggregator.rs` - 1分钟聚合器，包含 `validate_tick` 过滤函数
- `bar5min_aggregator.rs` - 5分钟聚合器，从1min bar聚合到5min bar
- `aggregator_manager.rs` - 多合约聚合管理器
- `common_utils.rs` - 共享工具（`fill_missing_minutes` 等）

**配置**：
- `trade_session_loader.rs` - 交易时段加载器
- `trade_session.csv` - 交易时段配置（168品种，6交易所）

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
9. 交易时段过滤

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

### 验证一致性
```bash
# 单个合约
python3 /root/project/md_toolkit/tools/validators/compare_bar_processors.py bb1710

# 批量对比
python3 /root/project/md_toolkit/tools/validators/compare_bar_processors.py --batch bb1710 y1701 m1503
```

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

- `script/README.md` - 脚本使用说明
- `docs/validate_tick_过滤规则.md` - 过滤规则详解（v1.4）
- `docs/trade_session_更新工具使用指南.md` - 交易时段配置
- `docs/README.md` - 文档索引

## 技术特点

- **高性能**：无锁数据结构、零拷贝、向量化处理
- **一致性**：on_batch和on_tick结果验证一致（16/17字段完全一致）
- **C++兼容**：`#[repr(C)]` 确保跨进程通信
- **容错性**：自动跳过损坏文件和空数据
