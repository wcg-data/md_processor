# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

md_processor 是一个高性能的市场数据处理系统，使用 Rust 编写，专门用于处理金融市场实时行情数据。

主要功能：
- 从共享内存读取 tick 数据（华泰证券市场快照）
- 将 tick 数据聚合为 1 分钟 K 线
- 通过 Kafka 发布处理后的数据
- 使用 ClickHouse 存储历史数据

## 常用命令

```bash
# 构建项目（调试和发布版本）
./script/build.sh

# 运行调试版本
./script/run.sh
# 或直接运行
./target/debug/md_processor

# 运行发布版本（性能更好）
./target/release/md_processor

# 单独构建
cargo build              # 调试版本
cargo build --release    # 发布版本

# 运行测试 Kafka 工具
cargo run --bin test_kafka

# 检查代码
cargo check
cargo clippy

# 运行测试
cargo test
```

## 代码架构

### 核心模块

@md_processor.rs - 主程序入口，初始化共享内存映射和消费者
@consumer.rs - 核心消费逻辑，从无锁环形缓冲区读取 tick 数据并处理
@bar_aggregator.rs - 将 tick 数据聚合为 1 分钟 K 线数据
@shared_memory.rs - 共享内存映射工具，用于进程间通信
@ring_buffer.rs - 无锁环形缓冲区实现，高性能数据共享
@kafka_client.rs - Kafka 客户端封装，发布处理后的数据
@md_structures.rs - 数据结构定义，包括 TickData 和 Bar1Min（C++ 兼容）

### 数据流程

1. **数据接收**：通过共享内存（"MD_SNAPSHOT_HUATAI"）接收实时 tick 数据
2. **数据读取**：Consumer 使用无锁环形缓冲区读取数据
3. **数据聚合**：BarAggregator 将 tick 聚合为分钟 K 线
4. **数据发布**：通过 Kafka 发布处理后的数据
5. **数据存储**：写入 ClickHouse 数据库

### 技术特点

- **高性能设计**：无锁数据结构、零拷贝、原子操作
- **FFI 兼容**：使用 `#[repr(C)]` 确保与 C++ 系统兼容
- **异步处理**：基于 Tokio 的异步运行时
- **实时性**：专为低延迟金融数据处理设计

## 开发注意事项

- 代码注释使用中文
- 保持模块化设计，每个文件专注单一功能
- 性能关键路径避免内存分配
- 使用原子操作保证线程安全
- 数据结构必须保持 C++ 兼容性（用于跨进程通信）

## 重要文档

### 数据过滤规则
@docs/validate_tick_过滤规则.md - validate_tick 函数的详细过滤规则文档
- 记录所有启用和禁用的过滤规则
- 说明 on_tick 和 on_batch 的一致性保证
- 提供过滤示例和调试建议

### 数据对比工具
@/root/project/md_toolkit/tools/validators/compare_bar_processors.py - 对比 on_batch 和 on_tick 处理结果
@/root/project/md_toolkit/tools/validators/README_对比工具.md - 对比工具使用指南