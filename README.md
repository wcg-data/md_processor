# MD Processor

一个基于 Rust 的高性能金融市场数据处理系统，专门用于处理实时行情数据并生成 K 线数据。

## 项目概述

MD Processor 是一个专业的量化交易基础设施组件，设计用于：
- 从共享内存读取华泰证券市场快照 tick 数据
- 实时聚合 tick 数据为 1 分钟和 5 分钟 K 线
- 通过 Kafka 发布处理后的数据
- 支持 Parquet 格式数据处理和存储
- 提供 ClickHouse 数据库存储支持

## 架构特点

### 高性能设计
- **无锁数据结构**：使用无锁环形缓冲区实现高并发数据共享
- **零拷贝**：最小化内存分配，优化关键路径性能
- **异步处理**：基于 Tokio 异步运行时
- **并行计算**：使用 Rayon 进行数据并行处理

### 跨系统兼容
- **C++ 互操作**：使用 `#[repr(C)]` 确保与 C++ 系统完全兼容
- **多格式支持**：支持 CSV、JSON、Parquet 等多种数据格式
- **共享内存**：通过内存映射实现进程间高效通信

## 核心模块

### 数据接收层
- `shared_memory.rs` - 共享内存映射，支持跨进程通信
- `ring_buffer.rs` - 无锁环形缓冲区实现
- `consumer.rs` - 核心消费逻辑，从缓冲区读取 tick 数据

### 数据处理层
- `bar1min_aggregator.rs` - 1 分钟 K 线聚合器
- `bar5min_aggregator.rs` - 5 分钟 K 线聚合器
- `aggregator_manager.rs` - 多合约聚合管理器
- `trade_session_loader.rs` - 交易时段配置加载

### 数据输出层
- `kafka_client.rs` - Kafka 生产者客户端
- `parquet_processor.rs` - Parquet 文件处理器
- `md_structures.rs` - 数据结构定义

## 快速开始

### 环境要求

- Rust 1.70+
- Kafka 集群
- ClickHouse 数据库（可选）

### 编译项目

```bash
# 构建调试和发布版本
./script/build.sh

# 或手动构建
cargo build              # 调试版本
cargo build --release    # 发布版本
```

### 运行程序

```bash
# 运行主程序
./script/run.sh

# 或直接运行
./target/debug/md_processor <共享内存名称>
./target/release/md_processor <共享内存名称>

# 运行其他工具
cargo run --bin test_kafka           # Kafka 测试工具
cargo run --bin trade_session_loader # 交易时段加载器
cargo run --bin parquet_processor    # Parquet 处理器
```

### 使用示例

```bash
# 启动行情处理器，连接到共享内存 "MD_SNAPSHOT_HUATAI"
./target/release/md_processor MD_SNAPSHOT_HUATAI
```

## 配置说明

### 交易时段配置

项目根目录的 `trade_session.csv` 文件定义了各品种的交易时段：

```csv
comd,session_start,session_end,night_start,night_end
cu,09:00:00,15:00:00,21:00:00,01:00:00
au,09:00:00,15:00:00,21:00:00,02:30:00
```

### Kafka 配置

在 `kafka_client.rs` 中配置 Kafka 连接参数：
- Bootstrap 服务器
- Topic 名称
- 生产者配置

## 数据结构

### TickData 结构
```rust
#[repr(C)]
pub struct TickData {
    pub contract: [u8; 9],      // 合约代码
    pub comd: [u8; 4],          // 品种代码
    pub exchange: [u8; 7],      // 交易所
    pub trade_time: [u8; 24],   // 交易时间
    pub open: f64,              // 开盘价
    pub high: f64,              // 最高价
    pub low: f64,               // 最低价
    pub close: f64,             // 收盘价
    // ... 更多字段
}
```

### BarData 结构
```rust
pub struct BarData {
    pub contract: String,       // 合约代码
    pub comd: String,          // 品种代码
    pub exchange: String,       // 交易所
    pub trade_time: String,     // 时间戳
    pub open: f64,             // 开盘价
    pub high: f64,             // 最高价
    pub low: f64,              // 最低价
    pub close: f64,            // 收盘价
    pub volume: u32,           // 成交量
    pub turnover: f64,         // 成交额
    // ... 更多字段
}
```

## 开发指南

### 代码规范

- 所有注释使用中文
- 保持模块化设计，每个文件专注单一功能
- 在性能关键路径避免内存分配
- 使用原子操作保证线程安全
- 数据结构必须保持 C++ 兼容性

### 测试

```bash
cargo test              # 运行单元测试
cargo check             # 代码检查
cargo clippy            # 代码质量检查
```

### 性能调优

- 使用 `--release` 模式进行生产部署
- 根据硬件配置调整缓冲区大小
- 监控内存使用和 CPU 占用率

## 部署说明

### 生产环境部署

1. 使用发布版本构建：`cargo build --release`
2. 确保共享内存权限正确设置
3. 配置 Kafka 集群连接
4. 设置适当的系统资源限制

### 监控指标

- 数据处理延迟
- 内存使用量
- Kafka 生产者指标
- 数据完整性检查

## 故障排除

### 常见问题

1. **共享内存访问失败**
   - 检查内存映射权限
   - 确认共享内存名称正确

2. **Kafka 连接问题**
   - 验证网络连接
   - 检查 Kafka 集群状态

3. **数据格式错误**
   - 确认 C++ 结构体对齐
   - 验证字节序设置

## 许可证

[添加适当的许可证信息]

## 贡献指南

1. Fork 项目
2. 创建功能分支
3. 提交更改
4. 发起 Pull Request

## 联系方式

[添加联系信息]