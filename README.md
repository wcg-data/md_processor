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

### 可执行程序（统一命名：数据源_processor_on_模式）
- `memory_processor_on_tick.rs` - 实时共享内存逐tick处理（177行，自包含）
- `parquet_processor_on_batch.rs` - 离线批量并行处理（970行，自包含）
- `parquet_processor_on_tick.rs` - 离线逐tick增量处理（224行，自包含）

### 数据处理库
- `bar1min_aggregator.rs` - 1 分钟 K 线聚合器（包含 validate_tick 过滤函数）
- `bar5min_aggregator.rs` - 5 分钟 K 线聚合器
- `aggregator_manager.rs` - 多合约聚合管理器

### 基础设施层
- `shared_memory.rs` - 共享内存映射，支持跨进程通信
- `ring_buffer.rs` - 无锁环形缓冲区实现
- `kafka_client.rs` - Kafka 生产者客户端
- `trade_session_loader.rs` - 交易时段配置加载器
- `md_structures.rs` - 数据结构定义（C++兼容）
- `common_utils.rs` - 共享工具函数

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
# 运行主程序（实时处理）
./script/run.sh

# 或直接运行
./target/debug/memory_processor_on_tick <共享内存名称>
./target/release/memory_processor_on_tick <共享内存名称>

# 运行 Parquet 处理器（批量历史数据处理）
cargo run --bin parquet_processor_on_batch <数据源> <频率> <合约>
cargo run --bin parquet_processor_on_tick <数据源> <频率> <合约>

# 运行其他工具
cargo run --bin test_kafka           # Kafka 测试工具
```

### 使用示例

```bash
# 1. 实时行情处理（连接到共享内存）
./target/release/memory_processor_on_tick MD_SNAPSHOT_HUATAI

# 2. 批量处理历史数据（从 Parquet 文件）
# on_batch: 向量化批处理（推荐，性能更高）
cargo run --bin parquet_processor_on_batch dongzheng_data 1min bb1710

# on_tick: 流式逐 tick 处理（用于验证一致性）
cargo run --bin parquet_processor_on_tick dongzheng_data 1min bb1710

# 3. 对比两种处理器结果
python3 /root/project/md_toolkit/tools/validators/compare_bar_processors.py bb1710

# 4. 更新交易时段配置
python3 update_trade_session.py
```

## 配置说明

### 交易时段配置

项目根目录的 `trade_session.csv` 文件定义了各品种的交易时段：

```csv
comd,auction_time,opening_time,closing_time
ni,20:55:00,21:00:00,01:00:00
ni,,09:00:00,10:15:00
ni,,10:30:00,11:30:00
ni,,13:30:00,15:00:00
```

**当前配置状态**：
- 品种数：168 个
- 交易时段数：610 个
- 支持交易所：6 个（SHFE、CFFEX、DCE、CZCE、INE、GFEX）

**自动更新工具**：
```bash
# 从 OpenCTP API 获取最新交易时段数据
python3 update_trade_session.py
```

详见：[trade_session_更新工具使用指南.md](docs/trade_session_更新工具使用指南.md)

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

## 📚 文档

项目包含详细的技术文档，位于 `docs/` 目录：

### 核心文档

1. **[validate_tick_过滤规则.md](docs/validate_tick_过滤规则.md)**
   - tick 数据过滤规则完整说明（v1.4）
   - 9 个启用规则 + 4 个禁用规则
   - 三层过滤策略：数据质量检查 + 数据清理 + 交易时段过滤
   - on_batch 和 on_tick 一致性保证

2. **[trade_session_更新工具使用指南.md](docs/trade_session_更新工具使用指南.md)**
   - 交易时段配置自动更新工具
   - 支持 168 个品种，6 个交易所
   - 集合竞价时间自动推断
   - 从 OpenCTP API 获取最新数据

### 文档目录

查看 **[docs/README.md](docs/README.md)** 获取完整的文档索引和快速参考。

### 相关工具

- **数据对比工具**: `/root/project/md_toolkit/tools/validators/compare_bar_processors.py`
  - 用途：验证 on_batch 和 on_tick 处理结果一致性
  - 文档：`/root/project/md_toolkit/tools/validators/README_对比工具.md`

## 联系方式

[添加联系信息]