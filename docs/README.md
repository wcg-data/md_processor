# md_processor 文档目录

本目录包含 md_processor 项目的详细技术文档。

## 📚 文档列表

### 1. validate_tick_过滤规则.md

**路径**: `docs/validate_tick_过滤规则.md`

**内容概要**:
- `validate_tick` 函数的完整过滤规则说明（v1.4）
- 9 个启用的过滤规则详解（带示例和验证结果）
- 4 个禁用的过滤规则说明（及禁用原因）
- on_tick 和 on_batch 的一致性保证
- 性能优化建议和调试指南
- 三层过滤策略说明（数据质量检查 + 数据清理 + 交易时段过滤）

**适用场景**:
1. 了解 tick 数据的过滤逻辑
2. 调试数据被意外过滤的问题
3. 修改过滤规则前的参考
4. 保证 on_tick 和 on_batch 一致性
5. 了解交易时段过滤的工作原理

**版本**: v1.4 (2025-10-14)

**快速查看**:
```bash
cat docs/validate_tick_过滤规则.md
```

---

### 2. trade_session_更新工具使用指南.md

**路径**: `docs/trade_session_更新工具使用指南.md`

**内容概要**:
- `update_trade_session.py` 自动更新工具的使用说明
- 从 OpenCTP API 获取交易时段数据的方法
- 集合竞价时间自动推断规则
- 支持的 6 个交易所（SHFE/CFFEX/DCE/CZCE/INE/GFEX）
- 168 个期货品种的交易时段配置
- 故障排除和版本历史

**适用场景**:
1. 更新 trade_session.csv 配置文件
2. 添加新上市的期货品种
3. 修正交易时段配置错误
4. 了解集合竞价时间规则

**使用方法**:
```bash
cd /root/project/md_processor
python3 update_trade_session.py
```

**相关文件**:
- 脚本: `update_trade_session.py`
- 配置: `trade_session.csv`
- 加载模块: `src/trade_session_loader.rs`

---

## 🔗 相关工具

### 数据对比工具

**位置**: `/root/project/md_toolkit/tools/validators/compare_bar_processors.py`

**用途**: 对比 on_batch 和 on_tick 两种处理器的结果一致性

**使用方法**:
```bash
# 单个合约对比
python3 /root/project/md_toolkit/tools/validators/compare_bar_processors.py bb1710

# 简化输出
python3 /root/project/md_toolkit/tools/validators/compare_bar_processors.py bb1710 --simple

# 批量对比
python3 /root/project/md_toolkit/tools/validators/compare_bar_processors.py --batch bb1710 y1701 AP010
```

**文档**: `/root/project/md_toolkit/tools/validators/README_对比工具.md`

---

## 📊 当前配置状态

### 过滤规则 (v1.4)

**启用的 9 个规则**:
1. ✅ close 价格有效性检查
2. ✅ 基础字段非空检查
3. ✅ turnover 非负检查
4. ✅ bid/ask 价格有效性检查
5. ✅ bid <= ask（条件性检查）
6. ✅ bid/ask 价格清理（0值转NULL）
7. ✅ mid_price 重新计算
8. ✅ volume/turnover 一致性检查
9. ✅ 交易时段过滤 ✨最新启用

**禁用的 4 个规则**:
1. ❌ open/high/low 价格检查（字段不可靠）
2. ❌ pre_settle 检查（某些合约为NULL）
3. ❌ 价格区间逻辑检查（依赖不可靠字段）
4. ❌ 成交均价区间检查（依赖不可靠字段）

### 交易时段配置

- **品种数**: 168 个
- **交易时段数**: 610 个
- **支持交易所**: 6 个（SHFE/CFFEX/DCE/CZCE/INE/GFEX）
- **配置文件**: `trade_session.csv`
- **更新工具**: `update_trade_session.py`

---

## 🔧 文档维护

### 何时更新文档

#### validate_tick_过滤规则.md

更新时机：
1. 修改过滤规则时 - 添加、删除或修改任何过滤条件
2. 启用/禁用规则时 - 注释或取消注释规则代码
3. 发现新的边界情况时 - 添加到示例表格中
4. 性能优化时 - 更新性能考虑部分
5. 验证测试后 - 更新过滤统计和验证结果

更新检查清单：
- [ ] 规则描述是否准确？
- [ ] 示例表格是否覆盖所有情况？
- [ ] on_batch 等价实现是否同步更新？
- [ ] 过滤统计数据是否需要更新？
- [ ] 版本历史是否记录了变更？

#### trade_session_更新工具使用指南.md

更新时机：
1. 脚本功能变更时
2. 支持新的交易所时
3. 集合竞价规则调整时
4. 添加新的故障排除方案时

---

## 🔍 快速参考

### 关键数据结构

**TickData** - 原始 tick 数据（C++ 兼容）
- 位置: `src/md_structures.rs`
- 字段: contract, comd, exchange, close, volume, turnover, bid/ask 等

**Bar1Min** - 1分钟 K 线数据
- 位置: `src/md_structures.rs`
- 字段: OHLC, volume, turnover, open_interest, vwap, log_return 等

### 核心模块

**过滤和聚合**:
- `src/bar1min_aggregator.rs` - 1分钟聚合器（包含 validate_tick）
- `src/bar5min_aggregator.rs` - 5分钟聚合器
- `src/aggregator_manager.rs` - 聚合管理器

**交易时段**:
- `src/trade_session_loader.rs` - 交易时段加载器
- `trade_session.csv` - 交易时段配置
- `update_trade_session.py` - 自动更新工具

**数据处理**:
- `src/parquet_processor_on_batch.rs` - 批量向量化处理
- `src/parquet_processor_on_tick.rs` - 流式逐tick处理

---

## 📞 获取帮助

### 常见问题

1. **数据被过滤了怎么办？**
   - 查看 `validate_tick_过滤规则.md` 的调试建议部分
   - 使用对比工具验证 on_batch 和 on_tick 是否一致

2. **如何更新交易时段配置？**
   - 查看 `trade_session_更新工具使用指南.md`
   - 运行 `python3 update_trade_session.py`

3. **如何验证两个处理器的一致性？**
   - 使用 `compare_bar_processors.py` 工具
   - 查看对比工具的使用文档

### 联系方式

如有问题或建议，请在项目仓库中提出 issue。

---

**最后更新**: 2025-10-14
**文档版本**: 1.1
