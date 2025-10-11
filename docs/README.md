# md_processor 文档目录

## 📋 过滤规则文档

### validate_tick_过滤规则.md

**路径**: `/root/project/md_processor/docs/validate_tick_过滤规则.md`

**内容概要**:
- `validate_tick` 函数的完整过滤规则说明
- 5 个启用的过滤规则详解（带示例）
- 8 个禁用的过滤规则说明（及禁用原因）
- on_tick 和 on_batch 的一致性保证
- 性能优化建议和调试指南

**适用场景**:
1. 了解 tick 数据的过滤逻辑
2. 调试数据被意外过滤的问题
3. 修改过滤规则前的参考
4. 保证 on_tick 和 on_batch 一致性

**快速查看**:
```bash
# 查看完整文档
cat /root/project/md_processor/docs/validate_tick_过滤规则.md

# 或在编辑器中打开
code /root/project/md_processor/docs/validate_tick_过滤规则.md
```

## 🔗 相关工具

### 数据对比工具

**位置**: `/root/project/md_toolkit/tools/validators/compare_bar_processors.py`

**使用方法**:
```bash
# 单个合约对比
/root/project/md_toolkit/compare_processors.sh bb1710

# 批量对比
/root/project/md_toolkit/compare_processors.sh --batch bb1710 y1701 AP010
```

**文档**: `/root/project/md_toolkit/tools/validators/README_对比工具.md`

## 📝 文档维护

### 何时更新 validate_tick_过滤规则.md

1. **修改过滤规则时** - 添加、删除或修改任何过滤条件
2. **启用/禁用规则时** - 注释或取消注释规则代码
3. **发现新的边界情况时** - 添加到示例表格中
4. **性能优化时** - 更新性能考虑部分

### 更新检查清单

- [ ] 规则描述是否准确？
- [ ] 示例表格是否覆盖所有情况？
- [ ] on_batch 等价实现是否同步更新？
- [ ] 过滤统计数据是否需要更新？
- [ ] 版本历史是否记录了变更？

## 🔍 快速参考

### 当前启用的 6 个过滤规则

1. ✅ **close 价格有效性** - 必须 > 0 且有限
2. ✅ **字段非空检查** - comd, contract, trade_time 不为空
3. ✅ **turnover 非负** - 成交额 >= 0
4. ✅ **bid/ask 有效性** - 价格 >= 0 且有限（允许0）
5. ✅ **bid <= ask（条件性）** - 仅当两者都 > 0 时检查
6. ✅ **bid/ask 价格清理** - 将0值转为NULL ✨新增

### 已禁用的 7 个规则（避免误过滤）

1. ❌ open/high/low 价格检查
2. ❌ 昨日结算价检查
3. ❌ mid_price 检查和重新计算
4. ❌ 价格区间逻辑检查
5. ❌ volume/turnover 一致性检查
6. ❌ 成交均价区间检查
7. ❌ 交易时段检查

## 📞 联系和反馈

如有问题或建议，请在项目仓库中提出 issue。
