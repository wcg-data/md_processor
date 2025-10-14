# trade_session.csv 更新工具使用说明

## 概述

`update_trade_session.py` 是一个自动化工具，用于从 OpenCTP API 获取最新的期货交易时段配置并更新 `trade_session.csv` 文件。

## 功能特性

✅ **自动获取**: 从 OpenCTP API 自动获取所有交易所的交易时段数据
✅ **智能推断**: 根据交易所规则自动推断集合竞价时间
✅ **格式兼容**: 生成的CSV文件完全兼容现有格式
✅ **自动备份**: 更新前自动备份现有文件
✅ **差异对比**: 显示新旧文件的差异统计

## 使用方法

### 基本使用

```bash
cd /root/project/md_processor
python3 update_trade_session.py
```

### 运行示例

```
================================================================================
交易时段配置更新工具
================================================================================
✓ 已备份现有文件至: trade_session.csv.backup_20251014_004055
正在从 http://dict.openctp.cn/times?markets=SHFE,CFFEX,DCE,ZCE,INE,GFEX 获取数据...
✓ 成功获取 366 条交易时段记录

正在组织交易时段数据...
✓ 成功组织 101 个品种的交易时段

正在写入 /root/project/md_processor/trade_session.csv...
✓ 成功写入 101 个品种，共 366 个交易时段

对比新旧文件...

统计信息:
  新文件品种数: 101
  旧文件品种数: 83
  新增品种数: 44
  移除品种数: 26
  共同品种数: 57

新增品种: ['HO', 'IO', 'MO', ...]
移除品种: ['AP', 'CF', 'CJ', ...]

================================================================================
✓ 交易时段配置更新完成!
✓ 新文件: /root/project/md_processor/trade_session.csv
✓ 备份文件: /root/project/md_processor/trade_session.csv.backup_20251014_004055
================================================================================
```

## 数据格式

### CSV文件格式

```csv
comd,auction_time,opening_time,closing_time
ni,20:55:00,21:00:00,01:00:00
ni,,09:00:00,10:15:00
ni,,10:30:00,11:30:00
ni,,13:30:00,15:00:00
```

**字段说明**:
- `comd`: 品种代码（如 ni, au, IF 等）
- `auction_time`: 集合竞价开始时间（只有第一个时段有，其他为空）
- `opening_time`: 交易时段开始时间
- `closing_time`: 交易时段结束时间

### 集合竞价规则

脚本根据以下规则自动推断集合竞价时间：

| 交易所 | 交易时段开始 | 集合竞价时间 | 说明 |
|--------|------------|------------|------|
| SHFE/INE/DCE/ZCE/GFEX | 09:00:00 | 08:55:00-09:00:00 | 日盘集合竞价 |
| SHFE/INE/DCE/ZCE/GFEX | 21:00:00 | 20:55:00-21:00:00 | 夜盘集合竞价 |
| CFFEX | 09:30:00 | 09:25:00-09:30:00 | 股指期货 |
| CFFEX | 09:15:00 | 09:10:00-09:15:00 | 国债期货 |

## 数据源

**API URL**: http://dict.openctp.cn/times?markets=SHFE,CFFEX,DCE,ZCE,INE,GFEX

**支持的交易所**:
- **SHFE** - 上海期货交易所
- **CFFEX** - 中国金融期货交易所
- **DCE** - 大连商品交易所
- **ZCE** - 郑州商品交易所
- **INE** - 上海国际能源交易中心
- **GFEX** - 广州期货交易所

## 更新频率建议

- **正常情况**: 每季度更新一次（新合约上市时）
- **交易所调整**: 当交易所调整交易时段时立即更新
- **节假日**: 节假日不需要特殊更新（交易时段不变）

## 备份文件管理

### 备份文件命名格式

```
trade_session.csv.backup_YYYYMMDD_HHMMSS
```

示例: `trade_session.csv.backup_20251014_004055`

### 清理旧备份

```bash
# 查看所有备份文件
ls -lh /root/project/md_processor/trade_session.csv.backup_*

# 删除30天前的备份
find /root/project/md_processor -name "trade_session.csv.backup_*" -mtime +30 -delete
```

## 验证新配置

更新后，建议运行测试验证新配置的正确性：

```bash
# 重新编译（确保加载新配置）
cd /root/project/md_processor
cargo build --release

# 测试一个合约
./target/release/parquet_processor_on_batch dongzheng_data 1min bb1710

# 对比结果
python3 /root/project/md_toolkit/tools/validators/compare_bar_processors.py bb1710
```

## 故障排除

### 1. 网络连接失败

**错误**: `✗ 网络请求失败: Connection timeout`

**解决方法**:
- 检查网络连接
- 检查防火墙设置
- 尝试手动访问 API URL

### 2. API返回错误

**错误**: `✗ API返回错误: ...`

**解决方法**:
- 检查 API 是否正常运行
- 等待一段时间后重试

### 3. 新增品种缺少交易时段

**现象**: 某些新品种在新文件中缺失

**原因**: OpenCTP API 可能尚未收录该品种

**解决方法**:
- 手动添加到 CSV 文件
- 联系 OpenCTP 更新数据

### 4. 集合竞价时间不正确

**原因**: 交易所调整了集合竞价规则

**解决方法**:
- 修改脚本中的 `AUCTION_RULES` 字典
- 重新运行脚本

## 版本历史

### v1.0 - 2025-10-14
- ✅ 初始版本
- ✅ 支持从 OpenCTP API 获取交易时段
- ✅ 自动推断集合竞价时间
- ✅ 自动备份和差异对比
- ✅ 支持 6 个交易所共 101 个品种

## 相关文件

- **脚本**: `/root/project/md_processor/update_trade_session.py`
- **配置文件**: `/root/project/md_processor/trade_session.csv`
- **加载模块**: `/root/project/md_processor/src/trade_session_loader.rs`
- **过滤规则文档**: `/root/project/md_processor/docs/validate_tick_过滤规则.md`

## 技术细节

### 数据处理流程

1. **获取数据**: 从 OpenCTP API 获取所有交易所的交易时段
2. **组织数据**: 按品种和时段号组织数据
3. **推断集合竞价**: 根据交易所和时段开始时间推断集合竞价时间
4. **写入CSV**: 按照现有格式写入CSV文件
5. **对比差异**: 与旧文件对比并显示统计信息

### API 响应格式

```json
{
  "rsp_code": 0,
  "rsp_message": "succeed",
  "data": [
    {
      "ExchangeID": "SHFE",
      "ProductID": "ni",
      "SegmentNo": 1,
      "TimeBegin": "21:00:00",
      "TimeEnd": "01:00:00",
      "ProductClass": "1",
      "Area": "China"
    }
  ]
}
```

## 注意事项

1. ⚠️ **更新前备份**: 脚本会自动备份，但建议手动再备份一次
2. ⚠️ **验证数据**: 更新后务必验证关键品种的交易时段
3. ⚠️ **重新编译**: 更新后需要重新编译程序以加载新配置
4. ⚠️ **测试验证**: 更新后在测试环境验证无误再部署到生产环境

## 贡献

如果发现以下情况，请及时更新脚本：
- 交易所调整集合竞价规则
- 新增交易所或交易品种
- API 格式变化
- 其他数据源

## 联系方式

- **项目**: md_processor
- **模块**: 交易时段配置管理
- **维护者**: 请参考项目 README
