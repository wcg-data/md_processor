统一收盘tick处理逻辑（方案B）

     目标

     1. 保持左闭右开 [t, t+1) - 符合行业标准
     2. 收盘tick特殊处理 - 合并到前一个bar，避免生成被过滤的超时段bar
     3. 标注结束时间 - 9:31 bar = [9:30, 9:31)，避免未来信息
     4. 支持多时段 - 通用逻辑，从trade_session获取所有收盘时间

     当前状态

     - ✅ on_batch (src/parquet_processor_on_batch.rs): 已正确实现
     - ⚠️ on_tick (src/bar1min_aggregator.rs): 已实现但需要验证和优化

     修复步骤

     步骤1: 回退当前修改，验证原始版本

     - 目的：确认原始版本的volume计算是正确的
     - 测试rb2405的23:00 bar和09:00 bar

     步骤2: 分析原始版本的问题

     - 问题：生成了23:01 bar（8手），但这个bar会被过滤
     - 结果：09:00 bar的prev_volume缺少8手（698 vs 690）

     步骤3: 重新设计on_tick修复

     关键改进点：
     1. 检测收盘tick时，将其合并到当前bar
     2. flush后保持prev_last_tick正确（不能清空）
     3. 不init新窗口（因为收盘后没有更多tick）
     4. 确保trade_time使用current_bar_minute+1

     修复位置：src/bar1min_aggregator.rs
     - Line 94-146: on_tick方法的收盘tick处理逻辑
     - Line 324-337: flush方法的trade_time计算
     - Line 590-607: is_closing_time辅助函数（已存在）

     步骤4: 验证修复效果

     测试rb2405的关键指标：
     - ✅ 23:00 bar: volume应包含收盘tick（5531手）
     - ✅ 23:01 bar: 不应该存在
     - ✅ 09:00 bar: 690手（与on_batch一致）
     - ✅ 其他bars: volume与on_batch完全一致

     步骤5: 全面测试

     测试多个品种，验证：
     - 多时段品种（如有夜盘和日盘）
     - 不同交易所的品种
     - 集合竞价和收盘tick的组合场景

     步骤6: 更新文档

     创建或更新说明文档，记录：
     - 收盘tick处理的设计原理
     - 与行业标准的对齐
     - on_tick和on_batch的一致性保证

     关键技术点

     收盘tick识别：
     fn is_closing_time(tick: &TickData, tick_datetime: &NaiveDateTime) -> bool {
         let comd = to_string_field(&tick.comd);
         let tick_time = tick_datetime.time();
         let trading_ranges = TRADE_SESSION_MAP.get_trading_ranges(&comd);
         
         for (_start, end) in trading_ranges {
             if tick_time == end {  // 精确匹配收盘时刻
                 return true;
             }
         }
         false
     }

     核心修复逻辑：
     else if is_closing_tick {
         // 收盘tick：合并到当前bar
         self.last_tick = Some(*tick);
         self.update_high_low(tick);
         
         // flush生成收盘bar（包含收盘tick）
         let flushed = self.flush();
         
         // 清空窗口状态，但保持prev_last_tick（flush已设置）
         self.current_bar_minute = None;
         self.last_tick = None;
         self.open = None;
         self.high = None;
         self.low = None;
         
         return flushed;
     }

     预期结果

     修复后，on_tick和on_batch将完全一致：
     - 收盘bar包含完整的收盘撮合数据
     - 不生成超时段的bar（避免被过滤）
     - 集合竞价bar的prev_volume准确
     - 符合行业标准的左闭右开+结束时间标注