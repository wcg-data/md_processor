 validate_tick目前的情况：

  调用位置（3处）

  1. parquet_processor_on_tick.rs - 离线tick处理
  2. aggregator_manager.rs - 实时多合约处理
  3. parquet_processor_on_batch.rs - 向量化批量处理（用Polars表达式实现）

  验证指标（9个启用）

  1. ✅ close价格有效性（>0且finite）
  2. ✅ 基础字段非空（comd, contract, trade_time）
  3. ✅ turnover非负
  4. ✅ bid/ask有效性（≥0且非infinite）
  5. ✅ bid≤ask（条件性，都>0时检查）
  6. ✅ bid/ask清理（0或NaN→设为NaN）
  7. ✅ mid_price重算
  8. ✅ volume/turnover一致性
  9. ✅ 时段过滤（交易时段+集合竞价）

  ⚠️ 不检查的字段

  - open_interest（持仓量） ← 关键！
  - volume（允许=0）
  - open/high/low价格
  - pre_settle

  问题链条

  oi=0的tick → 通过validate_tick → 生成oi=0的bar → 
  更新prev_last_tick → 第一个oi>0的bar用错误的prev计算oi_diff →
  fill_missing_minutes删除oi=0的bar →
  第一个保留bar的oi_diff没重置 → sum(oi_diff) ≠ total_change