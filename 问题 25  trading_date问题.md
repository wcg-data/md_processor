完整的ASCII图来说明这个BUG：

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                   集合竞价日期BUG完整分析                                    │
  └─────────────────────────────────────────────────────────────────────────────┘

  原始tick数据（rb2605夜盘集合竞价）:
  ══════════════════════════════════════════════════════════════
    trade_time: 2025-05-15 20:59:00  ← 自然日: 05-15
    date字段:   2025-05-16            ← 交易日: 05-16 ✓

  期货规则：夜盘属于下一个交易日
    2025-05-15 晚上21:00开盘 → 属于2025-05-16交易日

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  当前代码流程（有BUG）                                                       │
  └─────────────────────────────────────────────────────────────────────────────┘

  Step 1: check_auction_period (bar1min_aggregator.rs:637)
  ────────────────────────────────────────────────────────────
    let opening_datetime = tick_datetime.date().and_time(auction_end);
                           ^^^^^^^^^^^^^^^^^^
                           使用trade_time的日期（自然日）

    → opening_datetime = 2025-05-15 21:00:00  ⚠️ 错误！应该是05-16

  Step 2: flush_auction_bar (bar1min_aggregator.rs:235-239)
  ────────────────────────────────────────────────────────────
    let date = to_string_field(&last_tick.date);
    let trade_time = opening_time.format("%Y-%m-%d %H:%M:%S").to_string();
    
    bar = BarData {
        date,        // "2025-05-16" ← 来自tick.date（交易日）✓
        trade_time,  // "2025-05-15 21:00:00" ← 来自opening_time（自然日）⚠️
        ...
    }

  Step 3: fill_missing_minutes可能覆盖date（推测）
  ────────────────────────────────────────────────────────────
    补全的bar会复制前一个bar的所有字段，包括date
    如果第一个bar的date已经错误，后续补全会继承错误

  最终结果（实际生成的bar）:
  ══════════════════════════════════════════════════════════════
    date:       "2025-05-15"        ⚠️ 错误！应该是05-16
    trade_time: "2025-05-15 21:00:00" ⚠️ 错误！应该是05-16 21:00:00

    影响：
    ├─ 夜盘集合竞价bar被标注为前一天
    ├─ 与原始tick的交易日不一致
    └─ 日线聚合时会统计到错误的交易日

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  正确的做法                                                                  │
  └─────────────────────────────────────────────────────────────────────────────┘

  方案1: 使用交易日（推荐）
  ════════════════════════════════════════════════════════════
    check_auction_period应该返回:
      let trading_date = to_string_field(&tick.date);  // 交易日
      let opening_datetime = NaiveDate::parse(trading_date)
                                       .and_time(auction_end);

    生成的bar:
      date:       "2025-05-16"        ✓ 交易日
      trade_time: "2025-05-16 21:00:00" ✓ 交易日的21:00

  方案2: 同时保留两个日期（最灵活）
  ════════════════════════════════════════════════════════════
    BarData添加新字段:
      trading_date: String    // 交易日（用于日线聚合）
      natural_date: String    // 自然日（用于时间序列分析）

    但这需要修改数据结构，工作量较大

  核心问题总结

  1. 当前BUG

  夜盘集合竞价bar:
    原始tick交易日: 2025-05-16  ← 正确（交易所数据）
    生成的bar.date: 2025-05-15  ← 错误（使用了自然日）

  差异: 夜盘bar被错误地标注为前一天

  2. 根本原因

  代码混用了两个日期概念：
  - check_auction_period 使用 tick_datetime.date()（自然日）
  - flush_auction_bar 预期使用 last_tick.date（交易日）
  - 但最终结果使用了自然日

  3. 影响范围

  ✓ 日盘集合竞价：不受影响（自然日=交易日）
  ⚠️ 夜盘集合竞价：date字段错误（前一天）
    ├─ 影响合约：所有有夜盘的品种（rb, cu, ag, au, etc.）
    ├─ 影响数据：full_bar_1min_on_tick/ 和 memory_processor输出
    └─ 下游影响：日线数据聚合会统计到错误的交易日

  4. 正确的修复方案

  // bar1min_aggregator.rs:626-643
  fn check_auction_period(tick: &TickData, tick_datetime: &NaiveDateTime) -> Option<NaiveDateTime> {
      let comd = to_string_field(&tick.comd);
      let tick_time = tick_datetime.time();

      let auction_periods = TRADE_SESSION_MAP.get_auction_periods(&comd);

      for (auction_start, auction_end) in auction_periods {
          if tick_time >= auction_start && tick_time < auction_end {
              // ===== 修复：使用交易日而不是自然日 =====
              let trading_date = to_string_field(&tick.date);  // 交易日
              let trading_date_parsed = NaiveDate::parse_from_str(&trading_date, "%Y-%m-%d")
                  .ok()?;
              let opening_datetime = trading_date_parsed.and_time(auction_end);
              return Some(opening_datetime);
          }
      }

      None
  }

  5. 需要同步修复 on_batch

  // parquet_processor_on_batch.rs:264-270
  if is_auction {
      // ===== 当前代码（错误）=====
      // let last_minute_window = naive.date().and_time(last_minute);

      // ===== 修复后（正确）=====
      // 需要从DataFrame获取date列（交易日），而不是使用naive.date()
      // 这需要重构create_minute_windows函数的签名
  }

  你的发现非常重要！这是一个真实存在的BUG，会导致夜盘集合竞价数据的交易日标注错误。需要修复吗？
