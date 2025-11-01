
  Parquet输出（强类型）

  contract: String
  contract_yymm: String
  date: String
  trade_time: String
  open, high, low, close: f64
  prev_close: Option<f64>     // ⚠️ 可为NULL
  pre_settle: Option<f64>     // ⚠️ 可为NULL
  volume: u32
  open_interest: u32
  open_interest_diff: i32
  bid_price_1: f64            // ⚠️ 可能是0.0或NaN
  ask_price_1: f64            // ⚠️ 可能是0.0或NaN
  mid_price: f64              // ⚠️ 可能是0.0或NaN
  vwap: f64
  log_return: Option<f64>     // ⚠️ 可为NULL
  maturity_month: i16
  maturity_day: i16
