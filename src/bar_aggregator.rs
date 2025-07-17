// bar_aggregator.rs - 优化版本：仅实时更新 high/low，其他字段在 flush 时生成
/// bar聚合器

use chrono::{DateTime, NaiveDateTime, TimeZone,Utc,FixedOffset};
use crate::{TickData, Bar1Min};
use crate::common_utils::*;
use crate::trade_session_loader::TRADE_SESSION_MAP;

#[derive(Debug, Default)]
pub struct BarAggregator {
    pub current_bar_minute: Option<DateTime<Utc>>,
    pub last_tick: Option<TickData>,
    pub prev_last_tick: Option<TickData>,
    pub high: Option<f64>,
    pub low: Option<f64>,
}

impl BarAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    /// 验证 Tick 数据是否有效
    /// 包括：数值有效性、字段间逻辑关系、成交价区间、成交量成交额匹配、时间合法性等
    pub fn validate_tick(tick: &TickData) -> bool {
        // ---------- 基本数值校验 ----------
        // 开盘价、高、低、收盘必须为正且是有效数（非NaN/inf）
        if tick.open <= 0.0 { return false; }
        if tick.high <= 0.0 { return false; }
        if tick.low <= 0.0 { return false; }
        if !tick.close.is_finite() || tick.close <= 0.0 { return false; }

        // 昨日结算价需有效且大于0（用于涨跌停等校验）
        if !tick.pre_settle.is_finite() || tick.pre_settle <= 0.0 { return false; }

        // 成交额不能为负（允许为 0）
        if tick.turnover < 0.0 { return false; }

        // 买卖一价必须大于 0
        if tick.bid_price_1 <= 0.0 { return false; }
        if tick.ask_price_1 <= 0.0 { return false; }

        // 买一价不能大于卖一价（防止撮合错误）
        if tick.bid_price_1 > tick.ask_price_1 { return false; }

        // ---------- 合理价格区间校验 ----------
        // 若 open/low/high 都是有效数，则校验它们之间的逻辑关系
        if tick.open.is_finite() && tick.low.is_finite() && tick.high.is_finite() {
            if tick.high < tick.low { return false; }              // high 必须 ≥ low
            if tick.open < tick.low || tick.open > tick.high {     // open 必须在区间内
                return false;
            }
            if tick.close < tick.low || tick.close > tick.high {   // close 必须在区间内
                return false;
            }
        }

        // ---------- 成交量与成交额的一致性校验 ----------
        // 要求：
        // - volume = 0 时，turnover 必须为 0 或 NaN（表示“无成交”）
        // - volume > 0 时，turnover 必须为正
        if !(tick.volume == 0 && (tick.turnover == 0.0 || tick.turnover.is_nan())
            || (tick.volume > 0 && tick.turnover > 0.0)) {
            return false;
        }

        // ---------- 成交均价必须位于 [low-eps, high+eps] ----------
        let eps = 1e-6; // 容差，避免浮点误差带来的误判

        if tick.volume > 0
            && tick.turnover.is_finite()
            && tick.low.is_finite()
            && tick.high.is_finite()
        {
            let avg_price = tick.turnover / tick.volume as f64;
            if avg_price < tick.low - eps || avg_price > tick.high + eps {
                return false;
            }
        }

        // ---------- 时段合法性校验 ----------
        // 从 tick 中提取 datetime（自定义解析函数）
        let dt = match Self::parse_datetime_from_tick(tick) {
            Some(t) => t,
            None => return false, // 若无法解析时间戳，判为无效
        };

        // 获取时间部分（NaiveTime）用于交易时段判断
        let time = dt.time();
        let comd = to_string_field(&tick.comd); // 提取合约标识

        // 判断是否在交易时间段内（依赖 TRADE_SESSION_MAP）
        TRADE_SESSION_MAP.is_in_trading_session(&comd, time)
    }
    
    fn parse_datetime_from_tick(tick: &TickData) -> Option<DateTime<Utc>> {
        let ts_str = to_string_field(&tick.trade_time);
        NaiveDateTime::parse_from_str(&ts_str, "%Y-%m-%d %H:%M:%S%.f")
            .or_else(|_| NaiveDateTime::parse_from_str(&ts_str, "%Y-%m-%d %H:%M:%S"))
            .ok()
            .map(|naive| Utc.from_utc_datetime(&naive))
    }

    pub fn on_tick(&mut self, tick: &TickData) -> Option<Bar1Min> {

        let tick_time = Self::parse_datetime_from_tick(tick)?;
        let tick_minute = floor_to_minute(&tick_time);

        if let Some(current_bar_minute) = self.current_bar_minute {
            if tick_minute == current_bar_minute {
                self.last_tick = Some(*tick);
                self.high = Some(self.high.map_or(tick.close, |h| h.max(tick.close)));
                self.low = Some(self.low.map_or(tick.close, |l| l.min(tick.close)));
                return None;
            } else {
                let flushed = self.flush();
                self.init_bar(tick, tick_minute);
                return flushed;
            }
        } else {
            self.init_bar(tick, tick_minute);
            return None;
        }
    }

    fn init_bar(&mut self, tick: &TickData, tick_minute: DateTime<Utc>) {
        if self.prev_last_tick.is_none() {
            self.prev_last_tick = Some(*tick);
        }
        self.last_tick = Some(*tick);
        self.high = Some(tick.close);
        self.low = Some(tick.close);
        self.current_bar_minute = Some(tick_minute);
    }

    pub fn flush(&mut self) -> Option<Bar1Min> {
        if self.last_tick.is_none() {
            // 当前分钟没有任何 tick，到达时间后补空 bar（根据 prev_last_tick 构造）
            if let Some(prev) = self.prev_last_tick {
                let contract = to_string_field(&prev.contract);
                let comd = to_string_field(&prev.comd);
                let exchange = to_string_field(&prev.exchange);
                let date = to_string_field(&prev.date);
                let trade_time = self.current_bar_minute
                    .unwrap_or_else(|| Utc::now())
                    .with_timezone(&FixedOffset::east_opt(8 * 3600).unwrap())  // ← 转为 UTC+8
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string();

                // 空 bar 的 log_return 设为 0.0
                let bar = Bar1Min {
                    contract: contract.clone(),
                    contract_yymm: extract_contract_yymm(&contract, &date),
                    comd,
                    exchange,
                    date,
                    trade_time,
                    open: prev.close,
                    high: prev.close,
                    low: prev.close,
                    close: prev.close,
                    pre_settle: prev.pre_settle,
                    volume: 0,
                    turnover: 0.0,
                    open_interest: prev.open_interest,
                    open_interest_diff: 0,
                    bid_price_1: prev.bid_price_1,
                    bid_volume_1: prev.bid_volume_1,
                    ask_price_1: prev.ask_price_1,
                    ask_volume_1: prev.ask_volume_1,
                    mid_price: prev.mid_price,
                    vwap: prev.close,
                    log_return: std::f64::NAN, // 无pre_close，无法计算log_return
                    maturity_month: 0,
                    maturity_day: 0,
                };

                self.current_bar_minute = None;
                self.high = None;
                self.low = None;

                return Some(bar);
            } else {
                return None;
            }
        }
        let last = self.last_tick?;

        let contract = to_string_field(&last.contract);
        let comd = to_string_field(&last.comd);
        let exchange = to_string_field(&last.exchange);
        let date = to_string_field(&last.date);
        let trade_time = align_to_bar_minute(&Self::parse_datetime_from_tick(&last)?)
            .format("%Y-%m-%d %H:%M:%S").to_string();

        let prev_last_tick = self.prev_last_tick.unwrap_or(last);
        let volume = last.volume.saturating_sub(prev_last_tick.volume);
        let turnover = (last.turnover - prev_last_tick.turnover).max(0.0);
        let oi_diff = last.open_interest.wrapping_sub(prev_last_tick.open_interest) as i32;

        // 计算 log_return，若 prev_last_tick.close <= 0 则返回 0.0 以避免非法对数
        let log_return = if prev_last_tick.close > 0.0 {
            (last.close / prev_last_tick.close).ln()
        } else {
            std::f64::NAN
        };

        let bar = Bar1Min {
            contract: contract.clone(),
            contract_yymm: extract_contract_yymm(&contract, &date),
            comd,
            exchange,
            date,
            trade_time,
            open: prev_last_tick.close,
            high: self.high.unwrap_or(last.close),
            low: self.low.unwrap_or(last.close),
            close: last.close,
            pre_settle: last.pre_settle,
            volume,
            turnover,
            open_interest: last.open_interest,
            open_interest_diff: oi_diff,
            bid_price_1: last.bid_price_1,
            bid_volume_1: last.bid_volume_1,
            ask_price_1: last.ask_price_1,
            ask_volume_1: last.ask_volume_1,
            mid_price: last.mid_price,
            vwap: if volume == 0 { 0.0 } else { turnover / volume as f64 },
            log_return,
            maturity_month: 0,
            maturity_day: 0,
        };

        // 清空状态
        self.current_bar_minute = None;
        self.prev_last_tick = self.last_tick;
        self.last_tick = None;
        self.high = None;
        self.low = None;

        Some(bar)
    }
}
