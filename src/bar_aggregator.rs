// bar_aggregator.rs - 优化版本：仅实时更新 high/low，其他字段在 flush 时生成
use chrono::{DateTime, NaiveDateTime, TimeZone,Utc};
use crate::{TickData, Bar1Min};
use crate::common_utils::*;
use std::collections::HashMap;


#[derive(Default)]
pub struct BarAggregator {
    pub current_minute: Option<DateTime<Utc>>,
    pub last_tick: Option<TickData>,
    pub prev_last_tick: Option<TickData>,
    pub high: Option<f64>,
    pub low: Option<f64>,
}

impl BarAggregator {
    pub fn new() -> Self {
        Self::default()
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

        if let Some(current_minute) = self.current_minute {
            if tick_minute == current_minute {
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
        self.current_minute = Some(tick_minute);
    }

    pub fn flush(&mut self) -> Option<Bar1Min> {
        if self.last_tick.is_none() {
            // 当前分钟没有任何 tick，到达时间后补空 bar（根据 prev_last_tick 构造）
            if let Some(prev) = self.prev_last_tick {
                let contract = to_string_field(&prev.contract);
                let comd = to_string_field(&prev.comd);
                let exchange = to_string_field(&prev.exchange);
                let date = to_string_field(&prev.date);
                let trade_time = self.current_minute
                    .unwrap_or_else(|| Utc::now())
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string();

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
                    maturity_month: 0,
                    maturity_day: 0,
                };

                self.current_minute = None;
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
            maturity_month: 0,
            maturity_day: 0,
        }; 

        // 清空状态
        self.current_minute = None;
        self.prev_last_tick = self.last_tick;
        self.last_tick = None;
        self.high = None;
        self.low = None;

        Some(bar)
    }
}


/// 多合约管理器
#[derive(Default)]
pub struct AggregatorManager {
    aggregators: HashMap<String, BarAggregator>,
}

impl AggregatorManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// 推入任意 tick，自动识别合约，分发给对应的 BarAggregator
    pub fn on_tick(&mut self, tick: &TickData) -> Option<Bar1Min> {
        let contract = to_string_field(&tick.contract);
        let aggr = self.aggregators.entry(contract.clone()).or_insert_with(BarAggregator::new);
        aggr.on_tick(tick)
    }

    /// 所有合约批量 flush（时间触发）
    pub fn flush_all(&mut self) -> Vec<Bar1Min> {
        self.aggregators
            .values_mut()
            .filter_map(|aggr| aggr.flush())
            .collect()
    }
} 
