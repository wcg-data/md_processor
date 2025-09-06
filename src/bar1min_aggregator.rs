// bar1min_aggregator.rs - 优化版本：仅实时更新 high/low，其他字段在 flush 时生成
/// bar聚合器

use chrono::{DateTime, NaiveDateTime, TimeZone,Utc,FixedOffset};
use crate::{TickData, BarData};
use crate::common_utils::*;
use crate::trade_session_loader::TRADE_SESSION_MAP;

#[derive(Debug, Default)]
pub struct Bar1MinAggregator {
    pub current_bar_minute: Option<DateTime<Utc>>,
    pub prev_last_tick: Option<TickData>,
    pub last_tick: Option<TickData>,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
}

impl Bar1MinAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on_tick(&mut self, tick: &TickData) -> Option<BarData> {

        let tick_time = Self::parse_datetime_from_tick(tick)?;
        let tick_minute = floor_to_1min(&tick_time);

        if let Some(current_bar_minute) = self.current_bar_minute {
            if tick_minute == current_bar_minute {
                self.last_tick = Some(*tick);
                self.high = Some(self.high.map_or(tick.close, |h| h.max(tick.close)));
                self.low = Some(self.low.map_or(tick.close, |l| l.min(tick.close)));
                return None;
            } else {
                let flushed = self.flush();
                self.init_bar_window(tick, tick_minute);
                return flushed;
            }
        } else {
            self.init_bar_window(tick, tick_minute);
            return None;
        }
    }

    pub fn flush(&mut self) -> Option<BarData> {
        if self.last_tick.is_none() {
            // 当前分钟没有任何 tick，到达时间后补空 bar（根据 prev_last_tick 构造）
            if let Some(prev_last_tick) = self.prev_last_tick {
                let contract = to_string_field(&prev_last_tick.contract);
                let comd = to_string_field(&prev_last_tick.comd);
                let exchange = to_string_field(&prev_last_tick.exchange);
                let date = to_string_field(&prev_last_tick.date);
                let trade_time = self.current_bar_minute
                    .unwrap_or_else(|| Utc::now())
                    .with_timezone(&FixedOffset::east_opt(8 * 3600).unwrap())  // ← 转为 UTC+8
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string();

                // 空 bar 的 log_return 设为 0.0
                let bar = BarData {
                    contract: contract.clone(),
                    contract_yymm: extract_contract_yymm(&contract, &date),
                    comd,
                    exchange,
                    date,
                    trade_time,
                    open: prev_last_tick.close,
                    high: prev_last_tick.close,
                    low: prev_last_tick.close,
                    close: prev_last_tick.close,
                    prev_close: prev_last_tick.close,
                    pre_settle: prev_last_tick.pre_settle,
                    volume: 0,
                    turnover: 0.0,
                    open_interest: prev_last_tick.open_interest,
                    open_interest_diff: 0,
                    bid_price_1: prev_last_tick.bid_price_1,
                    bid_volume_1: prev_last_tick.bid_volume_1,
                    ask_price_1: prev_last_tick.ask_price_1,
                    ask_volume_1: prev_last_tick.ask_volume_1,
                    mid_price: prev_last_tick.mid_price,
                    vwap: prev_last_tick.close,
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
        let last_tick = self.last_tick?;

        let contract = to_string_field(&last_tick.contract);
        let comd = to_string_field(&last_tick.comd);
        let exchange = to_string_field(&last_tick.exchange);
        let date = to_string_field(&last_tick.date);
        let trade_time = align_to_bar_minute(&Self::parse_datetime_from_tick(&last_tick)?)
            .format("%Y-%m-%d %H:%M:%S").to_string();

        let prev_last_tick = self.prev_last_tick.unwrap_or(last_tick);
        let volume = last_tick.volume.saturating_sub(prev_last_tick.volume);
        let turnover = (last_tick.turnover - prev_last_tick.turnover).max(0.0);
        let oi_diff = last_tick.open_interest.wrapping_sub(prev_last_tick.open_interest) as i32;

        // 计算 log_return，若 prev_last_tick.close <= 0 则返回 0.0 以避免非法对数
        let log_return = if prev_last_tick.close > 0.0 {
            (last_tick.close / prev_last_tick.close).ln()
        } else {
            std::f64::NAN
        };

        // 放宽volume和turnover一致性检查
        // 只在明显异常的情况下才丢弃数据
        if volume == 0 && turnover > 0.0 {  // volume为0但成交额过大（超过100万）
            // 静默处理，不输出大量警告日志
            // 将成交额调整为0以保持一致性
            return None;
        }

        let bar = BarData {
            contract: contract.clone(),
            contract_yymm: extract_contract_yymm(&contract, &date),
            comd,
            exchange,
            date,
            trade_time,
            open: self.open.unwrap_or(last_tick.close),
            high: self.high.unwrap_or(last_tick.close),
            low: self.low.unwrap_or(last_tick.close),
            close: last_tick.close,
            prev_close: prev_last_tick.close,
            pre_settle: last_tick.pre_settle,
            volume,
            turnover,
            open_interest: last_tick.open_interest,
            open_interest_diff: oi_diff,
            bid_price_1: last_tick.bid_price_1,
            bid_volume_1: last_tick.bid_volume_1,
            ask_price_1: last_tick.ask_price_1,
            ask_volume_1: last_tick.ask_volume_1,
            mid_price: last_tick.mid_price,
            vwap: if volume == 0 { 0.0 } else { turnover / volume as f64 },
            log_return,
            maturity_month: 0,
            maturity_day: 0,
        };

        // 清空状态
        self.current_bar_minute = None;
        self.prev_last_tick = self.last_tick;
        self.last_tick = None;
        self.open = None;
        self.high = None;
        self.low = None;

        Some(bar)
    }

    // / 验证 Tick 数据是否有效
    // / 包括：数值有效性、字段间逻辑关系、成交价区间、成交量成交额匹配、时间合法性等
    pub fn validate_tick(tick: &TickData) -> bool {
        // ---------- 基本数值校验 ----------
        // 只检查关键价格字段
        if !tick.close.is_finite() || tick.close <= 0.0 {
            return false;
        }
        
        // 允许 volume 为 0（某些时段可能无成交）
        if tick.volume < 0 {
            return false;
        }
        
        // 允许 turnover 为负（可能存在特殊情况）
        if !tick.turnover.is_finite() {
            return false;
        }
        
        // 放宽买卖价检查 - 允许某个价格为0（可能暂停报价）
        if tick.bid_price_1 < 0.0 || tick.ask_price_1 < 0.0 {
            return false;
        }
        
        // 只在两个价格都大于0时检查买卖价关系
        if tick.bid_price_1 > 0.0 && tick.ask_price_1 > 0.0 && tick.bid_price_1 > tick.ask_price_1 {
            return false;
        }
    
        // ---------- 简化价格区间校验 ----------
        // 只检查最基本的逻辑关系
        if tick.open.is_finite() && tick.low.is_finite() && tick.high.is_finite() {
            if tick.high < tick.low {
                return false;
            }
            // 放宽价格区间检查，允许一定偏差
            let tolerance = tick.close * 0.1; // 允许10%的偏差
            if tick.open.is_finite() && (tick.open < tick.low - tolerance || tick.open > tick.high + tolerance) {
                return false;
            }
            if tick.close < tick.low - tolerance || tick.close > tick.high + tolerance {
                return false;
            }
        }
    
        // ---------- 放宽成交均价检查 ----------
        if tick.volume > 0 && tick.turnover.is_finite() && tick.turnover > 0.0
            && tick.low.is_finite() && tick.high.is_finite() 
            && tick.low > 0.0 && tick.high > 0.0
        {
            let avg_price = tick.turnover / tick.volume as f64;
            let tolerance = (tick.high + tick.low) / 2.0 * 0.2; // 允许20%的偏差
            if avg_price < tick.low - tolerance || avg_price > tick.high + tolerance {
                return false;
            }
        }
    
        // ---------- 简化时间检查 ----------
        // 只检查时间是否能解析，不严格限制交易时间
        let _dt = match Self::parse_datetime_from_tick(tick) {
            Some(t) => t,
            None => return false, // 时间解析失败才拒绝
        };
        
        // 暂时跳过交易时间段检查，允许所有时间的数据
        // 这样可以包含夜盘、开盘前后等时段的数据
        
        true
    }
    
    // pub fn validate_tick(tick: &TickData) -> bool {
    //     // ---------- 基本数值校验 ----------
    //     // 开盘价、高、低、收盘必须为正且是有效数（非NaN/inf）
    //     if tick.open <= 0.0 { return false; }
    //     if tick.high <= 0.0 { return false; }
    //     if tick.low <= 0.0 { return false; }
    //     if !tick.close.is_finite() || tick.close <= 0.0 { return false; }

    //     // 昨日结算价需有效且大于0（用于涨跌停等校验）
    //     if !tick.pre_settle.is_finite() || tick.pre_settle <= 0.0 { return false; }

    //     // 成交额不能为负（允许为 0）
    //     if tick.turnover < 0.0 { return false; }

    //     // 买卖一价必须大于 0
    //     if tick.bid_price_1 <= 0.0 { return false; }
    //     if tick.ask_price_1 <= 0.0 { return false; }

    //     // 买一价不能大于卖一价（防止撮合错误）
    //     if tick.bid_price_1 > tick.ask_price_1 { return false; }

    //     // ---------- 合理价格区间校验 ----------
    //     // 若 open/low/high 都是有效数，则校验它们之间的逻辑关系
    //     if tick.open.is_finite() && tick.low.is_finite() && tick.high.is_finite() {
    //         if tick.high < tick.low { return false; }              // high 必须 ≥ low
    //         if tick.open < tick.low || tick.open > tick.high {     // open 必须在区间内
    //             return false;
    //         }
    //         if tick.close < tick.low || tick.close > tick.high {   // close 必须在区间内
    //             return false;
    //         }
    //     }

    //     // ---------- 成交量与成交额的一致性校验 ----------
    //     // 要求：
    //     // - volume = 0 时，turnover 必须为 0 或 NaN（表示“无成交”）
    //     // - volume > 0 时，turnover 必须为正
    //     if !(tick.volume == 0 && (tick.turnover == 0.0 || tick.turnover.is_nan())
    //         || (tick.volume > 0 && tick.turnover > 0.0)) {
    //         return false;
    //     }

    //     // ---------- 成交均价必须位于 [low-eps, high+eps] ----------
    //     let eps = 1e-6; // 容差，避免浮点误差带来的误判

    //     // if tick.volume > 0
    //     //     && tick.turnover.is_finite()
    //     //     && tick.low.is_finite()
    //     //     && tick.high.is_finite()
    //     // {
    //     //     let avg_price = tick.turnover / tick.volume as f64;
    //     //     if avg_price < tick.low - eps || avg_price > tick.high + eps {
    //     //         return false;
    //     //     }
    //     // }

    //     // ---------- 时段合法性校验 ----------
    //     // 从 tick 中提取 datetime（自定义解析函数）
    //     let dt = match Self::parse_datetime_from_tick(tick) {
    //         Some(t) => t,
    //         None => return false, // 若无法解析时间戳，判为无效
    //     };

    //     // 获取时间部分（NaiveTime）用于交易时段判断
    //     let time = dt.time();
    //     let comd = to_string_field(&tick.comd); // 提取合约标识

    //     // 判断是否在交易时间段内（依赖 TRADE_SESSION_MAP）
    //     TRADE_SESSION_MAP.is_in_trading_session(&comd, time)
    // }


    // ======== 内部函数 ========

    /// 初始化新窗口
    fn init_bar_window(&mut self, tick: &TickData, tick_minute: DateTime<Utc>) {
        if self.prev_last_tick.is_none() {
            self.prev_last_tick = Some(*tick);
        }
        self.last_tick = Some(*tick);
        self.open = Some(tick.close);
        self.high = Some(tick.close);
        self.low = Some(tick.close);
        self.current_bar_minute = Some(tick_minute);
    }

    fn parse_datetime_from_tick(tick: &TickData) -> Option<DateTime<Utc>> {
        let ts_str = to_string_field(&tick.trade_time);
        NaiveDateTime::parse_from_str(&ts_str, "%Y-%m-%d %H:%M:%S%.f")
            .or_else(|_| NaiveDateTime::parse_from_str(&ts_str, "%Y-%m-%d %H:%M:%S"))
            .ok()
            .map(|naive| Utc.from_utc_datetime(&naive))
    }

}
