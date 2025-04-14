// bar_aggregator.rs 是行情聚合的核心文件，负责将Tick聚合为1分钟K线
use chrono::{DateTime, Timelike, Utc, NaiveDateTime,TimeZone};
use crate::{TickData, Bar1Min};
use std::ffi::CString;

pub struct BarAggregator {
    current_bar: Option<Bar1Min>,
    current_minute: Option<DateTime<Utc>>,
    first_volume: Option<u32>,
    first_turnover: Option<f64>,
    first_open_interest: Option<u32>,
}

/// 将 `[u8]` 字节数组（C风格、带 '\0' 结尾）转换为 Rust String
///
/// 自动去除尾部的空字符（\0）并转换为 UTF-8 字符串。
pub fn to_string_field(raw: &[u8]) -> String {
    String::from_utf8_lossy(raw)
        .trim_end_matches('\0')
        .to_string()
}

/// 从合约代码和日期中提取合约年月（YYMM）
///
/// # 示例：
/// - ag2506 + 2025-04-13 → "2506"
/// - AP505  + 2025-04-13 → "2505"
pub fn extract_contract_yymm(contract: &str, date: &str) -> String {
    let digits: String = contract.chars().filter(|c| c.is_ascii_digit()).collect();

    if digits.len() == 4 {
        // 形如 ag2506，直接返回
        digits
    } else if digits.len() == 3 {
        // 形如 AP505，需要结合年份
        let year_prefix = date.get(2..4).unwrap_or("00"); // 从"2025-04-13"中提取"25"
        format!("{}{}", year_prefix, digits)
    } else {
        // 无法解析，返回空串或默认
        "".to_string()
    }
}

impl BarAggregator {
    pub fn new() -> Self {
        Self {
            current_bar: None,
            current_minute: None,
            first_volume: None,
            first_turnover: None,
            first_open_interest: None,
        }
    }

    fn floor_to_minute(ts: &DateTime<Utc>) -> DateTime<Utc> {
        ts.with_second(0).unwrap().with_nanosecond(0).unwrap()
    }
    

    /// 将 TickData 转为 chrono 时间（处理格式为 "2024-04-13 13:45:00.123" 带毫秒）
    fn parse_datetime_from_tick(tick: &TickData) -> Option<DateTime<Utc>> {
        let ts_str = String::from_utf8_lossy(&tick.trade_time)
            .trim_end_matches('\0')
            .to_string();
        // println!("原始时间字符串: '{}'", ts_str);
        
        // 尝试带毫秒解析
        let naive = match NaiveDateTime::parse_from_str(&ts_str, "%Y-%m-%d %H:%M:%S%.f") {
            Ok(dt) => dt,
            Err(e) => {
                println!("带毫秒时间解析错误: {}", e);
                // 如果失败，尝试不带毫秒解析
                match NaiveDateTime::parse_from_str(&ts_str, "%Y-%m-%d %H:%M:%S") {
                    Ok(dt) => dt,
                    Err(e) => {
                        println!("不带毫秒时间解析错误: {}", e);
                        return None;
                    }
                }
            }
        };
        
        // println!("解析后时间: {}", naive);
        let tick_time = Utc.from_utc_datetime(&naive);
        // println!("转换为UTC时间: {}", tick_time);
        Some(tick_time)
    }

    pub fn on_tick(&mut self, tick: &TickData) -> Option<Bar1Min> {
        let tick_time = Self::parse_datetime_from_tick(tick)?;
        let tick_minute = Self::floor_to_minute(&tick_time);
        // println!("tick_minute: {:?}", tick_minute);

        let first_volume = self.first_volume.unwrap_or(tick.volume);
        let first_turnover = self.first_turnover.unwrap_or(tick.turnover);
        let first_open_interest = self.first_open_interest.unwrap_or(tick.open_interest);

        if let Some(ref mut bar) = self.current_bar {
            if Some(tick_minute) == self.current_minute {
                // 更新 bar
                bar.high = bar.high.max(tick.close);
                bar.low = bar.low.min(tick.close);
                bar.close = tick.close;
                bar.pre_settle = tick.pre_settle;

                bar.volume = tick.volume.saturating_sub(first_volume);
                bar.turnover = (tick.turnover - first_turnover).max(0.0);
                bar.open_interest_diff = (tick.open_interest as i32 - first_open_interest as i32);
                
                bar.open_interest = tick.open_interest;
                bar.ask_price_1 = tick.ask_price_1;
                bar.ask_volume_1 = tick.ask_volume_1;
                bar.bid_price_1 = tick.bid_price_1;
                bar.bid_volume_1 = tick.bid_volume_1;
                bar.mid_price = tick.mid_price;
                bar.vwap = if bar.volume == 0 {
                    0.0
                } else {
                    bar.turnover / bar.volume as f64
                };
                
                
                return None;
            } else {
                // 时间变了，flush当前 bar
                let flushed = self.current_bar.take();
                self.init_bar(tick, tick_minute);
                return flushed;
            }
        } else {
            self.init_bar(tick, tick_minute);
            return None;
        }
    }

    fn init_bar(&mut self, tick: &TickData, tick_minute: DateTime<Utc>) {
        let contract = to_string_field(&tick.contract);
        let comd = to_string_field(&tick.comd);
        let exchange = to_string_field(&tick.exchange);
        let date = to_string_field(&tick.date);
        let trade_time = to_string_field(&tick.trade_time);
        
        let bar = Bar1Min {
            contract: contract.clone(),
            contract_yymm: extract_contract_yymm(&contract,&date), // 提取 "cu2505" → "2505"
            comd,
            exchange,
            date,
            trade_time,
            open: tick.close,
            high: tick.close,
            low: tick.close,
            close: tick.close,
            pre_settle: tick.pre_settle,
            volume: tick.volume,
            turnover: tick.turnover,
            open_interest: tick.open_interest,
            open_interest_diff: 0, // 后续在 flush 时设置
            bid_price_1: tick.bid_price_1,
            bid_volume_1: tick.bid_volume_1,
            ask_price_1: tick.ask_price_1,
            ask_volume_1: tick.ask_volume_1,
            mid_price: tick.mid_price,
            vwap: tick.mid_price,
            maturity_month: 0,
            maturity_day: 0,
        };

        self.current_minute = Some(tick_minute);
        self.first_volume = Some(tick.volume);
        self.first_turnover = Some(tick.turnover);
        self.first_open_interest = Some(tick.open_interest);
        self.current_bar = Some(bar);
    }
    
    pub fn flush(&mut self) -> Option<Bar1Min> {
        self.first_volume = None;
        self.first_turnover = None;
        self.first_open_interest = None;
        self.current_bar.take()
    }
}

