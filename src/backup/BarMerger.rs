use chrono::{NaiveDateTime};
use crate::md_structures::{TickData, Bar1Min};
use std::ffi::CString;
use std::os::raw::c_char;

/// Tick聚合器，生成分钟K线
pub struct BarAggregator {
    ticks: Vec<TickData>,
    prev_volume: u32,
    volume_price_sum: f64,
}

/// 字节数组转str
fn bytes_to_str(bytes: &[u8]) -> &str {
    let len = bytes.iter().position(|&c| c == 0).unwrap_or(bytes.len());
    std::str::from_utf8(&bytes[..len]).unwrap_or("")
}

impl BarAggregator {
    /// 新建聚合器
    pub fn new() -> Self {
        Self {
            ticks: Vec::new(),
            prev_volume: 0,
            volume_price_sum: 0.0,
        }
    }

    /// 添加tick
    pub fn add_tick(&mut self, tick: TickData) {
        let volume_delta = if self.ticks.is_empty() {
            tick.volume
        } else {
            tick.volume.saturating_sub(self.prev_volume)
        };

        self.volume_price_sum += volume_delta as f64 * tick.close;
        self.prev_volume = tick.volume;
        self.ticks.push(tick);
    }

    /// 生成分钟bar
    pub fn generate_bar(&self, minute_time: NaiveDateTime) -> Option<Bar1Min> {
        if self.ticks.is_empty() {
            return None;
        }

        let first_tick = &self.ticks[0];
        let last_tick = &self.ticks[self.ticks.len() - 1];

        let high = self.ticks.iter().map(|t| t.high).fold(f64::NEG_INFINITY, f64::max);
        let low = self.ticks.iter().map(|t| t.low).fold(f64::INFINITY, f64::min);

        let total_volume = last_tick.volume;

        let vwap = if total_volume > 0 {
            self.volume_price_sum / total_volume as f64
        } else {
            last_tick.close
        };

        // 构造Bar1Min，注意字段对齐
        let mut bar = Bar1Min {
            contract: [0; 9],
            contract_yymm: [0; 5],
            comd: [0; 4],
            exchange: [0; 7],
            date: [0; 11],
            trade_time: [0; 24],
            open: first_tick.close,
            high,
            low,
            close: last_tick.close,
            pre_settle: last_tick.pre_settle,
            volume: total_volume,
            turnover: last_tick.turnover,
            open_interest: last_tick.open_interest as f64,
            open_interest_diff: (last_tick.open_interest as i64 - first_tick.open_interest as i64) as i32,
            bid_price_1: last_tick.bid_price_1,
            bid_volume_1: last_tick.bid_volume_1,
            ask_price_1: last_tick.ask_price_1,
            ask_volume_1: last_tick.ask_volume_1,
            mid_price: if last_tick.mid_price != 0.0 { last_tick.mid_price } else { last_tick.close },
            vwap,
            maturity_month: 0,
            maturity_day: 0,
        };

        // 填充字符串字段
        fill_c_char_array(&mut bar.contract, bytes_to_str(&first_tick.contract));
        fill_c_char_array(&mut bar.contract_yymm, &extract_yymm(bytes_to_str(&first_tick.contract)));
        fill_c_char_array(&mut bar.comd, bytes_to_str(&first_tick.comd));
        fill_c_char_array(&mut bar.exchange, bytes_to_str(&first_tick.exchange));
        fill_c_char_array(&mut bar.date, &minute_time.format("%Y-%m-%d").to_string());
        let trade_time_str = bytes_to_str(&first_tick.trade_time);
        let dt = chrono::NaiveDateTime::parse_from_str(
            trade_time_str,
            "%Y-%m-%d %H:%M:%S%.f",
        ).unwrap_or_else(|_| chrono::Local::now().naive_local());
        fill_c_char_array(&mut bar.trade_time, &dt.format("%Y-%m-%d %H:%M:%S").to_string());

        Some(bar)
    }
}

/// 将Rust字符串拷贝到C兼容的字节数组
fn fill_c_char_array(target: &mut [c_char], src: &str) {
    let bytes = src.as_bytes();
    let len = bytes.len().min(target.len() - 1);
    for i in 0..len {
        target[i] = bytes[i] as c_char;
    }
    target[len] = 0; // null结尾
}

/// 从合约代码中提取年月字符串
fn extract_yymm(contract: &str) -> String {
    if contract.len() >= 6 {
        contract[contract.len() - 4..].to_string()
    } else {
        "".to_string()
    }
}

