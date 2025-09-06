// bar5min_aggregator.rs - 高性能版本：使用引用避免克隆，优化内存分配
/// bar聚合器 - 优化版本

use crate::BarData;
use chrono::{DateTime, Utc, TimeZone};
use crate::common_utils::*;

#[derive(Debug, Default)]
pub struct Bar5MinAggregator {
    /// 当前 5 分钟窗口起点（已向下取整）
    pub current_bar_minute: Option<DateTime<Utc>>,
    
    /// 窗口内第一条 bar 的基础信息（避免存储完整 BarData）
    pub first_bar_time: Option<DateTime<Utc>>,
    pub first_contract: String,
    pub first_contract_yymm: String,
    pub first_comd: String,
    pub first_exchange: String,
    pub first_date: String,
    
    /// 聚合的关键数值
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub prev_close: f64,  // 窗口开始前的收盘价
    pub pre_settle: f64,
    
    /// 累计量价
    pub volume_sum: u32,
    pub turnover_sum: f64,
    
    /// 持仓相关
    pub start_open_interest: u32,  // 窗口开始时的持仓
    pub end_open_interest: u32,    // 窗口结束时的持仓
    
    /// 盘口数据（最后一条）
    pub bid_price_1: f64,
    pub bid_volume_1: u32,
    pub ask_price_1: f64,
    pub ask_volume_1: u32,
    pub mid_price: f64,
    
    /// 标记是否有数据
    pub has_data: bool,
}

impl Bar5MinAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    /// 增量接收 1 min Bar（使用引用，避免克隆）
    pub fn on_bar_1min(&mut self, bar: &BarData) -> Option<BarData> {
        let dt = parse_datetime_from_str(&bar.trade_time)?;
        let bar_floor = floor_to_5min(&dt);

        // 窗口切换检测
        if let Some(curr_floor) = self.current_bar_minute {
            if bar_floor == curr_floor {
                // 仍在当前窗口，增量更新
                self.update_state(bar);
                return None;
            } else {
                // 先 flush 旧窗口，再开始新窗口
                let finished = self.flush();
                self.init_bar_window(bar_floor, bar);
                return finished;
            }
        } else {
            self.init_bar_window(bar_floor, bar);
            return None;
        }
    }

    /// 强制 flush（如收盘或程序退出时调用）
    pub fn flush(&mut self) -> Option<BarData> {
        if !self.has_data {
            return None;
        }

        let trade_time = self
            .current_bar_minute?
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();

        let volume = self.volume_sum;
        let turnover = self.turnover_sum;
        let open_interest_diff = (self.end_open_interest as i64 - self.start_open_interest as i64) as i32;

        // 生成 5 min Bar
        let bar5 = BarData {
            contract: self.first_contract.clone(),
            contract_yymm: self.first_contract_yymm.clone(),
            comd: self.first_comd.clone(),
            exchange: self.first_exchange.clone(),
            date: self.first_date.clone(),
            trade_time,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            prev_close: self.prev_close,
            pre_settle: self.pre_settle,
            volume,
            turnover,
            open_interest: self.end_open_interest,
            open_interest_diff,
            bid_price_1: self.bid_price_1,
            bid_volume_1: self.bid_volume_1,
            ask_price_1: self.ask_price_1,
            ask_volume_1: self.ask_volume_1,
            mid_price: self.mid_price,
            vwap: if volume == 0 { 0.0 } else { turnover / volume as f64 },
            log_return: if self.prev_close > 0.0 { 
                (self.close / self.prev_close).ln() 
            } else { 
                f64::NAN 
            },
            maturity_month: 0,
            maturity_day: 0,
        };

        // 重置状态
        self.reset();
        Some(bar5)
    }

    // ======== 内部函数 ========

    /// 初始化新窗口（使用引用，只复制必要字段）
    fn init_bar_window(&mut self, floor_time: DateTime<Utc>, bar: &BarData) {
        self.current_bar_minute = Some(floor_time);
        self.first_bar_time = Some(floor_time);
        
        // 只在窗口开始时复制字符串（一次性成本）
        self.first_contract = bar.contract.clone();
        self.first_contract_yymm = bar.contract_yymm.clone();
        self.first_comd = bar.comd.clone();
        self.first_exchange = bar.exchange.clone();
        self.first_date = bar.date.clone();
        
        // 初始化数值字段
        self.open = bar.open;
        self.high = bar.high;
        self.low = bar.low;
        self.close = bar.close;
        self.prev_close = bar.prev_close;
        self.pre_settle = bar.pre_settle;
        
        self.volume_sum = bar.volume;
        self.turnover_sum = bar.turnover;
        
        self.start_open_interest = bar.open_interest.saturating_sub(bar.open_interest_diff as u32);
        self.end_open_interest = bar.open_interest;
        
        self.bid_price_1 = bar.bid_price_1;
        self.bid_volume_1 = bar.bid_volume_1;
        self.ask_price_1 = bar.ask_price_1;
        self.ask_volume_1 = bar.ask_volume_1;
        self.mid_price = bar.mid_price;
        
        self.has_data = true;
    }

    /// 增量更新窗口统计（避免不必要的内存操作）
    fn update_state(&mut self, bar: &BarData) {
        if !self.has_data {
            // 如果是窗口内第一条数据
            self.first_contract = bar.contract.clone();
            self.first_contract_yymm = bar.contract_yymm.clone();
            self.first_comd = bar.comd.clone();
            self.first_exchange = bar.exchange.clone();
            self.first_date = bar.date.clone();
            
            self.open = bar.open;
            self.prev_close = bar.prev_close;
            self.pre_settle = bar.pre_settle;
            self.start_open_interest = bar.open_interest.saturating_sub(bar.open_interest_diff as u32);
            self.has_data = true;
        }
        
        // 更新极值（高效的数值操作）
        self.high = self.high.max(bar.high);
        self.low = self.low.min(bar.low);
        
        // 更新最新值
        self.close = bar.close;
        self.end_open_interest = bar.open_interest;
        
        // 累计量价（使用 saturating 操作避免溢出）
        self.volume_sum = self.volume_sum.saturating_add(bar.volume);
        self.turnover_sum += bar.turnover;
        
        // 更新盘口（最后一条的盘口）
        self.bid_price_1 = bar.bid_price_1;
        self.bid_volume_1 = bar.bid_volume_1;
        self.ask_price_1 = bar.ask_price_1;
        self.ask_volume_1 = bar.ask_volume_1;
        self.mid_price = bar.mid_price;
    }
    
    /// 重置聚合器状态
    fn reset(&mut self) {
        *self = Self::default();
    }
}

// ---------- 辅助函数 ----------

fn parse_datetime_from_str(s: &str) -> Option<DateTime<Utc>> {
    // 优化：避免两次解析尝试，直接处理最常见格式
    let s = s.trim();
    if s.len() == 19 {
        // "YYYY-MM-DD HH:MM:SS" 格式
        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
            .ok()
            .map(|n| Utc.from_utc_datetime(&n))
    } else {
        // 包含毫秒的格式
        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
            .ok()
            .map(|n| Utc.from_utc_datetime(&n))
    }
}