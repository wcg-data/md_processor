use crate::{BarData};
use chrono::{DateTime, Utc};
  // 引入 TimeZone trait，才能调用 timestamp_opt / from_utc_datetime
use crate::common_utils::*;
use chrono::TimeZone; 

#[derive(Debug, Default)]
pub struct Bar5MinAggregator {
    /// 当前 5 分钟窗口起点（已向下取整）
    pub current_bar_minute: Option<DateTime<Utc>>,
    /// 窗口最后一条 1 min，用于 close、盘口、OI diff
    pub last_bar: Option<BarData>,
    /// 上一个窗口最后一条 1 min，用于 open、pre_settle、OI diff
    pub pre_last_bar: Option<BarData>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    /// 累计量价
    pub volume_sum: u32,
    pub turnover_sum: f64,
}

impl Bar5MinAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    /// 增量接收 1 min Bar，必要时返回聚合好的 5 min Bar
    pub fn on_bar_1min(&mut self, bar: BarData) -> Option<BarData> {
        let dt          = parse_datetime_from_str(&bar.trade_time)?;
        let bar_floor   = floor_to_5min(&dt);

        // -------- 窗口切换检测 --------
        if let Some(curr_floor) = self.current_bar_minute {
            if bar_floor == curr_floor {
                // -------- 仍在当前窗口，增量更新 --------
                self.update_state(&bar);
                return None
            } else {
                // 先 flush 旧窗口，再开始新窗口
                let finished = self.flush();
                self.init_bar_window(bar_floor, &bar);
                return finished;
            }
        } else {
            self.init_bar_window(bar_floor, &bar);
            return None;
        }
    }
    /// 强制 flush（如收盘或程序退出时调用）
    pub fn flush(&mut self) -> Option<BarData> {
        let pre_last_bar = self.pre_last_bar.as_ref()?;
        let last_bar  = self.last_bar.as_ref()?;

        let trade_time = self
            .current_bar_minute?
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();

        let volume   = self.volume_sum;
        let turnover = self.turnover_sum;

        // 生成 5 min Bar
        let bar5 = BarData {
            contract:        pre_last_bar.contract.clone(),
            contract_yymm:   pre_last_bar.contract_yymm.clone(),
            comd:            pre_last_bar.comd.clone(),
            exchange:        pre_last_bar.exchange.clone(),
            date:            pre_last_bar.date.clone(),
            trade_time,
            open:            self.open,
            high:            self.high,
            low:             self.low,
            close:           last_bar.close,
            pre_settle:      pre_last_bar.pre_settle,
            volume,
            turnover,
            open_interest:   last_bar.open_interest,
            open_interest_diff: (last_bar.open_interest as i64 - pre_last_bar.open_interest as i64) as i32,
            bid_price_1:     last_bar.bid_price_1,
            bid_volume_1:    last_bar.bid_volume_1,
            ask_price_1:     last_bar.ask_price_1,
            ask_volume_1:    last_bar.ask_volume_1,
            mid_price:       last_bar.mid_price,
            vwap:            if volume == 0 { 0.0 } else { turnover / volume as f64 },
            log_return:      if pre_last_bar.open > 0.0 { (last_bar.close / pre_last_bar.close).ln() } else { f64::NAN },
            maturity_month:  0,
            maturity_day:    0,
        };

        // 清空状态
        *self = Self::default();
        Some(bar5)
    }

    // ======== 内部函数 ========

    /// 初始化新窗口
    fn init_bar_window(&mut self, floor_time: DateTime<Utc>, bar: &BarData) {
        self.current_bar_minute = Some(floor_time);
        self.pre_last_bar = None;
        self.last_bar  = Some(bar.clone());
        self.open = bar.open;
        self.high = bar.high;
        self.low  = bar.low;
        self.volume_sum   = bar.volume;
        self.turnover_sum = bar.turnover;
        self.update_state(bar);
    }

    /// 增量更新窗口统计
    fn update_state(&mut self, bar: &BarData) {
        // 记录首条
        if self.pre_last_bar.is_none() {
            self.pre_last_bar = Some(bar.clone());
            self.open = bar.open;
            self.high = bar.high;
            self.low  = bar.low;
        }
        // 更新最后一条
        self.last_bar = Some(bar.clone());

        // 更新极值和累计量价
        self.high = self.high.max(bar.high);
        self.low  = self.low.min(bar.low);
        self.volume_sum   = self.volume_sum.saturating_add(bar.volume);
        self.turnover_sum += bar.turnover;
    }
}

// ---------- 辅助函数 ----------

fn parse_datetime_from_str(s: &str) -> Option<DateTime<Utc>> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .ok()
        .map(|n| Utc.from_utc_datetime(&n))
}