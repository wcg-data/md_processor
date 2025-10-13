// bar1min_aggregator.rs - 优化版本：仅实时更新 high/low，其他字段在 flush 时生成
/// bar聚合器

use chrono::{DateTime, NaiveDateTime, TimeZone,Utc,FixedOffset};
use crate::{TickData, BarData};
use crate::common_utils::*;
use crate::trade_session_loader::TRADE_SESSION_MAP;
use once_cell::sync::Lazy;

// 静态时区对象，避免每次创建
static UTC_PLUS_8: Lazy<FixedOffset> = Lazy::new(|| FixedOffset::east_opt(8 * 3600).unwrap());

#[derive(Debug, Default)]
pub struct Bar1MinAggregator {
    pub current_bar_minute: Option<DateTime<Utc>>,
    pub prev_last_tick: Option<TickData>,
    pub last_tick: Option<TickData>,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    // 缓存解析后的时间，避免重复解析
    cached_tick_time: Option<DateTime<Utc>>,
}

impl Bar1MinAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on_tick(&mut self, tick: &TickData) -> Option<BarData> {
        // 缓存时间解析结果
        let tick_time = Self::parse_datetime_from_tick(tick)?;
        self.cached_tick_time = Some(tick_time);
        let tick_minute = floor_to_1min(&tick_time);

        if let Some(current_bar_minute) = self.current_bar_minute {
            if tick_minute == current_bar_minute {
                self.last_tick = Some(*tick);
                // 优化：使用直接比较替代 map_or 闭包调用
                if let Some(ref mut h) = self.high {
                    if tick.close > *h {
                        *h = tick.close;
                    }
                } else {
                    self.high = Some(tick.close);
                }

                if let Some(ref mut l) = self.low {
                    if tick.close < *l {
                        *l = tick.close;
                    }
                } else {
                    self.low = Some(tick.close);
                }
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
                    .with_timezone(&*UTC_PLUS_8)  // 使用静态时区对象
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
                self.cached_tick_time = None; // 清空时间缓存

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
        // 直接解析last_tick的时间（不使用cached_tick_time，避免窗口切换时的缓存不一致问题）
        let parsed_time = Self::parse_datetime_from_tick(&last_tick)?;
        let trade_time = align_to_bar_minute(&parsed_time)
            .format("%Y-%m-%d %H:%M:%S").to_string();

        // 根据是否有上一个bar，分别处理volume/turnover/oi_diff计算
        let (volume, turnover, oi_diff, prev_close) = if let Some(prev) = self.prev_last_tick {
            // 有上一个bar：计算差分
            // 特殊处理累计值重置：如果 last < prev，说明累计值被重置（交易时段切换），直接使用当前累计值
            let vol = if last_tick.volume < prev.volume {
                last_tick.volume  // 累计值重置，直接使用当前累计值
            } else {
                last_tick.volume.saturating_sub(prev.volume)  // 正常差分
            };

            let turn = if last_tick.turnover < prev.turnover {
                last_tick.turnover  // 累计值重置，直接使用当前累计值
            } else {
                (last_tick.turnover - prev.turnover).max(0.0)  // 正常差分
            };

            (
                vol,
                turn,
                last_tick.open_interest.wrapping_sub(prev.open_interest) as i32,
                prev.close
            )
        } else {
            // 第一个bar：直接使用当前累计值
            (
                last_tick.volume,
                last_tick.turnover,
                last_tick.open_interest as i32,
                last_tick.close  // 第一个bar的prev_close也使用当前close
            )
        };

        // 计算 log_return
        let log_return = (last_tick.close / prev_close).ln();

        // volume和turnover一致性检查 - 暂时注释，与on_batch保持一致
        // 只在明显异常的情况下才丢弃数据
        // if volume == 0 && turnover > 0.0 {
        //     // 静默处理，不输出大量警告日志
        //     return None;
        // }

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
            prev_close,
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
        self.cached_tick_time = None; // 清空时间缓存

        Some(bar)
    }
    
    #[inline]
    pub fn validate_tick(tick: &mut TickData) -> bool {
        // ---------- 快速失败路径：最常用字段优先 ----------
        // 收盘价是最关键的字段，优先检查
        if tick.close <= 0.0 || !tick.close.is_finite() {
            return false;
        }

        // 其他价格字段检查
        // if tick.open <= 0.0 { return false; }
        // if tick.high <= 0.0 { return false; }
        // if tick.low <= 0.0 { return false; }

        // 字节级快速检查：避免字符串转换
        if tick.comd[0] == 0 { return false; } // 品种代码不能为空
        if tick.contract[0] == 0 { return false; } // 合约代码不能为空
        if tick.trade_time[0] == 0 { return false; } // 交易时间不能为空

        // // 昨日结算价需有效且大于0（用于涨跌停等校验）
        // if !tick.pre_settle.is_finite() || tick.pre_settle <= 0.0 { return false; }

        // 成交额不能为负（允许为 0）
        if tick.turnover < 0.0 { return false; }

        // bid/ask价格检查 - 与on_batch保持一致
        // 过滤掉bid<0或infinite的tick，但允许bid=0、ask=0以及NaN
        if tick.bid_price_1 < 0.0 || tick.bid_price_1.is_infinite() { return false; }
        if tick.ask_price_1 < 0.0 || tick.ask_price_1.is_infinite() { return false; }

        // bid <= ask 检查：只在bid和ask都>0时才要求bid<=ask（与on_batch保持一致）
        // 如果bid=0或ask=0，则跳过检查；只有当两者都>0时才要求bid<=ask
        if tick.bid_price_1 > 0.0 && tick.ask_price_1 > 0.0 && tick.bid_price_1 > tick.ask_price_1 {
            return false;
        }

        // 清理bid/ask为0或NaN的情况：将价格设为NaN，volume设为0（Parquet会存储为NULL）
        // 这样在读取时会正确识别为空值，统一表示"无买/卖盘"
        if tick.bid_price_1 == 0.0 || tick.bid_price_1.is_nan() {
            tick.bid_price_1 = f64::NAN;
            tick.bid_volume_1 = 0;
        }
        if tick.ask_price_1 == 0.0 || tick.ask_price_1.is_nan() {
            tick.ask_price_1 = f64::NAN;
            tick.ask_volume_1 = 0;
        }

        // 重新计算mid_price：只有bid和ask都有效时才计算，否则设为NaN
        // 修正原始数据中当ask=0时mid_price=bid/2的错误逻辑
        if tick.bid_price_1.is_nan() || tick.ask_price_1.is_nan() {
            tick.mid_price = f64::NAN;
        } else {
            tick.mid_price = (tick.bid_price_1 + tick.ask_price_1) / 2.0;
        }

        // ---------- 合理价格区间校验 ----------
        // // 若 open/low/high 都是有效数，则校验它们之间的逻辑关系
        // if tick.open.is_finite() && tick.low.is_finite() && tick.high.is_finite() {
        //     if tick.high < tick.low { return false; }              // high 必须 ≥ low
        //     if tick.open < tick.low || tick.open > tick.high {     // open 必须在区间内
        //         return false;
        //     }
        //     if tick.close < tick.low || tick.close > tick.high {   // close 必须在区间内
        //         return false;
        //     }
        // }

        // ---------- volume和turnover的一致性校验 ----------
        // 要求：
        // - volume = 0 时，turnover 必须为 0 或 NaN（表示"无成交"）
        // - volume > 0 时，turnover 必须为正
        if !(tick.volume == 0 && (tick.turnover == 0.0 || tick.turnover.is_nan())
            || (tick.volume > 0 && tick.turnover > 0.0)) {
            return false;
        }

        // ---------- 成交均价必须位于 [low-eps, high+eps] ----------
        let eps = 1e-6; // 容差，避免浮点误差带来的误判

        // if tick.volume > 0
        //     && tick.turnover.is_finite()
        //     && tick.low.is_finite()
        //     && tick.high.is_finite()
        // {
        //     let avg_price = tick.turnover / tick.volume as f64;
        //     if avg_price < tick.low - eps || avg_price > tick.high + eps {
        //         return false;
        //     }
        // }

        // ---------- 延迟昂贵操作到最后 ----------
        // 最昂贵的操作：时间解析 + 字符串分配 + 交易时段查询
        let dt = match Self::parse_datetime_from_tick(tick) {
            Some(t) => t,
            None => return false, // 若无法解析时间戳，判为无效
        };

        // 获取商品代码
        let comd = to_string_field(&tick.comd); // 最昂贵的字符串分配

        // 判断是否在交易时间段内（依赖 TRADE_SESSION_MAP）
        // 如果该品种没有交易时段配置，则跳过交易时段检查（允许通过）
        let trading_ranges = TRADE_SESSION_MAP.get_trading_ranges(&comd);
        if trading_ranges.is_empty() {
            return true;  // 没有配置则不过滤
        }
        TRADE_SESSION_MAP.is_in_trading_session(&comd, dt.naive_utc())
    }


    // ======== 内部函数 ========

    /// 初始化新窗口
    fn init_bar_window(&mut self, tick: &TickData, tick_minute: DateTime<Utc>) {
        // 不再初始化prev_last_tick，保持为None（第一个bar）或保持上一个bar的值
        // 这样第一个bar的volume计算才正确
        self.last_tick = Some(*tick);
        self.open = Some(tick.close);
        self.high = Some(tick.close);
        self.low = Some(tick.close);
        self.current_bar_minute = Some(tick_minute);
    }

    #[inline]
    fn parse_datetime_from_tick(tick: &TickData) -> Option<DateTime<Utc>> {
        let ts_str = to_string_field(&tick.trade_time);
        NaiveDateTime::parse_from_str(&ts_str, "%Y-%m-%d %H:%M:%S%.f")
            .or_else(|_| NaiveDateTime::parse_from_str(&ts_str, "%Y-%m-%d %H:%M:%S"))
            .ok()
            .map(|naive| Utc.from_utc_datetime(&naive))
    }

}
