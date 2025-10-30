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
    // 集合竞价相关字段
    auction_ticks: Vec<TickData>,       // 集合竞价时段的tick缓冲区
    in_auction_period: bool,            // 当前是否在集合竞价时段
    auction_opening_time: Option<NaiveDateTime>, // 当前集合竞价对应的开盘时间
}

impl Bar1MinAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on_tick(&mut self, tick: &TickData) -> Option<BarData> {
        // 缓存时间解析结果
        let tick_time = Self::parse_datetime_from_tick(tick)?;
        self.cached_tick_time = Some(tick_time);
        let tick_naive = tick_time.naive_utc();

        // 检查是否在集合竞价时段
        let auction_opening = Self::check_auction_period(tick, &tick_naive);

        if let Some(opening_time) = auction_opening {
            // 在集合竞价时段内
            if !self.in_auction_period {
                // 第一次进入集合竞价：flush前一个连续交易窗口
                // 修复：无论同一天还是跨天，都必须flush
                // 跨天意味着前一天交易结束（EOD），必须强制flush，否则最后窗口数据丢失
                let prev_bar = if self.last_tick.is_some() {
                    self.flush()
                } else {
                    None
                };

                // 开始累积集合竞价ticks
                self.auction_ticks.push(*tick);
                self.in_auction_period = true;
                self.auction_opening_time = Some(opening_time);

                return prev_bar;  // 返回前一个连续交易bar（如果有）
            } else {
                // 已经在集合竞价时段内
                // 【修复问题17补充】检查是否跨天：如果跨天，必须先flush前一天的集合竞价bar
                // 场景：连续多天都只有集合竞价tick（如bb2501的2025-01-02到2025-01-10）
                // 如果不flush，多天的auction_ticks会混在一起，导致使用最后一天的数据
                if !self.auction_ticks.is_empty() {
                    let last_auction_date = to_string_field(&self.auction_ticks.last().unwrap().date);
                    let curr_date = to_string_field(&tick.date);

                    if last_auction_date != curr_date {
                        // 跨天：flush前一天的集合竞价bar
                        let prev_auction_bar = self.flush_auction_bar();

                        // 重置集合竞价状态，开始新的一天
                        self.in_auction_period = false;
                        self.auction_opening_time = None;

                        // 当前tick是新一天的集合竞价tick，重新进入集合竞价时段
                        self.auction_ticks.push(*tick);
                        self.in_auction_period = true;
                        self.auction_opening_time = Some(opening_time);

                        return prev_auction_bar;
                    }
                }

                // 同一天，继续累积
                self.auction_ticks.push(*tick);
                return None;
            }
        } else if self.in_auction_period {
            // 刚离开集合竞价时段（第一个连续交易tick）
            // 生成集合竞价bar，然后处理当前tick
            let auction_bar = self.flush_auction_bar();

            // 重置集合竞价状态
            self.in_auction_period = false;
            self.auction_opening_time = None;

            // 处理当前tick（连续交易的第一个tick）
            let mut tick_minute = floor_to_1min(&tick_time);

            // 【修复】检查是否是收盘时刻（与on_batch逻辑一致）
            // 即使是离开集合竞价的第一个tick，如果它是收盘时刻，也要特殊处理
            let is_closing_tick = Self::is_closing_time(tick, &tick_naive);
            if is_closing_tick {
                tick_minute = tick_minute - chrono::Duration::minutes(1);
            }

            self.init_bar_window(tick, tick_minute);

            return auction_bar;
        }

        // 正常连续交易逻辑（符合左闭右开+收盘tick特殊处理）
        let mut tick_minute = floor_to_1min(&tick_time);

        // 检查是否是收盘时刻（与on_batch逻辑一致）
        // 收盘tick应合并到前一分钟窗口，不生成独立bar
        let is_closing_tick = Self::is_closing_time(tick, &tick_naive);
        if is_closing_tick {
            // 收盘tick特殊处理：分配到前一分钟窗口
            // 例如：23:00:00 → 视为22:59窗口的tick
            // 这样23:00 bar会包含完整的收盘数据，不会生成23:01 bar
            tick_minute = tick_minute - chrono::Duration::minutes(1);
        }

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

    /// 生成集合竞价bar
    /// 特征：timestamp=开盘时间, volume=累计值, oi_diff=0
    fn flush_auction_bar(&mut self) -> Option<BarData> {
        if self.auction_ticks.is_empty() {
            return None;  // 无集合竞价tick，跳过
        }

        let last_tick = *self.auction_ticks.last()?;
        let opening_time = self.auction_opening_time?;

        // 聚合OHLC
        let open_price = self.auction_ticks[0].close;
        let mut high_price = self.auction_ticks[0].close;
        let mut low_price = self.auction_ticks[0].close;
        let close_price = last_tick.close;

        for tick in &self.auction_ticks {
            if tick.close > high_price {
                high_price = tick.close;
            }
            if tick.close < low_price {
                low_price = tick.close;
            }
        }

        // 计算volume和turnover增量（与on_batch逻辑一致）
        // 使用 value < prev_value 检测重置（符合 volume&turnover累计规则说明.md）
        let (volume, turnover) = if let Some(prev) = &self.prev_last_tick {
            // 检测是否重置
            // 根据文档，volume和turnover同步重置，但为了稳妥，分别检测
            let volume_reset = last_tick.volume < prev.volume;
            let turnover_reset = last_tick.turnover < prev.turnover;

            let vol = if volume_reset {
                last_tick.volume  // 重置（如夜盘集合竞价），直接使用累积值
            } else {
                last_tick.volume.saturating_sub(prev.volume)  // 正常差分（如日盘集合竞价）
            };

            let turn = if turnover_reset {
                last_tick.turnover  // 重置，直接使用累积值
            } else {
                (last_tick.turnover - prev.turnover).max(0.0)  // 正常差分
            };

            (vol, turn)
        } else {
            // 第一个bar（文件开头），直接使用累积值
            (last_tick.volume, last_tick.turnover)
        };

        // 修复：即使 volume=0 也生成bar，保留昨结算价等信息
        // 删除了之前的跳过逻辑，确保数据连续性和 oi_diff 计算正确

        let contract = to_string_field(&last_tick.contract);
        let comd = to_string_field(&last_tick.comd);
        let exchange = to_string_field(&last_tick.exchange);
        let date = to_string_field(&last_tick.date);
        let trade_time = opening_time.format("%Y-%m-%d %H:%M:%S").to_string();

        // 计算oi_diff：只有文件第一个bar为0，其余所有bar（包括集合竞价）计算真实diff
        let oi_diff = if let Some(prev) = &self.prev_last_tick {
            last_tick.open_interest.wrapping_sub(prev.open_interest) as i32
        } else {
            // 文件第一个bar，无前置数据
            0
        };

        let bar = BarData {
            contract: contract.clone(),
            contract_yymm: extract_contract_yymm(&contract, &date),
            comd,
            exchange,
            date,
            trade_time,
            open: open_price,
            high: high_price,
            low: low_price,
            close: close_price,
            prev_close: f64::NAN,  // 集合竞价bar无prev_close
            pre_settle: last_tick.pre_settle,
            volume,
            turnover,
            open_interest: last_tick.open_interest,
            open_interest_diff: oi_diff,  // 完全连续计算（方案A）
            bid_price_1: last_tick.bid_price_1,
            bid_volume_1: last_tick.bid_volume_1,
            ask_price_1: last_tick.ask_price_1,
            ask_volume_1: last_tick.ask_volume_1,
            mid_price: last_tick.mid_price,
            vwap: if volume == 0 { 0.0 } else { turnover / volume as f64 },
            log_return: f64::NAN,  // 集合竞价bar无法计算log_return
            maturity_month: 0,
            maturity_day: 0,
        };

        // 更新prev_last_tick为集合竞价的最后一个tick
        // 这样后续的连续交易bar可以正确计算差分
        self.prev_last_tick = Some(last_tick);

        // 清空集合竞价缓冲区
        self.auction_ticks.clear();

        Some(bar)
    }

    pub fn flush(&mut self) -> Option<BarData> {
        // 【修复问题21】文件结束时，如果没有实际数据，清空集合竞价状态
        // 场景：最后交易日已停夜盘，文件结束时last_tick和auction_ticks都为空
        // 但in_auction_period可能还是true，auction_opening_time残留旧值
        // 此时应该清空状态，避免生成错误的空bar
        if self.last_tick.is_none() && self.auction_ticks.is_empty() {
            self.in_auction_period = false;
            self.auction_opening_time = None;
            return None;
        }

        // 【修复问题17】优先flush集合竞价缓冲区
        // 场景：程序结束时，如果只有集合竞价tick而无连续交易tick
        // 例如：bb2501 2025-01-02只有08:59:00集合竞价tick和15:02:43收盘后tick
        // 此时auction_ticks有数据但从未触发flush_auction_bar()，需要在这里flush
        if !self.auction_ticks.is_empty() && self.in_auction_period {
            return self.flush_auction_bar();
        }

        if self.last_tick.is_none() {
            // 当前分钟没有任何 tick，到达时间后补空 bar（根据 prev_last_tick 构造）
            if let Some(prev_last_tick) = self.prev_last_tick {
                let contract = to_string_field(&prev_last_tick.contract);
                let comd = to_string_field(&prev_last_tick.comd);
                let exchange = to_string_field(&prev_last_tick.exchange);
                let date = to_string_field(&prev_last_tick.date);
                // 修复：不进行时区转换，直接格式化
                // current_bar_minute已经是正确的时间（UTC，但实际表示的是北京时间）
                let trade_time = align_to_bar_minute(
                    &self.current_bar_minute.unwrap_or_else(|| Utc::now())
                )
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

        // 修复：优先使用current_bar_minute计算trade_time
        // 这样可以正确处理收盘tick（23:00:00合并到22:59窗口）的情况
        // 符合左闭右开+收盘特殊处理+标注结束时间的行业标准
        let trade_time = if let Some(minute) = self.current_bar_minute {
            // 使用current_bar_minute + 1min作为bar时间（标注结束时间）
            // 例如：22:59窗口（包含23:00:00收盘tick）→ 23:00:00 bar
            align_to_bar_minute(&minute)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        } else {
            // 降级方案：第一个bar可能没有current_bar_minute，使用last_tick时间
            let parsed_time = Self::parse_datetime_from_tick(&last_tick)?;
            align_to_bar_minute(&parsed_time)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        };

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
            // 第一个bar：直接使用当前累计值，prev_close设为NaN
            // 注意：第一个bar的oi_diff应该为0（没有前置bar无法计算差分）
            (
                last_tick.volume,
                last_tick.turnover,
                0,  // 修正：第一个bar的oi_diff应该为0，而不是open_interest本身
                f64::NAN  // 第一个bar的prev_close设为NaN（Parquet存储为NULL）
            )
        };

        // 计算 log_return（当prev_close为NaN时，log_return也会是NaN）
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
        // 包括连续交易时段和集合竞价时段
        // 如果该品种没有交易时段配置，则跳过交易时段检查（允许通过）
        let trading_ranges = TRADE_SESSION_MAP.get_trading_ranges(&comd);
        if trading_ranges.is_empty() {
            return true;  // 没有配置则不过滤
        }

        // 检查是否在连续交易时段
        if TRADE_SESSION_MAP.is_in_trading_session(&comd, dt.naive_utc()) {
            return true;
        }

        // 检查是否在集合竞价时段
        let auction_periods = TRADE_SESSION_MAP.get_auction_periods(&comd);
        let tick_time = dt.naive_utc().time();
        for (auction_start, auction_end) in auction_periods {
            if tick_time >= auction_start && tick_time < auction_end {
                return true;  // 在集合竞价时段，允许通过
            }
        }

        false  // 既不在连续交易时段，也不在集合竞价时段，过滤掉
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

    /// 检测tick是否在集合竞价时段
    /// 返回 Some(opening_time) 如果在集合竞价时段，否则返回 None
    fn check_auction_period(tick: &TickData, tick_datetime: &NaiveDateTime) -> Option<NaiveDateTime> {
        let comd = to_string_field(&tick.comd);
        let tick_time = tick_datetime.time();

        // 获取该品种的集合竞价时段配置
        let auction_periods = TRADE_SESSION_MAP.get_auction_periods(&comd);

        for (auction_start, auction_end) in auction_periods {
            // 检查tick时间是否在集合竞价时段内 [auction_start, auction_end)
            if tick_time >= auction_start && tick_time < auction_end {
                // 构造开盘时间：使用tick的日期 + auction_end时间
                let opening_datetime = tick_datetime.date().and_time(auction_end);
                return Some(opening_datetime);
            }
        }

        None
    }

    /// 检查是否是收盘时刻（与on_batch逻辑一致）
    ///
    /// 收盘tick应该合并到前一分钟窗口，遵循左闭右开+收盘特殊处理规则
    /// 例如：23:00:00是收盘时刻，应累积到22:59窗口，生成23:00 bar
    ///
    /// # Arguments
    /// * `tick` - 待检查的tick数据
    /// * `tick_datetime` - tick的解析时间（NaiveDateTime）
    ///
    /// # Returns
    /// * `true` - 如果tick时间精确等于任一交易时段的收盘时刻
    /// * `false` - 否则
    #[inline]
    fn is_closing_time(tick: &TickData, tick_datetime: &NaiveDateTime) -> bool {
        let comd = to_string_field(&tick.comd);
        let tick_time = tick_datetime.time();

        // 获取该品种的交易时段配置（支持多时段，如夜盘+日盘）
        let trading_ranges = TRADE_SESSION_MAP.get_trading_ranges(&comd);

        // 检查tick时间是否精确匹配任一时段的收盘时刻
        for (_start, end) in trading_ranges {
            if tick_time == end {
                return true;
            }
        }

        false
    }

}
