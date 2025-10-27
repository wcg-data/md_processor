use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

use chrono::{NaiveTime, NaiveDateTime};
use csv::Reader;
use once_cell::sync::Lazy;

#[derive(Debug, Clone)]
pub struct TradeTimeRange {
    pub start: NaiveTime,
    pub end: NaiveTime,
    pub auction_start: Option<NaiveTime>,  // 集合竞价开始时间
    pub auction_end: Option<NaiveTime>,    // 集合竞价结束时间（=开盘时间）
}

#[derive(Default, Debug)]
pub struct TradeSessionMap {
    pub sessions: HashMap<String, Vec<TradeTimeRange>>,
}

impl TradeSessionMap {
    pub fn load_from_csv<P: AsRef<Path>>(path: P) -> Self {
        let file = File::open(path).expect("Cannot open trade_session.csv");
        let mut rdr = Reader::from_reader(file);
        let mut map = HashMap::new();

        for result in rdr.records() {
            let record = result.unwrap();
            let comd = record[0].trim().to_string();

            let auction_str = record[1].trim();
            let open_str = record[2].trim();
            let close_str = record[3].trim();

            let open = NaiveTime::parse_from_str(open_str, "%H:%M:%S").unwrap();
            let close = NaiveTime::parse_from_str(close_str, "%H:%M:%S").unwrap();

            // 解析或推断集合竞价时间
            let (auction_start, auction_end) = if !auction_str.is_empty() {
                // CSV中配置了auction_time，直接解析
                let auction_time = NaiveTime::parse_from_str(auction_str, "%H:%M:%S").unwrap();
                (Some(auction_time), Some(open))
            } else {
                // 未配置，根据opening_time推断
                Self::infer_auction_time(open)
            };

            map.entry(comd)
                .or_insert_with(Vec::new)
                .push(TradeTimeRange {
                    start: open,
                    end: close,
                    auction_start,
                    auction_end,
                });
        }

        Self { sessions: map }
    }

    /// 根据开盘时间推断集合竞价时间
    /// - 09:00:00 → 08:55:00 (商品期货日盘)
    /// - 09:30:00 → 09:25:00 (股指期货)
    /// - 21:00:00 → 20:55:00 (夜盘)
    /// - 其他 → None (无集合竞价)
    fn infer_auction_time(opening_time: NaiveTime) -> (Option<NaiveTime>, Option<NaiveTime>) {
        let auction_start = if opening_time == NaiveTime::from_hms_opt(9, 0, 0).unwrap() {
            // 09:00开盘 → 08:55集合竞价
            Some(NaiveTime::from_hms_opt(8, 55, 0).unwrap())
        } else if opening_time == NaiveTime::from_hms_opt(9, 30, 0).unwrap() {
            // 09:30开盘 → 09:25集合竞价 (股指)
            Some(NaiveTime::from_hms_opt(9, 25, 0).unwrap())
        } else if opening_time == NaiveTime::from_hms_opt(21, 0, 0).unwrap() {
            // 21:00开盘 → 20:55集合竞价 (夜盘)
            Some(NaiveTime::from_hms_opt(20, 55, 0).unwrap())
        } else {
            None
        };

        if let Some(start) = auction_start {
            (Some(start), Some(opening_time))
        } else {
            (None, None)
        }
    }

    pub fn is_in_trading_session(&self, comd: &str, datetime: NaiveDateTime) -> bool {
        let time = datetime.time(); // 在函数内部提取时间部分
        
        match self.sessions.get(comd) {
            Some(ranges) => {
                ranges.iter().any(|r| {
                    if r.start <= r.end {
                        // 正常区间，如 09:00-10:15
                        time >= r.start && time <= r.end
                    } else {
                        // 跨午夜，如 21:00-01:00
                        time >= r.start || time <= r.end
                    }
                })
            }
            None => false,
        }
    }

    /// 获取所有品种的交易时间范围，用于向量化过滤
    pub fn get_all_trading_ranges(&self) -> Vec<(NaiveTime, NaiveTime)> {
        let mut all_ranges = Vec::new();

        for ranges in self.sessions.values() {
            for range in ranges {
                if range.start <= range.end {
                    // 正常区间
                    all_ranges.push((range.start, range.end));
                } else {
                    // 跨午夜区间，拆分为两个区间
                    // 修复：使用 23:59:59.999 确保包含所有 23:59:xx.xxx 的时间（包括 23:59:59.500 等带毫秒的时间戳）
                    all_ranges.push((range.start, NaiveTime::from_hms_milli_opt(23, 59, 59, 999).unwrap()));
                    all_ranges.push((NaiveTime::from_hms_opt(0, 0, 0).unwrap(), range.end));
                }
            }
        }

        // 去重并排序
        all_ranges.sort();
        all_ranges.dedup();
        all_ranges
    }

    /// 获取特定品种的交易时间范围
    pub fn get_trading_ranges(&self, comd: &str) -> Vec<(NaiveTime, NaiveTime)> {
        let mut ranges = Vec::new();

        if let Some(time_ranges) = self.sessions.get(comd) {
            for range in time_ranges {
                if range.start <= range.end {
                    // 正常区间
                    ranges.push((range.start, range.end));
                } else {
                    // 跨午夜区间，拆分为两个区间
                    // 修复：使用 23:59:59.999 确保包含所有 23:59:xx.xxx 的时间（包括 23:59:59.500 等带毫秒的时间戳）
                    ranges.push((range.start, NaiveTime::from_hms_milli_opt(23, 59, 59, 999).unwrap()));
                    ranges.push((NaiveTime::from_hms_opt(0, 0, 0).unwrap(), range.end));
                }
            }
        }

        ranges
    }

    /// 获取特定品种的集合竞价时段
    /// 返回 Vec<(auction_start, auction_end/opening_time)>
    ///
    /// 示例:
    /// - ag品种: [(20:55, 21:00), (08:55, 09:00)] (夜盘+日盘)
    /// - IF品种: [(09:25, 09:30)] (仅日盘)
    pub fn get_auction_periods(&self, comd: &str) -> Vec<(NaiveTime, NaiveTime)> {
        let mut auction_periods = Vec::new();

        if let Some(time_ranges) = self.sessions.get(comd) {
            for range in time_ranges {
                if let (Some(auction_start), Some(auction_end)) = (range.auction_start, range.auction_end) {
                    auction_periods.push((auction_start, auction_end));
                }
            }
        }

        auction_periods
    }

}


/* ---------- 全局静态，程序启动第一次用到时自动加载 ---------- */
pub static TRADE_SESSION_MAP: Lazy<TradeSessionMap> = Lazy::new(|| {
    // 视情况改成绝对路径或 env 变量
    TradeSessionMap::load_from_csv("/root/project/md_processor/trade_session.csv")
});

// // 测试代码
// fn main() {
//     let session = TradeSessionMap::load_from_csv("trade_session.csv");
//     println!("{:?}", session);
//     let sample_time = NaiveTime::parse_from_str("23:00:00", "%H:%M:%S").unwrap();
//     let sample_time2 = NaiveTime::parse_from_str("22:30:00", "%H:%M:%S").unwrap();
//     let sample_time3 = NaiveTime::parse_from_str("01:00:00", "%H:%M:%S").unwrap();

//     println!("ni x:00:00 -> {}", session.is_in_trading_session("nr", sample_time));      // true
//     println!("ni 22:30:00 -> {}", session.is_in_trading_session("ni", sample_time2));     // true（夜盘）
//     println!("ni 01:00:00 -> {}", session.is_in_trading_session("ni", sample_time3));     // true（夜盘跨日）

//     println!("SA x:00:00 -> {}", session.is_in_trading_session("SA", sample_time));      // true
//     println!("SA 22:30:00 -> {}", session.is_in_trading_session("SA", sample_time2));     // false
// }
