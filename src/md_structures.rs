use crate::kafka_client;

use serde::{Serialize, Deserialize};

/// TickData行情快照，需与C++结构体完全一致
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct TickData {
    pub contract: [u8; 9],
    pub comd: [u8; 4],
    pub exchange: [u8; 7],
    pub date: [u8; 11],
    pub trade_time: [u8; 24],
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub pre_settle: f64,
    pub volume: u32,
    pub turnover: f64,
    pub open_interest: u32,
    pub bid_price_1: f64,
    pub bid_volume_1: u32,
    pub ask_price_1: f64,
    pub ask_volume_1: u32,
    pub mid_price: f64,
}

impl TickData {
    /// 打印行情快照
    ///
    /// Args:
    ///     &self
    pub fn print(&self) {
        println!(
            "{},{},{},{},{},{:.5},{:.5},{:.5},{:.5},{:.5},{},{:.5},{},{:.5},{},{:.5},{},{:.5}",
            String::from_utf8_lossy(&self.contract).trim_end_matches('\0'),
            String::from_utf8_lossy(&self.comd).trim_end_matches('\0'),
            String::from_utf8_lossy(&self.exchange).trim_end_matches('\0'),
            String::from_utf8_lossy(&self.date).trim_end_matches('\0'),
            String::from_utf8_lossy(&self.trade_time).trim_end_matches('\0'),
            self.open,
            self.high,
            self.low,
            self.close,
            self.pre_settle,
            self.volume,
            self.turnover,
            self.open_interest,
            self.bid_price_1,
            self.bid_volume_1,
            self.ask_price_1,
            self.ask_volume_1,
            self.mid_price
        );
    }
}

// BarData, 纯Rust结构体
// #[derive(Debug, Clone)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bar1Min {
    pub contract: String,         // 合约代码，如 "cu2505"
    pub contract_yymm: String,    // 合约年月，如 "2505"
    pub comd: String,             // 品种，如 "cu"
    pub exchange: String,         // 交易所，如 "SHFE"
    pub date: String,             // 日期，如 "2025-04-13"
    pub trade_time: String,       // 时间戳，如 "2025-04-13 09:30:00.123"

    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub pre_settle: f64,

    pub volume: u32,
    pub turnover: f64,
    pub open_interest: u32,
    pub open_interest_diff: i32,
    pub bid_price_1: f64,
    pub bid_volume_1: u32,
    pub ask_price_1: f64,
    pub ask_volume_1: u32,
    pub mid_price: f64,
    pub vwap: f64,
    pub maturity_month: i16,
    pub maturity_day: i16,
}

impl Bar1Min {

    /// 打印分钟 K 线数据（CSV 格式）
    pub fn print(&self) {
        // use chrono::Local;
        // use std::time::SystemTime;
        // let now = SystemTime::now();
        // let datetime: chrono::DateTime<Local> = now.into();
        // println!("合并{}: {}.{:09}", self.contract,
        //     datetime.format("%Y-%m-%d %H:%M:%S"),
        //     now.duration_since(SystemTime::UNIX_EPOCH).unwrap().subsec_nanos()
        // );
        println!(
            ">>> {},{},{},{},{},{:.5},{:.5},{:.5},{:.5},{:.5},{},{:.5},{:.5},{},{:.5},{},{:.5},{},{:.5},{:.5}",
            self.contract,
            self.contract_yymm,
            self.comd,
            self.exchange,
            self.trade_time,
            self.open,
            self.high,
            self.low,
            self.close,
            self.pre_settle,
            self.volume,
            self.turnover,
            self.open_interest,
            self.open_interest_diff,
            self.bid_price_1,
            self.bid_volume_1,
            self.ask_price_1,
            self.ask_volume_1,
            self.mid_price,
            self.vwap
        );
    }


    // /// 向 Kafka 发送合约行情数据（Avro 格式）
    // pub fn send_to_kafka(&self) {
    //     // 将 Bar1Min 对象序列化为 Avro 格式
    //     // let data = to_avro(self).expect("Failed to serialize Bar1Min to Avro");

    //     // 将序列化后的 Avro 数据发送到 Kafka
    //     let topic = "test_topic"; // Kafka 主题
    //     let key = &self.contract; // 使用合约代码作为 key

    //     // 调用 Kafka 发送函数
    //     kafka_client::send_message(topic, key, &data);
    // }
}



// /// 分钟K线Bar结构，兼容C结构体
// #[repr(C)]
// #[derive(Debug, Clone, Copy)]
// pub struct Bar1Min {
//     pub contract: [c_char; 9],          // 合约代码
//     pub contract_yymm: [c_char; 5],     // 合约年月
//     pub comd: [c_char; 4],              // 品种代码
//     pub exchange: [c_char; 7],          // 交易所代码
//     pub date: [c_char; 11],             // 日期
//     pub trade_time: [c_char; 24],       // 交易时间
//     pub open: f64,                      // 开盘价
//     pub high: f64,                      // 最高价
//     pub low: f64,                       // 最低价
//     pub close: f64,                     // 收盘价
//     pub pre_settle: f64,                // 昨结算价
//     pub volume: u32,                    // 成交量
//     pub turnover: f64,                  // 成交额
//     pub open_interest: f64,             // 持仓量
//     pub open_interest_diff: i32,        // 持仓量变化
//     pub bid_price_1: f64,               // 买一价
//     pub bid_volume_1: u32,              // 买一量
//     pub ask_price_1: f64,               // 卖一价
//     pub ask_volume_1: u32,              // 卖一量
//     pub mid_price: f64,                 // 中间价
//     pub vwap: f64,                      // 成交量加权平均价
//     pub maturity_month: i16,            // 距离到期月份的月份数
//     pub maturity_day: i16,              // 距离到期月份第一天的天数
// }

// impl Bar1Min {
//     /// 打印分钟K线数据
//     ///
//     /// Args:
//     ///     &self
//     pub fn print(&self) {
//         println!(
//             "{},{},{},{},{},{:.2},{:.2},{:.2},{:.2},{:.2},{},{:.2},{:.2},{},{:.2},{},{:.2},{},{:.2},{:.2}",
//             unsafe { std::ffi::CStr::from_ptr(self.contract.as_ptr()).to_string_lossy() },
//             unsafe { std::ffi::CStr::from_ptr(self.contract_yymm.as_ptr()).to_string_lossy() },
//             unsafe { std::ffi::CStr::from_ptr(self.comd.as_ptr()).to_string_lossy() },
//             unsafe { std::ffi::CStr::from_ptr(self.exchange.as_ptr()).to_string_lossy() },
//             unsafe { std::ffi::CStr::from_ptr(self.trade_time.as_ptr()).to_string_lossy() },
//             self.open,
//             self.high,
//             self.low,
//             self.close,
//             self.pre_settle,
//             self.volume,
//             self.turnover,
//             self.open_interest,
//             self.open_interest_diff,
//             self.bid_price_1,
//             self.bid_volume_1,
//             self.ask_price_1,
//             self.ask_volume_1,
//             self.mid_price,
//             self.vwap
//         );
//     }
// }