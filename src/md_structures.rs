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
