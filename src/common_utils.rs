// common_utils, 处理字符串和时间相关函数

use chrono::{DateTime, Timelike, Utc};


pub fn to_string_field(raw: &[u8]) -> String {
    String::from_utf8_lossy(raw).trim_end_matches('\0').to_string()
}

pub fn extract_contract_yymm(contract: &str, date: &str) -> String {
    let digits: String = contract.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.len() == 4 {
        digits
    } else if digits.len() == 3 {
        let year_prefix = date.get(2..4).unwrap_or("00");
        format!("{}{}", year_prefix, digits)
    } else {
        "".to_string()
    }
}

pub fn align_to_bar_minute(ts: &DateTime<Utc>) -> DateTime<Utc> {
    ts.with_second(0).unwrap().with_nanosecond(0).unwrap() + chrono::Duration::minutes(1)
}

pub fn floor_to_minute(ts: &DateTime<Utc>) -> DateTime<Utc> {
    ts.with_second(0).unwrap().with_nanosecond(0).unwrap()
}