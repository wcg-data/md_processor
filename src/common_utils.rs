// common_utils, 处理字符串和时间相关函数

use chrono::{DateTime, Timelike, Utc, NaiveDate, Datelike, NaiveDateTime, NaiveTime};
use chrono::TimeZone;
use polars::prelude::*;
use std::collections::{HashMap, HashSet};
use crate::trade_session_loader::TRADE_SESSION_MAP;
use anyhow;

/// 将字符串转换为固定长度的字节数组（用于 C++ 兼容的结构体）
pub fn str_to_fixed<const N: usize>(s: &str) -> [u8; N] {
    let mut buf = [0u8; N];
    let bytes = s.as_bytes();
    let len = bytes.len().min(N);
    buf[..len].copy_from_slice(&bytes[..len]);
    buf
}

pub fn to_string_field(raw: &[u8]) -> String {
    String::from_utf8_lossy(raw).trim_end_matches('\0').to_string()
}

/// 提取合约年月：取contract的最后四位数字，若为三位数字则用date年份第三位补全
/// 支持多种合约格式：bb1710 (6位), rb2510 (6位), IF2507 (6位), AP010 (5位,3位数字)
pub fn extract_contract_yymm(contract: &str, date: &str) -> String {
    // 提取合约中的数字部分
    let digits: String = contract.chars().filter(|c| c.is_ascii_digit()).collect();

    if digits.len() >= 4 {
        // 取最后四位（处理 >= 4 位数字的情况）
        digits.chars().rev().take(4).collect::<String>().chars().rev().collect()
    } else if digits.len() == 3 {
        // 三位数字，需要用年份第三位补全
        // 从date中提取年份第三位（如2017年的1）
        let year_third_digit = date.chars()
            .nth(2)  // 年份的第三位（索引2）
            .unwrap_or('0');
        format!("{}{}", year_third_digit, &digits)
    } else {
        // 不足3位数字，返回原数字
        digits
    }
}

pub fn align_to_bar_minute(ts: &DateTime<Utc>) -> DateTime<Utc> {
    ts.with_second(0).unwrap().with_nanosecond(0).unwrap() + chrono::Duration::minutes(1)
}

pub fn floor_to_1min(ts: &DateTime<Utc>) -> DateTime<Utc> {
    ts.with_second(0).unwrap().with_nanosecond(0).unwrap()
}

/// 将时间向上取整到5分钟边界（右边界，符合行业标准）
/// 例如：22:51-22:55 → 22:55, 22:56-23:00 → 23:00
pub fn floor_to_5min(dt: &DateTime<Utc>) -> DateTime<Utc> {
    let ts = dt.timestamp();
    // 向上取整到5分钟边界：((ts + 299) / 300) * 300
    // 如果ts已经是边界（如22:55:00），保持不变
    // 如果ts不是边界（如22:51:00），向上取整到下一个边界（22:55:00）
    let ceiled = ((ts + 299) / 300) * 300;
    Utc.timestamp_opt(ceiled, 0).single().unwrap()
}

/// 从 Polars AnyValue 转换时间戳为 NaiveDateTime
/// 自动检测微秒级（> 1e15）或毫秒级（< 1e15）时间戳
pub fn parse_anyvalue_datetime(timestamp: i64) -> NaiveDateTime {
    if timestamp > 1_000_000_000_000_000 {  // > 1e15，微秒级时间戳
        NaiveDateTime::from_timestamp_micros(timestamp)
            .unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0))
    } else {
        // 毫秒级时间戳
        NaiveDateTime::from_timestamp_millis(timestamp)
            .unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0))
    }
}

/// 计算当前日期距离合约到期月份的月数差
/// contract_yymm 格式: "2505", "2412"
/// date 格式: "2025-04-13"
pub fn calculate_maturity_month(contract_yymm: &str, date: &str) -> i16 {
    if contract_yymm.len() != 4 || date.len() < 10 {
        return 0;
    }
    
    // 解析合约年月
    let contract_year = match contract_yymm[0..2].parse::<i32>() {
        Ok(yy) => 2000 + yy, // "25" -> 2025
        Err(_) => return 0,
    };
    let contract_month = match contract_yymm[2..4].parse::<i32>() {
        Ok(mm) => mm,
        Err(_) => return 0,
    };
    
    // 解析当前日期
    let current_date = match NaiveDate::parse_from_str(date, "%Y-%m-%d") {
        Ok(d) => d,
        Err(_) => return 0,
    };
    
    // 计算月数差
    let current_year = current_date.year();
    let current_month = current_date.month() as i32;
    
    let months_diff = (contract_year - current_year) * 12 + (contract_month - current_month);
    months_diff as i16
}

/// 计算当前日期距离合约到期月份第一天的天数差
/// contract_yymm 格式: "2505", "2412"
/// date 格式: "2025-04-13"
pub fn calculate_maturity_day(contract_yymm: &str, date: &str) -> i16 {
    if contract_yymm.len() != 4 || date.len() < 10 {
        return 0;
    }
    
    // 解析合约年月
    let contract_year = match contract_yymm[0..2].parse::<i32>() {
        Ok(yy) => 2000 + yy, // "25" -> 2025
        Err(_) => return 0,
    };
    let contract_month = match contract_yymm[2..4].parse::<u32>() {
        Ok(mm) => mm,
        Err(_) => return 0,
    };
    
    // 解析当前日期
    let current_date = match NaiveDate::parse_from_str(date, "%Y-%m-%d") {
        Ok(d) => d,
        Err(_) => return 0,
    };
    
    // 构造合约到期月份的第一天
    let maturity_first_day = match NaiveDate::from_ymd_opt(contract_year, contract_month, 1) {
        Some(d) => d,
        None => return 0,
    };
    
    // 计算天数差
    let days_diff = maturity_first_day.signed_duration_since(current_date).num_days();
    days_diff as i16
}

/// 补全缺失的分钟bar：高性能批量操作，正确处理prev_close
/// 从 parquet_processor_on_batch.rs 提取，用于离线和在线处理的代码复用
pub fn fill_missing_minutes(bars_df: DataFrame) -> anyhow::Result<DataFrame> {
    if bars_df.height() == 0 {
        return Ok(bars_df);
    }

    // 获取品种代码
    let comd = match bars_df.column("comd")?.get(0)? {
        AnyValue::String(v) => v.to_string(),
        _ => return Ok(bars_df),
    };

    // 获取该品种的交易时间段
    let trading_ranges = (*TRADE_SESSION_MAP).get_trading_ranges(&comd);
    if trading_ranges.is_empty() {
        println!("警告: 未找到品种 {} 的交易时间配置，跳过补全", comd);
        return Ok(bars_df);
    }

    // 先按时间排序原始数据
    let sorted_bars = bars_df.sort(["trade_time"], SortMultipleOptions::default())?;

    // 收集现有数据信息（包括close和open_interest）
    let mut existing_data: HashMap<String, (usize, f64, u32)> = HashMap::new();
    let mut date_range: HashSet<NaiveDate> = HashSet::new();

    let trade_times = sorted_bars.column("trade_time")?;
    let close_prices = sorted_bars.column("close")?;
    let open_interests = sorted_bars.column("open_interest")?;

    for i in 0..trade_times.len() {
        if let (AnyValue::String(time_str), close_val, oi_val) = (
            trade_times.get(i).unwrap(),
            close_prices.get(i).unwrap(),
            open_interests.get(i).unwrap()
        ) {
            if let Ok(parsed) = NaiveDateTime::parse_from_str(time_str, "%Y-%m-%d %H:%M:%S") {
                let close_price = match close_val {
                    AnyValue::Float64(price) => price,
                    _ => 0.0,
                };
                let open_interest = match oi_val {
                    AnyValue::UInt32(oi) => oi,
                    AnyValue::UInt64(oi) => oi as u32,
                    _ => 0u32,
                };
                existing_data.insert(time_str.to_string(), (i, close_price, open_interest));
                date_range.insert(parsed.date());
            }
        }
    }

    if date_range.is_empty() {
        return Ok(sorted_bars);
    }

    // ===== 修复：先识别哪些(日期, 时段)有原始数据 =====
    // 只补全有原始bar的交易时段，避免在最后交易日停夜盘时错误补全
    let mut session_has_data: HashSet<(NaiveDate, usize)> = HashSet::new();

    for time_str in existing_data.keys() {
        if let Ok(dt) = NaiveDateTime::parse_from_str(time_str, "%Y-%m-%d %H:%M:%S") {
            let time = dt.time();
            let date = dt.date();

            // 检查该bar属于哪个交易时段
            for (session_idx, (start_time, end_time)) in trading_ranges.iter().enumerate() {
                let in_session = if start_time > end_time {
                    // 跨日时段（如21:00-02:30）
                    time >= *start_time || time <= *end_time
                } else {
                    // 正常时段
                    time >= *start_time && time <= *end_time
                };

                if in_session {
                    // 标记该(日期, 时段)有数据
                    session_has_data.insert((date, session_idx));

                    // 如果是跨日时段的后半段（00:00-end_time），也要标记前一天的同一时段
                    if start_time > end_time && time <= *end_time {
                        let prev_date = date - chrono::Duration::days(1);
                        session_has_data.insert((prev_date, session_idx));
                    }
                    break;
                }
            }
        }
    }

    // 一次遍历：只为有原始数据的时段生成时间序列（修复最后交易日停夜盘问题）
    let mut complete_timeline: Vec<NaiveDateTime> = Vec::new();
    for date in &date_range {
        for (session_idx, (start_time, end_time)) in trading_ranges.iter().enumerate() {
            // 只补全有原始bar的时段
            if !session_has_data.contains(&(*date, session_idx)) {
                continue;
            }

            if start_time > end_time {
                // 跨日交易时段（如21:00-02:30）
                // 第一段：当日start_time到23:59
                let mut current_time = date.and_time(*start_time);
                let day_end = date.and_time(NaiveTime::from_hms_opt(23, 59, 0).unwrap());

                while current_time <= day_end {
                    complete_timeline.push(current_time);
                    current_time = current_time + chrono::Duration::minutes(1);
                }

                // 第二段：次日00:00到end_time
                let next_date = *date + chrono::Duration::days(1);
                current_time = next_date.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
                let end_datetime = next_date.and_time(*end_time);

                while current_time <= end_datetime {
                    complete_timeline.push(current_time);
                    current_time = current_time + chrono::Duration::minutes(1);
                }
            } else {
                // 正常日间交易时段
                let mut current_time = date.and_time(*start_time);
                let end_datetime = date.and_time(*end_time);

                while current_time <= end_datetime {
                    complete_timeline.push(current_time);
                    current_time = current_time + chrono::Duration::minutes(1);
                }
            }
        }
    }
    complete_timeline.sort();

    // 优化：一次遍历分离数据，直接使用正确的数据类型
    let mut existing_indices = Vec::<u32>::new();
    let mut missing_times = Vec::<String>::new();
    let mut missing_prev_closes = Vec::<f64>::new();
    let mut missing_open_interests = Vec::<u32>::new();
    let mut last_close_price = 0.0f64;
    let mut last_open_interest = 0u32;

    // 合并existing_data和complete_timeline，按时间排序后统一处理
    // 这样确保auction bar和trading bar都被正确处理，且last_close/oi的更新顺序正确
    let mut all_times = std::collections::HashSet::new();

    // 添加所有existing_data的时间
    for time_str in existing_data.keys() {
        if let Ok(parsed) = NaiveDateTime::parse_from_str(time_str, "%Y-%m-%d %H:%M:%S") {
            all_times.insert(parsed);
        }
    }

    // 添加所有complete_timeline的时间
    for time_point in &complete_timeline {
        all_times.insert(*time_point);
    }

    // 排序所有时间点
    let mut sorted_times: Vec<_> = all_times.into_iter().collect();
    sorted_times.sort();

    // 统一遍历所有时间点
    for time_point in sorted_times {
        let time_str = time_point.format("%Y-%m-%d %H:%M:%S").to_string();

        if let Some(&(row_idx, close_price, open_interest)) = existing_data.get(&time_str) {
            // 现有bar：保留
            existing_indices.push(row_idx as u32);
            last_close_price = close_price;
            last_open_interest = open_interest;
        } else if complete_timeline.contains(&time_point) && last_close_price > 0.0 {
            // 缺失的trading_hours bar且有参考值：补全
            missing_times.push(time_str);
            missing_prev_closes.push(last_close_price);
            missing_open_interests.push(last_open_interest);
        }
        // 其他情况（auction bar等不在complete_timeline中的时间）：不处理
    }

    // 保存长度（在move之前）
    let existing_count = existing_indices.len();
    let missing_count = missing_times.len();

    // 第一步：获取所有现有数据
    let existing_rows_df = if !existing_indices.is_empty() {
        let indices_ca = UInt32Chunked::new("indices", existing_indices);
        sorted_bars.take(&indices_ca)?
    } else {
        DataFrame::empty()
    };

    // 第二步：批量生成所有空bar
    let empty_bars_df = if !missing_times.is_empty() {
        let count = missing_times.len();

        // 从模板获取基础值
        let template_row = sorted_bars.slice(0, 1);
        let schema = sorted_bars.schema();
        let mut series_vec = Vec::new();

        // 预提取常用数据，避免重复克隆
        let missing_times_ref = &missing_times;
        let missing_prev_closes_ref = &missing_prev_closes;
        let missing_open_interests_ref = &missing_open_interests;

        for (field_name, _) in schema.iter() {
            let series = match field_name.as_str() {
                "trade_time" => Series::new("trade_time", missing_times_ref),
                "volume" => Series::new("volume", vec![0u32; count]),
                "turnover" => Series::new("turnover", vec![0.0f64; count]),
                "open_interest" => {
                    // 延续前一个bar的open_interest值
                    Series::new("open_interest", missing_open_interests_ref.clone())
                },
                "open_interest_diff" => Series::new("open_interest_diff", vec![0i32; count]),
                "log_return" => {
                    // 延续价格情况下，log_return为0
                    Series::new("log_return", vec![0.0f64; count])
                },
                "pre_settle" => {
                    // 修复：pre_settle在补全bar中应该保持null
                    // 需要根据模板列的类型来创建Series
                    let template_col = template_row.column("pre_settle")?;
                    let col_dtype = template_col.dtype();
                    match col_dtype {
                        DataType::Float64 => Series::new("pre_settle", vec![f64::NAN; count]),
                        DataType::Null => Series::new_null("pre_settle", count),
                        _ => Series::new("pre_settle", vec![f64::NAN; count]),
                    }
                },
                "prev_close" => {
                    // 使用延续价格
                    Series::new("prev_close", missing_prev_closes_ref.clone())
                },
                "open" | "high" | "low" | "close" => {
                    // 使用延续价格
                    Series::new(field_name, missing_prev_closes_ref.clone())
                },
                "date" => {
                    // 特殊处理date字段：为夜盘时段设置正确的交易日期
                    // 需要根据模板列的实际类型来决定创建String还是Date类型
                    let template_col = template_row.column("date")?;
                    let col_dtype = template_col.dtype();

                    match col_dtype {
                        DataType::Date => {
                            // 如果模板是Date类型，创建Date类型的Series
                            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                            let mut date_values: Vec<i32> = Vec::new();

                            for time_str in missing_times_ref {
                                if let Ok(dt) = NaiveDateTime::parse_from_str(time_str, "%Y-%m-%d %H:%M:%S") {
                                    let hour = dt.hour();
                                    let mut trading_date = dt.date();

                                    // 夜盘前半段（00:00-02:30）属于前一个交易日
                                    if hour < 3 {
                                        trading_date = trading_date - chrono::Duration::days(1);
                                    }

                                    // 计算从epoch开始的天数
                                    let days_since_epoch = (trading_date - epoch).num_days() as i32;
                                    date_values.push(days_since_epoch);
                                } else {
                                    // 解析失败时使用模板值
                                    let template_val = template_col.get(0)?;
                                    if let AnyValue::Date(d) = template_val {
                                        date_values.push(d);
                                    } else {
                                        date_values.push(0);
                                    }
                                }
                            }
                            Series::new("date", date_values).cast(&DataType::Date)?
                        },
                        DataType::String => {
                            // 如果模板是String类型，创建String类型的Series
                            let mut date_values: Vec<String> = Vec::new();

                            for time_str in missing_times_ref {
                                if let Ok(dt) = NaiveDateTime::parse_from_str(time_str, "%Y-%m-%d %H:%M:%S") {
                                    let hour = dt.hour();
                                    let mut trading_date = dt.date();

                                    // 夜盘前半段（00:00-02:30）属于前一个交易日
                                    if hour < 3 {
                                        trading_date = trading_date - chrono::Duration::days(1);
                                    }

                                    date_values.push(trading_date.format("%Y-%m-%d").to_string());
                                } else {
                                    // 解析失败时使用模板值
                                    let template_val = template_col.get(0)?;
                                    if let AnyValue::String(s) = template_val {
                                        date_values.push(s.to_string());
                                    } else {
                                        date_values.push("1970-01-01".to_string());
                                    }
                                }
                            }
                            Series::new("date", date_values)
                        },
                        _ => {
                            // 其他类型，回退到模板值
                            let template_val = template_col.get(0)?;
                            if let Ok(f64_val) = template_val.try_extract::<f64>() {
                                Series::new("date", vec![f64_val; count])
                            } else {
                                Series::new_null("date", count)
                            }
                        }
                    }
                },
                _ => {
                    // 其他字段从模板复制
                    let template_col = template_row.column(field_name)?;
                    let template_val = template_col.get(0)?;

                    // 直接根据列的数据类型来处理
                    let col_dtype = template_col.dtype();

                    match col_dtype {
                        DataType::String => {
                            if let AnyValue::String(s) = template_val {
                                Series::new(field_name, vec![s; count])
                            } else {
                                Series::new(field_name, vec![""; count])
                            }
                        },
                        DataType::Float64 => {
                            if let AnyValue::Float64(f) = template_val {
                                Series::new(field_name, vec![f; count])
                            } else {
                                Series::new(field_name, vec![0.0f64; count])
                            }
                        },
                        DataType::Float32 => {
                            if let AnyValue::Float32(f) = template_val {
                                Series::new(field_name, vec![f; count])
                            } else {
                                Series::new(field_name, vec![0.0f32; count])
                            }
                        },
                        DataType::UInt64 => {
                            if let AnyValue::UInt64(u) = template_val {
                                Series::new(field_name, vec![u; count])
                            } else {
                                Series::new(field_name, vec![0u64; count])
                            }
                        },
                        DataType::UInt32 => {
                            if let AnyValue::UInt32(u) = template_val {
                                Series::new(field_name, vec![u; count])
                            } else {
                                Series::new(field_name, vec![0u32; count])
                            }
                        },
                        DataType::UInt16 => {
                            if let AnyValue::UInt16(u) = template_val {
                                Series::new(field_name, vec![u as u32; count])
                            } else {
                                Series::new(field_name, vec![0u32; count])
                            }
                        },
                        DataType::UInt8 => {
                            if let AnyValue::UInt8(u) = template_val {
                                Series::new(field_name, vec![u as u32; count])
                            } else {
                                Series::new(field_name, vec![0u32; count])
                            }
                        },
                        DataType::Int64 => {
                            if let AnyValue::Int64(i) = template_val {
                                Series::new(field_name, vec![i; count])
                            } else {
                                Series::new(field_name, vec![0i64; count])
                            }
                        },
                        DataType::Int32 => {
                            if let AnyValue::Int32(i) = template_val {
                                Series::new(field_name, vec![i; count])
                            } else {
                                Series::new(field_name, vec![0i32; count])
                            }
                        },
                        DataType::Int16 => {
                            // 特殊处理Int16，因为这是maturity_month和maturity_day的类型
                            if let Ok(i16_col) = template_col.i16() {
                                if let Some(val) = i16_col.get(0) {
                                    Series::new(field_name, vec![val; count])
                                } else {
                                    Series::new(field_name, vec![0i16; count])
                                }
                            } else {
                                // 如果无法转换为i16列，根据字段名使用默认值
                                match field_name.as_str() {
                                    "maturity_month" => Series::new(field_name, vec![1i16; count]),
                                    "maturity_day" => Series::new(field_name, vec![1i16; count]),
                                    _ => Series::new(field_name, vec![0i16; count]),
                                }
                            }
                        },
                        DataType::Int8 => {
                            if let AnyValue::Int8(i) = template_val {
                                Series::new(field_name, vec![i; count])
                            } else {
                                Series::new(field_name, vec![0i8; count])
                            }
                        },
                        DataType::Boolean => {
                            if let AnyValue::Boolean(b) = template_val {
                                Series::new(field_name, vec![b; count])
                            } else {
                                Series::new(field_name, vec![false; count])
                            }
                        },
                        DataType::Null => Series::new_null(field_name, count),
                        DataType::Decimal(_, _) => {
                            // 处理Decimal类型
                            template_col.slice(0, 1).extend_constant(template_val, count - 1).unwrap_or_else(|_| {
                                eprintln!("警告: 无法扩展Decimal类型字段 '{}', 使用空值", field_name);
                                Series::new_null(field_name, count)
                            })
                        },
                        DataType::Date => {
                            // 处理Date类型（复制模板值）
                            if let AnyValue::Date(d) = template_val {
                                Series::new(field_name, vec![d; count]).cast(&DataType::Date).unwrap()
                            } else {
                                Series::new_null(field_name, count)
                            }
                        },
                        _ => {
                            // 对于其他未知类型，尝试提取为f64
                            if let Ok(f64_val) = template_val.try_extract::<f64>() {
                                Series::new(field_name, vec![f64_val; count])
                            } else {
                                // 根据字段名提供合理的默认值
                                match field_name.as_str() {
                                    "maturity_month" | "maturity_day" => {
                                        Series::new(field_name, vec![1i16; count])
                                    },
                                    "bid_volume_1" | "ask_volume_1" => {
                                        Series::new(field_name, vec![0u32; count])
                                    },
                                    "bid_price_1" | "ask_price_1" | "mid_price" | "vwap" => {
                                        Series::new(field_name, vec![0.0f64; count])
                                    },
                                    _ => {
                                        eprintln!("警告: 字段 '{}' 类型 {:?} 无法处理，使用空值", field_name, col_dtype);
                                        Series::new_null(field_name, count)
                                    }
                                }
                            }
                        }
                    }
                }
            };
            series_vec.push(series);
        }

        DataFrame::new(series_vec)?
    } else {
        DataFrame::empty()
    };

    // 第三步：合并现有数据和空bar数据
    let final_result = if existing_rows_df.height() > 0 && empty_bars_df.height() > 0 {
        existing_rows_df.vstack(&empty_bars_df)?
    } else if existing_rows_df.height() > 0 {
        existing_rows_df
    } else {
        empty_bars_df
    };

    // 最终排序
    let mut final_result = final_result.sort(["trade_time"], SortMultipleOptions::default())?;

    // 找到第一个有成交的bar，将其prev_close也设为NULL
    let volume_col = final_result.column("volume")?;
    let mut first_trade_idx: Option<usize> = None;
    for i in 0..volume_col.len() {
        if let AnyValue::UInt32(vol) = volume_col.get(i)? {
            if vol > 0 {
                first_trade_idx = Some(i);
                break;
            }
        }
    }

    if let Some(idx) = first_trade_idx {
        // 将第一个有成交bar的prev_close设为NULL
        let mut all_columns = Vec::new();

        for (col_name, _) in final_result.schema().iter() {
            if col_name == "prev_close" {
                // 替换prev_close列
                let prev_close_col = final_result.column("prev_close")?;
                let mut prev_close_values: Vec<Option<f64>> = Vec::new();

                for i in 0..prev_close_col.len() {
                    if i == idx {
                        prev_close_values.push(None); // 第一个有成交bar的prev_close设为NULL
                    } else if let AnyValue::Float64(val) = prev_close_col.get(i)? {
                        prev_close_values.push(Some(val));
                    } else {
                        prev_close_values.push(None); // 保持原有的NULL值
                    }
                }

                all_columns.push(Series::new("prev_close", prev_close_values));
            } else if col_name == "log_return" {
                // 同时处理log_return列：如果是第一个有成交bar，log_return设为NULL；同时将所有NaN转为NULL
                let log_return_col = final_result.column("log_return")?;
                let mut log_return_values: Vec<Option<f64>> = Vec::new();

                for i in 0..log_return_col.len() {
                    if i == idx {
                        log_return_values.push(None); // 第一个有成交bar的log_return设为NULL
                    } else if let AnyValue::Float64(val) = log_return_col.get(i)? {
                        if val.is_nan() {
                            log_return_values.push(None); // 将NaN转换为NULL
                        } else {
                            log_return_values.push(Some(val));
                        }
                    } else {
                        log_return_values.push(None); // 保持原有的NULL值
                    }
                }

                all_columns.push(Series::new("log_return", log_return_values));
            } else {
                // 其他列保持不变
                all_columns.push(final_result.column(col_name)?.clone());
            }
        }

        final_result = DataFrame::new(all_columns)?;
    } else if final_result.height() > 0 {
        // 特殊情况：所有bar都无成交（volume=0），仍需将第一个bar的prev_close和log_return设为NULL
        // 使用向量化操作高效处理
        let mut all_columns = Vec::new();

        for (col_name, _) in final_result.schema().iter() {
            if col_name == "prev_close" {
                // 将第一个bar的prev_close设为NULL，其余保持不变
                let prev_close_col = final_result.column("prev_close")?;
                let mut prev_close_values: Vec<Option<f64>> = Vec::new();

                for i in 0..prev_close_col.len() {
                    if i == 0 {
                        prev_close_values.push(None); // 第一个bar的prev_close设为NULL
                    } else if let AnyValue::Float64(val) = prev_close_col.get(i)? {
                        prev_close_values.push(Some(val));
                    } else {
                        prev_close_values.push(None);
                    }
                }

                all_columns.push(Series::new("prev_close", prev_close_values));
            } else if col_name == "log_return" {
                // 将第一个bar的log_return设为NULL，同时将所有NaN转为NULL
                let log_return_col = final_result.column("log_return")?;
                let mut log_return_values: Vec<Option<f64>> = Vec::new();

                for i in 0..log_return_col.len() {
                    if i == 0 {
                        log_return_values.push(None); // 第一个bar的log_return设为NULL
                    } else if let AnyValue::Float64(val) = log_return_col.get(i)? {
                        if val.is_nan() {
                            log_return_values.push(None);
                        } else {
                            log_return_values.push(Some(val));
                        }
                    } else {
                        log_return_values.push(None);
                    }
                }

                all_columns.push(Series::new("log_return", log_return_values));
            } else {
                // 其他列保持不变
                all_columns.push(final_result.column(col_name)?.clone());
            }
        }

        final_result = DataFrame::new(all_columns)?;
    }

    Ok(final_result)
}