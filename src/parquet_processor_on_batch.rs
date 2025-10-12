// src/parquet_processor.rs

use std::path::Path;
use chrono::{NaiveDateTime, NaiveDate, Datelike, Timelike, DateTime, Utc};
use std::fs::File;
use polars::prelude::*;
use std::thread;
use std::sync::Arc;


use md_processor::bar1min_aggregator::Bar1MinAggregator;
use md_processor::md_structures::{TickData};
use md_processor::trade_session_loader::TRADE_SESSION_MAP;

use md_processor::common_utils::{calculate_maturity_month, calculate_maturity_day, fill_missing_minutes};
// use md_processor::common_utils::{extract_contract_yymm, to_string_field};


fn str_to_fixed<const N: usize>(s: &str) -> [u8; N] {
    let mut buf = [0u8; N];
    let bytes = s.as_bytes();
    let len = bytes.len().min(N);
    buf[..len].copy_from_slice(&bytes[..len]);
    buf
}

/// 提取合约年月：取contract的最后四位数字，若为三位数字则用date年份第三位补全
fn extract_contract_yymm(contract: &str, date: &str) -> String {
    // 提取合约中的数字部分
    let digits: String = contract.chars().filter(|c| c.is_ascii_digit()).collect();
    
    if digits.len() >= 4 {
        // 取最后四位
        digits.chars().rev().take(4).collect::<String>().chars().rev().collect()
    } else if digits.len() == 3 {
        // 三位数字，需要用年份第三位补全
        // 从date中提取年份第三位（如2017年的1）
        let year_third_digit = date.chars()
            .nth(2)  // 年份的第三位（索引2）
            .unwrap_or('0');
        format!("{}{}", year_third_digit, &digits)
    } else {
        digits
    }
}



/// 从 parquet 文件加载并过滤数据
fn read_tick_from_parquet<P: AsRef<Path>>(parquet_path: P) -> anyhow::Result<DataFrame> {
    let path_str = parquet_path.as_ref().to_str().unwrap();
    
    // 检查文件是否损坏
    match std::fs::metadata(&parquet_path) {
        Ok(metadata) => {
            if metadata.len() < 8 {
                return Err(anyhow::anyhow!("文件太小，可能损坏"));
            }
        },
        Err(e) => return Err(anyhow::anyhow!("无法读取文件元数据: {}", e)),
    }
    
    // 尝试读取文件末尾检查 PAR1 标识
    if let Ok(mut file) = std::fs::File::open(&parquet_path) {
        use std::io::{Read, Seek, SeekFrom};
        if file.seek(SeekFrom::End(-4)).is_ok() {
            let mut buffer = [0u8; 4];
            if file.read_exact(&mut buffer).is_ok() {
                if &buffer != b"PAR1" {
                    return Err(anyhow::anyhow!("文件损坏：缺少 PAR1 结尾标识"));
                }
            }
        }
    }
    
    // 先加载原始数据查看结构
    let raw_df = LazyFrame::scan_parquet(path_str, Default::default())?
        .collect()?;
    
    // println!("原始数据行数: {}", raw_df.height());
    // println!("数据列名: {:?}", raw_df.get_column_names());
    
    // 基于 validate_tick 逻辑的高性能向量化过滤
    // 注意: 不读取原始 tick 数据的 open/high/low 字段，因为这些字段不可靠且未使用
    // OHLC 会在聚合时基于 close 价格重新计算
    let mut result_df = LazyFrame::scan_parquet(path_str, Default::default())?
        .select([
            col("contract"), col("comd"), col("exchange"), col("date"), col("trade_time"),
            col("close"), col("pre_settle"),
            col("volume"), col("turnover"), col("open_interest"),
            col("bid_price_1"), col("bid_volume_1"), col("ask_price_1"), col("ask_volume_1"), col("mid_price"),
        ])
        .filter(
            // 基本数据有效性过滤 - 核心必需字段检查
            col("close").is_not_null()
                .and(col("close").gt(lit(0.0)))
                .and(col("volume").is_not_null())
                .and(col("turnover").is_not_null())
                .and(col("turnover").gt_eq(lit(0.0)))  // turnover >= 0
                .and(col("bid_price_1").is_not_null())
                .and(col("bid_price_1").gt_eq(lit(0.0)))  // bid >= 0（允许0和NaN）
                .and((col("bid_price_1").is_nan()).or(col("bid_price_1").is_finite()))  // 允许NaN，拒绝Infinity
                .and(col("ask_price_1").is_not_null())
                .and(col("ask_price_1").gt_eq(lit(0.0)))  // ask >= 0（允许0和NaN）
                .and((col("ask_price_1").is_nan()).or(col("ask_price_1").is_finite()))  // 允许NaN，拒绝Infinity
                // bid <= ask 检查：只在bid和ask都>0时才要求bid<=ask（与validate_tick保持一致）
                // 如果bid=0或ask=0，则跳过检查；只有当两者都>0时才要求bid<=ask
                .and(
                    col("bid_price_1").eq(lit(0.0))  // bid=0, 通过
                    .or(col("ask_price_1").eq(lit(0.0)))  // ask=0, 通过
                    .or(col("bid_price_1").lt_eq(col("ask_price_1")))  // bid>0且ask>0时要求bid<=ask
                )
                // // volume和turnover一致性校验
                // .and( 
                //     // (volume = 0 且 turnover = 0) 或 (volume > 0 且 turnover > 0)
                //     (col("volume").eq(lit(0)).and(col("turnover").eq(lit(0.0))))
                //     .or(col("volume").gt(lit(0)).and(col("turnover").gt(lit(0.0))))
                // )
                // 字段非空检查
                .and(col("comd").is_not_null())
                .and(col("contract").is_not_null())
                .and(col("trade_time").is_not_null())
        )
        .sort(["trade_time"], SortMultipleOptions::default())
        .collect()?;
    
    // println!("基础过滤后数据行数: {}", result_df.height());

    // // 交易时间过滤（优化后的逐行过滤） - 暂时注释，与on_tick保持一致
    // // 先获取当前品种代码（假设整个文件都是同一品种）
    // let current_comd = match result_df.column("comd")?.get(0)? {
    //     AnyValue::String(v) => v.to_string(),
    //     _ => return Err(anyhow::anyhow!("无法获取品种代码")),
    // };

    // // 获取该品种的交易时间范围
    // let trading_ranges = (*TRADE_SESSION_MAP).get_trading_ranges(&current_comd);

    // if !trading_ranges.is_empty() {
    //     // 快速批量过滤：使用向量化操作提取时间列，然后批量检查
    //     let mut valid_indices = Vec::new();
    //     let trade_time_col = result_df.column("trade_time")?;

    //     for i in 0..result_df.height() {
    //         if let Ok(any_val) = trade_time_col.get(i) {
    //             let time_part = match any_val {
    //                 AnyValue::String(v) => {
    //                     if let Ok(naive_dt) = NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S%.f")
    //                         .or_else(|_| NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S")) {
    //                         naive_dt.time()
    //                     } else {
    //                         continue;
    //                     }
    //                 },
    //                 AnyValue::Datetime(v, _, _) => {
    //                     let naive = if v > 1e15 as i64 {
    //                         NaiveDateTime::from_timestamp_micros(v)
    //                             .unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0))
    //                     } else {
    //                         NaiveDateTime::from_timestamp_millis(v)
    //                             .unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0))
    //                     };
    //                     naive.time()
    //                 },
    //                 _ => continue,
    //             };

    //             // 检查时间是否在任一交易时段内
    //             let is_valid = trading_ranges.iter().any(|(start, end)| {
    //                 time_part >= *start && time_part <= *end
    //             });

    //             if is_valid {
    //                 valid_indices.push(i as u32);
    //             }
    //         }
    //     }

    //     // println!("品种 {} 交易时间过滤后数据行数: {}", current_comd, valid_indices.len());

    //     if valid_indices.is_empty() {
    //         return Err(anyhow::anyhow!("过滤后数据为空，可能是所有tick都不在 {} 的交易时间内", current_comd));
    //     }

    //     // 根据有效索引过滤DataFrame
    //     let indices_ca = UInt32Chunked::from_vec("", valid_indices);
    //     result_df = result_df.take(&indices_ca)?;
    // } else {
    //     println!("警告: 未找到品种 {} 的交易时间配置，跳过交易时间过滤", current_comd);
    // }

    Ok(result_df)
}


/// 为每个 tick 创建分钟窗口
fn create_minute_windows(mut df: DataFrame) -> anyhow::Result<DataFrame> {
    let trade_times = df.column("trade_time")?;
    let minute_windows: Vec<String> = (0..df.height())
        .map(|i| {
            match trade_times.get(i).unwrap_or(AnyValue::Null) {
                AnyValue::String(s) => {
                    if s.len() >= 16 {
                        format!("{}:00", &s[..16])
                    } else {
                        s.to_string()
                    }
                },
                AnyValue::Datetime(v, _unit, _tz) => {
                    let naive = if v > 1e15 as i64 {
                        NaiveDateTime::from_timestamp_micros(v)
                            .unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0))
                    } else {
                        NaiveDateTime::from_timestamp_millis(v)
                            .unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0))
                    };
                    // 向下取整到分钟（与on_tick的floor_to_1min保持一致，用于窗口划分）
                    let truncated = naive.with_second(0).unwrap().with_nanosecond(0).unwrap();
                    truncated.format("%Y-%m-%d %H:%M:00").to_string()
                },
                _ => String::new(),
            }
        })
        .collect();
    
    Ok(df.with_column(Series::new("minute_window", minute_windows))?.clone())
}

/// 执行分组聚合 - 向量化版本，优化常量字段处理
fn perform_group_aggregation(df: DataFrame) -> anyhow::Result<(DataFrame, String, String, String)> {
    // 检查DataFrame是否为空
    if df.height() == 0 {
        return Err(anyhow::anyhow!("DataFrame为空，无法提取常量字段"));
    }

    // 提取常量字段（整个文件都相同的字段）
    let contract = df.column("contract")?.get(0).unwrap().to_string().replace("\"", "");
    let comd = df.column("comd")?.get(0).unwrap().to_string().replace("\"", "");
    let exchange = df.column("exchange")?.get(0).unwrap().to_string().replace("\"", "");
    
    let mut grouped_df = df.lazy()
        .sort(["trade_time"], SortMultipleOptions::default()) // 确保先按时间排序
        .group_by([col("minute_window")])
        .agg([
            // 时间字段处理
            col("date").last().alias("date"),
            col("minute_window").first().alias("trade_time"),

            // OHLC 计算（已向量化）
            col("close").first().alias("open"),
            col("close").max().alias("high"),
            col("close").min().alias("low"),
            col("close").last().alias("close"),

            // 用于跨窗口计算的关键字段
            col("volume").last().alias("last_volume"),
            col("turnover").last().alias("last_turnover"),
            col("open_interest").last().alias("last_open_interest"),
            col("pre_settle").last().alias("pre_settle"),

            // 盘口数据
            col("bid_price_1").last().alias("bid_price_1"),
            col("bid_volume_1").last().alias("bid_volume_1"),
            col("ask_price_1").last().alias("ask_price_1"),
            col("ask_volume_1").last().alias("ask_volume_1"),
            col("mid_price").last().alias("mid_price"),
        ])
        .sort(["trade_time"], SortMultipleOptions::default()) // 确保结果按时间排序
        .collect()?;

    // 调整 trade_time: floor + 1 分钟（与on_tick的align_to_bar_minute输出格式保持一致）
    let trade_time_col = grouped_df.column("trade_time")?;
    let aligned_times: Vec<String> = (0..grouped_df.height())
        .map(|i| {
            match trade_time_col.get(i).unwrap_or(AnyValue::Null) {
                AnyValue::String(s) => {
                    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                        let aligned = dt + chrono::Duration::minutes(1);
                        aligned.format("%Y-%m-%d %H:%M:%S").to_string()
                    } else {
                        s.to_string()
                    }
                },
                _ => String::new(),
            }
        })
        .collect();

    grouped_df = grouped_df.with_column(Series::new("trade_time", aligned_times))?.clone();

    Ok((grouped_df, contract, comd, exchange))
}

/// 向量化计算跨窗口指标
fn build_bars_vectorized(grouped_df: DataFrame, contract: &str, comd: &str, exchange: &str) -> anyhow::Result<DataFrame> {
    // 先执行shift操作
    let shifted_df = grouped_df.lazy()
        .with_columns([
            // 使用shift获取上一行的值（跨窗口）
            col("close").shift(lit(1)).alias("prev_close"),
            col("last_volume").shift(lit(1)).alias("prev_volume"),
            col("last_turnover").shift(lit(1)).alias("prev_turnover"),
            col("last_open_interest").shift(lit(1)).alias("prev_open_interest"),
        ])
        .collect()?;

    let mut bars_df = shifted_df.lazy()
        .with_columns([
            // 计算真正的跨窗口增量
            // 特殊处理：当last < prev时（累计值被重置），直接使用last作为当前bar的volume
            // 这与on_tick的第一个bar逻辑一致：第一个bar直接使用累计值
            when(col("last_volume").lt(col("prev_volume")))
                .then(col("last_volume"))  // 累计值被重置，直接使用当前累计值
                .otherwise(col("last_volume") - col("prev_volume"))  // 正常差分
                .fill_null(col("last_volume").fill_null(0))  // 第一个bar（prev_volume为NULL）用当前值
                .alias("volume"),

            when(col("last_turnover").lt(col("prev_turnover")))
                .then(col("last_turnover"))  // 累计值被重置，直接使用当前累计值
                .otherwise(col("last_turnover") - col("prev_turnover"))  // 正常差分
                .fill_null(col("last_turnover").fill_null(0.0))  // 第一个bar用当前值
                .alias("turnover"),
            
            // 计算open_interest_diff
            // 第一个bar（prev为NULL）时，使用last_open_interest作为oi_diff（与on_tick保持一致）
            (col("last_open_interest").cast(DataType::Int64) - col("prev_open_interest").cast(DataType::Int64))
                .fill_null(col("last_open_interest").cast(DataType::Int64))
                .cast(DataType::Int32)
                .alias("open_interest_diff"),
        ])
        .with_columns([
            // 计算VWAP（必须在volume和turnover计算之后）
            when(col("volume").gt(0))
                .then(col("turnover") / col("volume"))
                .otherwise(lit(0.0))
                .alias("vwap"),
        ])
        .with_columns([
            // 确保数据类型正确
            col("volume").cast(DataType::UInt32),
            col("bid_volume_1").cast(DataType::UInt32),
            col("ask_volume_1").cast(DataType::UInt32),
            col("last_open_interest").alias("open_interest").cast(DataType::UInt32),
        ])
        .select([
            // 按照正确的字段顺序输出（不包含常量字段）
            col("date"),
            col("trade_time"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("prev_close"),
            col("pre_settle"),
            col("volume"),
            col("turnover"),
            col("open_interest"),
            col("open_interest_diff"),
            col("bid_price_1"),
            col("bid_volume_1"),
            col("ask_price_1"),
            col("ask_volume_1"),
            col("mid_price"),
            col("vwap"),
        ])
        .collect()?;

    // 手动计算log_return（使用真实对数，与on_tick保持一致）
    let close_col = bars_df.column("close")?;
    let prev_close_col = bars_df.column("prev_close")?;
    let mut log_returns: Vec<Option<f64>> = Vec::new();

    for i in 0..bars_df.height() {
        let close_val = match close_col.get(i)? {
            AnyValue::Float64(v) => Some(v),
            AnyValue::Null => None,
            _ => None,
        };
        let prev_close_val = match prev_close_col.get(i)? {
            AnyValue::Float64(v) => Some(v),
            AnyValue::Null => None,
            _ => None,
        };

        let log_return = match (close_val, prev_close_val) {
            (Some(close), Some(prev_close)) if prev_close > 0.0 => {
                Some((close / prev_close).ln())
            },
            (Some(_), Some(_)) => Some(f64::NAN), // prev_close <= 0 的情况
            _ => None, // 任一值为NULL时，log_return也为NULL
        };
        log_returns.push(log_return);
    }

    let mut bars_df = bars_df.with_column(Series::new("log_return", log_returns))?;
    
    // 后处理：添加常量字段和计算contract_yymm字段
    let date_col = bars_df.column("date")?;
    let row_count = bars_df.height();
    
    // 使用第一行的date来计算contract_yymm（所有行的date在这个上下文中相同）
    let sample_date = match date_col.get(0).unwrap_or(AnyValue::Null) {
        AnyValue::String(s) => s,
        _ => "",
    };
    let contract_yymm = extract_contract_yymm(contract, sample_date);
    
    // 添加所有常量字段
    // 计算 maturity_month 和 maturity_day
    let mut maturity_months = Vec::with_capacity(row_count);
    let mut maturity_days = Vec::with_capacity(row_count);
    
    let date_col = bars_df.column("date")?;
    for i in 0..row_count {
        let date = match date_col.get(i).unwrap_or(AnyValue::Null) {
            AnyValue::String(s) => s,
            _ => sample_date,
        };
        maturity_months.push(calculate_maturity_month(&contract_yymm, date));
        maturity_days.push(calculate_maturity_day(&contract_yymm, date));
    }

    let bars_df = bars_df
        .with_column(Series::new("contract", vec![contract; row_count]))
        .map_err(|e| anyhow::anyhow!("添加contract列失败: {}", e))?
        .with_column(Series::new("contract_yymm", vec![contract_yymm; row_count]))
        .map_err(|e| anyhow::anyhow!("添加contract_yymm列失败: {}", e))?
        .with_column(Series::new("comd", vec![comd; row_count]))
        .map_err(|e| anyhow::anyhow!("添加comd列失败: {}", e))?
        .with_column(Series::new("exchange", vec![exchange; row_count]))
        .map_err(|e| anyhow::anyhow!("添加exchange列失败: {}", e))?
        .with_column(Series::new("maturity_month", maturity_months))
        .map_err(|e| anyhow::anyhow!("添加maturity_month列失败: {}", e))?
        .with_column(Series::new("maturity_day", maturity_days))
        .map_err(|e| anyhow::anyhow!("添加maturity_day列失败: {}", e))?;
    
    // 重新排序字段以符合要求，并按 trade_time 排序
    let bars_df = bars_df.select([
        "contract",
        "contract_yymm", 
        "comd",
        "exchange",
        "date",
        "trade_time",
        "open",
        "high", 
        "low",
        "close",
        "prev_close",
        "pre_settle",
        "volume",
        "turnover",
        "open_interest",
        "open_interest_diff",
        "bid_price_1",
        "bid_volume_1",
        "ask_price_1", 
        "ask_volume_1",
        "mid_price",
        "vwap",
        "log_return",
        "maturity_month",
        "maturity_day",
    ])?
    .sort(["trade_time"], SortMultipleOptions::default())?;
    
    Ok(bars_df)
}

/// 补全缺失的分钟bar：高性能批量操作，正确处理prev_close
fn fill_missing_minutes_bk(bars_df: DataFrame) -> anyhow::Result<DataFrame> {
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

    // println!("为品种 {} 补全缺失分钟bar (高性能版本)", comd);

    // 先按时间排序原始数据
    let sorted_bars = bars_df.sort(["trade_time"], SortMultipleOptions::default())?;
    
    // 收集现有数据信息
    let mut existing_data: std::collections::HashMap<String, (usize, f64)> = std::collections::HashMap::new();
    let mut date_range: std::collections::HashSet<chrono::NaiveDate> = std::collections::HashSet::new();
    
    let trade_times = sorted_bars.column("trade_time")?;
    let close_prices = sorted_bars.column("close")?;
    
    for i in 0..trade_times.len() {
        if let (AnyValue::String(time_str), close_val) = (trade_times.get(i).unwrap(), close_prices.get(i).unwrap()) {
            if let Ok(parsed) = chrono::NaiveDateTime::parse_from_str(time_str, "%Y-%m-%d %H:%M:%S") {
                let close_price = match close_val {
                    AnyValue::Float64(price) => price,
                    _ => 0.0,
                };
                existing_data.insert(time_str.to_string(), (i, close_price));
                date_range.insert(parsed.date());
            }
        }
    }
    
    if date_range.is_empty() {
        return Ok(sorted_bars);
    }

    // 一次遍历：生成时间序列的同时分离数据，避免重复遍历（修复夜盘跨日问题）
    let mut complete_timeline: Vec<chrono::NaiveDateTime> = Vec::new();
    for date in &date_range {
        for (start_time, end_time) in &trading_ranges {
            if start_time > end_time {
                // 跨日交易时段（如21:00-02:30）
                // 第一段：当日start_time到23:59
                let mut current_time = date.and_time(*start_time);
                let day_end = date.and_time(chrono::NaiveTime::from_hms_opt(23, 59, 0).unwrap());
                
                while current_time <= day_end {
                    complete_timeline.push(current_time);
                    current_time = current_time + chrono::Duration::minutes(1);
                }
                
                // 第二段：次日00:00到end_time
                let next_date = *date + chrono::Duration::days(1);
                current_time = next_date.and_time(chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap());
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
    
    // println!("完整时间序列: {} 个时间点", complete_timeline.len());
    // println!("现有数据: {} 个时间点", existing_data.len());
    

    // 优化：一次遍历分离数据，直接使用正确的数据类型
    let mut existing_indices = Vec::<u32>::new();
    let mut missing_times = Vec::<String>::new();
    let mut missing_prev_closes = Vec::<f64>::new();
    let mut last_close_price = 0.0f64;
    let mut has_seen_first_trade = false;
    
    // 首先找到第一个有成交的时间点
    let mut first_trade_time: Option<chrono::NaiveDateTime> = None;
    let volume_col = sorted_bars.column("volume")?;
    for i in 0..volume_col.len() {
        if let AnyValue::UInt32(vol) = volume_col.get(i)? {
            if vol > 0 {
                if let AnyValue::String(time_str) = trade_times.get(i).unwrap() {
                    if let Ok(parsed) = chrono::NaiveDateTime::parse_from_str(time_str, "%Y-%m-%d %H:%M:%S") {
                        first_trade_time = Some(parsed);
                        break;
                    }
                }
            }
        }
    }
    
    for time_point in complete_timeline {
        // 如果存在第一个有成交的时间点，且当前时间点在其之前，则跳过
        if let Some(first_trade) = first_trade_time {
            if time_point < first_trade {
                continue; // 跳过第一个有成交tick之前的时间点
            }
        }
        
        let time_str = time_point.format("%Y-%m-%d %H:%M:%S").to_string();
        
        if let Some(&(row_idx, close_price)) = existing_data.get(&time_str) {
            // 现有数据
            existing_indices.push(row_idx as u32); // 直接使用u32，避免后续转换
            last_close_price = close_price;
            has_seen_first_trade = true;
        } else if has_seen_first_trade {
            // 只在第一个有成交tick之后补全缺失的bar
            missing_times.push(time_str);
            missing_prev_closes.push(last_close_price);
        }
    }
    
    // 第一步：获取所有现有数据
    let existing_rows_df = if !existing_indices.is_empty() {
        // 直接使用u32类型的indices，无需转换
        let indices_ca = UInt32Chunked::new("indices", existing_indices);
        sorted_bars.take(&indices_ca)?
    } else {
        DataFrame::empty()
    };
    
    // 第二步：批量生成所有空bar
    let empty_bars_df = if !missing_times.is_empty() {
        let count = missing_times.len();
        // println!("批量生成 {} 个空bar...", count);
        
        
        // 从模板获取基础值
        let template_row = sorted_bars.slice(0, 1);
        let schema = sorted_bars.schema();
        let mut series_vec = Vec::new();
        
        // 预提取常用数据，避免重复克隆
        let missing_times_ref = &missing_times;
        let missing_prev_closes_ref = &missing_prev_closes;
        
        for (field_name, _) in schema.iter() {
            let series = match field_name.as_str() {
                "trade_time" => Series::new("trade_time", missing_times_ref),
                "volume" => Series::new("volume", vec![0u32; count]),
                "turnover" => Series::new("turnover", vec![0.0f64; count]),
                "open_interest_diff" => Series::new("open_interest_diff", vec![0i32; count]),
                "log_return" => {
                    // 延续价格情况下，log_return为0
                    Series::new("log_return", vec![0.0f64; count])
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
                    let mut date_values: Vec<String> = Vec::new();
                    for time_str in missing_times_ref {
                        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(time_str, "%Y-%m-%d %H:%M:%S") {
                            let hour = dt.hour();
                            let mut trading_date = dt.date();
                            
                            // 夜盘前半段（00:00-02:30）属于前一个交易日
                            if hour < 3 {
                                trading_date = trading_date - chrono::Duration::days(1);
                            }
                            
                            date_values.push(trading_date.format("%Y-%m-%d").to_string());
                        } else {
                            // 解析失败时使用模板值
                            let template_col = template_row.column("date")?;
                            let template_val = template_col.get(0)?;
                            if let AnyValue::String(s) = template_val {
                                date_values.push(s.to_string());
                            } else {
                                date_values.push("".to_string());
                            }
                        }
                    }
                    Series::new("date", date_values)
                },
                _ => {
                    // 其他字段从模板复制
                    let template_col = template_row.column(field_name)?;
                    let template_val = template_col.get(0)?;
                    
                    // 直接根据列的数据类型来处理，而不是依赖AnyValue的匹配
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
                                // UInt16需要转换为u32
                                Series::new(field_name, vec![u as u32; count])
                            } else {
                                Series::new(field_name, vec![0u32; count])
                            }
                        },
                        DataType::UInt8 => {
                            if let AnyValue::UInt8(u) = template_val {
                                // UInt8需要转换为u32
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
                            // 直接从列中提取值，避免AnyValue匹配问题
                            if let Ok(i16_col) = template_col.i16() {
                                if let Some(val) = i16_col.get(0) {
                                    Series::new(field_name, vec![val; count])
                                } else {
                                    // 如果值为NULL，使用0
                                    Series::new(field_name, vec![0i16; count])
                                }
                            } else {
                                // 如果无法转换为i16列，根据字段名使用默认值
                                match field_name.as_str() {
                                    "maturity_month" => Series::new(field_name, vec![1i16; count]), // 默认1月
                                    "maturity_day" => Series::new(field_name, vec![1i16; count]),   // 默认1天
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
                        _ => {
                            // 对于其他未知类型，打印详细信息以便调试
                            eprintln!("警告: 字段 '{}' 遇到未处理的数据类型: {:?}", field_name, col_dtype);
                            
                            // 对于其他类型，尝试提取为f64
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
                                        // 对于未知字段，使用空值
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
        // 创建一个新的DataFrame，复制所有列，但修改prev_close列
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
                        log_return_values.push(None); // 第一个有成交bar的log_return设为NULL（因为prev_close为NULL）
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
    }
    
    // println!("补全完成，总bar数: {}", final_result.height());
    Ok(final_result)
}


/// 向量化历史数据处理：完全基于polars向量化操作
pub fn process_parquet_optimized<P: AsRef<Path>>(parquet_path: P, output_parquet_path: P) -> anyhow::Result<()> {
    // 1. 读取和过滤数据
    let df = read_tick_from_parquet(&parquet_path)?;

    // 2. 清理bid/ask为0或NaN的情况：将价格设为NaN，volume设为0（Parquet会存储为NULL）
    // 与on_tick的validate_tick保持一致
    let df = df.lazy()
        .with_columns([
            // 将bid_price_1=0或NaN转为NaN，并将对应的volume设为0
            when(col("bid_price_1").eq(lit(0.0)).or(col("bid_price_1").is_nan()))
                .then(lit(f64::NAN))
                .otherwise(col("bid_price_1"))
                .alias("bid_price_1_cleaned"),
            when(col("ask_price_1").eq(lit(0.0)).or(col("ask_price_1").is_nan()))
                .then(lit(f64::NAN))
                .otherwise(col("ask_price_1"))
                .alias("ask_price_1_cleaned"),
            // volume也同步设为0
            when(col("bid_price_1").eq(lit(0.0)).or(col("bid_price_1").is_nan()))
                .then(lit(0u32))
                .otherwise(col("bid_volume_1"))
                .alias("bid_volume_1_cleaned"),
            when(col("ask_price_1").eq(lit(0.0)).or(col("ask_price_1").is_nan()))
                .then(lit(0u32))
                .otherwise(col("ask_volume_1"))
                .alias("ask_volume_1_cleaned"),
        ])
        .with_columns([
            // 重命名回原始字段名
            col("bid_price_1_cleaned").alias("bid_price_1"),
            col("ask_price_1_cleaned").alias("ask_price_1"),
            col("bid_volume_1_cleaned").alias("bid_volume_1"),
            col("ask_volume_1_cleaned").alias("ask_volume_1"),
        ])
        .select([
            // 保留所有原始字段，排除临时清理字段
            col("contract"), col("comd"), col("exchange"), col("date"), col("trade_time"),
            col("close"), col("pre_settle"),
            col("volume"), col("turnover"), col("open_interest"),
            col("bid_price_1"), col("bid_volume_1"), col("ask_price_1"), col("ask_volume_1"), col("mid_price"),
        ])
        .collect()?;

    // 3. 重新计算mid_price：只有bid和ask都有效时才计算，否则设为NaN
    // 修正原始数据中当ask=0时mid_price=bid/2的错误逻辑
    let df = df.lazy()
        .with_columns([
            when(
                col("bid_price_1").is_not_nan().and(col("ask_price_1").is_not_nan())
            )
            .then((col("bid_price_1") + col("ask_price_1")) / lit(2.0))
            .otherwise(lit(f64::NAN))
            .alias("mid_price")
        ])
        .collect()?;

    // 4. 创建分钟窗口
    let df_with_minute = create_minute_windows(df)?;

    // 5. 执行分组聚合（向量化），提取常量字段
    let (grouped_df, contract, comd, exchange) = perform_group_aggregation(df_with_minute)?;

    // 6. 执行跨窗口计算（向量化）
    let mut bars_df = build_bars_vectorized(grouped_df, &contract, &comd, &exchange)?;

    // 7. 补全缺失分钟（可选，失败不影响主流程）
    let mut bars_df = match fill_missing_minutes(bars_df.clone()) {
        Ok(filled) => filled,
        Err(e) => {
            eprintln!("警告: 缺失分钟补全失败: {}, 使用原始数据", e);
            bars_df
        }
    };

    // 8. 直接写入parquet，避免复杂的数据转换
    let file = File::create(output_parquet_path)?;
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut bars_df)?;
    
    Ok(())
}

// =================================1min到5min合并====================================

/// 将NaiveDateTime向下取整到5分钟边界
fn floor_to_5min_naive(dt: &NaiveDateTime) -> NaiveDateTime {
    let ts = dt.and_utc().timestamp();
    DateTime::from_timestamp(ts - (ts % 300), 0).unwrap().naive_utc()
}

/// 将1分钟bar聚合为5分钟bar
fn aggregate_1min_to_5min(mut bars_1min_df: DataFrame) -> anyhow::Result<DataFrame> {
    if bars_1min_df.height() == 0 {
        return Ok(bars_1min_df);
    }
    
    // 1. 为每个1min bar创建5min窗口
    let trade_times = bars_1min_df.column("trade_time")?;
    let minute_5_windows: Vec<String> = (0..bars_1min_df.height())
        .map(|i| {
            match trade_times.get(i).unwrap_or(AnyValue::Null) {
                AnyValue::String(s) => {
                    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                        let floored = floor_to_5min_naive(&dt);
                        floored.format("%Y-%m-%d %H:%M:%S").to_string()
                    } else {
                        s.to_string()
                    }
                },
                _ => String::new(),
            }
        })
        .collect();
    
    let df_with_5min_window = bars_1min_df
        .with_column(Series::new("minute_5_window", minute_5_windows))?
        .clone();
    
    // 2. 提取常量字段（假设整个文件都是同一合约）
    let contract = df_with_5min_window.column("contract")?.get(0).unwrap().to_string().replace("\"", "");
    let comd = df_with_5min_window.column("comd")?.get(0).unwrap().to_string().replace("\"", "");
    let exchange = df_with_5min_window.column("exchange")?.get(0).unwrap().to_string().replace("\"", "");
    
    // 3. 按5分钟窗口分组聚合
    let grouped_5min: DataFrame = df_with_5min_window.lazy()
        .sort(["trade_time"], SortMultipleOptions::default())
        .group_by([col("minute_5_window")])
        .agg([
            // 时间和日期
            col("date").last().alias("date"),
            col("minute_5_window").first().alias("trade_time"),
            
            // OHLC：Open取第一个，High取最高，Low取最低，Close取最后一个
            col("open").first().alias("open"),
            col("high").max().alias("high"),
            col("low").min().alias("low"),
            col("close").last().alias("close"),
            
            // prev_close取第一个bar的prev_close
            col("prev_close").first().alias("prev_close"),
            col("pre_settle").last().alias("pre_settle"),
            
            // 成交量和成交额累计
            col("volume").sum().alias("volume"),
            col("turnover").sum().alias("turnover"),
            
            // 持仓量取最后一个
            col("open_interest").last().alias("open_interest"),
            
            // 持仓量变化：需要计算整个窗口的变化
            col("open_interest_diff").sum().alias("open_interest_diff"),
            
            // 盘口数据取最后一个
            col("bid_price_1").last().alias("bid_price_1"),
            col("ask_price_1").last().alias("ask_price_1"),
            col("bid_volume_1").last().alias("bid_volume_1"),
            col("ask_volume_1").last().alias("ask_volume_1"),
            col("mid_price").last().alias("mid_price"),
            
            // maturity字段继承（取第一个值即可，因为同一5分钟窗口内的值相同）
            col("maturity_month").first().alias("maturity_month"),
            col("maturity_day").first().alias("maturity_day"),
        ])
        .sort(["trade_time"], SortMultipleOptions::default())
        .collect()?;
    
    // 4. 计算VWAP和log_return
    let mut bars_5min = grouped_5min.lazy()
        .with_columns([
            // 计算VWAP
            when(col("volume").gt(0))
                .then(col("turnover") / col("volume"))
                .otherwise(lit(0.0))
                .alias("vwap"),
        ])
        .collect()?;
    
    // 计算log_return（手动计算，正确处理NULL）
    let close_col = bars_5min.column("close")?;
    let prev_close_col = bars_5min.column("prev_close")?;
    let mut log_returns: Vec<Option<f64>> = Vec::new();
    
    for i in 0..bars_5min.height() {
        let close_val = match close_col.get(i)? {
            AnyValue::Float64(v) => Some(v),
            AnyValue::Null => None,
            _ => None,
        };
        let prev_close_val = match prev_close_col.get(i)? {
            AnyValue::Float64(v) => Some(v),
            AnyValue::Null => None,
            _ => None,
        };
        
        let log_return = match (close_val, prev_close_val) {
            (Some(close), Some(prev_close)) if prev_close > 0.0 => {
                Some((close / prev_close).ln())
            },
            (Some(_), Some(_)) => Some(f64::NAN), // prev_close <= 0 的情况
            _ => None, // 任一值为NULL时，log_return也为NULL
        };
        log_returns.push(log_return);
    }
    
    let bars_5min = bars_5min.with_column(Series::new("log_return", log_returns))?;
    
    // 5. 添加常量字段
    let row_count = bars_5min.height();
    
    // 获取第一行的date来计算contract_yymm
    let date_col = bars_5min.column("date")?;
    let sample_date = match date_col.get(0).unwrap_or(AnyValue::Null) {
        AnyValue::String(s) => s,
        _ => "",
    };
    let contract_yymm = extract_contract_yymm(&contract, sample_date);
    
    // 添加所有常量字段
    let bars_5min = bars_5min
        .with_column(Series::new("contract", vec![contract.as_str(); row_count]))?
        .with_column(Series::new("contract_yymm", vec![contract_yymm.as_str(); row_count]))?
        .with_column(Series::new("comd", vec![comd.as_str(); row_count]))?
        .with_column(Series::new("exchange", vec![exchange.as_str(); row_count]))?;
    
    // 6. 重新排序字段以符合标准格式
    let final_df = bars_5min.select([
        "contract",
        "contract_yymm",
        "comd",
        "exchange",
        "date",
        "trade_time",
        "open",
        "high",
        "low",
        "close",
        "prev_close",
        "pre_settle",
        "volume",
        "turnover",
        "open_interest",
        "open_interest_diff",
        "bid_price_1",
        "bid_volume_1",
        "ask_price_1",
        "ask_volume_1",
        "mid_price",
        "vwap",
        "log_return",
        "maturity_month",
        "maturity_day",
    ])?
    .sort(["trade_time"], SortMultipleOptions::default())?;
    
    Ok(final_df)
}

/// 处理1分钟bar文件，生成5分钟bar
pub fn process_1min_to_5min<P: AsRef<Path>>(input_1min_path: P, output_5min_path: P) -> anyhow::Result<()> {
    println!("读取1分钟bar数据: {}", input_1min_path.as_ref().display());
    
    // 1. 读取1分钟bar数据
    let bars_1min_df = LazyFrame::scan_parquet(
        input_1min_path.as_ref().to_str().unwrap(),
        Default::default()
    )?
    .collect()?;
    
    println!("1分钟bar数量: {}", bars_1min_df.height());
    
    // 2. 聚合为5分钟bar
    let mut bars_5min_df = aggregate_1min_to_5min(bars_1min_df)?;
    
    println!("5分钟bar数量: {}", bars_5min_df.height());
    
    // 3. 创建输出目录（如果不存在）
    if let Some(parent) = output_5min_path.as_ref().parent() {
        std::fs::create_dir_all(parent)?;
    }
    
    // 4. 写入parquet文件
    let file = File::create(&output_5min_path)?;
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut bars_5min_df)?;
    
    println!("5分钟bar写入成功: {}", output_5min_path.as_ref().display());
    
    Ok(())
}

// =================================批量处理====================================
/// 发现parquet文件
fn find_parquet_files(dir: &str) -> anyhow::Result<Vec<String>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().map_or(false, |ext| ext == "parquet") {
            if let Some(path_str) = path.to_str() {
                files.push(path_str.to_string());
            }
        }
    }
    files.sort();
    Ok(files)
}

/// 提取合约名
fn extract_contract_name(file_path: &str) -> Option<String> {
    std::path::Path::new(file_path)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .map(|s| s.to_string())
}

/// 最简单的原生并行处理，支持tick->1min和1min->5min两种模式
fn process_batch_simple(data_source: &str, mode: &str, num_threads: usize) -> anyhow::Result<()> {
    let (input_dir, output_dir, input_suffix, output_suffix) = match mode {
        "1min" => {
            // tick -> 1min
            (
                format!("/data/future_data/{}/full_tick", data_source),
                format!("/data/future_data/{}/full_bar_1min", data_source),
                "",
                "_1min"
            )
        },
        "5min" => {
            // 1min -> 5min
            (
                format!("/data/future_data/{}/full_bar_1min", data_source),
                format!("/data/future_data/{}/full_bar_5min", data_source),
                "_1min",
                "_5min"
            )
        },
        _ => return Err(anyhow::anyhow!("不支持的模式: {}", mode)),
    };
    
    println!("扫描目录: {}", input_dir);
    let files = find_parquet_files(&input_dir)?;
    
    if files.is_empty() {
        println!("未发现文件");
        return Ok(());
    }
    
    println!("发现 {} 个文件，使用 {} 个线程", files.len(), num_threads);
    std::fs::create_dir_all(&output_dir)?;
    
    // 分割文件给不同线程
    let chunk_size = (files.len() + num_threads - 1) / num_threads;
    let files_arc = Arc::new(files);
    let output_dir_arc = Arc::new(output_dir);
    let mode_arc = Arc::new(mode.to_string());
    let input_suffix_arc = Arc::new(input_suffix.to_string());
    let output_suffix_arc = Arc::new(output_suffix.to_string());
    
    let mut handles = Vec::new();
    
    for thread_id in 0..num_threads {
        let files_clone = Arc::clone(&files_arc);
        let output_dir_clone = Arc::clone(&output_dir_arc);
        let mode_clone = Arc::clone(&mode_arc);
        let input_suffix_clone = Arc::clone(&input_suffix_arc);
        let output_suffix_clone = Arc::clone(&output_suffix_arc);
        
        let handle = thread::spawn(move || {
            let start = thread_id * chunk_size;
            let end = ((thread_id + 1) * chunk_size).min(files_clone.len());
            
            for i in start..end {
                let file_path = &files_clone[i];
                
                let mut contract = match extract_contract_name(file_path) {
                    Some(name) => name,
                    None => continue,
                };
                
                // 对于1min->5min模式，需要去掉文件名中的_1min后缀
                if mode_clone.as_str() == "5min" && contract.ends_with("_1min") {
                    contract = contract.strip_suffix("_1min").unwrap_or(&contract).to_string();
                }
                
                let output_path = format!("{}/{}{}.parquet", output_dir_clone, contract, output_suffix_clone);
                
                // 跳过已存在文件
                if std::path::Path::new(&output_path).exists() {
                    continue;
                }
                
                let result = match mode_clone.as_str() {
                    "1min" => process_parquet_optimized(file_path, &output_path),
                    "5min" => process_1min_to_5min(file_path, &output_path),
                    _ => Err(anyhow::anyhow!("不支持的模式")),
                };
                
                match result {
                    Ok(()) => println!("✓ 线程{}: {}", thread_id, contract),
                    Err(e) => {
                        let error_msg = e.to_string();
                        if error_msg.contains("PAR1") || error_msg.contains("File out of specification") {
                            eprintln!("⚠ 线程{}: {} - 文件损坏，跳过", thread_id, contract);
                        } else if error_msg.contains("文件太小") {
                            eprintln!("⚠ 线程{}: {} - 文件太小，跳过", thread_id, contract);
                        } else {
                            eprintln!("✗ 线程{}: {} - {}", thread_id, contract, e);
                        }
                    },
                }
            }
        });
        
        handles.push(handle);
    }
    
    // 等待所有线程完成
    for handle in handles {
        handle.join().map_err(|_| anyhow::anyhow!("线程执行失败"))?;
    }
    
    println!("所有线程处理完成");
    Ok(())
}

// //  主函数：并行处理
// fn main() -> anyhow::Result<()> {
//     let args: Vec<String> = std::env::args().collect();
    
//     if args.len() < 2 {
//         println!("使用方法: {} <data_source> [mode] [threads]", args[0]);
//         println!("参数说明:");
//         println!("  data_source: 数据源名称 (如 dongzheng_data)");
//         println!("  mode: 处理模式 (1min 或 5min, 默认1min)");
//         println!("    1min: tick -> 1min bar");
//         println!("    5min: 1min -> 5min bar");
//         println!("  threads: 并发线程数 (默认4)");
//         println!();
//         println!("示例:");
//         println!("  {} dongzheng_data 1min 8     # tick->1min，8线程", args[0]);
//         println!("  {} dongzheng_data 5min 4     # 1min->5min，4线程", args[0]);
//         return Ok(());
//     }
    
//     let data_source = args.get(1).map(|s| s.as_str()).unwrap_or("dongzheng_data");
//     let mode = args.get(2).map(|s| s.as_str()).unwrap_or("1min");
//     let num_threads = args.get(3)
//         .and_then(|s| s.parse::<usize>().ok())
//         .unwrap_or(4);
    
//     if mode != "1min" && mode != "5min" {
//         eprintln!("错误: 不支持的处理模式 '{}', 请使用 '1min' 或 '5min'", mode);
//         return Ok(());
//     }
    
//     println!("=== 原生并行处理 ===");
//     println!("数据源: {}", data_source);
//     println!("处理模式: {}", if mode == "1min" { "tick -> 1min" } else { "1min -> 5min" });
//     println!("线程数: {}", num_threads);
    
//     process_batch_simple(data_source, mode, num_threads)?;
//     Ok(())
// }


// 主函数：单文件处理(用于测试，请勿删除)
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    
    if args.len() < 4 {
        println!("使用方法: {} <source_name> <freq> <contract>", args[0]);
        println!("示例 tick->1min: {} dongzheng_data 1min bb1710", args[0]);
        println!("示例 1min->5min: {} dongzheng_data 5min bb1710", args[0]);
        return Ok(());
    }
    
    let source_name = &args[1];
    let freq = &args[2];
    let contract = &args[3];
    
    let data_dir = "/data/future_data";
    let data_source = format!("{}/{}", data_dir, source_name);

    match freq.as_str() {
        "1min" => {
            // tick -> 1min
            let input_path = format!("{}/full_tick/{}.parquet", data_source, contract);
            let output_path = format!("{}/full_bar_1min/{}_1min.parquet", data_source, contract);
            
            println!("处理模式: tick -> 1min");
            println!("输入文件: {}", input_path);
            println!("输出文件: {}", output_path);
            
            match process_parquet_optimized(&input_path, &output_path) {
                Ok(()) => println!("✓ 成功生成1分钟bar: {}", output_path),
                Err(e) => println!("✗ 处理失败: {}", e),
            }
        },
        "5min" => {
            // 1min -> 5min
            let input_path = format!("{}/full_bar_1min/{}_1min.parquet", data_source, contract);
            let output_path = format!("{}/full_bar_5min/{}_5min.parquet", data_source, contract);
            
            println!("处理模式: 1min -> 5min");
            println!("输入文件: {}", input_path);
            println!("输出文件: {}", output_path);
            
            match process_1min_to_5min(&input_path, &output_path) {
                Ok(()) => println!("✓ 成功生成5分钟bar: {}", output_path),
                Err(e) => println!("✗ 处理失败: {}", e),
            }
        },
        _ => {
            println!("错误: 不支持的频率 '{}', 请使用 '1min' 或 '5min'", freq);
        }
    }

    Ok(())
}
