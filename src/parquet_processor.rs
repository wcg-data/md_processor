// src/parquet_processor.rs

use std::path::Path;
use chrono::{NaiveDateTime, NaiveDate, Datelike, Timelike};
use std::fs::File;
use polars::prelude::*;
use std::thread;
use std::sync::Arc;


use md_processor::bar1min_aggregator::Bar1MinAggregator;
use md_processor::md_structures::{TickData};
use md_processor::trade_session_loader::TRADE_SESSION_MAP;
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

/// 优化版离线处理：支持数据排序、按合约分组聚合
pub fn process_parquet_on_tick<P: AsRef<Path>>(parquet_path: P,  output_parquet_path: P) -> anyhow::Result<()> {
    let output_parquet_path_display = output_parquet_path.as_ref().display().to_string();
    let df = LazyFrame::scan_parquet(parquet_path.as_ref().to_str().unwrap(), Default::default())?
        .select([
            col("contract"), col("comd"), col("exchange"), col("date"), col("trade_time"),
            col("close"), col("pre_settle"), col("volume"), col("turnover"), col("open_interest"),
            col("bid_price_1"), col("bid_volume_1"), col("ask_price_1"), col("ask_volume_1"), col("mid_price"),
        ])
        // 关键改进：按时间排序，确保tick数据按时间顺序处理
        .sort(["trade_time"], SortMultipleOptions::default())
        .collect()?;

    let mut aggregator = Bar1MinAggregator::new();
    let mut bars = Vec::new();

    for i in 0..df.height() {
        let row = df.get_row(i)?;
        let get_str = |idx| match row.0[idx] {
            AnyValue::String(v) => v,
            _ => "",
        };
        let get_f64 = |idx| match row.0[idx] {
            AnyValue::Float64(v) => v,
            _ => 0.0,
        };
        let get_u32 = |idx| match row.0[idx] {
            AnyValue::Int64(v) => v as u32,
            AnyValue::UInt32(v) => v,
            _ => 0,
        };

        let get_datetime_string = |idx| match &row.0[idx] {
            AnyValue::String(s) => s.to_string(),
            AnyValue::Datetime(v, _, _) => {
                // 自动检测时间戳单位并转换
                let naive = if *v > 1e15 as i64 {
                    // 微秒级时间戳 (> 1e15)
                    NaiveDateTime::from_timestamp_micros(*v)
                        .unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0))
                } else {
                    // 毫秒级时间戳 (< 1e15)  
                    NaiveDateTime::from_timestamp_millis(*v)
                        .unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0))
                };
                
                // 验证年份合理性
                let year = naive.year();
                if year < 2000 || year > 2030 {
                    eprintln!("警告: 检测到异常年份 {}, 原始时间戳: {}", year, *v);
                }
                
                naive.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
            },
            AnyValue::Date(days) => {
                let base = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let date = base + chrono::Duration::days(*days as i64);
                date.format("%Y-%m-%d").to_string()
            },
            _ => "".to_string(),
        };
        

        let tick = TickData {
            contract: str_to_fixed::<9>(get_str(0)),
            comd: str_to_fixed::<4>(get_str(1)),
            exchange: str_to_fixed::<7>(get_str(2)),
            date: str_to_fixed::<11>(&get_datetime_string(3)),
            trade_time: str_to_fixed::<24>(&get_datetime_string(4)),
            open: get_f64(5),
            high: get_f64(5),
            low: get_f64(5),
            close: get_f64(5),
            pre_settle: get_f64(6),
            volume: get_u32(7),
            turnover: get_f64(8),
            open_interest: get_u32(9),
            bid_price_1: get_f64(10),
            bid_volume_1: get_u32(11),
            ask_price_1: get_f64(12),
            ask_volume_1: get_u32(13),
            mid_price: get_f64(14),
            ..Default::default()
        };

        tick.print();

        if !Bar1MinAggregator::validate_tick(&tick) {
            continue;  // 静默跳过无效tick，减少日志输出
        }

        if let Some(bar) = aggregator.on_tick(&tick) {
            // bar.print();
            bars.push(bar);
        }
    }

    if let Some(bar) = aggregator.flush() {
        // bar.print();
        bars.push(bar);
    }

    // 构造 polars DataFrame
    let mut df_out = DataFrame::new(vec![
        Series::new("contract", bars.iter().map(|b| b.contract.as_str()).collect::<Vec<_>>()),
        Series::new("contract_yymm", bars.iter().map(|b| b.contract_yymm.as_str()).collect::<Vec<_>>()),
        Series::new("comd", bars.iter().map(|b| b.comd.as_str()).collect::<Vec<_>>()),
        Series::new("exchange", bars.iter().map(|b| b.exchange.as_str()).collect::<Vec<_>>()),
        Series::new("date", bars.iter().map(|b| b.date.as_str()).collect::<Vec<_>>()),
        Series::new("trade_time", bars.iter().map(|b| b.trade_time.as_str()).collect::<Vec<_>>()),
    
        Series::new("open", bars.iter().map(|b| b.open).collect::<Vec<_>>()),
        Series::new("high", bars.iter().map(|b| b.high).collect::<Vec<_>>()),
        Series::new("low", bars.iter().map(|b| b.low).collect::<Vec<_>>()),
        Series::new("close", bars.iter().map(|b| b.close).collect::<Vec<_>>()),
        Series::new("prev_close", bars.iter().map(|b| b.prev_close).collect::<Vec<_>>()),
        Series::new("pre_settle", bars.iter().map(|b| b.pre_settle).collect::<Vec<_>>()),
    
        Series::new("volume", bars.iter().map(|b| b.volume as u32).collect::<Vec<_>>()),
        Series::new("turnover", bars.iter().map(|b| b.turnover).collect::<Vec<_>>()),
        Series::new("open_interest", bars.iter().map(|b| b.open_interest as u32).collect::<Vec<_>>()),
        Series::new("open_interest_diff", bars.iter().map(|b| b.open_interest_diff).collect::<Vec<_>>()),
    
        Series::new("bid_price_1", bars.iter().map(|b| b.bid_price_1).collect::<Vec<_>>()),
        Series::new("bid_volume_1", bars.iter().map(|b| b.bid_volume_1 as u32).collect::<Vec<_>>()),
        Series::new("ask_price_1", bars.iter().map(|b| b.ask_price_1).collect::<Vec<_>>()),
        Series::new("ask_volume_1", bars.iter().map(|b| b.ask_volume_1 as u32).collect::<Vec<_>>()),
    
        Series::new("mid_price", bars.iter().map(|b| b.mid_price).collect::<Vec<_>>()),
        Series::new("vwap", bars.iter().map(|b| b.vwap).collect::<Vec<_>>()),
        Series::new("log_return", bars.iter().map(|b| b.log_return).collect::<Vec<_>>()),
    
        Series::new("maturity_month", bars.iter().map(|b| b.maturity_month).collect::<Vec<_>>()),
        Series::new("maturity_day", bars.iter().map(|b| b.maturity_day).collect::<Vec<_>>()),
    ])?;
    
    // 写入 parquet
    let file = File::create(output_parquet_path)?;
    ParquetWriter::new(file).finish(&mut df_out)?;


    println!("bar_1min 写入成功: {}", output_parquet_path_display);

    Ok(())
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
    
    println!("原始数据行数: {}", raw_df.height());
    println!("数据列名: {:?}", raw_df.get_column_names());
    
    // 基于 validate_tick 逻辑的高性能向量化过滤
    let mut result_df = LazyFrame::scan_parquet(path_str, Default::default())?
        .select([
            col("contract"), col("comd"), col("exchange"), col("date"), col("trade_time"),
            col("open"), col("high"), col("low"), col("close"), col("pre_settle"), 
            col("volume"), col("turnover"), col("open_interest"),
            col("bid_price_1"), col("bid_volume_1"), col("ask_price_1"), col("ask_volume_1"), col("mid_price"),
        ])
        .filter(
            // 基本数据有效性过滤
            col("close").is_not_null()
                .and(col("close").gt(lit(0.0)))
                .and(col("volume").is_not_null())
                .and(col("turnover").is_not_null())
        )
        .sort(["trade_time"], SortMultipleOptions::default())
        .collect()?;
    
    println!("基础过滤后数据行数: {}", result_df.height());
    
    // 交易时间过滤（优化后的逐行过滤）
    // 先获取当前品种代码（假设整个文件都是同一品种）
    let current_comd = match result_df.column("comd")?.get(0)? {
        AnyValue::String(v) => v.to_string(),
        _ => return Err(anyhow::anyhow!("无法获取品种代码")),
    };
    
    // 获取该品种的交易时间范围
    let trading_ranges = (*TRADE_SESSION_MAP).get_trading_ranges(&current_comd);
    
    if !trading_ranges.is_empty() {
        // 快速批量过滤：使用向量化操作提取时间列，然后批量检查
        let mut valid_indices = Vec::new();
        let trade_time_col = result_df.column("trade_time")?;
        
        for i in 0..result_df.height() {
            if let Ok(any_val) = trade_time_col.get(i) {
                let time_part = match any_val {
                    AnyValue::String(v) => {
                        if let Ok(naive_dt) = NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S%.f")
                            .or_else(|_| NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S")) {
                            naive_dt.time()
                        } else {
                            continue;
                        }
                    },
                    AnyValue::Datetime(v, _, _) => {
                        let naive = if v > 1e15 as i64 {
                            NaiveDateTime::from_timestamp_micros(v)
                                .unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0))
                        } else {
                            NaiveDateTime::from_timestamp_millis(v)
                                .unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0))
                        };
                        naive.time()
                    },
                    _ => continue,
                };
                
                // 检查时间是否在任一交易时段内
                let is_valid = trading_ranges.iter().any(|(start, end)| {
                    time_part >= *start && time_part <= *end
                });
                
                if is_valid {
                    valid_indices.push(i as u32);
                }
            }
        }
        
        println!("品种 {} 交易时间过滤后数据行数: {}", current_comd, valid_indices.len());
        
        if valid_indices.is_empty() {
            return Err(anyhow::anyhow!("过滤后数据为空，可能是所有tick都不在 {} 的交易时间内", current_comd));
        }
        
        // 根据有效索引过滤DataFrame
        let indices_ca = UInt32Chunked::from_vec("", valid_indices);
        result_df = result_df.take(&indices_ca)?;
    } else {
        println!("警告: 未找到品种 {} 的交易时间配置，跳过交易时间过滤", current_comd);
    }
    
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
    
    let grouped_df = df.lazy()
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
    
    Ok((grouped_df, contract, comd, exchange))
}

/// 向量化计算跨窗口指标
fn build_bars_vectorized(grouped_df: DataFrame, contract: &str, comd: &str, exchange: &str) -> anyhow::Result<DataFrame> {
    let mut bars_df = grouped_df.lazy()
        .with_columns([
            // 使用shift获取上一行的值（跨窗口）
            col("close").shift(lit(1)).alias("prev_close"),
            col("last_volume").shift(lit(1)).alias("prev_volume"),
            col("last_turnover").shift(lit(1)).alias("prev_turnover"),
            col("last_open_interest").shift(lit(1)).alias("prev_open_interest"),
        ])
        .with_columns([
            // 计算真正的跨窗口增量
            (col("last_volume") - col("prev_volume"))
                .fill_null(col("last_volume"))  // 第一个bar用当前值
                .alias("volume"),
            
            (col("last_turnover") - col("prev_turnover"))
                .fill_null(col("last_turnover"))
                .alias("turnover"),
            
            // 计算open_interest_diff
            (col("last_open_interest").cast(DataType::Int64) - col("prev_open_interest").cast(DataType::Int64))
                .fill_null(0)
                .cast(DataType::Int32)
                .alias("open_interest_diff"),
            
            // 计算log_return（暂时简化，后续可以优化）
            when(col("prev_close").gt(0))
                .then(col("close") / col("prev_close") - lit(1.0))  // 线性近似
                .otherwise(lit(f64::NAN))
                .alias("log_return"),
            
            // 暂时使用任意值作为contract_yymm，后续会被替换
            lit("temp").alias("contract_yymm"),
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
            col("ask_price_1"),
            col("bid_volume_1"),
            col("ask_volume_1"),
            col("mid_price"),
            col("vwap"),
            col("log_return"),
        ])
        .collect()?;
    
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
    let bars_df = bars_df
        .with_column(Series::new("contract", vec![contract; row_count]))
        .map_err(|e| anyhow::anyhow!("添加contract列失败: {}", e))?
        .with_column(Series::new("contract_yymm", vec![contract_yymm; row_count]))
        .map_err(|e| anyhow::anyhow!("添加contract_yymm列失败: {}", e))?
        .with_column(Series::new("comd", vec![comd; row_count]))
        .map_err(|e| anyhow::anyhow!("添加comd列失败: {}", e))?
        .with_column(Series::new("exchange", vec![exchange; row_count]))
        .map_err(|e| anyhow::anyhow!("添加exchange列失败: {}", e))?;
    
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
        "ask_price_1", 
        "bid_volume_1",
        "ask_volume_1",
        "mid_price",
        "vwap",
        "log_return",
    ])?
    .sort(["trade_time"], SortMultipleOptions::default())?;
    
    Ok(bars_df)
}

/// 补全缺失的分钟bar：高性能批量操作，正确处理prev_close
fn fill_missing_minutes(bars_df: DataFrame) -> anyhow::Result<DataFrame> {
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

    println!("为品种 {} 补全缺失分钟bar (高性能版本)", comd);

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

    // 一次遍历：生成时间序列的同时分离数据，避免重复遍历
    let mut complete_timeline: Vec<chrono::NaiveDateTime> = Vec::new();
    for date in &date_range {
        for (start_time, end_time) in &trading_ranges {
            let mut current_time = date.and_time(*start_time);
            let end_datetime = date.and_time(*end_time);
            
            while current_time <= end_datetime {
                complete_timeline.push(current_time);
                current_time = current_time + chrono::Duration::minutes(1);
            }
        }
    }
    complete_timeline.sort();
    
    println!("完整时间序列: {} 个时间点", complete_timeline.len());
    println!("现有数据: {} 个时间点", existing_data.len());
    
    // 优化：一次遍历分离数据，直接使用正确的数据类型
    let mut existing_indices = Vec::<u32>::new();
    let mut missing_times = Vec::<String>::new();
    let mut missing_prev_closes = Vec::<f64>::new();
    let mut last_close_price = 0.0f64;
    
    for time_point in complete_timeline {
        let time_str = time_point.format("%Y-%m-%d %H:%M:%S").to_string();
        
        if let Some(&(row_idx, close_price)) = existing_data.get(&time_str) {
            // 现有数据
            existing_indices.push(row_idx as u32); // 直接使用u32，避免后续转换
            last_close_price = close_price;
        } else {
            // 需要生成的空bar
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
        println!("批量生成 {} 个空bar...", count);
        
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
                "log_return" => Series::new("log_return", vec![f64::NAN; count]),
                "prev_close" => Series::new("prev_close", missing_prev_closes_ref),
                "open" | "high" | "low" | "close" => {
                    Series::new(field_name, missing_prev_closes_ref) // 价格延续
                },
                _ => {
                    // 其他字段从模板复制
                    let template_col = template_row.column(field_name)?;
                    let template_val = template_col.get(0)?;
                    
                    match template_val {
                        AnyValue::String(s) => Series::new(field_name, vec![s; count]),
                        AnyValue::Float64(f) => Series::new(field_name, vec![f; count]),
                        AnyValue::UInt32(u) => Series::new(field_name, vec![u; count]),
                        AnyValue::Int32(i) => Series::new(field_name, vec![i; count]),
                        AnyValue::Null => Series::new_null(field_name, count),
                        _ => return Err(anyhow::anyhow!("不支持的数据类型: {:?}", template_val)),
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
    let final_result = final_result.sort(["trade_time"], SortMultipleOptions::default())?;
    
    println!("补全完成，总bar数: {}", final_result.height());
    Ok(final_result)
}


/// 向量化历史数据处理：完全基于polars向量化操作
pub fn process_parquet_optimized<P: AsRef<Path>>(parquet_path: P, output_parquet_path: P) -> anyhow::Result<()> {
    // 1. 读取和过滤数据
    let df = read_tick_from_parquet(&parquet_path)?;

    // 2. 创建分钟窗口
    let df_with_minute = create_minute_windows(df)?;

    // 3. 执行分组聚合（向量化），提取常量字段
    let (grouped_df, contract, comd, exchange) = perform_group_aggregation(df_with_minute)?;
    
    // 4. 执行跨窗口计算（向量化）
    let bars_df = build_bars_vectorized(grouped_df, &contract, &comd, &exchange)?;
    
    // 4.5. 补全缺失分钟（可选，失败不影响主流程）
    let mut bars_df = match fill_missing_minutes(bars_df.clone()) {
        Ok(filled) => filled,
        Err(e) => {
            eprintln!("警告: 缺失分钟补全失败: {}, 使用原始数据", e);
            bars_df
        }
    };

    // 5. 直接写入parquet，避免复杂的数据转换
    let file = File::create(output_parquet_path)?;
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut bars_df)?;
    
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

/// 最简单的原生并行处理
fn process_batch_simple(data_source: &str, num_threads: usize) -> anyhow::Result<()> {
    let tick_dir = format!("/data/future_data/{}/full_tick", data_source);
    let output_dir = format!("/data/future_data/{}/full_bar_1min", data_source);
    
    println!("扫描目录: {}", tick_dir);
    let files = find_parquet_files(&tick_dir)?;
    
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
    
    let mut handles = Vec::new();
    
    for thread_id in 0..num_threads {
        let files_clone = Arc::clone(&files_arc);
        let output_dir_clone = Arc::clone(&output_dir_arc);
        
        let handle = thread::spawn(move || {
            let start = thread_id * chunk_size;
            let end = ((thread_id + 1) * chunk_size).min(files_clone.len());
            
            for i in start..end {
                let file_path = &files_clone[i];
                
                let contract = match extract_contract_name(file_path) {
                    Some(name) => name,
                    None => continue,
                };
                
                let output_path = format!("{}/{}_1min.parquet", output_dir_clone, contract);
                
                // 跳过已存在文件
                if std::path::Path::new(&output_path).exists() {
                    continue;
                }
                
                match process_parquet_optimized(file_path, &output_path) {
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

/// 主函数
// fn main() -> anyhow::Result<()> {
//     let args: Vec<String> = std::env::args().collect();
    
//     let data_source = args.get(1).map(|s| s.as_str()).unwrap_or("dongzheng_data");
//     let num_threads = args.get(2)
//         .and_then(|s| s.parse::<usize>().ok())
//         .unwrap_or(4);
    
//     println!("=== 原生并行处理 ===");
//     println!("数据源: {}", data_source);
//     println!("线程数: {}", num_threads);
    
//     process_batch_simple(data_source, num_threads)?;
//     Ok(())
// }


// 单文件处理(请勿删除)
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    
    if args.len() < 3 {
        println!("使用方法: {} <source_name> <contract>", args[0]);
        println!("示例: {} dongzheng_data bb1710", args[0]);
        return Ok(());
    }
    
    let source_name = &args[1];
    let contract = &args[2];
    
    let data_dir = "/data/future_data";
    let data_source = format!("{}/{}", data_dir, source_name);

    let input_path = format!("{}/full_tick/{}.parquet", data_source, contract);
    let output_path = format!("{}/full_bar_1min/{}_1min.parquet", data_source, contract);

    println!("开始处理: {}", input_path);

    match process_parquet_optimized(&input_path, &output_path) {
        Ok(()) => println!("✓ 成功处理 {}", output_path),
        Err(e) => println!("✗ 处理失败: {}", e),
    }

    Ok(())
}
