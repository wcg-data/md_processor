// src/parquet_processor.rs

use std::path::Path;
use chrono::{NaiveDateTime, NaiveDate, Datelike, Timelike};
use std::fs::File;
use polars::prelude::*;
use std::thread;
use std::sync::Arc;


use md_processor::bar1min_aggregator::Bar1MinAggregator;
use md_processor::md_structures::{TickData, BarData};
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
    
    Ok(LazyFrame::scan_parquet(path_str, Default::default())?
        .select([
            col("contract"), col("comd"), col("exchange"), col("date"), col("trade_time"),
            col("close"), col("pre_settle"), col("volume"), col("turnover"), col("open_interest"),
            col("bid_price_1"), col("bid_volume_1"), col("ask_price_1"), col("ask_volume_1"), col("mid_price"),
        ])
        .filter(
            col("close").gt(lit(0.0))
                .and(col("close").is_finite())
                .and(col("volume").is_not_null())
                .and(col("turnover").is_not_null())
        )
        .sort(["trade_time"], SortMultipleOptions::default())
        .collect()?)
}

/// 将BarData写入Parquet文件
fn write_bars_to_parquet<P: AsRef<Path>>(bars: &[BarData], output_path: P) -> anyhow::Result<()> {
    
    let df_out = DataFrame::new(vec![
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
    
        Series::new("volume", bars.iter().map(|b| b.volume).collect::<Vec<_>>()),
        Series::new("turnover", bars.iter().map(|b| b.turnover).collect::<Vec<_>>()),
        Series::new("open_interest", bars.iter().map(|b| b.open_interest).collect::<Vec<_>>()),
        Series::new("open_interest_diff", bars.iter().map(|b| b.open_interest_diff).collect::<Vec<_>>()),
    
        Series::new("bid_price_1", bars.iter().map(|b| b.bid_price_1).collect::<Vec<_>>()),
        Series::new("bid_volume_1", bars.iter().map(|b| b.bid_volume_1).collect::<Vec<_>>()),
        Series::new("ask_price_1", bars.iter().map(|b| b.ask_price_1).collect::<Vec<_>>()),
        Series::new("ask_volume_1", bars.iter().map(|b| b.ask_volume_1).collect::<Vec<_>>()),
    
        Series::new("mid_price", bars.iter().map(|b| b.mid_price).collect::<Vec<_>>()),
        Series::new("vwap", bars.iter().map(|b| b.vwap).collect::<Vec<_>>()),
        Series::new("log_return", bars.iter().map(|b| b.log_return).collect::<Vec<_>>()),
    ])?;
    
    let file = File::create(output_path)?;
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut df_out.clone())?;
    
    Ok(())
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

/// 向量化历史数据处理：完全基于polars向量化操作
pub fn process_parquet_optimized<P: AsRef<Path>>(parquet_path: P, output_parquet_path: P) -> anyhow::Result<()> {
    // 1. 读取和过滤数据
    let df = read_tick_from_parquet(&parquet_path)?;

    // 2. 创建分钟窗口
    let df_with_minute = create_minute_windows(df)?;

    // 3. 执行分组聚合（向量化），提取常量字段
    let (grouped_df, contract, comd, exchange) = perform_group_aggregation(df_with_minute)?;
    
    // 4. 执行跨窗口计算（向量化）
    let mut bars_df = build_bars_vectorized(grouped_df, &contract, &comd, &exchange)?;

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

// /// 主函数
// fn main() -> anyhow::Result<()> {
//     let args: Vec<String> = std::env::args().collect();
    
//     let data_source = args.get(1).map(|s| s.as_str()).unwrap_or("dongzheng_data");
//     let num_threads = args.get(2)
//         .and_then(|s| s.parse::<usize>().ok())
//         .unwrap_or(32);
    
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
