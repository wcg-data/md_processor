// src/parquet_processor.rs

use std::path::{Path, PathBuf};
use std::fs;
use chrono::{NaiveDateTime, NaiveDate, Datelike, Timelike};
use std::fs::File;
use polars::prelude::*;
use rayon::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;


use md_processor::bar1min_aggregator::Bar1MinAggregator;
use md_processor::md_structures::{TickData, BarData};


fn str_to_fixed<const N: usize>(s: &str) -> [u8; N] {
    let mut buf = [0u8; N];
    let bytes = s.as_bytes();
    let len = bytes.len().min(N);
    buf[..len].copy_from_slice(&bytes[..len]);
    buf
}

/// 优化版离线处理：支持数据排序、按合约分组聚合
pub fn process_parquet<P: AsRef<Path>>(parquet_path: P,  output_parquet_path: P) -> anyhow::Result<()> {
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

/// 高性能历史数据处理：按分钟内存分组 + 复用 flush 逻辑
pub fn process_parquet_optimized<P: AsRef<Path>>(parquet_path: P, output_parquet_path: P) -> anyhow::Result<()> {
    let output_parquet_path_display = output_parquet_path.as_ref().display().to_string();
    println!("开始优化处理: {}", parquet_path.as_ref().display());
    
    use chrono::{Utc, TimeZone};

    // 1. 先读取数据，在内存中处理分钟窗口创建（避免复杂的Polars字符串操作）
    let mut df = LazyFrame::scan_parquet(parquet_path.as_ref().to_str().unwrap(), Default::default())?
        .select([
            col("contract"), col("comd"), col("exchange"), col("date"), col("trade_time"),
            col("close"), col("pre_settle"), col("volume"), col("turnover"), col("open_interest"),
            col("bid_price_1"), col("bid_volume_1"), col("ask_price_1"), col("ask_volume_1"), col("mid_price"),
        ])
        .filter(
            // 基本数据有效性过滤
            col("close").gt(lit(0.0))
                .and(col("close").is_finite())
                .and(col("volume").is_not_null())
                .and(col("turnover").is_not_null())
        )
        .sort(["trade_time"], SortMultipleOptions::default())
        .collect()?;

    println!("数据读取完成，共 {} 条tick", df.height());

    // 2. 创建分钟窗口列 - 处理 datetime 类型
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
                    // 直接从时间戳转换为分钟窗口
                    let naive = if v > 1e15 as i64 {
                        NaiveDateTime::from_timestamp_micros(v)
                            .unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0))
                    } else {
                        NaiveDateTime::from_timestamp_millis(v)
                            .unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0))
                    };
                    // 向下取整到分钟
                    let truncated = naive.with_second(0).unwrap().with_nanosecond(0).unwrap();
                    truncated.format("%Y-%m-%d %H:%M:00").to_string()
                },
                _ => String::new(),
            }
        })
        .collect();

    // 3. 添加分钟窗口列
    let df_with_minute = df.with_column(
        Series::new("minute_window", minute_windows)
    )?;

    // 4. 使用 Polars group_by 进行向量化聚合
    let grouped_df = df_with_minute
        .clone()
        .lazy()
        .group_by([col("minute_window")])
        .agg([
            // 向量化聚合 - 获取每个分钟的首尾tick和OHLC
            col("contract").first().alias("first_contract"),
            col("comd").first().alias("first_comd"), 
            col("exchange").first().alias("first_exchange"),
            col("date").first().alias("first_date"),
            col("trade_time").first().alias("first_time"),
            col("close").first().alias("first_close"),
            col("pre_settle").first().alias("first_pre_settle"),
            col("volume").first().alias("first_volume"),
            col("turnover").first().alias("first_turnover"),
            col("open_interest").first().alias("first_oi"),
            col("bid_price_1").first().alias("first_bid_price"),
            col("bid_volume_1").first().alias("first_bid_volume"),
            col("ask_price_1").first().alias("first_ask_price"),
            col("ask_volume_1").first().alias("first_ask_volume"),
            col("mid_price").first().alias("first_mid_price"),

            // 最后tick的数据
            col("contract").last().alias("last_contract"),
            col("comd").last().alias("last_comd"),
            col("exchange").last().alias("last_exchange"),
            col("date").last().alias("last_date"),
            col("trade_time").last().alias("last_time"),
            col("close").last().alias("last_close"),
            col("pre_settle").last().alias("last_pre_settle"),
            col("volume").last().alias("last_volume"),
            col("turnover").last().alias("last_turnover"),
            col("open_interest").last().alias("last_oi"),
            col("bid_price_1").last().alias("last_bid_price"),
            col("bid_volume_1").last().alias("last_bid_volume"),
            col("ask_price_1").last().alias("last_ask_price"),
            col("ask_volume_1").last().alias("last_ask_volume"),
            col("mid_price").last().alias("last_mid_price"),

            // OHLC向量化计算
            col("close").first().alias("open"),
            col("close").max().alias("high"),
            col("close").min().alias("low"),
            col("close").last().alias("close"),
        ])
        .sort(["minute_window"], SortMultipleOptions::default())
        .collect()?;

    println!("Polars向量化分组完成，生成 {} 个分钟窗口", grouped_df.height());
    

    // 2. 只遍历分组结果（从433万 → 约10万）
    let mut bars = Vec::with_capacity(grouped_df.height());
    let mut prev_tick: Option<TickData> = None;
    
    for i in 0..grouped_df.height() {
        // 从聚合结果提取首尾tick和OHLC
        let first_tick = extract_tick_from_grouped_row(&grouped_df, i, "first")?;
        let last_tick = extract_tick_from_grouped_row(&grouped_df, i, "last")?;
        
        
        // 直接从聚合结果获取OHLC（已由Polars向量化计算）
        let open = grouped_df.column("open")?.f64()?.get(i).unwrap_or(0.0);
        let high = grouped_df.column("high")?.f64()?.get(i).unwrap_or(0.0);
        let low = grouped_df.column("low")?.f64()?.get(i).unwrap_or(0.0);
        
        // 解析时间窗口
        let minute_str = grouped_df.column("minute_window")?.str()?.get(i).unwrap_or("");
        let minute_window = chrono::NaiveDateTime::parse_from_str(minute_str, "%Y-%m-%d %H:%M:%S")
            .map(|dt| Utc.from_utc_datetime(&dt))
            .unwrap_or_else(|_| Utc::now());
            
        // 构建一个有效的last_tick，直接使用minute_window作为时间
        let mut fixed_last_tick = last_tick;
        fixed_last_tick.trade_time = str_to_fixed::<24>(minute_str);
        
        // 构建聚合器状态 - 复用flush()业务逻辑
        let mut aggregator = Bar1MinAggregator {
            current_bar_minute: Some(minute_window),
            prev_last_tick: prev_tick,
            last_tick: Some(fixed_last_tick),
            open: Some(open),
            high: Some(high),
            low: Some(low),
        };

        // 使用flush生成最终BarData（保持所有业务逻辑）
        if let Some(bar) = aggregator.flush() {
            bars.push(bar);
        }
        
        prev_tick = Some(last_tick);
        
        if i % 1000 == 0 && i > 0 {
            println!("处理进度: {}/{} ({:.1}%)", i, grouped_df.height(), (i as f64 / grouped_df.height() as f64) * 100.0);
        }
    }

    // 4. 写入结果
    write_bars_to_parquet(&bars, output_parquet_path)?;
    
    println!("优化处理完成，生成 {} 条1分钟K线，输出: {}", bars.len(), output_parquet_path_display);
    Ok(())
}

/// 从 Polars 聚合结果提取 TickData（用于向量化版本）
fn extract_tick_from_grouped_row(df: &DataFrame, row_idx: usize, prefix: &str) -> anyhow::Result<TickData> {
    let get_str = |col_name: &str| -> &str {
        df.column(&format!("{}_{}", prefix, col_name))
            .ok().and_then(|col| col.str().ok())
            .and_then(|str_col| str_col.get(row_idx))
            .unwrap_or("")
    };
    
    let get_f64 = |col_name: &str| -> f64 {
        df.column(&format!("{}_{}", prefix, col_name))
            .ok().and_then(|col| col.f64().ok())
            .and_then(|f64_col| f64_col.get(row_idx))
            .unwrap_or(0.0)
    };
    
    let get_u32 = |col_name: &str| -> u32 {
        df.column(&format!("{}_{}", prefix, col_name))
            .ok().and_then(|col| col.i64().ok())
            .and_then(|i64_col| i64_col.get(row_idx))
            .map(|v| v as u32)
            .unwrap_or(0)
    };

    Ok(TickData {
        contract: str_to_fixed::<9>(get_str("contract")),
        comd: str_to_fixed::<4>(get_str("comd")),
        exchange: str_to_fixed::<7>(get_str("exchange")),
        date: str_to_fixed::<11>(get_str("date")),
        trade_time: str_to_fixed::<24>(get_str("time")),
        open: get_f64("close"),
        high: get_f64("close"),
        low: get_f64("close"),
        close: get_f64("close"),
        pre_settle: get_f64("pre_settle"),
        volume: get_u32("volume"),
        turnover: get_f64("turnover"),
        open_interest: get_u32("oi"),
        bid_price_1: get_f64("bid_price"),
        bid_volume_1: get_u32("bid_volume"),
        ask_price_1: get_f64("ask_price"),
        ask_volume_1: get_u32("ask_volume"),
        mid_price: get_f64("mid_price"),
    })
}

/// 将DataFrame行转换为TickData（用于优化版本）
fn convert_row_to_tick(df: &DataFrame, row_idx: usize) -> anyhow::Result<TickData> {
    let row = df.get_row(row_idx)?;
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
            let naive = if *v > 1e15 as i64 {
                NaiveDateTime::from_timestamp_micros(*v)
                    .unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0))
            } else {
                NaiveDateTime::from_timestamp_millis(*v)
                    .unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0))
            };
            naive.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
        },
        _ => "".to_string(),
    };

    Ok(TickData {
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
    })
}

/// 从聚合结果行构建TickData
fn build_tick_from_row(df: &DataFrame, row_idx: usize, is_last: bool) -> anyhow::Result<TickData> {
    let contract = df.column("contract")?.str()?.get(row_idx).unwrap_or("");
    let comd = df.column("comd")?.str()?.get(row_idx).unwrap_or("");
    let exchange = df.column("exchange")?.str()?.get(row_idx).unwrap_or("");
    let date = df.column("date")?.str()?.get(row_idx).unwrap_or("");
    
    let time_col = if is_last { "last_time" } else { "first_time" };
    let trade_time = df.column(time_col)?.str()?.get(row_idx).unwrap_or("");
    
    let close = df.column("close")?.f64()?.get(row_idx).unwrap_or(0.0);
    let pre_settle = df.column("pre_settle")?.f64()?.get(row_idx).unwrap_or(0.0);
    
    let volume_col = if is_last { "last_volume" } else { "first_volume" };
    let turnover_col = if is_last { "last_turnover" } else { "first_turnover" };
    let oi_col = if is_last { "last_oi" } else { "first_oi" };
    
    let volume = df.column(volume_col)?.i64()?.get(row_idx).map(|v| v as u32).unwrap_or(0);
    let turnover = df.column(turnover_col)?.f64()?.get(row_idx).unwrap_or(0.0);
    let open_interest = df.column(oi_col)?.i64()?.get(row_idx).map(|v| v as u32).unwrap_or(0);
    
    let bid_price_1 = df.column("bid_price_1")?.f64()?.get(row_idx).unwrap_or(0.0);
    let bid_volume_1 = df.column("bid_volume_1")?.i64()?.get(row_idx).map(|v| v as u32).unwrap_or(0);
    let ask_price_1 = df.column("ask_price_1")?.f64()?.get(row_idx).unwrap_or(0.0);
    let ask_volume_1 = df.column("ask_volume_1")?.i64()?.get(row_idx).map(|v| v as u32).unwrap_or(0);
    let mid_price = df.column("mid_price")?.f64()?.get(row_idx).unwrap_or(0.0);

    Ok(TickData {
        contract: str_to_fixed::<9>(contract),
        comd: str_to_fixed::<4>(comd),
        exchange: str_to_fixed::<7>(exchange),
        date: str_to_fixed::<11>(date),
        trade_time: str_to_fixed::<24>(trade_time),
        open: close,
        high: close,
        low: close,
        close,
        pre_settle,
        volume,
        turnover,
        open_interest,
        bid_price_1,
        bid_volume_1,
        ask_price_1,
        ask_volume_1,
        mid_price,
    })
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

// 单文件处理
fn main() -> anyhow::Result<()> {
    let data_source = "dongzheng_data";
    let contract = "cu2110";

    let parquet_file = format!("/root/data/future_data/{}/full_tick/{}.parquet", data_source, contract);
    let output_parquet_path = format!("/root/data/future_data/{}/full_bar_1min/{}_1min.parquet",data_source, contract);

    // 使用优化版本
    process_parquet_optimized(parquet_file, output_parquet_path)?;
    Ok(())
}


// fn main() -> anyhow::Result<()> {
//     let input_dir = Path::new("/root/data/future_data/dongzheng_data/full_tick");
//     let output_dir = Path::new("/root/data/future_data/dongzheng_data/full_bar_1min");
//     // let input_dir = Path::new("/root/data/future_data/huatai_data/full_tick");
//     // let output_dir = Path::new("/root/data/future_data/huatai_data/full_bar_1min");
//     fs::create_dir_all(output_dir)?;
//     // 优化并发设置：减少线程数，增加栈大小
//     rayon::ThreadPoolBuilder::new()
//     .num_threads(6)  // 降低线程数从16到6，减少内存压力
//     .stack_size(64 * 1024 * 1024) // 增加栈大小到64MB
//     .build_global()
//     .unwrap();

//     // 1. 收集所有 parquet 文件（排除已知损坏的文件）
//     let mut files: Vec<PathBuf> = fs::read_dir(input_dir)?
//         .filter_map(Result::ok)
//         .map(|entry| entry.path())
//         .filter(|p| p.extension().map(|ext| ext == "parquet").unwrap_or(false))
//         .filter(|p| {
//             // 跳过已知损坏的文件
//             let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
//             !["pp1609.parquet", "ad2606.parquet", "eg2107.parquet"].contains(&name)
//         })
//         .collect();
    
//     // 按文件名排序以确保处理顺序一致
//     files.sort();
    
//     let total_files = files.len();
//     println!("发现 {} 个有效的 parquet 文件，开始处理...", total_files);
    
//     // 2. 添加进度统计
//     let processed_count = Arc::new(AtomicUsize::new(0));
//     let success_count = Arc::new(AtomicUsize::new(0));
//     let failed_count = Arc::new(AtomicUsize::new(0));
    
//     // 3. rayon 并发处理每个文件
//     files.par_iter().for_each(|input_path| {
//         let file_name = input_path.file_stem().unwrap().to_string_lossy();
//         let output_path = output_dir.join(format!("{}_1min.parquet", file_name));

//         match process_parquet(input_path, &output_path) {
//             Ok(_) => {
//                 success_count.fetch_add(1, Ordering::Relaxed);
//                 let current = processed_count.fetch_add(1, Ordering::Relaxed) + 1;
//                 if current % 100 == 0 || current == total_files {
//                     println!("✅ 进度: {}/{} ({:.1}%) - 成功: {}, 失败: {}", 
//                         current, total_files, 
//                         (current as f64 / total_files as f64) * 100.0,
//                         success_count.load(Ordering::Relaxed),
//                         failed_count.load(Ordering::Relaxed)
//                     );
//                 }
//             },
//             Err(e) => {
//                 failed_count.fetch_add(1, Ordering::Relaxed);
//                 let current = processed_count.fetch_add(1, Ordering::Relaxed) + 1;
//                 eprintln!("❌ 处理失败 ({}/{}): {:?}, 错误: {:?}", 
//                     current, total_files, input_path, e);
//             }
//         }
//     });
    
//     // 4. 输出最终统计
//     let final_success = success_count.load(Ordering::Relaxed);
//     let final_failed = failed_count.load(Ordering::Relaxed);
//     println!("\n=== 处理完成统计 ===");
//     println!("总文件数: {}", total_files);
//     println!("成功处理: {} ({:.1}%)", final_success, (final_success as f64 / total_files as f64) * 100.0);
//     println!("处理失败: {} ({:.1}%)", final_failed, (final_failed as f64 / total_files as f64) * 100.0);

//     Ok(())
// }