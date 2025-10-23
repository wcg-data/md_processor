// bin/parquet_processor_on_tick.rs - 使用在线处理代码的离线tick处理器

use std::path::Path;
use std::fs::File;
use anyhow;
use polars::prelude::*;
use chrono::{NaiveDateTime, NaiveDate, Datelike};

use md_processor::bar1min_aggregator::Bar1MinAggregator;
use md_processor::bar5min_aggregator::Bar5MinAggregator;
use md_processor::md_structures::{TickData, BarData};
use md_processor::common_utils::{calculate_maturity_month, calculate_maturity_day, fill_missing_minutes, str_to_fixed};

/// 从 BarData 向量构建输出 DataFrame
fn build_bars_dataframe(bars: &[BarData]) -> PolarsResult<DataFrame> {
    DataFrame::new(vec![
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
        Series::new("prev_close", bars.iter().map(|b| if b.prev_close.is_nan() { None } else { Some(b.prev_close) }).collect::<Vec<_>>()),
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
        Series::new("log_return", bars.iter().map(|b| if b.log_return.is_nan() { None } else { Some(b.log_return) }).collect::<Vec<_>>()),

        Series::new("maturity_month", bars.iter().map(|b| calculate_maturity_month(&b.contract_yymm, &b.date)).collect::<Vec<_>>()),
        Series::new("maturity_day", bars.iter().map(|b| calculate_maturity_day(&b.contract_yymm, &b.date)).collect::<Vec<_>>()),
    ])
}

/// 优化版离线处理：支持数据排序、按合约分组聚合
/// 这个函数复用在线处理的Bar1MinAggregator代码，确保离线和在线处理逻辑一致
pub fn process_parquet_on_tick<P: AsRef<Path>>(parquet_path: P, output_parquet_path: P) -> anyhow::Result<()> {
    let output_parquet_path_display = output_parquet_path.as_ref().display().to_string();
    let df = LazyFrame::scan_parquet(parquet_path.as_ref().to_str().unwrap(), Default::default())?
        .select([
            col("contract"), col("comd"), col("exchange"), col("date"), col("trade_time"),
            col("open"), col("high"), col("low"), col("close"), col("pre_settle"),
            col("volume"), col("turnover"), col("open_interest"),
            col("bid_price_1"), col("bid_volume_1"), col("ask_price_1"), col("ask_volume_1"), col("mid_price"),
        ])
        // 关键改进：按时间排序，确保tick数据按时间顺序处理
        .sort(["trade_time"], SortMultipleOptions::default())
        .collect()?;

    let mut aggregator = Bar1MinAggregator::new();
    let mut bars = Vec::new();
    let mut valid_ticks = 0;
    let mut invalid_ticks = 0;

    for i in 0..df.height() {
        let row = match df.get_row(i) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("ERROR: get_row({}) failed: {}", i, e);
                return Err(e.into());
            }
        };
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
            AnyValue::Float64(v) => v as u32,  // 支持Float64类型（parquet中volume可能是float）
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
        

        let mut tick = TickData {
            contract: str_to_fixed::<9>(get_str(0)),
            comd: str_to_fixed::<4>(get_str(1)),
            exchange: str_to_fixed::<7>(get_str(2)),
            date: str_to_fixed::<11>(&get_datetime_string(3)),
            trade_time: str_to_fixed::<24>(&get_datetime_string(4)),
            open: get_f64(5),
            high: get_f64(6),
            low: get_f64(7),
            close: get_f64(8),
            pre_settle: get_f64(9),
            volume: get_u32(10),
            turnover: get_f64(11),
            open_interest: get_u32(12),
            bid_price_1: get_f64(13),
            bid_volume_1: get_u32(14),
            ask_price_1: get_f64(15),
            ask_volume_1: get_u32(16),
            mid_price: get_f64(17),
            ..Default::default()
        };

        // 使用与在线处理相同的validate_tick逻辑（现在会清理异常的bid/ask价格）
        if !Bar1MinAggregator::validate_tick(&mut tick) {
            invalid_ticks += 1;
            continue;  // 静默跳过无效tick，减少日志输出
        }
        valid_ticks += 1;

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
    let mut df_out = build_bars_dataframe(&bars)?;

    // 应用缺失分钟补全功能（复用在线代码逻辑）
    df_out = match fill_missing_minutes(df_out.clone()) {
        Ok(filled_df) => {
            println!("✓ 缺失分钟补全完成");
            filled_df
        },
        Err(e) => {
            eprintln!("警告: 缺失分钟补全失败: {}, 使用原始数据", e);
            df_out
        }
    };
    
    // 写入 parquet
    let file = File::create(output_parquet_path)?;
    ParquetWriter::new(file).finish(&mut df_out)?;

    println!("bar_1min 写入成功: {}", output_parquet_path_display);

    Ok(())
}

/// 1min → 5min 聚合处理（使用 Bar5MinAggregator）
pub fn process_1min_to_5min<P: AsRef<Path>>(parquet_1min_path: P, output_5min_path: P) -> anyhow::Result<()> {
    let output_path_display = output_5min_path.as_ref().display().to_string();

    // 读取1min bar数据
    let df = LazyFrame::scan_parquet(parquet_1min_path.as_ref().to_str().unwrap(), Default::default())?
        .sort(["trade_time"], SortMultipleOptions::default())  // 确保按时间排序
        .collect()?;

    let mut aggregator = Bar5MinAggregator::new();
    let mut bars_5min = Vec::new();

    // 逐行读取1min bar并聚合成5min
    for i in 0..df.height() {
        let row = df.get_row(i)?;

        let get_str = |idx| match row.0[idx] {
            AnyValue::String(v) => v.to_string(),
            _ => String::new(),
        };
        let get_f64 = |idx| match row.0[idx] {
            AnyValue::Float64(v) => v,
            AnyValue::Float32(v) => v as f64,
            AnyValue::Null => f64::NAN,
            _ => 0.0,
        };
        let get_u32 = |idx| match row.0[idx] {
            AnyValue::Int64(v) => v as u32,
            AnyValue::UInt32(v) => v,
            AnyValue::Float64(v) => v as u32,
            _ => 0,
        };
        let get_i32 = |idx| match row.0[idx] {
            AnyValue::Int32(v) => v,
            AnyValue::Int64(v) => v as i32,
            _ => 0,
        };
        let get_i16 = |idx| match row.0[idx] {
            AnyValue::Int16(v) => v,
            AnyValue::Int32(v) => v as i16,
            AnyValue::Int64(v) => v as i16,
            _ => 0,
        };

        // 构造 BarData（1min）
        let bar_1min = BarData {
            contract: get_str(0),
            contract_yymm: get_str(1),
            comd: get_str(2),
            exchange: get_str(3),
            date: get_str(4),
            trade_time: get_str(5),
            open: get_f64(6),
            high: get_f64(7),
            low: get_f64(8),
            close: get_f64(9),
            prev_close: get_f64(10),
            pre_settle: get_f64(11),
            volume: get_u32(12),
            turnover: get_f64(13),
            open_interest: get_u32(14),
            open_interest_diff: get_i32(15),
            bid_price_1: get_f64(16),
            bid_volume_1: get_u32(17),
            ask_price_1: get_f64(18),
            ask_volume_1: get_u32(19),
            mid_price: get_f64(20),
            vwap: get_f64(21),
            log_return: get_f64(22),
            maturity_month: get_i16(23),
            maturity_day: get_i16(24),
        };

        // 聚合成5min
        if let Some(bar_5min) = aggregator.on_bar_1min(&bar_1min) {
            bars_5min.push(bar_5min);
        }
    }

    // flush 最后一个5min bar
    if let Some(bar_5min) = aggregator.flush() {
        bars_5min.push(bar_5min);
    }

    // 构造 DataFrame 并写入
    let mut df_out = build_bars_dataframe(&bars_5min)?;

    let file = File::create(output_5min_path)?;
    ParquetWriter::new(file).finish(&mut df_out)?;

    println!("bar_5min 写入成功: {}", output_path_display);

    Ok(())
}

/// 主函数：使用在线处理代码的离线tick/bar处理器
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 4 {
        println!("使用方法: {} <source_name> <freq> <contract>", args[0]);
        println!("说明: 使用在线处理代码的离线处理器");
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
            // tick -> 1min (使用在线处理逻辑)
            let input_path = format!("{}/full_tick/{}.parquet", data_source, contract);
            let output_path = format!("{}/full_bar_1min/{}_1min_on_tick.parquet", data_source, contract);

            println!("处理模式: tick -> 1min (使用在线处理代码)");
            println!("输入文件: {}", input_path);
            println!("输出文件: {}", output_path);

            match process_parquet_on_tick(&input_path, &output_path) {
                Ok(()) => println!("✓ 成功生成1分钟bar: {}", output_path),
                Err(e) => println!("✗ 处理失败: {}", e),
            }
        }
        "5min" => {
            // 1min -> 5min (使用 Bar5MinAggregator)
            let input_path = format!("{}/full_bar_1min/{}_1min_on_tick.parquet", data_source, contract);
            let output_path = format!("{}/full_bar_5min/{}_5min_on_tick.parquet", data_source, contract);

            println!("处理模式: 1min -> 5min (使用 Bar5MinAggregator)");
            println!("输入文件: {}", input_path);
            println!("输出文件: {}", output_path);

            match process_1min_to_5min(&input_path, &output_path) {
                Ok(()) => println!("✓ 成功生成5分钟bar: {}", output_path),
                Err(e) => println!("✗ 处理失败: {}", e),
            }
        }
        _ => {
            println!("错误: freq 必须是 '1min' 或 '5min'");
            return Ok(());
        }
    }

    Ok(())
}