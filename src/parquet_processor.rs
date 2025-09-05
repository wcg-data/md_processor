// src/parquet_processor.rs

use std::path::{Path, PathBuf};
use std::fs;
use chrono::{NaiveDateTime, NaiveDate, Datelike};
use std::fs::File;
use polars::prelude::*;
use rayon::prelude::*;


use md_processor::bar1min_aggregator::Bar1MinAggregator;
use md_processor::md_structures::TickData;


fn str_to_fixed<const N: usize>(s: &str) -> [u8; N] {
    let mut buf = [0u8; N];
    let bytes = s.as_bytes();
    let len = bytes.len().min(N);
    buf[..len].copy_from_slice(&bytes[..len]);
    buf
}

/// 最简单的：读取 parquet -> 转 TickData -> 按 on_tick -> flush 输出 bar
pub fn process_parquet<P: AsRef<Path>>(parquet_path: P,  output_parquet_path: P) -> anyhow::Result<()> {
    let output_parquet_path_display = output_parquet_path.as_ref().display().to_string();
    let df = LazyFrame::scan_parquet(parquet_path.as_ref().to_str().unwrap(), Default::default())?
        .select([
            col("contract"), col("comd"), col("exchange"), col("date"), col("trade_time"),
            col("close"), col("pre_settle"), col("volume"), col("turnover"), col("open_interest"),
            col("bid_price_1"), col("bid_volume_1"), col("ask_price_1"), col("ask_volume_1"), col("mid_price"),
        ])
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

        // tick.print();

        if !Bar1MinAggregator::validate_tick(&tick) {
            println!("valid tick");
            continue;
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


// // 单文件处理
// fn main() -> anyhow::Result<()> {
//     let data_source = "dongzheng_data";
//     let contract = "cu2110";

//     let parquet_file = format!("/root/data/future_data/{}/full_tick/{}.parquet", data_source, contract);
//     let output_parquet_path = format!("/root/data/future_data/{}/full_bar_1min/{}_1min.parquet",data_source, contract);

//     process_parquet(parquet_file, output_parquet_path)?;
//     Ok(())
// }


fn main() -> anyhow::Result<()> {
    let input_dir = Path::new("/root/data/future_data/dongzheng_data/full_tick");
    let output_dir = Path::new("/root/data/future_data/dongzheng_data/full_bar_1min");
    // let input_dir = Path::new("/root/data/future_data/huatai_data/full_tick");
    // let output_dir = Path::new("/root/data/future_data/huatai_data/full_bar_1min");
    fs::create_dir_all(output_dir)?;
    rayon::ThreadPoolBuilder::new()
    .num_threads(8)
    .stack_size(32 * 1024 * 1024) // ← 新增：把每个 worker 的栈调到 32MB
    .build_global()
    .unwrap();

    // 1. 收集所有 parquet 文件
    let files: Vec<PathBuf> = fs::read_dir(input_dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|p| p.extension().map(|ext| ext == "parquet").unwrap_or(false))
        .collect();

    // 2. rayon 并发处理每个文件
    files.par_iter().for_each(|input_path| {
        let file_name = input_path.file_stem().unwrap().to_string_lossy();
        let output_path = output_dir.join(format!("{}_1min.parquet", file_name));

        match process_parquet(input_path, &output_path) {
            Ok(_) => println!("✅ 处理完成: {:?}", input_path),
            Err(e) => eprintln!("❌ 处理失败: {:?}, 错误: {:?}", input_path, e),
        }
    });

    Ok(())
}