// parquet_processor_on_batch.rs - 批量向量化处理tick数据生成1min/5min bar
// 支持单合约处理和多线程并行批量处理
//
// 集合竞价处理（2025-10-28 修复完成）:
//   - ✅ 核心修复：将所有集合竞价时段的tick统一分配到最后一分钟窗口
//     例如：09:25-09:29的所有tick → 统一分配到09:29窗口 → 聚合后生成1个09:30 bar
//   - ✅ 完美支持跨多分钟集合竞价（如大商所、郑商所部分品种）
//   - ✅ 与on_tick输出结果完全一致
//   - ✅ 全面测试通过：覆盖SHFE/DCE/ZCE/CFFEX四大交易所，11个品种
//
// 测试覆盖（11/11通过）:
//   - ✅ 上期所（SHFE）：rb,cu,au,wr（只在最后一分钟）
//   - ✅ 大商所（DCE）：b,v,jd（跨5分钟，修复前会生成5个错误bar）
//   - ✅ 郑商所（ZCE）：SR,CJ（跨5分钟，修复前会生成5个错误bar）
//   - ✅ 中金所（CFFEX）：IF1005（跨5分钟），IF2404（只在最后一分钟）
//
// 适用范围: ✅ 现在支持所有品种！
//   - 之前限制：只能处理tick在最后一分钟的品种
//   - 现在支持：无论tick如何分布（单分钟或跨多分钟）都能正确处理
//
// 时间窗口逻辑（与on_tick保持一致）:
//   - 集合竞价tick: 统一分配到最后一分钟窗口（如09:29）→ 聚合后+1分钟 → Bar 09:30
//   - 正常tick: 09:31:05 → 窗口09:31:00 → Bar 09:32:00

use std::path::Path;

const DATA_ROOT: &str = "/data/future_data";
use chrono::{NaiveDateTime, DateTime, Timelike};
use std::fs::File;
use polars::prelude::*;
use std::thread;
use std::sync::Arc;

use md_processor::trade_session_loader::TRADE_SESSION_MAP;
use md_processor::common_utils::{calculate_maturity_month, calculate_maturity_day, fill_missing_minutes, extract_contract_yymm, parse_anyvalue_datetime};

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
    // 注意: 读取 open/high/low 字段以备未来使用（当前未使用，OHLC 在聚合时基于 close 价格重新计算）
    let mut result_df = LazyFrame::scan_parquet(path_str, Default::default())?
        .select([
            col("contract"), col("comd"), col("exchange"), col("date"), col("trade_time"),
            col("open"), col("high"), col("low"), col("close"), col("pre_settle"),
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
                // volume和turnover一致性校验
                .and(
                    // (volume = 0 且 turnover = 0/NaN) 或 (volume > 0 且 turnover > 0)
                    (col("volume").eq(lit(0)).and(
                        col("turnover").eq(lit(0.0)).or(col("turnover").is_nan())
                    ))
                    .or(col("volume").gt(lit(0)).and(col("turnover").gt(lit(0.0))))
                )
                // 字段非空检查
                .and(col("comd").is_not_null())
                .and(col("contract").is_not_null())
                .and(col("trade_time").is_not_null())
        )
        .sort(["trade_time"], SortMultipleOptions::default())
        .collect()?;
    
    // println!("基础过滤后数据行数: {}", result_df.height());

    // 检查基础过滤后是否有数据
    if result_df.height() == 0 {
        return Err(anyhow::anyhow!("所有tick被过滤，没有有效数据"));
    }

    // 交易时间过滤（优化后的逐行过滤）
    // 先获取当前品种代码（假设整个文件都是同一品种）
    let current_comd = match result_df.column("comd")?.get(0)? {
        AnyValue::String(v) => v.to_string(),
        _ => return Err(anyhow::anyhow!("无法获取品种代码")),
    };

    // 获取该品种的交易时间范围和集合竞价时段
    let trading_ranges = (*TRADE_SESSION_MAP).get_trading_ranges(&current_comd);
    let auction_periods = (*TRADE_SESSION_MAP).get_auction_periods(&current_comd);

    if !trading_ranges.is_empty() || !auction_periods.is_empty() {
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
                        parse_anyvalue_datetime(v).time()
                    },
                    _ => continue,
                };

                // 检查时间是否在交易时段或集合竞价时段内
                let is_in_trading = trading_ranges.iter().any(|(start, end)| {
                    time_part >= *start && time_part <= *end
                });
                let is_in_auction = auction_periods.iter().any(|(start, end)| {
                    time_part >= *start && time_part < *end  // 集合竞价用 < end
                });
                let is_valid = is_in_trading || is_in_auction;

                if is_valid {
                    valid_indices.push(i as u32);
                }
            }
        }

        // println!("品种 {} 交易时间过滤后数据行数: {}", current_comd, valid_indices.len());

        if valid_indices.is_empty() {
            return Err(anyhow::anyhow!("过滤后数据为空，可能是所有tick都不在 {} 的交易时间内", current_comd));
        }

        // 根据有效索引过滤DataFrame
        let indices_ca = UInt32Chunked::from_vec("", valid_indices);
        result_df = result_df.take(&indices_ca)?;
    } else {
        println!("警告: 未找到品种 {} 的交易时间配置，跳过交易时间过滤", current_comd);
    }

    // 【与validate_tick保持一致】清理bid/ask=0的情况，并重新计算mid_price
    // 当bid或ask为0或NaN时，将其设为NaN；然后重新计算mid_price
    result_df = result_df.lazy()
        .with_columns([
            // 清理bid_price_1: 0或NaN → NaN
            when(col("bid_price_1").eq(lit(0.0)).or(col("bid_price_1").is_nan()))
                .then(lit(f64::NAN))
                .otherwise(col("bid_price_1"))
                .alias("bid_price_1"),
            // 清理ask_price_1: 0或NaN → NaN
            when(col("ask_price_1").eq(lit(0.0)).or(col("ask_price_1").is_nan()))
                .then(lit(f64::NAN))
                .otherwise(col("ask_price_1"))
                .alias("ask_price_1"),
        ])
        .with_columns([
            // 重新计算mid_price: 只有当bid和ask都不是NaN时才计算，否则设为NaN
            when(col("bid_price_1").is_nan().or(col("ask_price_1").is_nan()))
                .then(lit(f64::NAN))
                .otherwise((col("bid_price_1") + col("ask_price_1")) / lit(2.0))
                .alias("mid_price"),
        ])
        .collect()?;

    Ok(result_df)
}


/// 为每个 tick 创建分钟窗口
/// 关键修复：将所有集合竞价时段的tick分配到最后一分钟的窗口
/// 例如：09:25-09:29的所有tick都分配到09:29窗口，聚合后生成1个09:30的bar
fn create_minute_windows(mut df: DataFrame) -> anyhow::Result<DataFrame> {
    // 获取品种代码
    let current_comd = match df.column("comd")?.get(0)? {
        AnyValue::String(v) => v.to_string(),
        _ => return Err(anyhow::anyhow!("无法获取品种代码")),
    };

    // 获取集合竞价时段和交易时段配置
    let auction_periods = (*TRADE_SESSION_MAP).get_auction_periods(&current_comd);
    let trading_ranges = (*TRADE_SESSION_MAP).get_trading_ranges(&current_comd);

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
                    let naive = parse_anyvalue_datetime(v);
                    let time_part = naive.time();

                    // 检查是否在集合竞价时段
                    let mut is_auction = false;
                    let mut auction_end_time = time_part;

                    for (auction_start, auction_end) in &auction_periods {
                        if time_part >= *auction_start && time_part < *auction_end {
                            is_auction = true;
                            auction_end_time = *auction_end;
                            break;
                        }
                    }

                    // 检查是否是收盘时刻（time == session_end）
                    // 修复：将收盘tick合并到前一分钟窗口，不生成独立的bar
                    // 例如：23:00:00 → 合并到 22:59 窗口 → 23:00 bar包含完整收盘数据
                    let mut is_closing_tick = false;
                    for (_start, end) in &trading_ranges {
                        if time_part == *end {
                            is_closing_tick = true;
                            break;
                        }
                    }

                    if is_auction {
                        // 集合竞价tick：分配到最后一分钟的窗口
                        // 例如：09:25-09:29的tick都分配到09:29窗口
                        // 注意：auction_end是开盘时间（如09:30），最后一分钟是09:29
                        let last_minute = auction_end_time - chrono::Duration::minutes(1);
                        let last_minute_window = naive.date().and_time(last_minute);
                        last_minute_window.format("%Y-%m-%d %H:%M:00").to_string()
                    } else if is_closing_tick {
                        // 收盘tick：合并到前一分钟窗口
                        // 例如：23:00:00 → 分配到 22:59 窗口
                        // 这样 23:00 bar 包含完整的收盘数据（包括收盘撮合）
                        // 不会生成 23:01 bar，避免数据分散
                        let prev_minute = time_part - chrono::Duration::minutes(1);
                        let prev_minute_window = naive.date().and_time(prev_minute);
                        prev_minute_window.format("%Y-%m-%d %H:%M:00").to_string()
                    } else {
                        // 正常tick：向下取整到分钟
                        let truncated = naive.with_second(0).unwrap().with_nanosecond(0).unwrap();
                        truncated.format("%Y-%m-%d %H:%M:00").to_string()
                    }
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

/// 过滤掉超出交易时段的bar（在计算oi_diff之前调用，避免shift拿到超时bar的oi）
fn filter_trading_hours_bars(df: DataFrame, comd: &str) -> anyhow::Result<DataFrame> {
    let trading_ranges = (*TRADE_SESSION_MAP).get_trading_ranges(comd);
    let auction_periods = (*TRADE_SESSION_MAP).get_auction_periods(comd);

    if trading_ranges.is_empty() && auction_periods.is_empty() {
        // 没有时段限制，直接返回
        return Ok(df);
    }

    let mut valid_indices = Vec::new();
    let trade_time_col = df.column("trade_time")?;

    for i in 0..df.height() {
        if let Ok(any_val) = trade_time_col.get(i) {
            let time_part = match any_val {
                AnyValue::String(v) => {
                    if let Ok(naive_dt) = NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S") {
                        naive_dt.time()
                    } else {
                        continue;
                    }
                },
                _ => continue,
            };

            // 检查时间是否在交易时段或集合竞价时段内
            let is_in_trading = trading_ranges.iter().any(|(start, end)| {
                time_part >= *start && time_part <= *end
            });
            let is_in_auction = auction_periods.iter().any(|(start, end)| {
                time_part >= *start && time_part < *end
            });
            let is_valid = is_in_trading || is_in_auction;

            if is_valid {
                valid_indices.push(i as u32);
            }
        }
    }

    if valid_indices.is_empty() {
        return Err(anyhow::anyhow!("过滤后bar数据为空"));
    }

    let filtered_df = df.take(&ChunkedArray::from_vec("", valid_indices))?;
    Ok(filtered_df)
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
            // 修正：第一个bar（prev为NULL）时，oi_diff应该为0（无前置bar无法计算差分）
            // 之前错误地使用last_open_interest，导致第一个bar的oi_diff等于OI本身
            (col("last_open_interest").cast(DataType::Int64) - col("prev_open_interest").cast(DataType::Int64))
                .fill_null(lit(0))  // 第一个bar的oi_diff=0
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

/// 向量化历史数据处理：完全基于polars向量化操作
pub fn process_parquet_optimized<P: AsRef<Path>>(parquet_path: P, output_parquet_path: P) -> anyhow::Result<()> {
    // 1. 读取和过滤数据
    let df = read_tick_from_parquet(&parquet_path)?;

    // 1.5. 检查过滤后是否有数据
    if df.height() == 0 {
        return Err(anyhow::anyhow!("所有tick被过滤，没有有效数据"));
    }

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
            col("open"), col("high"), col("low"), col("close"), col("pre_settle"),
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

    // 5.5. 过滤掉超出交易时段的bar（必须在计算oi_diff之前，避免shift拿到超时bar的oi）
    let grouped_df = filter_trading_hours_bars(grouped_df, &comd)?;

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

/// 将NaiveDateTime向上取整到5分钟边界（右边界，符合行业标准）
/// 例如：22:51-22:55 → 22:55, 22:56-23:00 → 23:00
fn floor_to_5min_naive(dt: &NaiveDateTime) -> NaiveDateTime {
    let ts = dt.and_utc().timestamp();
    // 向上取整到5分钟边界：((ts + 299) / 300) * 300
    // 如果ts已经是边界（如22:55:00），保持不变
    // 如果ts不是边界（如22:51:00），向上取整到下一个边界（22:55:00）
    let ceiled = ((ts + 299) / 300) * 300;
    DateTime::from_timestamp(ceiled, 0).unwrap().naive_utc()
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
fn process_batch_simple(data_source_name: &str, mode: &str, num_threads: usize) -> anyhow::Result<()> {
    let data_source = format!("{}/{}", DATA_ROOT, data_source_name);

    let (input_dir, output_dir, input_suffix, output_suffix) = match mode {
        "1min" => {
            // tick -> 1min
            (
                format!("{}/full_tick", data_source),
                format!("{}/full_bar_1min", data_source),
                "",
                "_1min"
            )
        },
        "5min" => {
            // 1min -> 5min
            (
                format!("{}/full_bar_1min", data_source),
                format!("{}/full_bar_5min", data_source),
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

                // 跳过特殊测试合约（88、99等后缀）
                // 例如：Y88, I88A2, Y99, etc.
                if contract.contains("88") || contract.contains("99") {
                    println!("⊘ 线程{}: {} - 跳过测试合约", thread_id, contract);
                    continue;
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
                        } else if error_msg.contains("所有tick被过滤") {
                            eprintln!("⊘ 线程{}: {} - 无有效tick数据，跳过", thread_id, contract);
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

// 主函数：支持批量并行和单合约处理
fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        println!("用法: {} <data_source> [mode] [threads|contract]", args[0]);
        println!("示例:");
        println!("  {} dongzheng_data 1min      # 批量处理，4线程", args[0]);
        println!("  {} dongzheng_data 1min 8    # 批量处理，8线程", args[0]);
        println!("  {} dongzheng_data 1min bb1710  # 单合约", args[0]);
        return Ok(());
    }

    let data_source = args.get(1).map(|s| s.as_str()).unwrap_or("dongzheng_data");
    let mode = args.get(2).map(|s| s.as_str()).unwrap_or("1min");

    if mode != "1min" && mode != "5min" {
        eprintln!("错误: mode必须是1min或5min");
        return Ok(());
    }

    // 第3个参数：数字=线程数（批量），字符串=合约名（单个）
    if let Some(third) = args.get(3) {
        if let Ok(threads) = third.parse::<usize>() {
            // 批量处理
            println!("批量处理: {} {} ({}线程)", data_source, mode, threads);
            process_batch_simple(data_source, mode, threads)?;
        } else {
            // 单合约处理
            // 跳过特殊测试合约
            if third.contains("88") || third.contains("99") {
                println!("⊘ {} - 跳过测试合约", third);
                return Ok(());
            }

            println!("单合约: {} {} {}", data_source, mode, third);
            let data_path = format!("{}/{}", DATA_ROOT, data_source);
            let (input_dir, output_dir, input_suffix, output_suffix) =
                if mode == "1min" {
                    ("full_tick", "full_bar_1min", "", "_1min")
                } else {
                    ("full_bar_1min", "full_bar_5min", "_1min", "_5min")
                };

            let input = format!("{}/{}/{}{}.parquet", data_path, input_dir, third, input_suffix);
            let output = format!("{}/{}/{}{}.parquet", data_path, output_dir, third, output_suffix);

            let result = if mode == "1min" {
                process_parquet_optimized(&input, &output)
            } else {
                process_1min_to_5min(&input, &output)
            };

            match result {
                Ok(()) => println!("✓ 成功"),
                Err(e) => eprintln!("✗ 失败: {}", e),
            }
        }
    } else {
        // 默认批量处理，4线程
        println!("批量处理: {} {} (4线程)", data_source, mode);
        process_batch_simple(data_source, mode, 4)?;
    }

    Ok(())
}


// // 主函数：单文件处理(用于测试，请勿删除)
// fn main() -> Result<(), Box<dyn std::error::Error>> {
//     let args: Vec<String> = std::env::args().collect();

//     if args.len() < 4 {
//         println!("使用方法: {} <source_name> <freq> <contract>", args[0]);
//         println!("示例 tick->1min: {} dongzheng_data 1min bb1710", args[0]);
//         println!("示例 1min->5min: {} dongzheng_data 5min bb1710", args[0]);
//         return Ok(());
//     }

//     let source_name = &args[1];
//     let freq = &args[2];
//     let contract = &args[3];

//     let data_dir = "/data/future_data";
//     let data_source = format!("{}/{}", data_dir, source_name);

//     match freq.as_str() {
//         "1min" => {
//             // tick -> 1min
//             let input_path = format!("{}/full_tick/{}.parquet", data_source, contract);
//             let output_path = format!("{}/full_bar_1min/{}_1min.parquet", data_source, contract);

//             println!("处理模式: tick -> 1min");
//             println!("输入文件: {}", input_path);
//             println!("输出文件: {}", output_path);

//             match process_parquet_optimized(&input_path, &output_path) {
//                 Ok(()) => println!("✓ 成功生成1分钟bar: {}", output_path),
//                 Err(e) => println!("✗ 处理失败: {}", e),
//             }
//         },
//         "5min" => {
//             // 1min -> 5min
//             let input_path = format!("{}/full_bar_1min/{}_1min.parquet", data_source, contract);
//             let output_path = format!("{}/full_bar_5min/{}_5min.parquet", data_source, contract);

//             println!("处理模式: 1min -> 5min");
//             println!("输入文件: {}", input_path);
//             println!("输出文件: {}", output_path);

//             match process_1min_to_5min(&input_path, &output_path) {
//                 Ok(()) => println!("✓ 成功生成5分钟bar: {}", output_path),
//                 Err(e) => println!("✗ 处理失败: {}", e),
//             }
//         },
//         _ => {
//             println!("错误: 不支持的频率 '{}', 请使用 '1min' 或 '5min'", freq);
//         }
//     }

//     Ok(())
// }
