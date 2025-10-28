// test_auction_batch.rs - 测试 on_batch 处理器的集合竞价逻辑
//
// 测试目标：
// 1. 对于跨多分钟的集合竞价tick（如IF1005），应该只生成1个集合竞价bar
// 2. 集合竞价bar的时间戳应该是开盘时间（09:30）
// 3. 集合竞价bar应该包含所有集合竞价时段的tick数据

use polars::prelude::*;
use std::path::Path;

// 导入处理函数（需要在 lib.rs 中暴露）
#[path = "../src/parquet_processor_on_batch.rs"]
mod parquet_processor_on_batch;

#[test]
fn test_auction_bar_generation_for_multi_minute_ticks() {
    println!("测试：跨多分钟集合竞价tick处理");
    println!("{}", "=".repeat(80));

    let contract = "IF1005";
    let data_dir = "/data/future_data/dongzheng_data";

    // 1. 处理数据
    let input_file = format!("{}/full_tick/{}.parquet", data_dir, contract);
    let output_file = format!("/tmp/{}_1min_test.parquet", contract);

    // 检查输入文件是否存在
    if !Path::new(&input_file).exists() {
        println!("⚠️  跳过测试: 输入文件不存在 {}", input_file);
        return;
    }

    // 调用处理函数
    let result = md_processor::parquet_processor_on_batch::process_parquet_optimized(
        &input_file,
        &output_file
    );

    assert!(result.is_ok(), "处理失败: {:?}", result.err());
    println!("✓ 数据处理完成");

    // 2. 读取输出文件
    let df = LazyFrame::scan_parquet(&output_file, Default::default())
        .expect("无法读取输出文件")
        .collect()
        .expect("无法收集数据");

    // 3. 获取第一天的数据
    let first_date = df.column("date")
        .expect("无date列")
        .get(0)
        .expect("无数据")
        .to_string()
        .replace("\"", "");

    println!("第一天: {}", first_date);

    // 4. 筛选09:26-09:30的bar（集合竞价时段应该只有09:30这一个bar）
    let morning_bars = df.clone().lazy()
        .filter(
            col("date").eq(lit(&first_date))
                .and(col("trade_time").str().slice(lit(11), Some(5)).gt_eq(lit("09:26")))
                .and(col("trade_time").str().slice(lit(11), Some(5)).lt_eq(lit("09:30")))
        )
        .collect()
        .expect("筛选失败");

    println!("\n09:26-09:30的bar数量: {}", morning_bars.height());
    println!("-" * 80);

    // 打印这些bar的详情
    for i in 0..morning_bars.height() {
        let time = morning_bars.column("trade_time").unwrap().get(i).unwrap().to_string().replace("\"", "");
        let volume = morning_bars.column("volume").unwrap().get(i).unwrap();
        let oi = morning_bars.column("open_interest").unwrap().get(i).unwrap();
        println!("{}: vol={}, oi={}", time, volume, oi);
    }

    // 5. 验证结果
    println!("\n验证结果:");
    println!("=" * 80);

    // 应该只有1个bar（09:30）
    assert_eq!(
        morning_bars.height(),
        1,
        "❌ 失败: 期望只有1个集合竞价bar（09:30），实际有{}个bar",
        morning_bars.height()
    );

    // 验证这个bar的时间是09:30
    let bar_time = morning_bars.column("trade_time")
        .unwrap()
        .get(0)
        .unwrap()
        .to_string()
        .replace("\"", "");

    assert!(
        bar_time.contains("09:30:00"),
        "❌ 失败: 期望集合竞价bar时间为09:30:00，实际为{}",
        bar_time
    );

    // 验证volume（应该包含所有集合竞价tick）
    // IF1005第一天应该有约3600+的volume
    let volume: i32 = match morning_bars.column("volume").unwrap().get(0).unwrap() {
        AnyValue::Int32(v) => v,
        AnyValue::UInt32(v) => v as i32,
        _ => panic!("volume类型错误"),
    };

    println!("✓ 集合竞价bar时间: {}", bar_time);
    println!("✓ 集合竞价bar volume: {}", volume);

    // volume应该大于3000（包含所有集合竞价tick）
    assert!(
        volume > 3000,
        "❌ 失败: 集合竞价bar的volume应该>3000（包含所有集合竞价tick），实际为{}",
        volume
    );

    // 6. 验证09:26-09:29没有错误的bar
    let wrong_bars = df.lazy()
        .filter(
            col("date").eq(lit(&first_date))
                .and(col("trade_time").str().slice(lit(11), Some(5)).gt_eq(lit("09:26")))
                .and(col("trade_time").str().slice(lit(11), Some(5)).lt(lit("09:30")))
        )
        .collect()
        .expect("筛选失败");

    assert_eq!(
        wrong_bars.height(),
        0,
        "❌ 失败: 不应该有09:26-09:29的bar，实际有{}个",
        wrong_bars.height()
    );

    println!("✓ 09:26-09:29没有错误的bar");

    println!("\n✅ 所有测试通过！");
    println!("=" * 80);

    // 清理临时文件
    let _ = std::fs::remove_file(&output_file);
}

#[test]
fn test_auction_bar_generation_for_single_minute_ticks() {
    println!("测试：单分钟集合竞价tick处理");
    println!("=" * 80);

    let contract = "IF2404";  // 2024年的合约，只在09:29有tick
    let data_dir = "/data/future_data/dongzheng_data";

    let input_file = format!("{}/full_tick/{}.parquet", data_dir, contract);
    let output_file = format!("/tmp/{}_1min_test.parquet", contract);

    if !Path::new(&input_file).exists() {
        println!("⚠️  跳过测试: 输入文件不存在 {}", input_file);
        return;
    }

    let result = md_processor::parquet_processor_on_batch::process_parquet_optimized(
        &input_file,
        &output_file
    );

    assert!(result.is_ok(), "处理失败: {:?}", result.err());
    println!("✓ 数据处理完成");

    let df = LazyFrame::scan_parquet(&output_file, Default::default())
        .expect("无法读取输出文件")
        .collect()
        .expect("无法收集数据");

    let first_date = df.column("date")
        .expect("无date列")
        .get(0)
        .expect("无数据")
        .to_string()
        .replace("\"", "");

    println!("第一天: {}", first_date);

    let morning_bars = df.lazy()
        .filter(
            col("date").eq(lit(&first_date))
                .and(col("trade_time").str().slice(lit(11), Some(5)).gt_eq(lit("09:26")))
                .and(col("trade_time").str().slice(lit(11), Some(5)).lt_eq(lit("09:30")))
        )
        .collect()
        .expect("筛选失败");

    println!("\n09:26-09:30的bar数量: {}", morning_bars.height());

    // 对于只在09:29有tick的合约，应该只有1个09:30的bar
    assert_eq!(
        morning_bars.height(),
        1,
        "期望只有1个集合竞价bar，实际有{}个",
        morning_bars.height()
    );

    let bar_time = morning_bars.column("trade_time")
        .unwrap()
        .get(0)
        .unwrap()
        .to_string()
        .replace("\"", "");

    assert!(
        bar_time.contains("09:30:00"),
        "期望集合竞价bar时间为09:30:00，实际为{}",
        bar_time
    );

    println!("✓ 集合竞价bar时间: {}", bar_time);
    println!("\n✅ 测试通过！");

    let _ = std::fs::remove_file(&output_file);
}
