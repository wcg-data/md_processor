// memory_processor_on_tick.rs - 从共享内存实时处理tick数据并聚合成分钟K线

use std::{collections::HashSet, thread, time::Duration, env};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use chrono::{Timelike, Local};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Mutex;

use md_processor::shared_memory::map_shared_ring_buffer;
use md_processor::ring_buffer::SharedRingBuffer;
use md_processor::bar1min_aggregator::Bar1MinAggregator;
use md_processor::aggregator_manager::AggregatorManager;
use md_processor::trade_session_loader::TRADE_SESSION_MAP;
// use md_processor::kafka_client;  // 临时注释，后续改回Kafka时取消注释
use md_processor::md_structures::BarData;

/// 聚合模式：单合约或多合约
pub enum AggregationMode {
    /// 单合约模式，只处理指定合约的 tick 并聚合成分钟 K 线
    Single(HashSet<String>),
    /// 多合约模式，自动为出现的每个合约维护一个聚合器
    Multi,
}

// ========== 临时CSV输出功能（后续会改回Kafka） ==========

/// 全局CSV文件句柄（带缓冲区）
struct CsvWriter {
    file_1min: Mutex<std::io::BufWriter<std::fs::File>>,
    file_5min: Mutex<std::io::BufWriter<std::fs::File>>,
}

lazy_static::lazy_static! {
    static ref CSV_WRITER: CsvWriter = {
        let file_1min = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open("output/md_snapshot_RTdongzheng_1min.csv.20251027")
            .expect("无法创建1min CSV文件");

        let file_5min = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open("output/md_snapshot_RTdongzheng_5min.csv.20251027")
            .expect("无法创建5min CSV文件");

        let mut writer_1min = std::io::BufWriter::new(file_1min);
        let mut writer_5min = std::io::BufWriter::new(file_5min);

        // 写入CSV表头（与parquet文件列名一致）
        let header = "contract,contract_yymm,comd,exchange,date,trade_time,open,high,low,close,prev_close,pre_settle,volume,turnover,open_interest,open_interest_diff,bid_price_1,bid_volume_1,ask_price_1,ask_volume_1,mid_price,vwap,log_return,maturity_month,maturity_day\n";
        writer_1min.write_all(header.as_bytes()).expect("无法写入1min表头");
        writer_5min.write_all(header.as_bytes()).expect("无法写入5min表头");

        CsvWriter {
            file_1min: Mutex::new(writer_1min),
            file_5min: Mutex::new(writer_5min),
        }
    };
}

/// 临时函数：将BarData写入CSV（替代Kafka）
fn write_bar_to_csv(bar: &BarData, freq: &str) {
    let csv_line = format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
        bar.contract.trim_end_matches('\0'),
        bar.contract_yymm.trim_end_matches('\0'),
        bar.comd.trim_end_matches('\0'),
        bar.exchange.trim_end_matches('\0'),
        bar.date,
        bar.trade_time.trim_end_matches('\0'),
        bar.open,
        bar.high,
        bar.low,
        bar.close,
        bar.prev_close,
        bar.pre_settle,
        bar.volume,
        bar.turnover,
        bar.open_interest,
        bar.open_interest_diff,
        if bar.bid_price_1 == 0.0 { String::new() } else { bar.bid_price_1.to_string() },
        if bar.bid_volume_1 == 0 { String::new() } else { bar.bid_volume_1.to_string() },
        if bar.ask_price_1 == 0.0 { String::new() } else { bar.ask_price_1.to_string() },
        if bar.ask_volume_1 == 0 { String::new() } else { bar.ask_volume_1.to_string() },
        if bar.mid_price == 0.0 { String::new() } else { bar.mid_price.to_string() },
        bar.vwap,
        if bar.log_return.is_nan() { String::new() } else { bar.log_return.to_string() },
        bar.maturity_month,
        bar.maturity_day
    );

    match freq {
        "1min" => {
            let mut writer = CSV_WRITER.file_1min.lock().unwrap();
            writer.write_all(csv_line.as_bytes()).expect("写入1min CSV失败");
        }
        "5min" => {
            let mut writer = CSV_WRITER.file_5min.lock().unwrap();
            writer.write_all(csv_line.as_bytes()).expect("写入5min CSV失败");
        }
        _ => {}
    }
}

/// 核心处理函数：从共享内存消费tick数据并实时聚合
pub fn process_memory_on_tick(
    ring_buffer: &mut SharedRingBuffer,
    mode: AggregationMode
) -> anyhow::Result<()> {
    println!("tick 消费者启动，等待数据… mode = {:?}", match &mode {
        AggregationMode::Single(set) => format!("Single({})", set.len()),
        AggregationMode::Multi => "Multi".into(),
    });

    // 创建信号处理的标志，用于优雅退出
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    // 捕获 Ctrl+C 信号
    ctrlc::set_handler(move || {
        println!("收到退出信号，准备结束处理…");
        r.store(false, Ordering::SeqCst);
    })
    .expect("无法设置 Ctrl+C 处理器");

    match mode {
        AggregationMode::Single(contracts) => {
            single_contract_loop(ring_buffer, &running, contracts)?
        }
        AggregationMode::Multi => {
            multi_contract_loop(ring_buffer, &running)?
        }
    }

    println!("tick 消费者已安全退出");
    Ok(())
}

/// 单合约聚合处理
fn single_contract_loop(
    ring_buffer: &mut SharedRingBuffer,
    running: &Arc<AtomicBool>,
    contracts_filter: HashSet<String>,
) -> anyhow::Result<()> {
    let mut aggregator = Bar1MinAggregator::new();

    while running.load(Ordering::SeqCst) {
        match ring_buffer.pop_market_data() {
            Some(tick) => {
                let contract_str = String::from_utf8_lossy(&tick.contract)
                    .trim_end_matches('\0')
                    .to_string();
                if contracts_filter.contains(&contract_str) {
                    if let Some(bar) = aggregator.on_tick(&mut tick.clone()) {
                        // kafka_client::send_bar_data(&bar, "1min");  // 临时注释
                        write_bar_to_csv(&bar, "1min");  // 临时CSV输出
                    }
                }
            }
            None => thread::sleep(Duration::from_millis(10)),
        }
    }

    // 程序退出前 flush 最后一条
    if let Some(bar) = aggregator.flush() {
        // kafka_client::send_bar_data(&bar, "1min");  // 临时注释
        write_bar_to_csv(&bar, "1min");  // 临时CSV输出
    }

    Ok(())
}

/// 多合约聚合处理
fn multi_contract_loop(
    ring_buffer: &mut SharedRingBuffer,
    running: &Arc<AtomicBool>
) -> anyhow::Result<()> {
    let mut manager = AggregatorManager::new();
    let mut last_minute = Local::now().minute();
    let mut prev_active_contracts: HashSet<String> = HashSet::new();

    while running.load(Ordering::SeqCst) {
        let now_minute = Local::now().minute();

        // ---------------- 每分钟触发 ----------------
        if now_minute != last_minute {
            last_minute = now_minute;

            // 更新活跃合约状态
            let now_naive = Local::now().naive_local();
            manager.update_active_contracts(&TRADE_SESSION_MAP, now_naive);

            // 检测交易时段结束的合约（之前活跃，现在不活跃）
            let current_active = manager.get_active_contracts().clone();
            let ended_contracts: Vec<String> = prev_active_contracts
                .iter()
                .filter(|c| !current_active.contains(*c))
                .cloned()
                .collect();

            // 刷新交易时段结束的合约（确保最后的5min窗口被输出）
            if !ended_contracts.is_empty() {
                println!("检测到 {} 个合约交易时段结束，主动flush: {:?}",
                    ended_contracts.len(), ended_contracts);
                for bar5min in manager.flush_contracts_session_ended(&ended_contracts) {
                    // kafka_client::send_bar_data(&bar5min, "5min");  // 临时注释
                    write_bar_to_csv(&bar5min, "5min");  // 临时CSV输出
                }
            }

            // 更新前一分钟的活跃合约集合
            prev_active_contracts = current_active;

            // ① flush 1 min bar（活跃合约）
            for bar1min in manager.flush_all_bar1min_active() {
                // kafka_client::send_bar_data(&bar1min, "1min");  // 临时注释
                write_bar_to_csv(&bar1min, "1min");  // 临时CSV输出

                // ② 把 1 min 喂入 5 min 聚合器
                if let Some(bar5min) = manager.on_bar_1min(&bar1min) {
                    // kafka_client::send_bar_data(&bar5min, "5min");  // 临时注释
                    write_bar_to_csv(&bar5min, "5min");  // 临时CSV输出
                }
            }
        }

        // ---------------- Tick 驱动 ----------------
        if let Some(mut tick) = ring_buffer.pop_market_data() {
            // ③ Tick → 1 min
            if let Some(bar1min) = manager.on_tick(&mut tick) {
                // kafka_client::send_bar_data(&bar1min, "1min");  // 临时注释
                write_bar_to_csv(&bar1min, "1min");  // 临时CSV输出

                // ④ 及时投喂 5 min 聚合器
                if let Some(bar5min) = manager.on_bar_1min(&bar1min) {
                    // kafka_client::send_bar_data(&bar5min, "5min");  // 临时注释
                    write_bar_to_csv(&bar5min, "5min");  // 临时CSV输出
                }
            }
        } else {
            thread::sleep(Duration::from_millis(5));
        }
    }

    // ---------- 程序退出前：flush 最后一批 ----------
    for bar1min in manager.flush_all_bar1min_active() {
        // kafka_client::send_bar_data(&bar1min, "1min");  // 临时注释
        write_bar_to_csv(&bar1min, "1min");  // 临时CSV输出
        if let Some(bar5min) = manager.on_bar_1min(&bar1min) {
            // kafka_client::send_bar_data(&bar5min, "5min");  // 临时注释
            write_bar_to_csv(&bar5min, "5min");  // 临时CSV输出
        }
    }

    // 最后确保剩余 5 min bar 也 flush
    for bar5min in manager.flush_all_bar5min_active() {
        // kafka_client::send_bar_data(&bar5min, "5min");  // 临时注释
        write_bar_to_csv(&bar5min, "5min");  // 临时CSV输出
    }

    Ok(())
}

/// 主函数：程序入口
fn main() {
    // 获取命令行参数
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("用法：{} <共享内存名称>", args[0]);
        eprintln!("示例：{} /md_shm", args[0]);
        return;
    }

    let shm_name = &args[1];

    // 映射共享内存
    let (ring_buffer, _mmap) = match map_shared_ring_buffer(shm_name) {
        Ok((rb, mmap)) => (rb, mmap),
        Err(e) => {
            eprintln!("共享内存映射失败: {}", e);
            return;
        }
    };

    if !ring_buffer.is_initialized.load(Ordering::Relaxed) {
        eprintln!("警告：共享内存未初始化");
    }

    // 设置聚合模式（多合约模式）
    let mode = AggregationMode::Multi;

    // 启动核心处理循环
    if let Err(e) = process_memory_on_tick(ring_buffer, mode) {
        eprintln!("处理失败: {}", e);
    }
}