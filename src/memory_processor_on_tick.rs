// memory_processor_on_tick.rs - 从共享内存实时处理tick数据并聚合成分钟K线

use std::{collections::HashSet, thread, time::Duration, env};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use chrono::{Timelike, Local};

use md_processor::shared_memory::map_shared_ring_buffer;
use md_processor::ring_buffer::SharedRingBuffer;
use md_processor::bar1min_aggregator::Bar1MinAggregator;
use md_processor::aggregator_manager::AggregatorManager;
use md_processor::kafka_client;

/// 聚合模式：单合约或多合约
pub enum AggregationMode {
    /// 单合约模式，只处理指定合约的 tick 并聚合成分钟 K 线
    Single(HashSet<String>),
    /// 多合约模式，自动为出现的每个合约维护一个聚合器
    Multi,
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
                        kafka_client::send_bar_data(&bar, "1min");
                    }
                }
            }
            None => thread::sleep(Duration::from_millis(10)),
        }
    }

    // 程序退出前 flush 最后一条
    if let Some(bar) = aggregator.flush() {
        kafka_client::send_bar_data(&bar, "1min");
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

    while running.load(Ordering::SeqCst) {
        let now_minute = Local::now().minute();

        // ---------------- 每分钟触发 ----------------
        if now_minute != last_minute {
            last_minute = now_minute;

            // ① flush 1 min bar（活跃合约）
            for bar1min in manager.flush_all_bar1min_active() {
                kafka_client::send_bar_data(&bar1min, "1min");

                // ② 把 1 min 喂入 5 min 聚合器
                if let Some(bar5min) = manager.on_bar_1min(&bar1min) {
                    kafka_client::send_bar_data(&bar5min, "5min");
                }
            }
        }

        // ---------------- Tick 驱动 ----------------
        if let Some(mut tick) = ring_buffer.pop_market_data() {
            // ③ Tick → 1 min
            if let Some(bar1min) = manager.on_tick(&mut tick) {
                kafka_client::send_bar_data(&bar1min, "1min");

                // ④ 及时投喂 5 min 聚合器
                if let Some(bar5min) = manager.on_bar_1min(&bar1min) {
                    kafka_client::send_bar_data(&bar5min, "5min");
                }
            }
        } else {
            thread::sleep(Duration::from_millis(5));
        }
    }

    // ---------- 程序退出前：flush 最后一批 ----------
    for bar1min in manager.flush_all_bar1min_active() {
        kafka_client::send_bar_data(&bar1min, "1min");
        if let Some(bar5min) = manager.on_bar_1min(&bar1min) {
            kafka_client::send_bar_data(&bar5min, "5min");
        }
    }

    // 最后确保剩余 5 min bar 也 flush
    for bar5min in manager.flush_all_bar5min_active() {
        kafka_client::send_bar_data(&bar5min, "5min");
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