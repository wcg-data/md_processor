// consumer.rs 是行情消费的核心文件，负责从共享内存中读取行情数据，并进行处理。

use std::{collections::HashSet, thread, time::Duration};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use chrono::{Timelike, Local};
use crate::ring_buffer::SharedRingBuffer;
use crate::bar1min_aggregator::{Bar1MinAggregator};
use crate::bar5min_aggregator::{Bar5MinAggregator};
use crate::aggregator_manager::AggregatorManager;

use crate::kafka_client;

/// 枚举聚合模式：单合约或多合约
pub enum AggregationMode {
    /// 单合约模式，只处理指定合约的 tick 并聚合成分钟 K 线
    /// `HashSet<String>` 用于快速判断是否在需要处理的合约集合中
    Single(HashSet<String>),
    /// 多合约模式，自动为出现的每个合约维护一个聚合器
    Multi,
}

/// 消费主循环，根据聚合模式从共享环形缓冲区读取 tick 数据并处理
pub fn consume_loop_tick(ring_buffer: &mut SharedRingBuffer, mode: AggregationMode) {
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
        AggregationMode::Single(contracts) => single_contract_loop(ring_buffer, &running, contracts),
        AggregationMode::Multi => multi_contract_loop(ring_buffer, &running),
    }

    println!("tick 消费者已安全退出");
}

/// 单合约聚合处理
fn single_contract_loop(
    ring_buffer: &mut SharedRingBuffer,
    running: &Arc<AtomicBool>,
    contracts_filter: HashSet<String>,
) {
    let mut aggregator = Bar1MinAggregator::new();

    while running.load(Ordering::SeqCst) {
        match ring_buffer.pop_market_data() {
            Some(tick) => {
                let contract_str = String::from_utf8_lossy(&tick.contract)
                    .trim_end_matches('\0')
                    .to_string();
                if contracts_filter.contains(&contract_str) {
                    // tick.print(); // 如需调试可解除注释
                    if let Some(bar) = aggregator.on_tick(&mut tick.clone()) {
                        // bar.print();
                        // bar.send_to_kafka();
                        kafka_client::send_bar_data(&bar, "1min");
                    }
                }
            }
            None => thread::sleep(Duration::from_millis(10)),
        }
    }

    // 程序退出前 flush 最后一条
    if let Some(bar) = aggregator.flush() {
        // bar.print();
        // bar.send_to_kafka();
        kafka_client::send_bar_data(&bar, "1min");
    }
}

// 多合约聚合处理
fn multi_contract_loop(ring_buffer: &mut SharedRingBuffer, running: &Arc<AtomicBool>) {
    let mut manager = AggregatorManager::new();
    let mut last_minute = Local::now().minute();

    while running.load(Ordering::SeqCst) {
        let now_minute = Local::now().minute();

        // ---------------- 每分钟触发 ----------------
        if now_minute != last_minute {
            last_minute = now_minute;

            // ① flush 1 min bar（活跃合约）
            for bar1min in manager.flush_all_bar1min_active() {
                kafka_client::send_bar_data(&bar1min, "1min");

                // ② 把 1 min 喂入 5 min 聚合器
                if let Some(bar5min) = manager.on_bar_1min(&bar1min) {
                    kafka_client::send_bar_data(&bar5min, "5min");
                }
            }
        }

        // ---------------- Tick 驱动 ----------------
        if let Some(mut tick) = ring_buffer.pop_market_data() {
            // ③ Tick → 1 min
            if let Some(bar1min) = manager.on_tick(&mut tick) {
                kafka_client::send_bar_data(&bar1min, "1min");

                // ④ 及时投喂 5 min 聚合器
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

    // 最后确保剩余 5 min bar 也 flush
    for bar5min in manager.flush_all_bar5min_active() {
        kafka_client::send_bar_data(&bar5min, "5min");
    }
}
