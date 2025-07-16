// consumer.rs 是行情消费的核心文件，负责从共享内存中读取行情数据，并进行处理。

use std::{collections::HashSet, thread, time::Duration};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use chrono::{Timelike, Utc};
use crate::ring_buffer::SharedRingBuffer;
use crate::bar_aggregator::BarAggregator;
use crate::bar_aggregator::AggregatorManager;
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
    let mut aggregator = BarAggregator::new();

    while running.load(Ordering::SeqCst) {
        match ring_buffer.pop_market_data() {
            Some(tick) => {
                let contract_str = String::from_utf8_lossy(&tick.contract)
                    .trim_end_matches('\0')
                    .to_string();
                if contracts_filter.contains(&contract_str) {
                    // tick.print(); // 如需调试可解除注释
                    if let Some(bar) = aggregator.on_tick(&tick) {
                        // bar.print();
                        // bar.send_to_kafka();
                        kafka_client::send_bar_1min(&bar).unwrap_or_else(|e| eprintln!("发送失败: {:?}", e));
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
        kafka_client::send_bar_1min(&bar).unwrap_or_else(|e| eprintln!("发送失败: {:?}", e));
    }
}

// 多合约聚合处理
fn multi_contract_loop(ring_buffer: &mut SharedRingBuffer, running: &Arc<AtomicBool>) {
    let mut manager = AggregatorManager::new();
    let mut last_minute = Utc::now().minute();

    while running.load(Ordering::SeqCst) {
        if let Some(tick) = ring_buffer.pop_market_data() {
            // tick.print(); // 如需调试可解除注释
            if let Some(bar) = manager.on_tick(&tick) {
                // bar.print();  // 若需要实时查看可解除注释
                // bar.send_to_kafka();
                //打印当前时间，格式为：2025-07-17 10:00:00.000
                let now = Utc::now();
                let formatted_time = now.format("%Y-%m-%d %H:%M:%S.%3f");
                println!("当前时间：{}", formatted_time);
                kafka_client::send_bar_1min(&bar).unwrap_or_else(|e| eprintln!("发送失败: {:?}", e));
            }
        } else {
            // 时间驱动强制合并: 每分钟统一 flush 所有合约
            let now_minute = Utc::now().minute();
            if now_minute != last_minute {
                last_minute = now_minute;
                for bar in manager.flush_all() {
                    // bar.print();
                    // bar.send_to_kafka();
                    //打印当前时间，格式为：2025-07-17 10:00:00.000
                    let now = Utc::now();
                    let formatted_time = now.format("%Y-%m-%d %H:%M:%S.%3f");
                    println!("当前时间：{}", formatted_time);
                    kafka_client::send_bar_1min(&bar).unwrap_or_else(|e| eprintln!("发送失败: {:?}", e));
                }
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    // 程序退出前 flush 最后一批
    for bar in manager.flush_all() {
        // bar.print();
        // bar.send_to_kafka();
        kafka_client::send_bar_1min(&bar).unwrap_or_else(|e| eprintln!("发送失败: {:?}", e));
    }
}

