// consumer.rs 是行情消费的核心文件，负责从共享内存中读取行情数据，并进行处理。

use std::{thread, time::Duration};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use chrono::Utc;
use chrono::Timelike;
use crate::ring_buffer::SharedRingBuffer;
// use crate::bar_aggregator::BarAggregator;
use crate::bar_aggregator::AggregatorManager;

/// 消费循环，读取tick数据并处理
pub fn consume_loop_tick(ring_buffer: &mut SharedRingBuffer) {
    println!("消费者启动，等待数据...");
    
    // 创建信号处理的标志，用于优雅退出
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    
    // 捕获Ctrl+C信号
    ctrlc::set_handler(move || {
        println!("收到退出信号，准备结束处理...");
        r.store(false, Ordering::SeqCst);
    }).expect("无法设置Ctrl+C处理器");

    // // 单合约合并
    // let mut aggregator = BarAggregator::new();
    
    // // 主循环
    // while running.load(Ordering::SeqCst) {
    //     match ring_buffer.pop_market_data() {
    //         Some(tick) => {
    //             // 只处理ag2505合约的tick数据
    //             let contract_str = String::from_utf8_lossy(&tick.contract).trim_end_matches('\0').to_string();
    //             if contract_str == "au2506" || contract_str == "ag2506" {
    //                 // 打印tick数据
    //                 tick.print();
    //                 if let Some(bar) = aggregator.on_tick(&tick) {
    //                     // println!("生成新的分钟K线:");
    //                     bar.print();
    //                 }
    //             }
    //         }
    //         None => {
    //             thread::sleep(Duration::from_millis(10));
    //         }
    //     }
    // }

    // 多合约合并
    let mut manager = AggregatorManager::new();
    let mut last_minute = Utc::now().minute();

    while running.load(Ordering::SeqCst) {
        if let Some(tick) = ring_buffer.pop_market_data() {
            // tick.print();
            if let Some(bar) = manager.on_tick(&tick) {
                // tick.print();
            }
        } else {
            // 时间驱动强制合并：每分钟统一 flush 所有合约
            let now_minute = Utc::now().minute();
            if now_minute != last_minute {
                last_minute = now_minute;
                for bar in manager.flush_all() {
                    bar.print();
                    bar.send_to_kafka();
                }
            }
            thread::sleep(Duration::from_millis(10));
        }
    }


    // // 程序退出前flush最后一条
    println!("程序结束，输出最后一条K线数据");

    // // 单合约退出
    // if let Some(bar) = aggregator.flush() {
    //     println!("最后一条分钟K线:");
    //     bar.print();
    // }

    // // 多合约退出
    for bar in manager.flush_all() {
        bar.print();
        bar.send_to_kafka();
    }
    
    println!("消费者已安全退出");
}
