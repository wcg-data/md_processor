use crate::ring_buffer::SharedRingBuffer;
use crate::md_structures::TickData;
use crate::BarMerger::{BarAggregator};
use chrono::{NaiveDateTime, Timelike, Datelike};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// 字节数组转str
fn bytes_to_str(bytes: &[u8]) -> &str {
    let len = bytes.iter().position(|&c| c == 0).unwrap_or(bytes.len());
    std::str::from_utf8(&bytes[..len]).unwrap_or("")
}

pub fn consume_loop_tick(ring_buffer: &mut SharedRingBuffer) {
    println!("消费者启动，等待数据...");

    loop {
        match ring_buffer.pop_market_data() {
            Some(tick) => {
                tick.print();
            }
            None => {
                thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

/// 持续消费行情数据
///
/// # 参数
/// * `ring_buffer` - 环形缓冲区
/// * `aggregators` - 聚合器映射
/// 持续消费行情数据并聚合成K线
///
/// 该函数会从环形缓冲区中不断获取Tick数据，并根据合约和时间进行分组聚合，
/// 生成分钟级别的K线数据。当某个分钟的数据完成后，会输出并从内存中移除。
///
/// # 参数
/// * `ring_buffer` - 共享的环形缓冲区，用于获取行情数据
/// * `aggregators` - 聚合器映射，按合约和时间分组存储BarAggregator
pub fn consume_loop(
    ring_buffer: &mut crate::ring_buffer::SharedRingBuffer,
    aggregators: Arc<Mutex<HashMap<String, HashMap<i64, BarAggregator>>>>,
) {
    println!("消费者启动，等待数据...");

    loop {
        match ring_buffer.pop_market_data() {
            Some(tick_data) => {
                // 将TickData转换成Tick
                let tick: crate::md_structures::TickData = tick_data.into();

                // 解析交易时间字符串为日期时间对象
                let trade_time_str = bytes_to_str(&tick.trade_time);
                let dt = chrono::NaiveDateTime::parse_from_str(
                    trade_time_str,
                    "%Y-%m-%d %H:%M:%S%.f",
                ).unwrap_or_else(|_| chrono::Local::now().naive_local());

                // 计算分钟级别的时间戳作为聚合键
                let minute_key = dt.timestamp() / 60; // 按分钟分组

                // 获取聚合器的互斥锁
                let mut aggrs = aggregators.lock().unwrap();

                // 获取合约标识并查找或创建对应的合约映射
                let contract_key = bytes_to_str(&tick.contract).to_string();
                let contract_map = aggrs.entry(contract_key).or_default();

                // 查找或创建对应分钟的聚合器
                let aggregator = contract_map
                    .entry(minute_key)
                    .or_insert_with(BarAggregator::new);

                // 将Tick数据添加到聚合器中
                aggregator.add_tick(tick);

                // 检查并输出已完成的bar（当前分钟之前的数据）
                let now_ts = chrono::Local::now().timestamp();

                // 收集已完成的分钟K线
                let mut finished = Vec::new();
                for (&min_key, agg) in contract_map.iter() {
                    // 如果当前时间已经超过了这个分钟，则认为该分钟的数据已完成
                    if now_ts / 60 > min_key {
                        // 生成并输出K线数据
                        if let Some(bar) = agg.generate_bar(
                            NaiveDateTime::from_timestamp_opt(min_key * 60, 0).unwrap(),
                        ) {
                            // println!("合成Bar: {:?}", bar);
                            // println!("合成Bar:"); 
                            bar.print();
                        }
                        // 标记该分钟数据为已完成
                        finished.push(min_key);
                    }
                }
                
                // 清理已完成的分钟数据，释放内存
                for key in finished {
                    contract_map.remove(&key);
                }
            }
            None => {
                // 没有数据时短暂休眠，避免CPU空转
                thread::sleep(Duration::from_millis(5));
            }
        }
    }
}