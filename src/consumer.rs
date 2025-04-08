use crate::ring_buffer::SharedRingBuffer;
use crate::md_structures::TickData;
// use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

/// 持续消费行情数据
///
/// Args:
///     ring_buffer (&mut SharedRingBuffer): 环形缓冲区引用
pub fn consume_loop(ring_buffer: &mut SharedRingBuffer) {
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
