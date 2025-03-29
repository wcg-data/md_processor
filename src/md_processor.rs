use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::ptr;
use std::thread;
use std::cell::UnsafeCell;

// 定义 MarketDataType 结构体
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct MarketDataType {
    trade_date: [u8; 16],
    trade_time: [u8; 16],
    trade_time_millisec: i32,
    contract: [u8; 32],
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: i32,
    turnover: f64,
    open_interest: f64,
    bid_price_1: f64,
    bid_volume_1: i32,
    ask_price_1: f64,
    ask_volume_1: i32,
    mid_price: f64,
    pre_settle_price: f64,
    comd: [u8; 16],
    exchange: [u8; 16],
}

// 定义锁自由环形缓冲区
struct RingBuffer {
    buffer: UnsafeCell<Vec<MarketDataType>>,
    head: AtomicUsize,
    tail: AtomicUsize,
    size: usize,
}

// 实现 Send 和 Sync trait
unsafe impl Send for RingBuffer {}
unsafe impl Sync for RingBuffer {}

impl RingBuffer {
    // 初始化环形缓冲区
    fn new(size: usize) -> Self {
        let mut buffer = Vec::with_capacity(size);
        unsafe {
            buffer.set_len(size);
        }

        RingBuffer {
            buffer: UnsafeCell::new(buffer),
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            size,
        }
    }

    // 从环形缓冲区读取数据
    fn pop(&self) -> Option<MarketDataType> {
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Relaxed);

        if tail == head {
            return None; // 缓冲区为空
        }

        let index = tail % self.size;
        let data = unsafe { ptr::read(&(*self.buffer.get())[index]) };

        // 更新尾部指针
        self.tail.store((tail + 1) % self.size, Ordering::Release);

        Some(data)
    }

    // 向环形缓冲区写入数据
    fn push(&self, data: &MarketDataType) -> bool {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Relaxed);

        if (head + 1) % self.size == tail {
            return false;
        }

        let index = head % self.size;
        unsafe {
            ptr::write(&mut (*self.buffer.get())[index], *data);
        }
        self.head.store((head + 1) % self.size, Ordering::Release);

        true
    }
}

// 示例消费函数，模拟读取数据
fn consume(ring_buffer: Arc<RingBuffer>) {
    loop {
        if let Some(data) = ring_buffer.pop() {
            println!("{:?}", data);
        } else {
            // 如果队列为空，稍微休眠或执行其他操作
            thread::yield_now(); // 自旋等待
        }
    }
}

fn main() {
    let buffer_size = 1024;
    let ring_buffer = Arc::new(RingBuffer::new(buffer_size));

    // 启动多个线程进行消费
    let mut handles = vec![];
    for _ in 0..4 {
        let rb_clone = Arc::clone(&ring_buffer);
        let handle = thread::spawn(move || {
            consume(rb_clone);
        });
        handles.push(handle);
    }

    // 模拟生产数据
    let mut count = 0;
    loop {
        let mut data = MarketDataType {
            trade_date: [0; 16],
            trade_time: [0; 16],
            trade_time_millisec: 123,
            contract: [0; 32],
            open: 100.0 + count as f64,
            high: 105.0 + count as f64,
            low: 95.0 + count as f64,
            close: 102.0 + count as f64,
            volume: 1000 + count,
            turnover: 5000.0 + count as f64,
            open_interest: 2000.0 + count as f64,
            bid_price_1: 102.0 + count as f64,
            bid_volume_1: 100 + count,
            ask_price_1: 103.0 + count as f64,
            ask_volume_1: 100 + count,
            mid_price: 102.5 + count as f64,
            pre_settle_price: 101.0 + count as f64,
            comd: [0; 16],
            exchange: [0; 16],
        };

        // 复制字符串数据到固定大小的数组中
        data.trade_date[..8].copy_from_slice(b"20230329");
        data.trade_time[..8].copy_from_slice(b"12:34:56");
        data.contract[..8].copy_from_slice(b"SHFE1234");
        data.comd[..8].copy_from_slice(b"COMD1234");
        data.exchange[..8].copy_from_slice(b"EXCH1234");

        if !ring_buffer.push(&data) {
            println!("Ring buffer is full, dropping data...");
        }

        count += 1;
        if count > 10000 {
            break;
        }
    }

    // 等待消费线程结束
    for handle in handles {
        handle.join().unwrap();
    }
}
