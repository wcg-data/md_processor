use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::fs::File;
use std::io::Read;
use std::os::unix::io::AsRawFd;
use memmap2::MmapMut; // We will use `memmap2` crate for memory-mapping
use std::thread;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct MarketDataType {
    pub trade_date: [u8; 16],
    pub trade_time: [u8; 16],
    pub trade_time_millisec: i32,
    pub contract: [u8; 32],
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: i32,
    pub turnover: f64,
    pub open_interest: f64,
    pub bid_price_1: f64,
    pub bid_volume_1: i32,
    pub ask_price_1: f64,
    pub ask_volume_1: i32,
    pub mid_price: f64,
    pub pre_settle_price: f64,
    pub comd: [u8; 16],
    pub exchange: [u8; 16],
}

#[repr(C)]
pub struct SharedRingBuffer {
    pub is_initialized: AtomicBool,
    pub head: AtomicUsize,
    pub tail: AtomicUsize,
    pub buffer: [MarketDataType; 100000000], // 1亿条数据
}

impl SharedRingBuffer {
    pub fn pop(&self) -> Option<MarketDataType> {
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Relaxed);
        
        // If the ring buffer is empty
        if tail == head {
            return None;
        }

        // Read data at the tail index
        let data = unsafe { *self.buffer.get_unchecked(tail) };

        // Move the tail pointer
        self.tail.store((tail + 1) % 100000000, Ordering::Release);

        Some(data)
    }
}

pub fn create_or_open_shared_memory(shm_name: &str) -> Arc<SharedRingBuffer> {
    // Assuming the shared memory file already exists
    let file = File::open(format!("/dev/shm/{}", shm_name)).unwrap();
    let mmap = unsafe { MmapMut::map_mut(&file).unwrap() };

    // Assuming shared memory is mapped correctly to SharedRingBuffer
    let buffer = unsafe { &*(mmap.as_mut_ptr() as *mut SharedRingBuffer) };
    Arc::new(buffer.clone())
}

fn main() {
    let shm_name = "my_ring_buffer";
    let ring_buffer = create_or_open_shared_memory(shm_name);

    // Assuming initialization is done and memory is mapped

    // Reading data in a loop (this simulates the consumer)
    loop {
        if let Some(data) = ring_buffer.pop() {
            println!("{:?}", data);  // Process the data here
        } else {
            // Yield to avoid busy-waiting, this mimics `std::this_thread::yield` in C++
            thread::yield_now();
        }
    }
}
