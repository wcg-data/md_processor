// use std::io;
// use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use memmap2::{Mmap, MmapOptions};
use std::fs::OpenOptions;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::{thread, time};


const RING_BUFFER_SIZE: usize = 100000000; // Adjust this to match the buffer size in C++

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct MarketDataType {
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
    pre_settle: f64,
    comd: [u8; 16],
    exchange: [u8; 16],
}

#[repr(C)]
struct SharedRingBuffer {
    is_initialized: AtomicBool,
    head: AtomicUsize,
    tail: AtomicUsize,
    buffer: [MarketDataType; RING_BUFFER_SIZE],
}

fn read_shared_memory(shm_name: &str) -> Option<Arc<Mmap>> {
    // Open the shared memory segment
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(shm_name)
        .expect("Failed to open shared memory");

    // Memory-map the shared memory file
    let mmap = unsafe {
        MmapOptions::new()
            .map(&file)
            .expect("Failed to map shared memory")
    };

    Some(Arc::new(mmap))
}

// Helper function to clean byte slices into strings
fn bytes_to_str_trimmed(bytes: &[u8]) -> Result<&str, std::str::Utf8Error> {
    // Find the first null terminator
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    std::str::from_utf8(&bytes[..end])
}

// Function to process and print a single market data entry
fn process_market_data(data: &MarketDataType) {
    // Safely convert byte arrays to strings, trimming null terminators
    let contract_str = bytes_to_str_trimmed(&data.contract).unwrap_or("INVALID_UTF8");
    let trade_date_str = bytes_to_str_trimmed(&data.trade_date).unwrap_or("INVALID_UTF8");
    // Assuming trade_time might not be null-terminated consistently, use lossy conversion
    let trade_time_str = String::from_utf8_lossy(&data.trade_time);
    let comd_str = bytes_to_str_trimmed(&data.comd).unwrap_or("INVALID_UTF8");
    let exchange_str = bytes_to_str_trimmed(&data.exchange).unwrap_or("INVALID_UTF8");

    // Find the first null terminator in trade_time_str if needed, or trim whitespace
    let trade_time_cleaned = trade_time_str.split('\0').next().unwrap_or(&trade_time_str).trim();


    println!(
        // "Contract: {}, Trade Date: {}, Trade Time: {}.{}, Open: {}, High: {}, Low: {}, Close: {}, Volume: {}, Turnover: {}, OI: {}, BP1: {}, BV1: {}, AP1: {}, AV1: {}, Mid: {}, PreSettle: {}, Comd: {}, Exchange: {}",
        "{} {}.{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        trade_date_str,
        trade_time_cleaned, // Use cleaned trade time
        data.trade_time_millisec,
        contract_str,
        data.open,
        data.high,
        data.low,
        data.close,
        data.volume,
        data.turnover,
        data.open_interest,
        data.bid_price_1,
        data.bid_volume_1,
        data.ask_price_1,
        data.ask_volume_1,
        data.mid_price,
        data.pre_settle,
        comd_str,
        exchange_str
    );
}

fn main() {
    let shm_name = "/dev/shm/my_ring_buffer";

    // Read and map the shared memory
    if let Some(mmap) = read_shared_memory(shm_name) {
        // Get a reference to the shared ring buffer structure.
        // SAFETY: Assumes the shared memory layout matches SharedRingBuffer
        // and that the producer initializes it correctly.
        // The lifetime 'static is tied to the mmap which lives for the process duration.
        let ring_buffer: &'static SharedRingBuffer = unsafe { &*(mmap.as_ptr() as *const SharedRingBuffer) };

        println!("Starting market data processor loop...");

         // Check if the buffer seems initialized (optional, based on producer logic)
        if !ring_buffer.is_initialized.load(Ordering::Acquire) {
             println!("Warning: Shared buffer not marked as initialized by producer.");
             // Depending on requirements, you might wait here or proceed cautiously.
        }

        loop {
            let head = ring_buffer.head.load(Ordering::Acquire);
            let mut tail = ring_buffer.tail.load(Ordering::Relaxed); // Load current shared tail

            if head == tail {
                // Buffer is empty, wait a bit
                thread::sleep(time::Duration::from_millis(50));
                continue; // Go back to check head/tail again
            }

             // Consume all data from tail up to head
            while tail != head {
                let index = tail % RING_BUFFER_SIZE;

                // Copy data safely before processing. MarketDataType is Copy.
                // SAFETY: We know tail != head, and index is derived from tail.
                // Assumes producer writes valid data before updating head.
                let data_copy = ring_buffer.buffer[index];

                process_market_data(&data_copy);

                // Advance tail for the next iteration *locally*
                tail = tail.wrapping_add(1);

                // Update the shared tail pointer *after processing each item*
                // This ensures that even if the consumer crashes, the tail
                // reflects the last successfully processed item.
                ring_buffer.tail.store(tail, Ordering::Release);
            }
            // If we consumed data, loop immediately to check for more
            // No sleep here if we were busy consuming
        }

    } else {
        eprintln!("Failed to read shared memory.");
    }
}
