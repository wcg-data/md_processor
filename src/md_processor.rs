use memmap2::MmapOptions;
use std::fs::{File, OpenOptions};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::path::Path; // Import Path

// --- Assume these structs are defined as before ---
const RING_BUFFER_SIZE: usize = 100000000; // IMPORTANT: Must match C++ value

#[repr(C)]
#[derive(Debug)] // Add Debug for printing TickData easily
struct TickData {
    contract: [u8; 9],
    comd: [u8; 4],
    exchange: [u8; 7],
    date: [u8; 11],
    trade_time: [u8; 24],
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    pre_settle: f64,
    volume: u32,
    turnover: f64,
    open_interest: u32,
    bid_price_1: f64,
    bid_volume_1: u32,
    ask_price_1: f64,
    ask_volume_1: u32,
    mid_price: f64,
}

#[repr(C, align(64))] // Ensure alignment matches C++ alignas(64)
struct SharedRingBuffer {
    is_initialized: AtomicBool,
    head: AtomicUsize, // Written by producer
    tail: AtomicUsize, // Written by consumer
    buffer: [TickData; RING_BUFFER_SIZE],
}

// Helper function to print TickData (optional)
fn print_tick_data(tick: &TickData) {
    // Safely convert byte arrays to strings (handle potential non-UTF8 data)
    let contract_str = String::from_utf8_lossy(&tick.contract);
    let date_str = String::from_utf8_lossy(&tick.date);
    let time_str = String::from_utf8_lossy(&tick.trade_time);

    println!(
        "Tick: Contract={}, Date={}, Time={}, Close={}, Volume={}",
        contract_str.trim_end_matches('\0'), // Trim null terminators if present
        date_str.trim_end_matches('\0'),
        time_str.trim_end_matches('\0'),
        tick.close,
        tick.volume
    );
    // Print other fields as needed...
}


fn main() -> io::Result<()> {
    let shm_name = "my_ring_buffer"; // Must match C++
    let file_path_str = format!("/dev/shm/{}", shm_name);
    let file_path = Path::new(&file_path_str);

    // 1. Open shared memory file (ensure it exists first)
    if !file_path.exists() {
         eprintln!("Error: Shared memory file {} does not exist. Run the C++ producer first.", file_path_str);
         return Err(io::Error::new(io::ErrorKind::NotFound, "Shared memory file not found"));
    }

    let file = OpenOptions::new()
        .read(true)
        .write(true) // Need write access to update tail
        .open(file_path)?;

    // 2. Map shared memory (mutable)
    // Use MmapMut for mutable mapping
    let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };

    // 3. Get a mutable reference to the SharedRingBuffer
    // SAFETY: We are asserting unique mutable access to this shared memory region
    // from the Rust side for the duration of this reference. Proper synchronization
    // (atomics, logic) is crucial if C++ also modifies `tail` or other shared state
    // concurrently in complex ways (though typically only the producer modifies `head`
    // and only the consumer modifies `tail`).
    let ring_buffer: &mut SharedRingBuffer = unsafe {
        // Ensure the pointer is correctly aligned before dereferencing
        let ptr = mmap.as_mut_ptr();
        if ptr.align_offset(std::mem::align_of::<SharedRingBuffer>()) != 0 {
             eprintln!("Error: Shared memory pointer is not aligned correctly.");
             return Err(io::Error::new(io::ErrorKind::Other, "Shared memory alignment error"));
        }
         &mut *(ptr as *mut SharedRingBuffer)
    };

    // Optional: Check if initialized by C++ (add error handling as needed)
    if !ring_buffer.is_initialized.load(Ordering::Relaxed) {
        println!("Warning: Ring buffer is not marked as initialized by the producer.");
        // Decide whether to wait or exit, e.g.:
        // std::thread::sleep(std::time::Duration::from_millis(100));
        // return Ok(()); // Or retry later
    }


    println!("Consumer started. Checking for data...");

    loop { // Keep checking for new data indefinitely or add exit condition
        // 4. Read head and tail pointers atomically
        // Use Acquire ordering for head to ensure we see the producer's latest writes
        // to the buffer *before* we see the updated head.
        let head = ring_buffer.head.load(Ordering::Acquire);
        // Relaxed might be okay here if only this consumer modifies tail, but Acquire
        // is safer if there were theoretical reads of tail by others.
        let mut current_tail = ring_buffer.tail.load(Ordering::Acquire);

        if head == current_tail {
            // println!("Buffer is empty. Waiting...");
            // Avoid busy-waiting which consumes CPU
            std::thread::sleep(std::time::Duration::from_millis(10)); // Sleep briefly
            continue; // Check again
        }

        println!("Reading data (Head: {}, Tail: {})", head, current_tail);

        // 5. Consume data until tail catches up to head
        while current_tail != head {
            // Access data using the current_tail index
            // SAFETY: Accessing buffer elements relies on the C++ producer correctly
            // writing valid data and updating `head` *after* the write is complete,
            // and the consumer only reading between `current_tail` and `head`.
            let tick = unsafe { &ring_buffer.buffer.get_unchecked(current_tail) }; // Use get_unchecked for performance if bounds are guaranteed by logic
            // Or safer version: let tick = &ring_buffer.buffer[current_tail];

            // --- Process the tick data ---
            print_tick_data(tick);
            // ... your actual data processing logic ...


            // --- CRITICAL STEP: Update tail pointer in shared memory ---
            // Calculate the next tail position
            let next_tail = (current_tail + 1) % RING_BUFFER_SIZE;

            // Atomically store the new tail value.
            // Use Release ordering to ensure that our processing of the `tick` data
            // happens *before* the tail update is visible to the producer (if it checks).
            ring_buffer.tail.store(next_tail, Ordering::Release);

            // Update our local copy for the next iteration
            current_tail = next_tail;

             println!("Consumed item, updated Tail to: {}", current_tail);
        }

        // Optional: Add a condition to break the outer loop if needed,
        // e.g., based on a signal or after a certain amount of inactivity.
        // For this example, it runs indefinitely, polling for new data.
    }

    // Note: The program as written above loops forever.
    // You might want a way to shut it down gracefully (e.g., signal handling).
    // Ok(())
}