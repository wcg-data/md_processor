use memmap2::MmapOptions;
use std::fs::{File, OpenOptions};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::path::Path; // 导入 Path

// --- 假设这些结构体已按之前的定义 ---
const RING_BUFFER_SIZE: usize = 10000000; // 重要：必须与 C++ 的值匹配

#[repr(C)]
#[derive(Debug)] // 添加 Debug 以便轻松打印 TickData
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

// #[repr(C, align(64))] // 确保对齐方式与 C++ 的 alignas(64) 匹配
// struct SharedRingBuffer {
//     is_initialized: AtomicBool,
//     head: AtomicUsize, // 由生产者写入
//     tail: AtomicUsize, // 由消费者写入
//     buffer: [TickData; RING_BUFFER_SIZE],
// }

#[repr(C, align(64))] // 确保整体对齐
struct SharedRingBuffer {
    is_initialized: AtomicBool,
    _pad1: [u8; 64 - std::mem::size_of::<AtomicBool>()], // 填充到64字节

    head: AtomicUsize,
    _pad2: [u8; 64 - std::mem::size_of::<AtomicUsize>()], // 填充到64字节

    tail: AtomicUsize,
    _pad3: [u8; 64 - std::mem::size_of::<AtomicUsize>()], // 填充到64字节

    buffer: [TickData; RING_BUFFER_SIZE], // TickData数组
}

// 打印 TickData 的辅助函数 (可选)
fn print_tick_data(tick: &TickData) {
    // 安全地将字节数组转换为字符串（处理潜在的非 UTF8 数据）
    let contract_str = String::from_utf8_lossy(&tick.contract);
    let date_str = String::from_utf8_lossy(&tick.date);
    let time_str = String::from_utf8_lossy(&tick.trade_time);

    println!(
        // "Tick: Contract={}, Comd={}, Exchange={}, Date={}, Time={}, Open={:.6}, High={:.6}, Low={:.6}, Close={:.6}, PreSettle={:.6}, Volume={}, Turnover={:.6}, OpenInterest={}, BidPrice1={:.6}, BidVolume1={}, AskPrice1={:.6}, AskVolume1={}, MidPrice={:.6}",
        "{},{},{},{},{},{:.5},{:.5},{:.5},{:.5},{:.5},{},{:.5},{},{:.5},{},{:.5},{},{:.5}",
        contract_str.trim_end_matches('\0'),
        String::from_utf8_lossy(&tick.comd).trim_end_matches('\0'),
        String::from_utf8_lossy(&tick.exchange).trim_end_matches('\0'),
        date_str.trim_end_matches('\0'),
        time_str.trim_end_matches('\0'),
        tick.open,
        tick.high,
        tick.low,
        tick.close,
        tick.pre_settle,
        tick.volume,
        tick.turnover,
        tick.open_interest,
        tick.bid_price_1,
        tick.bid_volume_1,
        tick.ask_price_1,
        tick.ask_volume_1,
        tick.mid_price
    );
    // 根据需要打印其他字段...
}


fn main() -> io::Result<()> {
    let shm_name = "MD_SNAPSHOT_CTP"; // 必须与 C++ 匹配
    let file_path_str = format!("/dev/shm/{}", shm_name);
    let file_path = Path::new(&file_path_str);

    // 1. 打开共享内存文件（首先确保它存在）
    if !file_path.exists() {
         eprintln!("错误：共享内存文件 {} 不存在。请先运行 C++ 生产者。", file_path_str);
         return Err(io::Error::new(io::ErrorKind::NotFound, "未找到共享内存文件"));
    }

    let file = OpenOptions::new()
        .read(true)
        .write(true) // 需要写入权限以更新 tail
        .open(file_path)?;

    // 2. 映射共享内存（可变）
    // 使用 MmapMut 进行可变映射
    let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };

    // 3. 获取 SharedRingBuffer 的可变引用
    // 安全性：我们断言在此引用的持续时间内，Rust 端对该共享内存区域具有唯一的可变访问权限。
    // 正确的同步（原子操作，逻辑）至关重要，如果 C++ 也以复杂方式并发修改 `tail` 或其他共享状态
    // （尽管通常只有生产者修改 `head` 并且只有消费者修改 `tail`）。
    let ring_buffer: &mut SharedRingBuffer = unsafe {
        // 在解引用之前确保存储指针已正确对齐
        let ptr = mmap.as_mut_ptr();
        if ptr.align_offset(std::mem::align_of::<SharedRingBuffer>()) != 0 {
             eprintln!("错误：共享内存指针未正确对齐。");
             return Err(io::Error::new(io::ErrorKind::Other, "共享内存对齐错误"));
        }
         &mut *(ptr as *mut SharedRingBuffer)
    };

    // 可选：检查是否由 C++ 初始化（根据需要添加错误处理）
    if !ring_buffer.is_initialized.load(Ordering::Relaxed) {
        println!("警告：环形缓冲区未被生产者标记为已初始化。");
        // 决定是等待还是退出，例如：
        // std::thread::sleep(std::time::Duration::from_millis(100));
        // return Ok(()); // 或者稍后重试
    }


    println!("消费者已启动。正在检查数据...");

    loop { // 无限期地继续检查新数据或添加退出条件
        // 4. 原子地读取 head 和 tail 指针
        // 对 head 使用 Acquire 顺序，以确保在看到更新的 head 之前，我们能看到生产者对缓冲区的最新写入。
        let head = ring_buffer.head.load(Ordering::Acquire);
        // 如果只有这个消费者修改 tail，Relaxed 可能也可以，但是 Acquire
        // 更安全，以防理论上存在其他读取者读取 tail 的情况。
        let mut current_tail = ring_buffer.tail.load(Ordering::Acquire);

        if head == current_tail {
            // println!("缓冲区为空。正在等待...");
            // 避免消耗 CPU 的忙等待
            std::thread::sleep(std::time::Duration::from_millis(10)); // 短暂休眠
            continue; // 再次检查
        }

        println!("正在读取数据 (Head: {}, Tail: {})", head, current_tail);

        // 5. 消费数据，直到 tail 追上 head
        while current_tail != head {
            // 使用 current_tail 索引访问数据
            // 安全性：访问缓冲区元素依赖于 C++ 生产者正确地
            // 写入有效数据并在写入完成后更新 `head`，
            // 并且消费者只在 `current_tail` 和 `head` 之间读取。
            let tick = unsafe { &ring_buffer.buffer.get_unchecked(current_tail) }; // 如果逻辑保证了边界，则使用 get_unchecked 以提高性能
            // 或者更安全的版本：let tick = &ring_buffer.buffer[current_tail];

            // --- 处理 tick 数据 ---
            print_tick_data(tick);
            // ... 你的实际数据处理逻辑 ...


            // --- 关键步骤：更新共享内存中的 tail 指针 ---
            // 计算下一个 tail 位置
            let next_tail = (current_tail + 1) % RING_BUFFER_SIZE;

            // 原子地存储新的 tail 值。
            // 使用 Release 顺序以确保我们对 `tick` 数据的处理
            // 发生在 tail 更新对生产者可见（如果它检查的话）*之前*。
            ring_buffer.tail.store(next_tail, Ordering::Release);

            // 更新我们的本地副本以进行下一次迭代
            current_tail = next_tail;

            //  println!("已消费项目，将 Tail 更新为：{}", current_tail);
        }

        // 可选：如果需要，添加一个条件来中断外部循环，
        // 例如，基于信号或在一段时间不活动之后。
        // 对于这个例子，它无限期运行，轮询新数据。
    }

    // 注意：上面编写的程序会永远循环。
    // 你可能需要一种优雅地关闭它的方法（例如，信号处理）。
    // Ok(())
}