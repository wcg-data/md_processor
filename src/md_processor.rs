// md_processor.rs 是行情处理的主程序

// 正确导入模块
use md_processor::shared_memory::map_shared_ring_buffer;
use md_processor::consumer;

fn main() {
    let shm_name = "MD_SNAPSHOT_CTP";

    let (ring_buffer, _mmap) = match map_shared_ring_buffer(shm_name) {
        Ok((rb, mmap)) => (rb, mmap),
        Err(e) => {
            eprintln!("共享内存映射失败: {}", e);
            return;
        }
    };

    if !ring_buffer.is_initialized.load(std::sync::atomic::Ordering::Relaxed) {
        eprintln!("警告：共享内存未初始化");
    }

    // 直接打印tick
    consumer::consume_loop_tick(ring_buffer);
}