mod md_structures;
mod ring_buffer;
mod shared_memory;
mod consumer;

use shared_memory::map_shared_ring_buffer;
use consumer::consume_loop;

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

    consume_loop(ring_buffer);
}
