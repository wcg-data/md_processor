// md_processor.rs 是行情处理的主程序

// 正确导入模块
use md_processor::shared_memory::map_shared_ring_buffer;
use md_processor::consumer;
use std::env;

fn main() {
    // 获取命令行参数
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("用法：{} <共享内存名称>", args[0]);
        return;
    }

    let shm_name = &args[1]; // 从命令行获取共享内存名称

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
