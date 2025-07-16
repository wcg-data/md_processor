use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use memmap2::{MmapMut, MmapOptions};

use crate::ring_buffer::SharedRingBuffer;

/// 重置共享环形缓冲区的 tail 指针到当前 head，忽略历史数据
pub fn reset_shared_ring_buffer_tail(shm_name: &str) -> io::Result<()> {
    let file_path = format!("/dev/shm/{}", shm_name);
    let path = Path::new(&file_path);

    if !path.exists() {
        return Err(io::Error::new(io::ErrorKind::NotFound, "共享内存文件不存在"));
    }

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;

    let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };
    let ptr = mmap.as_mut_ptr();

    let ring_buffer = unsafe { &mut *(ptr as *mut SharedRingBuffer) };

    let head = ring_buffer.head.load(std::sync::atomic::Ordering::Relaxed);
    ring_buffer.tail.store(head, std::sync::atomic::Ordering::Relaxed);

    Ok(())
}

fn main() {
    reset_shared_ring_buffer_tail("MD_SNAPSHOT_CTP");
}
