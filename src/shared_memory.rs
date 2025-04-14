use memmap2::{MmapMut, MmapOptions};
use std::fs::OpenOptions;
use std::io;
use std::path::Path;

use crate::ring_buffer::SharedRingBuffer;

/// 打开并映射共享内存，返回指向 `SharedRingBuffer` 的可变引用
///
/// Args:
///     shm_name (str): 共享内存名称
///
/// Returns:
///     io::Result<(&'static mut SharedRingBuffer, MmapMut)>: 环形缓冲区引用和映射对象
pub fn map_shared_ring_buffer(shm_name: &str) -> io::Result<(&'static mut SharedRingBuffer, MmapMut)> {
    let file_path_str = format!("/dev/shm/{}", shm_name);
    let file_path = Path::new(&file_path_str);

    if !file_path.exists() {
        return Err(io::Error::new(io::ErrorKind::NotFound, "共享内存文件不存在"));
    }

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(file_path)?;

    let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };

    let ptr = mmap.as_mut_ptr();
    if ptr.align_offset(std::mem::align_of::<SharedRingBuffer>()) != 0 {
        return Err(io::Error::new(io::ErrorKind::Other, "共享内存对齐错误"));
    }

    let ring_buffer = unsafe { &mut *(ptr as *mut SharedRingBuffer) };

    Ok((ring_buffer, mmap))
}

