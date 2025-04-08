use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::md_structures::TickData;

/// 环形缓冲区大小，必须与C++保持一致
pub const RING_BUFFER_SIZE: usize = 10_000_000;

/// 共享无锁环形缓冲区，布局需与C++完全一致
#[repr(C, align(64))]
pub struct SharedRingBuffer {
    pub is_initialized: AtomicBool,
    _pad1: [u8; 64 - std::mem::size_of::<AtomicBool>()],

    pub head: AtomicUsize,
    _pad2: [u8; 64 - std::mem::size_of::<AtomicUsize>()],

    pub tail: AtomicUsize,
    _pad3: [u8; 64 - std::mem::size_of::<AtomicUsize>()],

    pub buffer: [TickData; RING_BUFFER_SIZE],
}

impl SharedRingBuffer {
    /// 从环形缓冲区中弹出一条数据
    ///
    /// Args:
    ///     &mut self: 可变引用
    ///
    /// Returns:
    ///     Option<TickData]: 有数据则返回一份拷贝，否则None
    pub fn pop_market_data(&mut self) -> Option<TickData> {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);

        if head == tail {
            return None; // 空
        }

        // 读取数据
        let tick = unsafe { self.buffer.get_unchecked(tail).clone() };

        // 计算下一个tail
        let next_tail = (tail + 1) % RING_BUFFER_SIZE;

        // 更新tail指针
        self.tail.store(next_tail, Ordering::Release);

        Some(tick)
    }

    /// 重置tail指针为0
    ///
    /// Args:
    ///     &mut self
    pub fn reset_tail(&mut self) {
        self.tail.store(0, Ordering::Release);
    }
}