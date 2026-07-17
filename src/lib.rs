#![no_std]
#![feature(allocator_api)]

pub mod spinlock;
pub mod arc;
pub mod lazylock;

pub use spinlock::SpinLock;
pub use arc::{Arc, Weak};
pub use lazylock::LazyLock;

// This only guarantees that it will begin at the beginning of a new cache line,
// not that it will be the sole element of that cache line. If the type is big
// enough then it might not even fit in that cache line.
#[repr(align(64))]
struct BeginCacheLine<T>(T);

impl<T> core::ops::Deref for BeginCacheLine<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> core::ops::DerefMut for BeginCacheLine<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
