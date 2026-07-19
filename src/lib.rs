#![no_std]
#![feature(allocator_api)]

pub mod spinlock;
pub mod arc;
pub mod lazylock;
pub mod mutex;

pub use spinlock::SpinLock;
pub use arc::{Arc, Weak};
pub use lazylock::LazyLock;
pub use mutex::Mutex;

mod futex;

// This only guarantees that it will begin at the beginning of a new cache line,
// not that it will be the sole element of that cache line. If the type is big
// enough then it might not even fit in that cache line.
#[repr(align(64))]
struct CacheLine<T>(T);

impl<T> core::ops::Deref for CacheLine<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> core::ops::DerefMut for CacheLine<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

mod os_error {
    extern crate libc;

    pub type OsError = i32;

    pub fn last_os_error() -> i32 {
        unsafe { *libc::__errno_location() }
    }

}
