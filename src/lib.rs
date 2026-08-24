#![cfg_attr(not(test), no_std)] // if not testing then set no_std
#![feature(allocator_api)]

pub mod spinlock;
pub mod arc;
pub mod lazylock;
pub mod mutex;
pub mod condvar;
pub mod spsc;

pub mod stackpool;

#[doc(hidden)]
pub use spinlock::SpinLock;

#[doc(hidden)]
pub use arc::{Arc, Weak};

#[doc(hidden)]
pub use lazylock::LazyLock;

#[doc(hidden)]
pub use mutex::Mutex;

#[doc(hidden)]
pub use condvar::Condvar;

mod futex;

/// This only guarantees that it will begin at the beginning of a new cache line,
/// not that it will be the sole element of that cache line. If the type is big
/// enough then it might not even fit in that cache line.
///
/// To ensure that it is the only element in a cacheline, assuming it is small
/// enough to fit in one, it must be the last field of the struct or should be
/// followed only by other fields of type `CacheLine<T>`, with `#[repr(C)]` on
/// the struct.
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

/// Spin for `$count` number of iterations.
macro_rules! spinloop {
    ($count:literal) => {{
        let mut i = 0;
        while i < $count {
            i += 1;
            core::hint::spin_loop();
        }
    }}
}
pub(crate) use spinloop;
