#![allow(unused)]

use crate::os_error::{self, OsError};

use core::sync::atomic::AtomicU32;

extern crate libc;

/// Wait if the futex `word` is `expected` value. It might be possible to
/// spuriously wake up, so preferably use it inside a loop.
///
/// Return value: In case of failure return the `errno` code.
pub fn wait(word: &AtomicU32, expected: u32) -> Result<(), OsError> {
    let op = libc::FUTEX_WAIT | libc::FUTEX_PRIVATE_FLAG;

    let ret = unsafe {
        libc::syscall(libc::SYS_futex, word as *const _, op, expected,
            core::ptr::null::<()>())
    };

    if ret == 0 {
        Ok(())
    } else {
        Err(os_error::last_os_error())
    }
}

/// Wake all the threads waiting on the futex `word`.
///
/// Return value: 
/// In case of success, returns the number of threads woken up.
/// In case of failure, returns `errno`
pub fn wake_all(word: &AtomicU32) -> Result<usize, OsError> {
    let op = libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG;

    let ret = unsafe {
        libc::syscall(libc::SYS_futex, word as *const _, op, libc::INT_MAX)
    };

    if ret == -1 {
        Err(os_error::last_os_error())
    } else {
        // Return number of threads woken up.
        Ok(ret as usize)
    }
}

/// Wake one of the threads waiting on the futex `word`. It might be any thread.
///
/// Return value: In case of failure, returns `errno`, otherwise return number
/// of threads woken up, which will be either zero or one.
pub fn wake_one(word: &AtomicU32) -> Result<usize, OsError> {
    let op = libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG;

    let ret = unsafe {
        libc::syscall(libc::SYS_futex, word as *const _, op, 1) // Wake One
    };

    if ret == -1 {
        Err(os_error::last_os_error())
    } else {
        Ok(ret as usize)
    }
}
