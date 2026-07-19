use crate::futex;
use crate::CacheLine;

use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicU32, Ordering};

const STATE_UNLOCKED: u32 = 0; // Mutex is unlocked.
const STATE_LOCKED:   u32 = 1; // Mutex is locked, with no waiting threads.
const STATE_WAITING:  u32 = 2; // Mutex is locked, with at least one waiting thread.

/// A mutual exclusion primitive for thread safe exclusive access to some shared
/// data.
///
/// At a time only one thread will have exclusive access to the data, all other
/// threads need to wait while this thread is doing its job. The could wait by
/// either spinning(`lock_or_spin()`) or going to sleep(`lock_or_wait()`), or a
/// combination of both(`lock()`).
#[repr(C)]
pub struct Mutex<T> {
    data:  UnsafeCell<T>,
    state: CacheLine<AtomicU32>,       // One of STATE_* 
}

unsafe impl<T: Send> Send for Mutex<T> {}
unsafe impl<T: Send> Sync for Mutex<T> {}

impl<T> Drop for Mutex<T> {
    fn drop(&mut self) {
        // Destructor of `T` must run after all the threads are done with it.
        // This code must run after all the threads have dropped their mutex
        // guard.
        assert!(self.state.load(Ordering::Acquire) == STATE_UNLOCKED);
    }
}

impl<T> Mutex<T> {
    /// Create a new `Mutex`.
    pub fn new(value: T) -> Self {
        Self {
            state: CacheLine(AtomicU32::new(0)),
            data:  UnsafeCell::new(value),
        }
    }

    /// Acquire the lock without any spinning. Try once then go to sleep.
    pub fn lock_or_wait(&self) -> MutexGuard<'_, T> {
        // The cleanest way to do this.
        if self.state.compare_exchange(STATE_UNLOCKED, STATE_LOCKED, Ordering::Acquire, 
            Ordering::Relaxed).is_err() {

            while self.state.swap(STATE_WAITING, Ordering::Relaxed) != STATE_UNLOCKED {
                _ = futex::wait(&self.state, STATE_WAITING);
            }
        }

        MutexGuard {
            inner: self,
        }
    }

    /// Acquire the lock with spinning only. No sleeping.
    pub fn lock_or_spin(&self) -> MutexGuard<'_, T> {
        // Try once, return if successfull.
        //
        // If failure, then spin for a while.
        //
        // Then try again.
        //
        // Do it in a loop.

        loop {
            if self.state.compare_exchange(STATE_UNLOCKED, STATE_LOCKED, Ordering::Acquire, 
                Ordering::Relaxed).is_ok() {
                break MutexGuard {
                    inner: self,
                };
            }

            const THRESHOLD: u32 = 256;

            let mut spin_count = 0;
            while self.state.load(Ordering::Relaxed) == STATE_LOCKED && spin_count < THRESHOLD {
                spin_count += 1;
                core::hint::spin_loop()
            }
        }
    }

    /// Initially spin, then go to sleep if that does not works out.
    pub fn lock(&self) -> MutexGuard<'_, T> {
        // The cleanest way to do this.
        if self.state.compare_exchange(STATE_UNLOCKED, STATE_LOCKED, Ordering::Acquire, 
            Ordering::Relaxed).is_err() {

            const THRESHOLD: u32 = 256;

            // spin loop
            //
            // try one final time
            //
            // go to sleep

            let mut attempt = 0;
            while attempt < THRESHOLD && self.state.load(Ordering::Relaxed) == STATE_LOCKED {
                attempt += 1;
                core::hint::spin_loop();
            }

            while self.state.swap(STATE_WAITING, Ordering::Relaxed) != STATE_UNLOCKED {
                _ = futex::wait(&self.state, STATE_WAITING);
            }
        }

        MutexGuard {
            inner: self,
        }
    }
}

/// A guard type used to access the inner `T` by the thread that currently holds
/// the mutex lock.
///
/// If the destructor of this guard does not runs then the mutex
/// will forever remain locked.
pub struct MutexGuard<'a, T> {
    inner: &'a Mutex<T>,
}

impl<'a, T> Drop for MutexGuard<'a, T> {
    fn drop(&mut self) {
        let state = self.inner.state.swap(STATE_UNLOCKED, Ordering::Release);

        // state will be either STATE_WAITING or STATE_LOCKED, only
        // STATE_WAITING is interested to us.
        if state == STATE_WAITING {
            _ = futex::wake_one(&self.inner.state);
        }
    }
}

impl<'a, T> core::ops::Deref for MutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.get()
    }
}

impl<'a, T> core::ops::DerefMut for MutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.get_mut()
    }
}

impl<'a, T> MutexGuard<'a, T> {
    fn get(&self) -> &T {
        unsafe { &*self.inner.data.get() }
    }

    fn get_mut(&mut self) -> &mut T {
        unsafe { &mut *self.inner.data.get() }
    }
}
