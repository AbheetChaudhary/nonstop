//! This module provides a `SpinLock` type that is used to share exclusive
//! access to some data in a thread safe manner.

use crate::CacheLine;

use core::sync::atomic::{AtomicBool, Ordering};
use core::cell::UnsafeCell;

fn rdtsc() -> u64 {
    unsafe { core::arch::x86_64::_rdtsc() }
}

#[inline]
/// Spin for `cycles` number of clock cycles. This is not going to be exaclty
/// accurate.
fn spin_loop_for_cycles(max_cycles: u64) {
    let mut begin = rdtsc();
    while rdtsc() < begin + max_cycles {
        core::hint::spin_loop();
        core::hint::black_box(&mut begin);
    }
}

/// A spinlock that protects exclusive ownership of a value of type `T`.
///
/// At a time only one thread will have exclusive access to the inner `T`. All
/// other threads, if they exist, need to wait by spinning.
#[repr(C)]
pub struct SpinLock<T> {
    /// The data that the lock is protecting.
    data:   UnsafeCell<T>,

    /// Flag for whether someone holds the lock or not.
    ///
    /// It is in its own cache line to avoid false sharing between the lock
    /// holding thread that accesses the data and other spinning threads.
    ///
    /// This will let the lock holding thread to keep the data exclusively in
    /// it's own cache.
    locked: CacheLine<AtomicBool>, 

}

impl<T> Drop for SpinLock<T> {
    fn drop(&mut self) {
        // Assert that no one holds the lock. It must be unlocked because we are
        // in drop of the `SpinLock`
        //
        // Acquire ordering to synchronize with the Release stores in the drops
        // of all other `SpinLockGuard`'s
        assert!(!self.locked.load(Ordering::Acquire));

        // UnsafeCell<T> gets dropped.
    }
}

// unsafe impl<T: Send> Send for SpinLock<T> {} // Blanket implemented.

// SAFETY:
//
// `T` must be `Send` because a shared reference to the spinlock could be used
// to get an exclusive reference to the inner `T`, thus being able to take the
// ownership of the inner `T`.
unsafe impl<T: Send> Sync for SpinLock<T> {}

impl<T> SpinLock<T> {
    /// Create a new `SpinLock` type that guards the exclusive ownership of `T`
    /// in a thread-safe way.
    pub fn new(value: T) -> Self {
        Self {
            data:   UnsafeCell::new(value),
            locked: CacheLine(AtomicBool::new(false)),
        }
    }

    /// Acquire the lock. It spins and blocks the current thread until it can
    /// acquire the lock.
    ///
    /// If the current lock holding thread never drops the lock then this will
    /// spin forever.
    ///
    /// There is also no way to know whether the previous lock holding thread
    /// had panicked and left the data in an inconsistent state before it
    /// eventually got unlocked by the drop of `SpinLockGuard`.
    pub fn lock(&self) -> SpinLockGuard<'_, T> {
        // By default, we spin for up to 1024 cycles.
        let default_max_backoff: u32 = 1024;

        self.lock_with_max_backoff(default_max_backoff)
    }

    /// Acquire the lock, or spin for up to `max_backoff` cycles.
    ///
    /// It uses exponential backoff strategy where spinning starts from 32
    /// cycles and doubles up to `max_backoff` which should be a power of 2. If
    /// it is not then it is upgraded up to the next power of 2. If even after
    /// upgrading `max_backoff` is less than 32 then 32 is used as the max
    /// backoff.
    pub fn lock_with_max_backoff(&self, max_backoff: u32) -> SpinLockGuard<'_, T> {
        // Up align the requested backoff to the next power of 2.
        let backoff_up_aligned: u64 = (max_backoff as u64).next_power_of_two();

        // If the requested backoff is less than 32 then we will use 32,
        // otherwise use the up aligned backoff.
        let max_backoff = backoff_up_aligned.max(32);

        // Number of lower bits to use.
        let backoff_bits = max_backoff - 1;

        loop {
            // Spin while it is locked.
            //
            // Relaxed ordering to keep it as light as possible. The cache line
            // could be in shared state across all the spinning CPUs.
            let mut backoff = 1;
            while self.locked.load(Ordering::Relaxed) {
                spin_loop_for_cycles(backoff);

                // Double the backoff for the next round.
                backoff <<= 1;

                // Keep only the lower log(max_backoff) bits.
                backoff &= backoff_bits;

                // Reset backoff if it crossed max_backoff.
                //
                // Compiler will optimize this if, so no branching will happen
                // here.
                if backoff == 0 {
                    backoff = 1;
                }
            }

            // Try to store true, if we win then synchronize with release-store
            // of false by last winner thread. Otherwise try again in next
            // iteration.
            if !self.locked.swap(true, Ordering::Acquire) {
                break SpinLockGuard {
                    inner: self,
                };
            }
        }
    }

    /// Try to acquire the lock without blocking/spinning. It tries once and
    /// immediately returns with either `Some` in case of success of `None` in
    /// case of failure.
    pub fn try_lock(&self) -> Option<SpinLockGuard<'_, T>> {
        match self.locked.compare_exchange(false, true, Ordering::Acquire,
            Ordering::Relaxed) {
            Ok(_) => {
                let guard = SpinLockGuard {
                    inner: self,
                };

                Some(guard)
            }

            Err(_) => None,
        }
    }

    /// Consumes the `SpinLock` and returns the inner data.
    pub fn into_inner(self) -> T {
        // Synchronize with all the previous owning threads and with whatever
        // modifications they did to the data.
        assert!(self.locked.load(Ordering::Acquire) == false, "Reached invalid \
            state. Have exclusive ownership of the SpinLock despite it being \
            locked by some other thread.");

        // Put self in a manually drop to prevent it's drop from being called.
        let s = core::mem::ManuallyDrop::new(self);

        // Return the inner data.
        //
        // We can safely do this as we have the complete ownership of the
        // spinlock and have successfully synchronized with all the previous
        // modifications of the data done by other threads.
        unsafe { s.data.get().read() }
    }
}

/// RAII wrapper type that the spin lock holder uses to access the inner `T`.
///
/// It implements `Deref` and `DerefMut` for the inner `T`.
///
/// The drop implementation will unlock the `SpinLock` for other threads to use.
/// This works even in cases when the current lock holding thread panics.
/// Although there is no way for the next lock holding thread to know whether
/// the previous thread panicked and has left the data in an inconsistent state.
pub struct SpinLockGuard<'lock, T> {
    inner: &'lock SpinLock<T>,
}

// SAFETY:
//
// `T` must be `Send` because the lock guard could be used to get an exclusive
// reference to the inner `T`.
unsafe impl<T: Send> Send for SpinLockGuard<'_, T> {}

// SAFETY:
//
// `T` must be `Sync` because a shared reference to the lock guard can be used
// to get a shared reference to the inner `T`.
unsafe impl<T: Sync> Sync for SpinLockGuard<'_, T> {}

impl<'lock, T> Drop for SpinLockGuard<'lock, T> {
    fn drop(&mut self) {
        self.inner.locked.store(false, Ordering::Release);
    }
}

/// A shared reference to the lock guard converts into a shared reference to the
/// inner `T` if needed.
impl<'lock, T> core::ops::Deref for SpinLockGuard<'lock, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY:
        //
        // Having the guard gives one exclusive ownership of the inner `T`, so
        // it is fine to get a shared reference.
        unsafe { &*self.inner.data.get() }
    }
}

/// An exclusive reference to the lock guard converts into an exclusive
/// reference to the inner `T` if needed.
impl<'lock, T> core::ops::DerefMut for SpinLockGuard<'lock, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY:
        //
        // Having the guard gives one exclusive ownership of the inner `T`, so
        // it is fine to get an exclusive reference.
        unsafe { &mut *self.inner.data.get() }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    use std::{thread, time};

    #[test]
    #[ignore]
    fn panic_safety() {
        let sl = SpinLock::new(0i32);

        thread::scope(|s| {
            let t1 = s.spawn(|| {
                // This thread locks then panics while holding the lock, and
                // makes no updates to the inner data.
                let mut lock = sl.lock();
                panic!("panicking with lock");
                *lock += 12;
            });

            let t2 = s.spawn(|| {
                // This thread waits for a while to ensure that the other thread
                // gets the lock first and could panic while holding the lock.
                thread::sleep(time::Duration::from_millis(5));

                // This thread first updates the data then panics.
                let mut lock = sl.lock();
                *lock += 12;
                panic!("panicking with lock");
            });

            _ = t1.join();
            _ = t2.join();
        });

        let inner = sl.into_inner();

        // t2 must have been able to update the value.
        assert!(inner == 12);
    }
}
