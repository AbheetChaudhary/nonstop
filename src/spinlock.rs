use crate::BeginCacheLine;

use core::sync::atomic::{AtomicBool, Ordering};
use core::cell::UnsafeCell;

/// A simple spinlock implementation that protects exclusive ownership of a value
/// of type `T`.
///
/// At a time only one thread will have exclusive access to the inner `T`. All
/// other threads, if they exist, need to wait by spinning.
#[repr(C)]
pub struct SpinLock<T> {
    data:   UnsafeCell<T>,

    // Flag for whether someone holds the lock or
    // not. It is in its own cache line to avoid
    // false sharing between the lock holding
    // thread that accesses the data and other
    // spinning threads.
    locked: BeginCacheLine<AtomicBool>, 

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
            locked: BeginCacheLine(AtomicBool::new(false)),
            data:   UnsafeCell::new(value),
        }
    }

    /// Acquire the lock. It spins and blocks the current thread until it can
    /// acquire the lock.
    ///
    /// If the current lock holding thread panics while holding the lock then it
    /// will just keep spinning forever.
    pub fn lock(&self) -> SpinLockGuard<'_, T> {
        loop {
            // Spin while it is locked.
            //
            // Relaxed ordering to keep it as light as possible. The cache line
            // could be in shared state across all the spinning CPUs.
            while self.locked.load(Ordering::Relaxed) {
                core::hint::spin_loop();
            }

            // Synchronize with the Release store of the drop of the previous
            // lock holders guard.
            if self.locked.compare_exchange(false, true, Ordering::Acquire, 
                Ordering::Relaxed).is_err() {
                continue;
            }
            // This thread won the race and successfully acquired the lock.

            break SpinLockGuard {
                inner: self,
            };
        }
    }

    /// Try to acquire the lock without blocking/spinning. It tries to acquire
    /// the lock once and immediately returns with either success(`Some(...)`)
    /// or failure(`None`).
    pub fn try_lock(&self) -> Option<SpinLockGuard<'_, T>> {
        if self.locked.compare_exchange(false, true, Ordering::Acquire, 
            Ordering::Relaxed).is_ok() {
            
            let guard = SpinLockGuard {
                inner: self
            };

            Some(guard)
        } else {
            None
        }
    }
}

/// RAII wrapper type that the spin lock holder uses to access the inner `T`.
///
/// It implements `Deref` and `DerefMut` for the inner `T`. The drop
/// implementation will unlock the `SpinLock` for other threads to use.
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

/// A shared reference to the lock guard collapses to a shared reference to the
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

/// An exclusive reference to the lock guard collapses to an exclusive reference
/// to the inner `T` if needed.
impl<'lock, T> core::ops::DerefMut for SpinLockGuard<'lock, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY:
        //
        // Having the guard gives one exclusive ownership of the inner `T`, so
        // it is fine to get an exclusive reference.
        unsafe { &mut *self.inner.data.get() }
    }
}

