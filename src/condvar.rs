//! A condition variable to be used with mutexes.
//!
//! It works in `no_std` environments as well.


// NOTE: this code ended up looking a lot similar to the one discussed in 'Rust
// Atomics and Locks" by Mara Bos, but it is not copied.

use crate::mutex::MutexGuard;
use crate::futex;

use core::sync::atomic::{AtomicU32, Ordering};

pub struct Condvar {
    /// Number of notifications (_one/_all) that have happened.
    counter: AtomicU32,

    /// Number of threads that are currently waiting for a notification.
    waiters: AtomicU32,
}

impl Condvar {
    pub fn new() -> Self {
        Self {
            counter: AtomicU32::new(0),
            waiters: AtomicU32::new(0),
        }
    }

    /// Notify one of the waiting threads.
    ///
    /// This function does not makes any syscall in case there are no waiting
    /// threads.
    pub fn notify_one(&self) {
        if self.waiters.load(Ordering::Relaxed) > 0 {
            // Increment counter so that the woken up thread does not concludes
            // that a spurious notification woke it up.
            self.counter.fetch_add(1, Ordering::Relaxed);

            _ = futex::wake_one(&self.counter);
        }
    }

    /// Notify all the waiting threads.
    ///
    /// This function does not makes any syscall in case there are no waiting
    /// threads.
    ///
    /// Try to avoid using this one, as in the end only one of the woken up
    /// threads will be able to acquire the mutex and rest will block.
    pub fn notify_all(&self) {
        if self.waiters.load(Ordering::Relaxed) > 0 {
            // Increment counter so that the woken up thread does not concludes
            // that a spurious notification woke it up.
            self.counter.fetch_add(1, Ordering::Relaxed);

            _ = futex::wake_all(&self.counter);
        }
    }

    /// Unlock the mutex and start waiting for a notification.
    ///
    /// This function takes a mutex guard and unlocks the mutex, then goes to
    /// sleep. After waking up, it blocks and tries to reaquire the mutex.
    ///
    /// If it gets woken up due to a spurious notification then it goes back to
    /// sleep again.
    pub fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
        // Increment waiting thread count.
        self.waiters.fetch_add(1, Ordering::Relaxed);

        // Cache the current count.
        let old_counter = self.counter.load(Ordering::Relaxed);

        let inner = guard.inner;

        // Drop the guard.
        drop(guard);

        while self.counter.load(Ordering::Relaxed) == old_counter {
            _ = futex::wait(&self.counter, old_counter);
        }

        // Decrement waiting thread count.
        self.waiters.fetch_sub(1, Ordering::Relaxed);

        // Try to lock the mutex again. This will block.
        inner.lock()
    }
}

#[cfg(test)]
mod tests {
    use crate::{Mutex, Condvar};
    use std::thread;
    use std::time::Duration;

    #[test]
    #[ignore]
    fn test_condvar() {
        let mutex = Mutex::new(0);
        let condvar = Condvar::new();

        let mut wakeups = 0;

        thread::scope(|s| {
            s.spawn(|| {
                thread::sleep(Duration::from_secs(1));
                *mutex.lock() = 123;
                condvar.notify_one();
            });

            let mut m = mutex.lock();
            while *m < 100 {
                m = condvar.wait(m);
                wakeups += 1;
            }

            assert_eq!(*m, 123);
        });

        // Check that the main thread actually did wait (not busy-loop),
        // while still allowing for a few spurious wake ups.
        assert!(wakeups < 10);
    }
}
