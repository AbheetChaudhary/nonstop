use core::mem::MaybeUninit;
use core::sync::atomic::{AtomicBool, Ordering};
use core::cell::UnsafeCell;

/// A thread safe lazy initialization primitive.
///
/// At a time only one thread will try to initialize this. That thread will
/// either successfully initialize it or will hang forever. Other threads will
/// spin while the data is being initialized.
///
/// If the initializer thread panics then its drop will alert the waiting
/// threads, but there is no recovery. Once the initializer thread panics, all
/// other waiting threads panic as well.
pub struct LazyLock<T, F = fn() -> T> {
    value:          UnsafeCell<MaybeUninit<T>>,
    ready:          AtomicBool, // Value is ok to read from.
    initializing:   AtomicBool, // Someone is initializing it.
    poisoned:       AtomicBool, // The last initializer panicked!
    initializer:    UnsafeCell<Option<F>>, // The initializing function,
                                           // F: FnOnce() -> T
}

// SAFETY:
// 
// The lazylock could be used to send the inner `T` to another thread, so `T`
// must be `Send`.
//
// `T` does not need to be `Sync` because at a time only the
// thread that owns the lazylock can get a shared reference to `T`. If multiple
// threads have a shared reference to the lock then they could also get a shared
// reference to `T`, but the required conditions for that to be correct are
// already encoded in the conditions for lazylock to be `Sync`.
unsafe impl<T: Send, F: Send> Send for LazyLock<T, F> {}

// SAFETY:
// If other threads are able to get a shared reference to lazylock then they
// will be able to stick a `T` in it which will be potentially dropped by
// another thread when the lazy lock gets dropped. So `T` must be `Send`.
//
// A shared reference to lazylock gives a shared reference to `T`, so `T` must
// be `Sync` as well.
unsafe impl<T: Send + Sync, F: Send> Sync for LazyLock<T, F> {}

impl<T, F> Drop for LazyLock<T, F> {
    fn drop(&mut self) {
        if self.ready.load(Ordering::Acquire) {
            // Value is ready, drop it in place.
            unsafe {
                (&mut *self.value.get()).assume_init_drop()
            }
        }
    }
}

impl<T, F: FnOnce() -> T> LazyLock<T, F> {
    pub const fn new(f: F) -> Self {
        LazyLock {
            value:          UnsafeCell::new(MaybeUninit::uninit()),
            ready:          AtomicBool::new(false),
            initializing:   AtomicBool::new(false),
            poisoned:       AtomicBool::new(false),
            initializer:    UnsafeCell::new(Some(f)),
        }
    }

    /// Try to get a lock guard which will then initialize the inner value.
    /// One of the other threads might be able to do this before us.
    fn init_lock(&self) -> Option<LazyLockGuard<'_, T, F>> {
        match self.initializing.compare_exchange(
            false,
            true,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => {
                let guard = LazyLockGuard::new(self);
                Some(guard)
            }
            Err(_) => None,
        }

    }
}

impl<T, F: FnOnce() -> T> core::ops::Deref for LazyLock<T, F> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        'outer: loop {
            // The value is ready, take a shared reference to it and exit.
            if self.ready.load(Ordering::Acquire) {
                // SAFETY:
                // Value has been properly initialized, so we can get a shared
                // reference to it.
                break unsafe { (&*self.value.get()).assume_init_ref() };
            } else {
                // The value is not ready, it is either being initialized or the
                // initialization has not begun yet.

                // The value is being initialized.
                if self.initializing.load(Ordering::Relaxed) {
                    // Wait for the value to be initialized successfully or for
                    // the initializer to panic.

                    loop {
                        core::hint::spin_loop();

                        // The initialization completed successfully. `ready`
                        // will be set to true. Relaxed load is fine as in the
                        // next iteration of the loop we do an Acquire load
                        // before handing out the reference.
                        if self.ready.load(Ordering::Relaxed) {
                            continue 'outer;
                        } else if self.poisoned.load(Ordering::Relaxed) {
                            // Initialization panicked!
                            panic!("poisoned LazyLock initializer");
                        }
                    }
                } else {
                    // No initialization has started yet.

                    // Starting initialization now. Another thread might beat
                    // us.
                    match self.init_lock() {
                        Some(guard) => {
                            // SAFETY:
                            // At a time, only one guard will exist, so it is
                            // valid to create a mutable reference.
                            //
                            // Unchecked unwrap is valid because we initialized
                            // it with Some(_).
                            let initializer = unsafe {
                                (&mut *guard.initializer.get())
                                    .take()
                                    .unwrap_unchecked()
                            };
                            // Here another thread took ownership of the
                            // initializing closure.

                            // Might panic!, guard's drop will set poisoned.
                            let val = initializer();

                            // SAFETY:
                            // Safe to get mutable reference as only one guard
                            // exist at a time.
                            unsafe {
                                (&mut *guard.value.get()).as_mut_ptr().write(val);
                            }

                            // Release it for future accesses.
                            guard.ready.store(true, Ordering::Release);

                        } // guard drops here.
                        None => { /*continue */ }
                    }
                }
            }
        }
    }
}

/// A guard type that is held by the thread that is currently initializing the
/// inner `T`.
///
/// Only one thread will ever hold this guard. That thread will either
/// successfully initialize the `T` or will panic, thus panicking all other
/// waiting threads.
pub struct LazyLockGuard<'lock, T, F> {
    inner: &'lock LazyLock<T, F>,
    _marker: core::marker::PhantomData<*const ()>, // no Send or Sync
}

impl<'lock, T, F> LazyLockGuard<'lock, T, F> {
    fn new(lazy_lock: &'lock LazyLock<T, F>) -> Self {
        LazyLockGuard {
            inner: lazy_lock,
            _marker: core::marker::PhantomData,
        }
    }
}

impl<'lock, T, F> Drop for LazyLockGuard<'lock, T, F> {
    fn drop(&mut self) {
        // Value is not initialized by the end of the guards lifetime, assume
        // panic happened.
        if !self.inner.ready.load(Ordering::Relaxed) {
            self.inner.poisoned.store(true, Ordering::Relaxed);
        }

        self.inner.initializing.store(false, Ordering::Relaxed);
    }
}

impl<'lock, T, F> core::ops::Deref for LazyLockGuard<'lock, T, F> {
    type Target = LazyLock<T, F>;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}
