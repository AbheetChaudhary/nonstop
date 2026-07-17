use crate::BeginCacheLine;

extern crate alloc;

use alloc::alloc::Global;

use core::alloc::{Allocator, Layout};
use core::cell::UnsafeCell;
use core::mem::MaybeUninit;
use core::ptr::NonNull;
use core::sync::atomic::{AtomicUsize, fence, Ordering};

#[repr(C)]
struct Inner<T> {
    value:        UnsafeCell<MaybeUninit<T>>,

    // The number of `Arc`s currently sharing the ownership of the value. If
    // this number goes to zero then the value gets dropped, but not the
    // allocation of `Inner`.
    strong_count: BeginCacheLine<AtomicUsize>,

    // The number of `Weak`s currently alive. As long as any one of them is
    // alive then the allocation of `Inner` stays alive. If any `Arc` is alive
    // too then a `Weak` could be upgraded.
    weak_count:   BeginCacheLine<AtomicUsize>,
}

/// A weak reference pointer that might be upgradable to a strong reference
/// `Arc`.
///
/// A `Weak` might exist even when the `T` has been deallocated, and there are no
/// strong references(`Arc`s) left around. Such a weak will be just keeping the
/// inner allocation alive, eventually freeing it when the last remaining `Weak`
/// gets dropped.
pub struct Weak<T> {
    inner:   NonNull<Inner<T>>,

    // Dropping `Weak` might lead to drop of a `Inner<T>`.
    _phantom: core::marker::PhantomData<Inner<T>>,
}

// SAFETY: same reasons that were used to make `Arc` send or sync.
unsafe impl<T: Send + Sync> Send for Weak<T> {}
unsafe impl<T: Send + Sync> Sync for Weak<T> {}

impl<T> Clone for Weak<T> {
    fn clone(&self) -> Self {
        if self.inner()
            .weak_count.fetch_add(1, Ordering::Relaxed) == isize::MAX as usize {

            panic!("degenerate! too many weak references!");
        }

        Self {
            inner: self.inner,
            _phantom: core::marker::PhantomData,
        }
    }
}

impl<T> Drop for Weak<T> {
    fn drop(&mut self) {
        // If there are no `Weak`s then it is time to free the allocation.
        if self.inner().weak_count.fetch_sub(1, Ordering::Release) == 1 {
            // Synchronize with the drop of all other weaks before us. Because
            // this code must run after the drop code of all other weak's have
            // already run.
            fence(Ordering::Acquire);

            // By now the value must have been dropped by the last Arc's drop.
            // Guarantee that there is indeed no remaining Arc.
            assert_eq!(self.inner().strong_count.load(Ordering::Relaxed), 0);

            let layout = Layout::new::<Inner<T>>();

            // SAFETY: the pointer was allocated using this allocator, the
            // layout is also correct.
            unsafe { Global.deallocate(self.inner.cast(), layout); }
        }
    }
}

impl<T> Weak<T> {
    /// Could only upgrade if there is atleast one strong reference alive.
    pub fn upgrade(&self) -> Option<Arc<T>> {
        // Current strong reference count.
        let mut strong = self.inner().strong_count.load(Ordering::Relaxed);

        loop {
            if strong == 0 {
                // No strong references alive. Cannot upgrade.
                return None;
            }

            // If strong is this high, then weak count must be at least this
            // high. It is a degenerate situation.
            if strong == isize::MAX as usize {
                panic!("degenerate! too many strong references!");
            }

            match self.inner().strong_count.compare_exchange_weak(strong, strong + 1,
                Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => { 
                    // Successfully incremented the strong reference count,
                    // now we can create an Arc from this weak.

                    break Some(Arc {ptr: self.clone()}); // Increments weak
                                                         // count.
                }

                Err(c) => {
                    if c != 0 {
                        // Retry, as the strong reference count changed, but
                        // there are still non-zero number of them alive, so
                        // upgrading is still possible.
                        strong = c;
                    } else {
                        // All the strong references just disappeared,
                        // cannot upgrade.
                        break None;
                    }
                }
            }
        }
    }

    fn inner(&self) -> &Inner<T> {
        // SAFETY:
        //
        // As long as a weak is alive(we are the weak), it is safe to
        // dereference the non null here.
        unsafe { self.inner.as_ref() }
    }
}

/// A thread-safe reference counting pointer.
///
/// Used to share ownership of the inner `T` across multiple threads. When the
/// last `Arc` drops it also runs the drop code of `T`.
///
/// To break reference cycles another weak reference, `Weak` pointer, is also
/// provided.
pub struct Arc<T> {
    ptr: Weak<T>,
}

// SAFETY:
//
// An `Arc` or a shared reference to an `Arc` could be used to get a shared
// reference to the inner `T`. The final `Arc` will also be responsible to drop
// the `T` after taking ownership of it.
//
// For this to be safe, `T` must be both `Send` and `Sync` in both these cases.
unsafe impl<T: Send + Sync> Send for Arc<T> {}
unsafe impl<T: Send + Sync> Sync for Arc<T> {}

impl<T> core::ops::Deref for Arc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.data()
    }
}

impl<T> Drop for Arc<T> {
    fn drop(&mut self) {
        // Just decrement the strong count and optionally drop the value if we
        // are the last strong reference.
        if self.ptr.inner().strong_count.fetch_sub(1, Ordering::Release) == 1 {
            // We are the last strong reference.

            // This drop code must run after all other Arc's have been dropped.
            fence(Ordering::Acquire);

            // SAFETY:
            //
            // We are inside the last instance of an Arc. Avoid creating a
            // mutable reference to Inner<T>, as other Weaks might be using a
            // shared reference to Inner<T>.
            unsafe {
                (*self.ptr.inner().value.get()).assume_init_drop();
            }
        }

        // The weak pointer gets dropped, thus running the drop code of Weak.
    }
}

impl<T> Clone for Arc<T> {
    fn clone(&self) -> Self {
        let cloned = Self {
            ptr: Weak::clone(&self.ptr),
        };

        // Since weak count is always greater than or equal to strong_count and 
        // weak count has not overflown in the previous weak cloning, so we must
        // be safe to assume that this will not overflow too.
        self.ptr.inner().strong_count.fetch_add(1, Ordering::Relaxed);

        cloned
    }
}

impl<T> Arc<T> {
    /// Create a new `Arc`.
    pub fn new(val: T) -> Self {
        let inner = Inner {
            value:        UnsafeCell::new(MaybeUninit::new(val)),
            strong_count: BeginCacheLine(AtomicUsize::new(1)),
            weak_count:   BeginCacheLine(AtomicUsize::new(1)),
        };

        let layout = Layout::new::<Inner<T>>();

        let ptr = Global.allocate(layout)
            .expect("alloc error occured").cast::<Inner<T>>();

        // SAFETY: we just successfully allocated the memory with proper layout.
        unsafe { ptr.as_ptr().write(inner); }

        let weak = Weak {
            inner: ptr,
            _phantom: core::marker::PhantomData,
        };

        Self {
            ptr: weak,
        }
    }

    /// Get mutable access to the inner data when there is just one Arc and just
    /// one weak, i.e. only `self` must be the sole `Arc` remaining.
    pub fn get_mut(arc: &mut Arc<T>) -> Option<&mut T> {
        if arc.weak_count_relaxed() != 1 {
            return None
        }

        // Happens before relation of every single drop of other Arc clones.
        fence(Ordering::Acquire);

        // There are no other Weak's so no one is going to secretly promote a
        // Weak to an Arc behind our back. The only Weak must be the one inside
        // our Arc. Meaning that there is only one Arc, just us, and we have an
        // exclusive reference to ourself.

        let ret = unsafe { &mut *arc.ptr.inner().value.get().cast::<T>() };

        Some(ret)
    }

    // Get a shared reference to the inner `T`.
    fn data(&self) -> &T {
        // SAFETY: It should be fine because as long as there is at least one
        // `Arc` alive(we are), the inner `T` has not been deallocated yet.
        unsafe { &*self.ptr.inner().value.get().cast::<T>() }
    }

    /// Relaxed read of the strong count, is not reliable.
    pub fn strong_count_relaxed(&self) -> usize {
        self.ptr.inner().strong_count.load(Ordering::Relaxed)
    }

    /// Relaxed read of the weak count, is not reliable.
    pub fn weak_count_relaxed(&self) -> usize {
        self.ptr.inner().weak_count.load(Ordering::Relaxed)
    }

    /// Downgrade the `Arc` and create a new `Weak`.
    pub fn downgrade(&self) -> Weak<T> {
        Weak::clone(&self.ptr)
    }
}

#[test]
fn test_get_mut() {
    let mut a = Arc::new(String::from("hello, world!"));

    {
        let b = Arc::clone(&a);
    }

    let mut c = Arc::clone(&a);
    assert!(Arc::get_mut(&mut c).is_none());

    drop(c);

    *Arc::get_mut(&mut a).unwrap() = String::from("hi there");

    assert!(&*a == "hi there");
}

