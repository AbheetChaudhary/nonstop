//! A treiber stack based multi-producer, single-consumer pool.
//!
//! It is really bad in terms of performance and should be rarely used. All the
//! producers and the consumer are all racing to read-modify-write the top of
//! the stack, there is a lot of contention.

extern crate alloc;

use alloc::alloc::Global;

use core::{
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering, fence},
    ptr::NonNull,
    alloc::{Layout, Allocator},
};


/// Creates the producer and consumer sides of the stack pool.
pub fn new_stack_pool<T>() -> (Producer<T>, Consumer<T>) {
    let layout = Layout::new::<StackPool<T>>();
    let stack = Global.allocate(layout).expect("failed to allocate").cast::<StackPool<T>>();

    unsafe {
        stack.write(StackPool {
            top: AtomicPtr::new(core::ptr::null_mut()),
            pc_count: AtomicUsize::new(2 + 1), // one producer one consumer
        })
    };

    (Producer { stack }, Consumer { stack })
}

/// A stack is a linked list of `Node<T>`'s.
struct Node<T> {
    elem: T,
    next: AtomicPtr<Node<T>>,
}

impl<T> Node<T> {
    /// Create a new non-null pointer to node.
    fn new_non_null(elem: T) -> NonNull<Node<T>> {
        // let node = Node {
        //     elem,
        //     next: AtomicPtr::new(core::ptr::null_mut()),
        // };

        let layout = Layout::new::<Node<T>>();
        let non_null = Global.allocate(layout)
            .expect("failed to allocate").cast::<Node<T>>();

        unsafe {
            // NOTE: miri has a problem with this store. IDK why???
            (&raw mut (*non_null.as_ptr()).elem).write(elem);

            (&raw mut (*non_null.as_ptr()).next)
                .write(AtomicPtr::new(core::ptr::null_mut()));
        }

        non_null
    }

    /// Delete a node pointed to by this pointer.
    ///
    /// It does not calls drop for the inner `T` of the `Node<T>`.
    unsafe fn delete(ptr: *mut Node<T>) {
        let non_null = NonNull::new(ptr).unwrap();
        let layout = Layout::new::<Node<T>>();
        unsafe { Global.deallocate(non_null.cast(), layout); }
    }
}

/// The inner stack pool of the producer and consumer types.
///
/// It is just a linked list made up of `Node<T>`s.
struct StackPool<T> {
    /// Pointer to the top of the stack pool, if it is null then the pool is
    /// currently empty.
    top: AtomicPtr<Node<T>>,

    /// A measure of how many producer and consumers are alive right now.
    ///
    /// Once all the consumers are gone it will not be possible to insert any
    /// more elements in the stack pool.
    ///
    /// It is `2 * #producer + #consumer`. Since at any time there is atmost one
    /// consumer and any number of producers so this number have certain
    /// meanings:
    ///
    /// - 1      -> sole consumer, all the producers are gone.
    /// - 2n     -> Only producers are remaining, given `n != 0`
    /// - 2n + 1 -> The consumer and at least one producer is alive, given
    ///             `n != 0`
    /// - 0      -> This state is acheived only when every producer and consumer
    ///             has been dropped and we are in the drop implementation of
    ///             the last one.
    pc_count: AtomicUsize,
}

impl<T> StackPool<T> {
    fn relaxed_c_count(&self) -> usize {
        // The consumer is the remainder.
        self.pc_count.load(Ordering::Relaxed) % 2
    }

    /*
    fn relaxed_p_count(&self) -> usize {
        let pc_count = self.pc_count.load(Ordering::Relaxed);

        if pc_count % 2 == 0 {
            pc_count / 2
        } else {
            // Remove the consumer.
            (pc_count - 1) / 2
        }
    }
    */
}

/// The producer side of the stackpool.
pub struct Producer<T> {
    /// Pointer to the inner stack pool, both the consumer and the producer
    /// share this same pointer.
    stack: NonNull<StackPool<T>>,
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        // Increment the global pc_count by 2.

        let mut current = self.inner_stack().pc_count.load(Ordering::Relaxed);

        loop {
            let new = current.checked_add(2)
                .expect("program is in a very degenerate state");

            match self.inner_stack().pc_count.compare_exchange(current, new,
                Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => {
                    break Self { stack: self.stack };
                }

                Err(n) => {
                    current = n;
                }
            }
        }
    }
}

unsafe impl<T: Send> Send for Producer<T> {}

impl<T> Producer<T> {
    /// Insert a new element into the stack pool.
    ///
    /// If there is no consumer on the other side to receive then return the
    /// element with `Err(T)`.
    pub fn insert(&self, elem: T) -> Result<(), T> {
        // Early return if there are no more consumers.
        if self.inner_stack().relaxed_c_count() == 0 {
            return Err(elem);
        }

        // NOTE: Between last checking for alive consumers and eventually
        // pushing the element all the consumers might get dropped.

        // Create a node on the heap with the given element.
        let node_non_null = Node::new_non_null(elem);
        
        // Current top of the stack.
        let mut top = self.inner_stack().top.load(Ordering::Relaxed);

        loop {
            // Set the current top as the next node pointer for the node that we
            // are about to insert.
            unsafe { node_non_null.as_ref().next.store(top, Ordering::Relaxed); }

            // If the top has not changed then insert the node. Even if it was
            // changed in between before reverting back to the old value we
            // still won't have ABA problem.
            match self.inner_stack().top.compare_exchange(top, node_non_null.as_ptr(), 
                Ordering::Release, Ordering::Relaxed) {
                Ok(_) => {
                    // Inserted the elemnt to the top of the stack.
                    break;
                }

                Err(new_top) => {
                    // Some other thread beat us to the top. Update the current
                    // top and try again.
                    top = new_top;
                }
            }
        }

        Ok(())
    }

    /// Get a pointer to the inner stackpool.
    fn inner_stack(&self) -> &StackPool<T> {
        // SAFETY: this is safe because as long as at least one producer or
        // consumer is alive nobody deallocates the stackpool pointer, so we
        // could get a shared reference to it.
        unsafe { self.stack.as_ref() }
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        // Decrement the pc_count by 2.
        //
        // If the count before decrementing was 2 then that means we are the
        // last producer and even the consumer is gone.
        if self.inner_stack().pc_count.fetch_sub(2, Ordering::Release) == 2 {
            // We are the last among the producers and the consumer.

            // Synchronize with the release decrement of pc_count by all the
            // drops that happened before us.
            fence(Ordering::Acquire);

            let mut top = self.inner_stack().top.load(Ordering::Acquire);

            while !top.is_null() {
                let node = unsafe { top.read() };

                let next_top = node.next.load(Ordering::Acquire);

                // Free the memory that Node takes on the heap.
                unsafe { Node::delete(top) };

                // Update the top with next_node.
                top = next_top;

                // The inner T will get dropped here.
                drop(node);
            }

            // Deallocate the StackPool itself.
            unsafe {
                Global.deallocate(self.stack.cast(), Layout::new::<StackPool<T>>())
            };
        }
    }
}

/// The consumer side of the stackpool.
pub struct Consumer<T> {
    /// Pointer to the inner stack pool, both the consumer and the producer
    /// share this same pointer.
    stack: NonNull<StackPool<T>>,
}

impl<T> Consumer<T> {
    /// Remove a node from the stack pool if there is one. Return `None` if the
    /// pool is currently empty.
    ///
    /// `None` return value does not mean that all the producers are dropped.
    pub fn remove(&self) -> Option<T> {
        // If the top of the stack is currently null then return None.
        if self.inner_stack().top.load(Ordering::Relaxed).is_null() {
            return None;
        }

        // Acquire load because we are going to dereference this pointer the
        // first thing in the loop below.
        let mut top = self.inner_stack().top.load(Ordering::Acquire);

        loop {
            // We have already checked that the top is not null. Even if other
            // producers change the top, as long as there is only one consumer
            // it will never become null again.
            let top_node = unsafe { &*top };

            // Pointer to the next top node after we remove the current one.
            let next = top_node.next.load(Ordering::Relaxed);

            // If the top of the stack is still the same then make next the new
            // top.
            match self.inner_stack().top.compare_exchange(top, next, Ordering::Acquire,
                Ordering::Relaxed) {
                Ok(_) => {
                    // Read the element.
                    let elem = unsafe { (&raw const (*top).elem).read() };

                    // Drop the node heap allocation without dropping the inner
                    // element.
                    unsafe { Node::delete(top) };

                    break Some(elem);
                }

                Err(new_top) => {
                    // Some other producer thread beat us to the top, try again.
                    top = new_top;
                }
            }
        }
    }

    /// Check whether there are any producers active or not.
    ///
    /// This does a relaxed read of the active producers count. If this function
    /// returns `true` then it is possible that all the producers get dropped
    /// before the next use of the channel.
    ///
    /// If this function returns `false` then that is conclusive that all the
    /// producers are indeed gone.
    pub fn producers_exist(&self) -> bool {
        let pc_count = self.inner_stack().pc_count.load(Ordering::Relaxed);

        if pc_count == 1 {
            // All the producers are gone.

            // Synchronize with the release store of pc_count in the drop of all
            // the producers. This is not necessary here but doesn't costs much
            // to be safe.
            fence(Ordering::Acquire);
            false
        } else {
            true
        }
    }

    fn inner_stack(&self) -> &StackPool<T> {
        unsafe { self.stack.as_ref() }
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        // Decrement the pc_count by 1.
        //
        // If the count before decrementing was 1 then that all the producers
        // are gone.
        if self.inner_stack().pc_count.fetch_sub(1, Ordering::Release) == 1 {
            // It is our job to free the memory.

            // Synchronize with the release decrement of pc_count by all the
            // drops that happened before us.
            fence(Ordering::Acquire);

            let mut top = self.inner_stack().top.load(Ordering::Acquire);

            while !top.is_null() {
                let node = unsafe { top.read() };

                let next_top = node.next.load(Ordering::Acquire);

                // Free the memory that Node takes on the heap.
                unsafe { Node::delete(top) };

                // Update the top with next_node.
                top = next_top;

                // The inner T will get dropped here.
                drop(node);
            }

            // Deallocate the StackPool itself.
            unsafe {
                Global.deallocate(self.stack.cast(), Layout::new::<StackPool<T>>())
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    #[ignore] // This panics for some reason with miri, but passes otherwisse.
    fn stackpool_works() {
        let (tx, rx) = new_stack_pool::<usize>();

        thread::scope(|s| {
            for _ in 0..10 {
                let tx = tx.clone();
                s.spawn(move || {
                    for i in 1..=100 {
                        tx.insert(i);
                    }
                });
            }

            drop(tx);

            let mut sum = 0;

            'outer: loop {
                let elem = 'inner: loop {
                    let v = rx.remove();

                    if v.is_none() {
                        if !rx.producers_exist() {
                            let v = rx.remove();

                            if v.is_none() {
                                break 'outer;
                            } else {
                                break 'inner v.unwrap();
                            }
                        }
                    } else {
                        break 'inner v.unwrap();
                    }
                };

                sum += elem;
            }

            println!("sum: {sum}");
            assert!(sum == 5050 * 10);
        });
    }
}
