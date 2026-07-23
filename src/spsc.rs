//! Single-Producer Single-Consumer queue with FIFO property.
//!
//! The queue always has a power-of-2 inner `capacity`, out of which only
//! `capacity - 1` could be used to store elements. If the queue is filled
//! then the producer when trying to push will spin until the consumer pops
//! something on its end or gets dropped.
//! 
//! The last one among the producer and the consumer to get dropped will free the
//! elements that are currently stuck inside the queue.

use crate::{CacheLine, spinloop};

extern crate alloc;

use alloc::alloc::Global;
use core::{
    sync::atomic::{AtomicUsize, Ordering, fence},
    ptr::NonNull,
    alloc::{Allocator, Layout},
};

/// Internal data structure of the `Consumer` and `Producer` structs that holds
/// the element buffer, head and tail values, and the state of the channel.
///
/// `head` is the number of elements pushed by the producer, `tail` is the
/// number of elements that the consumer has popped.
///
/// Both head and tail values are masked to get the index in the buffer where
/// producer will do the next write, and where the consumer will pop the next
/// element from.
///
/// If masked head and tail are same then there is nothing for the consumer to
/// consume, it has caught up with the producer.
///
/// When trying to add a new element to the queue, the producer ensures that
/// after writing and incrementing the head value, the masked head must not be
/// same as the masked tail, as that would make the consumer conclude that it
/// has caught up with the producer. 
///
/// Basically the producer will only write to an index if the consumer is not
/// already waiting there.:w
#[repr(C)]
struct InnerFifo<T> {
    buffer:   NonNull<T>,             // Buffer to store elements.
    capacity: usize,                  // Power-of-2 capacity of channel.
    head:     CacheLine<AtomicUsize>, // Number of elements pushed by the producer
    tail:     CacheLine<AtomicUsize>, // Number of elements popped by the consumer
    state:    CacheLine<AtomicUsize>, // 2 -> both sides are alive.
                                      // 1 -> one side got dropped.
                                      // 0 -> both sides got dropped.
}

impl<T> Drop for InnerFifo<T> {
    fn drop(&mut self) {
        let cap_mask = self.capacity - 1;

        // Relaxed is fine. Whoever called for drop of InnerFifo has already put
        // the right barriers.
        let head = self.head.load(Ordering::Relaxed);

        // Mask head count to get index where next write would have happened.
        let head_in_buffer = head & cap_mask;

        // Relaxed is fine. Whoever called for drop of InnerFifo has already put
        // the right barriers.
        let tail = self.tail.load(Ordering::Relaxed);

        // Number of elements to drop.
        let to_drop = head - tail;

        // Mask tail count to get index where next element will come from.
        let mut tail_in_buffer = tail & cap_mask;

        let mut drop_count = 0;

        // Drop individual elements until tail catches with head.
        while tail_in_buffer != head_in_buffer {
            // SAFETY: Tail has not yet caught up to head so there is an element
            // to drop, and the addition keeps the pointer within allocation
            // boundary because it has been masked with the power-of-2 capacity.
            unsafe { self.buffer.add(tail_in_buffer).drop_in_place() };
            drop_count += 1;

            tail_in_buffer = (tail_in_buffer + 1) & cap_mask;
        }

        assert_eq!(drop_count, to_drop);

        // Free the buffer memory.
        let buffer_layout = Layout::array::<T>(self.capacity).unwrap();

        // SAFETY: We allocated the buffer, we free the buffer.
        unsafe { Global.deallocate(self.buffer.cast(), buffer_layout) };
    }
}

/// Create a new FIFO queue which has `cap` inner capacity. `capacity` should be
/// a power-of-2, if it is not then it will be rounded up.
///
/// # Panics
///
/// It panics if the capacity(after rounding) does not fit in `usize`.
pub fn fifo<T>(cap: usize) -> (Producer<T>, Consumer<T>) {
    let capacity = cap.next_power_of_two();
    assert!(capacity != 0);
    let layout = Layout::array::<T>(capacity).expect("too big capacity requested");

    let buffer = Global.allocate(layout)
        .expect("failed to allocate queue buffer").cast::<T>();

    let fifo = InnerFifo {
        buffer,
        capacity,
        head:  CacheLine(AtomicUsize::new(0)),
        tail:  CacheLine(AtomicUsize::new(0)),
        state: CacheLine(AtomicUsize::new(2)),
    };

    let fifo_ptr = Global.allocate(Layout::new::<InnerFifo<T>>())
        .expect("failed to allocate").cast::<InnerFifo<T>>();

    // SAFETY: We have allocated a well aligned heap space for InnerFifo to
    // store one.
    unsafe {
        fifo_ptr.write(fifo);
    }

    let prod = Producer {
        fifo: fifo_ptr,
        capacity,
        cached_consumer_idx: AtomicUsize::new(0),
    };

    let cons = Consumer {
        fifo: fifo_ptr,
        capacity,
        cached_producer_idx: AtomicUsize::new(0),
    };

    (prod, cons)
}

/// The consumer/receiver side of the FIFO queue.
pub struct Consumer<T> {
    // This ensures the not-Sync nature. Only one thread could consume, that
    // thread might change because this type is Send, but it will still be only
    // one thread.
    fifo:                NonNull<InnerFifo<T>>,

    // Both sides cache the capacity of the inner buffer.
    capacity:            usize,

    // Cached masked value of head count. The consumer will consume until the
    // masked tail count becomes this. Then it will read the updated value and
    // consume more if there are more elements to consume.
    //
    // This would work even if it was a `usize`, but `AtomicUsize` would not
    // require us to use `&mut self` in `pop()`.
    cached_producer_idx: AtomicUsize,
}

// SAFETY:
//
// It is Send only when T is Send because if another thread gets a hold of this
// type then it can be used to send a T to another thread.
unsafe impl<T> Send for Consumer<T> where T: Send {}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        if self.inner().state.fetch_sub(1, Ordering::Relaxed) == 1 {
            // Acquire everything before calling for drop of InnerFifo.
            fence(Ordering::Acquire);

            // The producer has already dropped. We need to clean up the
            // elements that are in the buffer.

            // Drop the InnerFifo which will drop all the un-popped elements
            // from the buffer and deallocate the buffer as well.

            // SAFETY: The producer has already dropped, since we are the last
            // to drop so it is our job to free the buffer.
            unsafe { self.fifo.drop_in_place() };

            // Free the InnerFifo memory.
            let fifo_layout = Layout::new::<InnerFifo<T>>();

            // SAFETY: we allocated the heap space for storing one InnerFifo, we
            // free it.
            unsafe { Global.deallocate(self.fifo.cast(), fifo_layout) };
        }
    }
}

impl<T> Consumer<T> {
    /// Pop an element from the queue. If the queue is currently empty but alive
    /// then it will block the thread. If the producer has already dropped and
    /// there is nothing in the queue then it returns `Err`. If there is
    /// something in the queue, even if the producer has already dropped, it
    /// returns the elements in the queue.
    pub fn pop(&self) -> Result<T, ()> {
        let cap_mask = self.capacity() - 1;

        let tail = self.inner().tail.load(Ordering::Relaxed);

        // Masked tail count.
        let idx_in_buffer = tail & cap_mask;

        // If we have caught up to the producer as per the cached producer
        // index.
        if idx_in_buffer == self.cached_producer_idx.load(Ordering::Relaxed) {
            loop {
                // Read the latest head count value.
                let current_head = self.inner().head.load(Ordering::Relaxed);

                // If latest masked head count is still the old then spin for a
                // while before trying again. If not then update the cached
                // value and start consuming the new elements.
                if idx_in_buffer == current_head & cap_mask {
                    spinloop!(512);
                } else {
                    // The head has moved forward the cached value.
                    self.cached_producer_idx.store(current_head & cap_mask,
                        Ordering::Relaxed);
                    break;
                }

                // We have already caught up with the producer, and spinned for
                // a while to give the producer a chance to push more elements.
                if self.queue_is_closed() {
                    // Number of elements pushed by producer.
                    let head = self.inner().head.load(Ordering::Relaxed);

                    // Number of elements popped by the consumer.
                    let tail = self.inner().tail.load(Ordering::Relaxed);

                    // The queue is already closed, if it is empty as well then
                    // return Err
                    if head == tail {
                        return Err(());
                    }
                }
            }
        }
        // The producer has pushed something, synchronize with the release of
        // that.
        fence(Ordering::Acquire);

        // Read the value from buffer.

        // SAFETY: Add is safe because the add amount is masked to stay in the
        // allocation only. The read is safe because we have ensured the
        // producer has written something there and have properly synchronized
        // with its write.
        let elem = unsafe {
            let ptr = self.inner().buffer.add(idx_in_buffer);
            ptr.read()
        };

        // The read of the element in the lines above must happen before the
        // producer retries to use the idx_in_buffer to write again. This would
        // not have been necessary in a normal scenario, but since we are in a
        // ring buffer and the indices get reused so this synchronization is
        // necessary.
        self.inner().tail.fetch_add(1, Ordering::Release);

        Ok(elem)
    }

    /// Checks whether the other side is alive. Even if the queue is not open,
    /// there might still be un-popped elements in the queue.
    pub fn queue_is_open(&self) -> bool {
        self.inner().state.load(Ordering::Relaxed) == 2
    }

    /// Checks whether the queue is closed. Even if the queue is closed, there
    /// might still be un-popped elements in the queue.
    pub fn queue_is_closed(&self) -> bool {
        !self.queue_is_open()
    }

    /// The capacity of the inner buffer used by the queue. It is always a
    /// power-of-2 and might be greater than the capacity supplied to
    /// `fifo<T>()` function while creating the queue.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get a shared reference to the InnerFifo structure.
    fn inner(&self) -> &InnerFifo<T> {
        // SAFETY: The fifo pointer is safe to dereference. It was properly
        // initialized and has not been deallocated yet.
        unsafe { self.fifo.as_ref() }
    }
}

/// The producer/sender side of the FIFO queue.
pub struct Producer<T> {
    // This ensures the not-Sync nature. Only one thread could produce, that
    // thread might change because this type is Send, but it will still be only
    // one thread.
    fifo:                NonNull<InnerFifo<T>>,

    // Both sides cache the capacity of the inner buffer.
    capacity:            usize,

    // Cached masked value of tail count. The producer will push until the
    // masked value of the head count after pushing will be equal to this. If
    // that's the case then read the latest masked tail count and push more
    // elements if the tail has moved forward.
    //
    // This would work even if it was a `usize`, but `AtomicUsize` would not
    // require us to use `&mut self` in `push()`.
    cached_consumer_idx: AtomicUsize,
}

// SAFETY:
//
// It is Send only when T is Send because if another thread gets a hold of this
// type then it can be used to send a T to another thread.
unsafe impl<T> Send for Producer<T> where T: Send {}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        if self.inner().state.fetch_sub(1, Ordering::Relaxed) == 1 {
            // Acquire everything before calling for drop of InnerFifo.
            fence(Ordering::Acquire);

            // The consumer has already dropped. We need to clean up the
            // elements that are in the buffer.

            // Drop the InnerFifo which will drop all the un-popped elements
            // from the buffer and deallocate the buffer as well.

            // SAFETY: The consumer has already dropped, since we are the last
            // to drop so it is our job to free the buffer.
            unsafe { self.fifo.drop_in_place() };

            // Free the InnerFifo memory.
            let fifo_layout = Layout::new::<InnerFifo<T>>();

            // SAFETY: we allocated the heap space for storing one InnerFifo, we
            // free it.
            unsafe { Global.deallocate(self.fifo.cast(), fifo_layout) };
        }
    }
}

impl<T> Producer<T> {
    /// Push an element to the queue if it is still open. If the queue is not
    /// open then return the element back.
    pub fn push(&self, val: T) -> Result<(), T> {
        // Check whether the queue is open.
        if self.queue_is_closed() {
            return Err(val);
        }

        // The queue might close after checking but before pushing, so we may
        // needlessly push one element which will definitely not be popped.
        //
        // There might be more elements pushed when the queue was alive but are
        // still stuck in the buffer because the consumer did not popped them
        // before closing the queue.

        let cap_mask = self.capacity() - 1;

        let head = self.inner().head.load(Ordering::Relaxed);

        // Masked head index.
        let idx_in_buffer = head & cap_mask;

        // Masked head index after pushing.
        let head_after_push = (head + 1) & cap_mask;

        // If after pushing the masked head and tail counts become same then we
        // need to wait.
        if head_after_push == self.cached_consumer_idx.load(Ordering::Relaxed) {
            // Head must never catch up to tail, only tail should catch up to head.
            loop {
                // If the queue is closed then we skip pushing even if the
                // masked tail index has moved forward.
                if self.queue_is_closed() {
                    return Err(val);
                }

                // Latest tail count.
                let current_tail = self.inner().tail.load(Ordering::Relaxed);

                // If the latest tail count, on an alive queue, is still the
                // same as cached value then spin for a while, letting the
                // consumer to move the tail forward.
                if head_after_push == current_tail & cap_mask {
                    spinloop!(512);
                } else {
                    // If the tail moved from the cached value then update the
                    // cache and break out.

                    self.cached_consumer_idx.store(current_tail & cap_mask,
                        Ordering::Relaxed);
                    break;
                }
            }
        }
        // Acquire with the release increment of the tail count. Since we are
        // reusing the indices in a ring buffer so this synchronization is
        // necessary. We could overwrite new value to an index only after the
        // consumer has read from it.
        fence(Ordering::Acquire);

        // Safe to write now.

        // SAFETY: The pointer stays within allocation, the index is masked with
        // capacity. The write is safe because we have synchronized with the tail
        // and ensure that any free space we see is truly free to write and the
        // consumer has already read the values we wrote there last time.
        unsafe {
            let ptr = self.inner().buffer.add(idx_in_buffer);
            ptr.write(val);
        }

        // Release the memory operations so when the consumer finds an index
        // ready to read from, it really is safe to read from there.
        self.inner().head.fetch_add(1, Ordering::Release);

        Ok(())
    }

    /// Checks whether the other side is alive. Even if the queue is not open,
    /// there might still be un-popped elements in the queue.
    pub fn queue_is_open(&self) -> bool {
        self.inner().state.load(Ordering::Relaxed) == 2
    }

    /// Checks whether the queue is closed. Even if the queue is closed, there
    /// might still be un-popped elements in the queue.
    pub fn queue_is_closed(&self) -> bool {
        !self.queue_is_open()
    }

    /// The capacity of the inner buffer used by the queue. It is always a
    /// power-of-2 and might be greater than the capacity supplied to
    /// `fifo<T>()` function while creating the queue.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get a shared reference to the InnerFifo structure.
    fn inner(&self) -> &InnerFifo<T> {
        // SAFETY: The fifo pointer is safe to dereference. It was properly
        // initialized and has not been deallocated yet.
        unsafe { self.fifo.as_ref() }
    }
}
