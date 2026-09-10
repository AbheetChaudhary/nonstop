//! MPSC LIFO like pool with batched reads.
//!
//! This module provides slightly better version of the stack pool by seperating
//! the pools from which the consumer removes and the producers insert.
//!
//! All the producers work with one pool while the consumer consumes from a
//! different pool. Once the consumer pool gets empty, the consumer then
//! switches the pools. Over time all the producers switch to the other pool and
//! the consumer starts removing elements from the new pool.
//!
//! This method heavily favors the consumer as it gets uncontested access to its
//! pool. There might be some slow producers still trying to insert in the
//! consumers pool. They either succeed or quickly realize that the pools have
//! switched and try inserting to the other pool.

use crate::CacheLine;

extern crate alloc;

use alloc::alloc::Global;
use core::{
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering, fence},
    ptr::NonNull,
    alloc::{Layout, Allocator},
};

struct Node<T> {
    elem: T,
    next: *mut Node<T>,
}

impl<T> Node<T> {
    fn new_ptr(elem: T) -> *mut Node<T> {
        let node = Node {
            elem,
            next: core::ptr::null_mut(),
        };

        let layout = Layout::new::<Node<T>>();
        let ptr = Global.allocate(layout)
            .expect("failed to allocate memory for Node<T>").cast::<Node<T>>().as_ptr();

        unsafe {
            ptr.write(node)
        };

        ptr
    }

    unsafe fn delete_ptr(ptr: *mut Node<T>) {
        let non_null = NonNull::new(ptr).unwrap();
        let layout = Layout::new::<Node<T>>();
        unsafe { Global.deallocate(non_null.cast(), layout); }
    }
}

#[repr(C)]
struct LeftRightPool<T> {
    pc_count: AtomicUsize,
    stacks: [CacheLine<AtomicPtr<Node<T>>>; 2],
    writer_stack: CacheLine<AtomicUsize>,
    reader_stack: CacheLine<AtomicUsize>,
}

impl<T> Drop for LeftRightPool<T> {
    fn drop(&mut self) {
        assert!(self.pc_count.load(Ordering::Relaxed) == 0, "this should be \
            dropped only after the last of the producers or the consumer has \
            realized that it is the last.");

        // Empty all the stacks.
        for i in 0..2 {
            // Relaxed ordering is fine here because whoever called the drop of
            // this would have already synchronized with all the previous
            // producers.
            let mut top = self.stacks[i].load(Ordering::Relaxed);

            while !top.is_null() {
                let Node { elem, next } = unsafe { top.read() };
                drop(elem);

                // Free the Node memory on the stack.
                unsafe { Node::delete_ptr(top); }

                top = next;
            }

        }
    }
}

impl<T> LeftRightPool<T> {
    fn relaxed_c_count(&self) -> usize {
        // The consumer is the remainder.
        self.pc_count.load(Ordering::Relaxed) % 2
    }

    fn relaxed_p_count(&self) -> usize {
        let pc_count = self.pc_count.load(Ordering::Relaxed);

        if pc_count % 2 == 0 {
            pc_count / 2
        } else {
            // Remove the consumer.
            (pc_count - 1) / 2
        }
    }
}

pub fn new_left_right_pool<T>() -> (Producer<T>, Consumer<T>) {
    let layout = Layout::new::<LeftRightPool<T>>();
    let stack_ptr = Global.allocate_zeroed(layout)
        .expect("failed to allocate").cast::<LeftRightPool<T>>();

    let stack = LeftRightPool {
        pc_count: AtomicUsize::new(2 + 1), // one producer one consumer
        stacks: [
            CacheLine(AtomicPtr::new(core::ptr::null_mut())),
            CacheLine(AtomicPtr::new(core::ptr::null_mut())),
        ],
        writer_stack: CacheLine(AtomicUsize::new(0)),
        reader_stack: CacheLine(AtomicUsize::new(1)),
    };

    unsafe {
        stack_ptr.write(stack);
    }

    (Producer { pool: stack_ptr }, Consumer { pool: stack_ptr })
}

pub struct Producer<T> {
    pool: NonNull<LeftRightPool<T>>,
}

unsafe impl<T: Send> Send for Producer<T> {}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        self.inner_pool().pc_count.fetch_add(2, Ordering::Relaxed);

        Self {
            pool: self.pool,
        }
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        if self.inner_pool().pc_count.fetch_sub(2, Ordering::Release) == 2 {
            fence(Ordering::Acquire);

            let pool = unsafe { self.pool.read() };

            // Drop the leftrightpool and all the nodes currently stuck inside
            // it.
            drop(pool);
            
            // Free the heap memory that contains the LeftRightPool itself.
            unsafe {
                Global.deallocate(self.pool.cast(),
                    Layout::new::<LeftRightPool<T>>())
            }
        }
    }
}

impl<T> Producer<T> {
    pub fn insert(&self, elem: T) -> Result<(), T> {
        // If the consumer is already gone then return Err.
        if self.inner_pool().relaxed_c_count() == 0 {
            return Err(elem);
        }

        // Although we checked that the consumer is alive, but before we insert
        // the element the consumer might drop, in that case we will be
        // needlessly inserting an extra element. This could, in the worst case
        // accumulate up to 2 * #producers stale elements in the channel.

        let node_ptr = Node::new_ptr(elem);

        // Index of the writer stack.
        let mut writer_stack_idx = self.inner_pool()
            .writer_stack.load(Ordering::Acquire);

        // Reference to the writer stack.
        let mut writer_stack = &self.inner_pool()
            .stacks[writer_stack_idx];

        let mut top = writer_stack.load(Ordering::Acquire);

        loop {
            // If writer stack changed, then update the stack index, stack
            // and the top.
            if writer_stack_idx != self.inner_pool().writer_stack
                .load(Ordering::Acquire) {

                writer_stack_idx ^= 1;
                writer_stack = &self.inner_pool().stacks[writer_stack_idx];
                top = writer_stack.load(Ordering::Acquire);
            }

            unsafe {
                (*node_ptr).next = top;
            }

            match writer_stack.compare_exchange(top, node_ptr, 
                Ordering::Release, Ordering::Relaxed) {
                Ok(_) => break,

                Err(new_top) => {
                    top = new_top;
                }
            }
        }

        Ok(())
    }

    fn inner_pool(&self) -> &LeftRightPool<T> {
        unsafe { self.pool.as_ref() }
    }
}

pub struct Consumer<T> {
    pool: NonNull<LeftRightPool<T>>,
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        if self.inner_pool().pc_count.fetch_sub(1, Ordering::Release) == 1 {
            fence(Ordering::Acquire);

            let pool = unsafe { self.pool.read() };

            // Drop the leftrightpool and all the nodes currently stuck inside
            // it.
            drop(pool);
            
            // Free the heap memory that contains the LeftRightPool itself.
            unsafe {
                Global.deallocate(self.pool.cast(),
                    Layout::new::<LeftRightPool<T>>())
            }
        }
    }
}

impl<T> Consumer<T> {
    fn reader_stack_idx(&self) -> usize {
        self.inner_pool().reader_stack.load(Ordering::Relaxed)
    }

    /// Tries to remove the top of the current reader stack.
    ///
    /// If there is nothing then it returns `None`.
    fn try_remove(&self) -> Option<T> {
        let reader_stack = &self.inner_pool().stacks[self.reader_stack_idx()];

        let reader_top = reader_stack.load(Ordering::Acquire);
        if reader_top == core::ptr::null_mut() {
            return None;
        }

        // We don't need this loop here.
        //
        // The original idea was that it might be possible that some of the
        // producers might be using the old writer index, i.e. the current
        // reader index. They might try to insert at the top of the reader
        // stack.
        //
        // But the writers are cautious and use compare exchange to insert. The
        // consumer could just remove the top of the stack and overwrite the top
        // with the next pointer. Even if there are any writers working with the
        // reader stack, their CAS will fail and in their next iteration they
        // will update the writer stack index.

        /*
        loop {
            // This will be the next top of the reader stack after we remove the
            // current one.
            let next = unsafe {
                (&(*reader_top)).next.load(Ordering::Relaxed)
            };

            match reader_stack.compare_exchange(reader_top, next, Ordering::Relaxed,
                Ordering::Acquire) {
                Ok(_) => {
                    // Successfully removed the reader_top, it is safe to read
                    // and destructure from it.

                    let Node { elem, _ } = unsafe { reader_top.read() };
                    unsafe { Node::delete_ptr(reader_top) };
                    break Some(elem);
                }

                Err(new_top) => {
                    reader_top = new_top;
                }
            }
        }
        */

        let Node { elem, next } = unsafe { reader_top.read() };

        // Just unconditionally/fearlessly remove the top of the stack. Read the
        // explanation above the loop.
        reader_stack.store(next, Ordering::Release);

        unsafe { Node::delete_ptr(reader_top) };

        Some(elem)
    }

    /// Tries to remove an element from the `LeftRightPool`.
    ///
    /// If there are any elements in the pool then this function returns `Ok`,
    /// otherwise if the pool is empty and all the producers have dropped then
    /// it returns `Err`.
    ///
    /// If the pool is empty and the produers are neither dropping or inserting
    /// anything then this function blocks and stays blocked.
    pub fn remove(&self) -> Result<T, ()> {
        loop {
            // Fast path: try to remove one element, return that if successful.
            if let Some(elem) = self.try_remove() {
                break Ok(elem);
            }

            // If reader stack is empty then swap the stacks and try again in
            // next iteration.
            self.swap_stacks();

            if let Some(elem) = self.try_remove() {
                break Ok(elem);
            } else if self.inner_pool().relaxed_p_count() == 0 {
                // Since all the producers are gone, it is a real possiblity
                // that the channel is empty and nothing is coming.

                // Retry both stacks one last time and if they are both empty
                // then return Err.

                if let Some(elem) = self.try_remove() {
                    break Ok(elem);
                }

                self.swap_stacks();

                if let Some(elem) = self.try_remove() {
                    break Ok(elem);
                }

                break Err(());
            }
        }
    }

    /// Checks whether the reader stack is empty.
    fn is_reader_empty(&self) -> bool {
        let reader_stack = &self.inner_pool().stacks[self.reader_stack_idx()];

        // Get the top of the reader stack.
        let r_top = reader_stack.load(Ordering::Relaxed);

        r_top == core::ptr::null_mut()
    }

    /// Swap the reader and writer stacks.
    fn swap_stacks(&self) {
        // Make sure reader stack is empty.
        assert!(self.is_reader_empty());

        // Switch the writer stack to the current reader stack.
        let curr_reader_idx = self.reader_stack_idx();
        self.inner_pool().writer_stack.store(curr_reader_idx, Ordering::Release);

        // Update the reader stack.
        self.inner_pool().reader_stack.store(1 ^ curr_reader_idx, Ordering::Relaxed);

        // Spin for a while, hopefully some of the writers which were using the
        // old writer stack, i.e. the current reader stack might switch by the
        // end.
        let mut idx = 0;
        while idx < 32 {
            idx += 1;
            core::hint::spin_loop();
            core::hint::black_box(&mut idx);
        }
    }

    fn inner_pool(&self) -> &LeftRightPool<T> {
        unsafe { self.pool.as_ref() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::thread;

    #[test]
    #[ignore]
    fn it_works() {
        let (tx, rx) = new_left_right_pool::<usize>();

        let mut sum = 0;

        thread::scope(|s| {
            for _ in 0..16 {
                let tx = tx.clone();

                s.spawn(move || {
                    for i in 1..=100 {
                        tx.insert(i);
                    }
                });
            }

            drop(tx);


            while let Ok(n) = rx.remove() {
                sum += n;
            }

            drop(rx);
        });

        assert!(sum == 16 * 5050);
    }
}
