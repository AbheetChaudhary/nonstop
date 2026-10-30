Learning about concurrent data structures programming and challenges by making some.

Currently available(Linux only):

- [x] `Arc`(atomic reference counting)
- [x] `SpinLock` (guarding exclusive access in a thread safe way)
- [x] `LazyLock` (thread safe lazy initialization primitive)
- [x] `Mutex` (guarding exclusive access, without spinning)
- [x] `spsc::fifo` (Lockfree single-producer, single-consumer FIFO queue)
- [x] `Condvar` (Condition variables)
- [x] `StackPool` (Treiber stack based MPSC pool)
- [x] `LeftRightPool` (A better `StackPool`, optimized for batched reads)
- [ ] Hazard Pointer (thread safe memory reclamation)
- [ ] Epoch based concurrent memory reclamation
- [ ] Concurrent linked lists and queues.

## Viewing Docs
If comfortable in reading Rust, then one could directly navigate the source
code. I have tried to keep it well commented.

To generate HTML docs, run:

```shell
cargo doc
```
Then open the HTML file in your browser.

## References
- [Rust Atomics and Locks by Mara Bos](https://mara.nl/atomics/)
- [Futexes Are Tricky - Ulrich Drepper](https://cis.temple.edu/~giorgio/cis307/readings/futex.pdf)
- [Erik Rigtorp's Blog](https://rigtorp.se/)
- Some CPPCON talks related to lockfree data structures.

## NOTE
This repository is only for learning purposes, for any high stakes program
please use other well established crates in the ecosystem.

## LLM Usage Disclosure

**For writing code or designing data structures:**

_None._

...better resource were used for this. See references above.

**For debugging:**

_Some usage._

...like using it to quickly find subtle off by one like errors.
