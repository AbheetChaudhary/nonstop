Learning about concurrent data structures programming and challenges by making some.

Currently available(Linux only):

- [x] `Arc`(atomic reference counting)
- [x] `SpinLock` (guarding exclusive access in a thread safe way)
- [x] `LazyLock` (thread safe lazy initialization primitive)
- [ ] `Mutex` (guarding exclusive access, without spinning)
- [ ] Hazard Pointer (thread safe memory reclamation)
- [ ] Epoch based concurrent memory reclamation
- [ ] Concurrent linked lists and queues.

## Viewing Docs
If comfortable, then one could directly navigate the source code. I have tried to
keep it well commented.

To generate HTML docs, run:

```shell
cargo doc
```
Then open the HTML file in your browser.

## NOTE
This repository is only for learning purposes.
