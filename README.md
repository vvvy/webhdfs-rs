# webhdfs

[![Build Status](https://travis-ci.org/vvvy/webhdfs-rs.svg?branch=master)](https://travis-ci.org/vvvy/webhdfs-rs)

Hadoop webhdfs client library for Rust.

* Built on Tokio and Hyper. 
* Provides both synchronous and asynchronous APIs.

See  [`src/bin/webhdfs.rs`](src/bin/webhdfs.rs) or [`tests/it.rs`](tests/it.rs) for usage examples.

NOTE: This is a work in progress. 

NOTE: This is an Alpha phase software. API could change towards version 1.0.

TODO list (2nd checkbox is IT coverage)

- [X] `[X]` Async read and write
- [X] `[X]` Sync read and write
- [X] `[X]` Stat and dir
- [X] `[ ]` Basic filesystem mutations (concat, mkdirs, rename, delete, symlink)
- [X] `[ ]` File-based configuration
- [ ] `[ ]` Other file and directory operations (permissions, owner, ACL, times, checksum)
- [ ] `[ ]` XAttr operations
- [ ] `[ ]` Snapshot operations
- [X] `[ ]` Basic delegation token support
- [ ] `[ ]` Full delegation token support
- [ ] `[ ]` Hadoop 2.7+
- [ ] `[ ]` webhdfs tool

## Building and testing

```
cargo test --lib -- --nocapture
```

## Integration tests

See [Integration tests](INTEGRATION-TESTS.md)
