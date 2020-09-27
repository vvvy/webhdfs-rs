# webhdfs

Hadoop webhdfs client library for Rust.

[![Crates.io][crates-badge]][crates-url]
[![Build Status][travis-badge]][travis-url]

[crates-badge]: https://img.shields.io/crates/v/webhdfs.svg
[crates-url]: https://crates.io/crates/webhdfs
[travis-badge]: https://travis-ci.org/vvvy/webhdfs-rs.svg?branch=master
[travis-url]: https://travis-ci.org/vvvy/webhdfs-rs

[Documentation](https://docs.rs/webhdfs)

* Built on Tokio and Hyper. 
* Provides both synchronous and asynchronous APIs.

See  [`src/bin/webhdfs.rs`](src/bin/webhdfs.rs) or [`tests/it.rs`](tests/it.rs) for usage examples.

NOTE: APIs are not yet stabilized and may change towards version 1.0.

TODO list (2nd checkbox is IT coverage)

- [X] Async read and write (`[X]`)
- [X] Sync read and write (`[X]`)
- [X] Stat and dir (`[X]`)
- [X] Basic filesystem mutations (concat, mkdirs, rename, delete, symlink)
- [X] File-based configuration
- [ ] Other file and directory operations (permissions, owner, ACL, times, checksum)
- [ ] XAttr operations
- [ ] Snapshot operations
- [X] Basic delegation token support
- [ ] Full delegation token support
- [ ] webhdfs tool
- [X] failover support (`[X]`)

## Building and testing

```
cargo test --lib -- --nocapture
```

## Integration tests

See [Integration tests](INTEGRATION-TESTS.md)

## Changelog

See [CHANGES.md](CHANGES.md)
