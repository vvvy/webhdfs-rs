# webhdfs

Hadoop webhdfs client library for Rust.

* Built on Tokio and Hyper. 
* Provides both synchronous and asynchronous APIs.

See [`tests/it.rs`](tests/it.rs) for usage examples.

NOTE: This is a work in progress. Currently some basic functionality is implemented.

NOTE: This is Alpha phase software, and thus is unstable. APIs will change towards version 1.0.

## Building and tesing

```
cargo test --lib -- --nocapture
```

## Integration tests

See [Integration tests](INTEGRATION-TESTS.md)
