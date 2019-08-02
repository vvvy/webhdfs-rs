# webhdfs

Hadoop webhdfs client library for Rust.

* Built on Tokio and Hyper. 
* Provides both synchronous and asynchronous APIs.

See `tests/it.rs` for usage examples.

NOTE: This is a work in progress. Currently some basic functionality is implemented.
NOTE: This is Alpha phase software, and thus is unstable. APIs will change towards version 1.0.

## Building and tesing

```
cargo test --lib -- --nocapture
```

## Integration tests

Intergation tests are set up with Apache Bigtop provisioner 3-node cluster. See comments in `itt.sh` for details.

DISCLAIMER: the integration tests were developed on a Windows 10 machine (using Docker Desktop and both Cygwin and WSL). While best
effort was applied to ensure Linux and OS X compatibility, it hasn't been tested there (yet). Sholud you notice any bugs and incompatibilies, please create an issue under the Github project.

Steps:
    1. Launch the Bigtop cluster (ensure the supplemental configuration is applied)
    2. Run `itt.sh --prepare`
    3. Run `cargo test --test it -- --nocapture`
    4. Run `itt.sh --verify`

Sample output from `cargo test --test it -- --nocapture`:

```
entrypoint='localhost:32775'
source='/user/root/test-data/soc-pokec-relationships.txt'
program='r:128m:./test-data/seg-0 s:0 r:1m:./test-data/seg-1 r:128m:./test-data/seg-2'
natmap={"792bc6221497.bigtop.apache.org:50070": "localhost:32775", "792bc6221497.bigtop.apache.org:50075": "localhost:32774", "62bd375b9b0f.bigtop.apache.org:50075": "localhost:32776", "865a9775aefb.bigtop.apache.org:50075": "localhost:32778"}
Dir: Ok(ListStatusResponse { file_statuses: FileStatuses { file_status: [FileStatus { access_time: 1564727295615, block_size: 134217728, group: "hadoop", length: 423941508, modification_time: 1564668155321, owner: "root", path_suffix: "soc-pokec-relationships.txt", permission: "644", replication: 3, type_: "FILE" }] } })
Stat: Ok(FileStatusResponse { file_status: FileStatus { access_time: 1564727295615, block_size: 134217728, group: "hadoop", length: 423941508, modification_time: 1564668155321, owner: "root", path_suffix: "", permission: "644", replication: 3, type_: "FILE" } })
Read(134217728, "./test-data/seg-0")...
Seek(0)...
Read(1048576, "./test-data/seg-1")...
Read(134217728, "./test-data/seg-2")...
test test_read ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

Sample output from `./itt.sh --validate`

```
./test-data/seg-0: OK
./test-data/seg-1: OK
./test-data/seg-2: OK
```