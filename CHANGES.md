# webhdfs

### Version 0.3.3

* `tokio` 1.2
* `hyper` 0.14
* `hyper-tls` 0.5
* `bytes` 1.0

### Version 0.3.2

* Sync `read` now returns 0 on EOF. PR #8

### Version 0.3.1

* Fixed: http dir request don't return whole string. #5

### Version 0.3.0

* Namenode failover handling
* Configuration for HTTPS
* Remote intergration test method

### Version 0.2.2

* Migration to async await
* Bump versions of `hyper`, `hyper-tls`, `tokio`, `futures`, `http`, `bytes`

### Version 0.2.1

* Bugfix: wrong expected response type for `MKDIRS`, `DELETE`, `RENAME`
* `MKDIRS` added to IT
* NAT mapings can be specified in `webhdfs.toml`
* Misc doc fixes and enhancements

### Version 0.2.0

* Added write
* Added basic FS mutations: `MKDIRS`, `DELETE`, `RENAME`, `CONCAT`, `SYMLINK`

### Version 0.1.0

Initial release. Only reads from HDFS.



