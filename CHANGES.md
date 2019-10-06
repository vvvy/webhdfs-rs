# webhdfs

### Version 0.1.0

Initial release. Only reads from HDFS.

### Version 0.2.0

* Added write
* Added basic FS mutations: `MKDIRS`, `DELETE`, `RENAME`, `CONCAT`, `SYMLINK`

### Version 0.2.1

* Bugfix: wrong expected response type for `MKDIRS`, `DELETE`, `RENAME`
* `MKDIRS` added to IT
* NAT mapings can be specified in `webhdfs.toml`
* Misc doc fixes and enhancements
