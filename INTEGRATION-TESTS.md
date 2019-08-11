# `webhdfs` integration tests

Intergation tests are set up with Apache Bigtop provisioner 3-node cluster. See comments in [`itt.sh`](itt.sh) and `itt.sh --help` for details.

DISCLAIMER: the integration tests were developed on a Windows 10 machine (using Docker Desktop and both Cygwin and WSL). While the best effort was made to ensure Linux and OS X compatibility, the test scripts have not been tested there (yet), and may contain bugs. Sholud you notice any bugs and incompatibilies, please create an issue in the webhdfs Github project.

Steps:

    1. Launch the Bigtop cluster (ensure the supplemental configuration is applied)
    2. Run `itt.sh --run`

Sample output from `itt.sh --run`:

```
vvv@VVV-ZEN /cygdrive/c/devel/src/webhdfs-rs
$ ./itt.sh --run
262144+0 records in
262144+0 records out
134217728 bytes (134 MB, 128 MiB) copied, 3.79161 s, 35.4 MB/s
2048+0 records in
2048+0 records out
1048576 bytes (1.0 MB, 1.0 MiB) copied, 0.0435213 s, 24.1 MB/s
262144+0 records in
262144+0 records out
134217728 bytes (134 MB, 128 MiB) copied, 3.58243 s, 37.5 MB/s
82801+1 records in
82801+1 records out
42394150 bytes (42 MB, 40 MiB) copied, 1.12798 s, 37.6 MB/s
331204+1 records in
331204+1 records out
169576604 bytes (170 MB, 162 MiB) copied, 4.60579 s, 36.8 MB/s
165602+1 records in
165602+1 records out
84788301 bytes (85 MB, 81 MiB) copied, 2.27223 s, 37.3 MB/s
248403+1 records in
248403+1 records out
127182453 bytes (127 MB, 121 MiB) copied, 3.48071 s, 36.5 MB/s
WARNING: The DOCKER_IMAGE variable is not set. Defaulting to a blank string.
WARNING: The MEM_LIMIT variable is not set. Defaulting to a blank string.
WARNING: The DOCKER_IMAGE variable is not set. Defaulting to a blank string.
WARNING: The MEM_LIMIT variable is not set. Defaulting to a blank string.
WARNING: The DOCKER_IMAGE variable is not set. Defaulting to a blank string.
WARNING: The MEM_LIMIT variable is not set. Defaulting to a blank string.
WARNING: The DOCKER_IMAGE variable is not set. Defaulting to a blank string.
WARNING: The MEM_LIMIT variable is not set. Defaulting to a blank string.
WARNING: The DOCKER_IMAGE variable is not set. Defaulting to a blank string.
WARNING: The MEM_LIMIT variable is not set. Defaulting to a blank string.
WARNING: The DOCKER_IMAGE variable is not set. Defaulting to a blank string.
WARNING: The MEM_LIMIT variable is not set. Defaulting to a blank string.
WARNING: The DOCKER_IMAGE variable is not set. Defaulting to a blank string.
WARNING: The MEM_LIMIT variable is not set. Defaulting to a blank string.
WARNING: The DOCKER_IMAGE variable is not set. Defaulting to a blank string.
WARNING: The MEM_LIMIT variable is not set. Defaulting to a blank string.
WARNING: The DOCKER_IMAGE variable is not set. Defaulting to a blank string.
WARNING: The MEM_LIMIT variable is not set. Defaulting to a blank string.
WARNING: The DOCKER_IMAGE variable is not set. Defaulting to a blank string.
WARNING: The MEM_LIMIT variable is not set. Defaulting to a blank string.
WARNING: The DOCKER_IMAGE variable is not set. Defaulting to a blank string.
WARNING: The MEM_LIMIT variable is not set. Defaulting to a blank string.
    Finished dev [unoptimized + debuginfo] target(s) in 1.74s
     Running target\debug\deps\it-45ec8ddd9e6a4768.exe

running 1 test
Integration test -- start

entrypoint='localhost:32779'
source='/user/root/test-data/soc-pokec-relationships.txt'
readscript='r:128m:./test-data/seg-0 s:0 r:1m:./test-data/seg-1 r:128m:./test-data/seg-2'
target='/user/root/test-data/soc-pokec-relationships.txt.w'
writescript='./test-data/wseg-0 ./test-data/wseg-1 ./test-data/wseg-2 ./test-data/wseg-3'
natmap={"6bb4e3f67eb1.bigtop.apache.org:50075": "localhost:32776", "4706500458f4.bigtop.apache.org:50075": "localhost:32778", "4706500458f4.bigtop.apache.org:50070": "localhost:32779", "80410e5775e2.bigtop.apache.org:50075": "localhost:32774"}
Test dir and stat
Dir: Ok(ListStatusResponse { file_statuses: FileStatuses { file_status: [FileStatus { access_time: 1565447247120, block_size: 134217728, group: "hadoop", length: 423941508, modification_time: 1565447266279, owner: "root", path_suffix: "soc-pokec-relationships.txt", permission: "644", replication: 3, type_: "FILE" }] } })
Stat: Ok(FileStatusResponse { file_status: FileStatus { access_time: 1565447247120, block_size: 134217728, group: "hadoop", length: 423941508, modification_time: 1565447266279, owner: "root", path_suffix: "", permission: "644", replication: 3, type_: "FILE" } })
Read test
alloc_mb(len=134217728)...done
Read(134217728, "./test-data/seg-0")...
test webhdfs_test ... test webhdfs_test has been running for over 60 seconds
Seek(0)...
Read(1048576, "./test-data/seg-1")...
Read(134217728, "./test-data/seg-2")...
Write test
./test-data/wseg-0
./test-data/wseg-1
./test-data/wseg-2
./test-data/wseg-3
test webhdfs_test ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

./test-data/seg-0: OK
./test-data/seg-1: OK
./test-data/seg-2: OK
WARNING: The DOCKER_IMAGE variable is not set. Defaulting to a blank string.
WARNING: The MEM_LIMIT variable is not set. Defaulting to a blank string.
Write checksums Ok
WARNING: The DOCKER_IMAGE variable is not set. Defaulting to a blank string.
WARNING: The MEM_LIMIT variable is not set. Defaulting to a blank string.
Deleted /user/root/test-data/soc-pokec-relationships.txt.w

```

# Useful tips and tricks

 1. If the provisioned containers are stopped and then started, Namenode and Resource Manager services are unable to bind to
    some ports (50070 etc.) and terminate abnormally upon startup. This is likely due to ip addresses of the nodes having changed
    upon container restart. You need to regenerate hosts files inside containers. The simplest way to achieve this is to slightly
    modify and run bigtop's `docker-hadoop.sh --provision`: find `-p|--provision)` clause in the mode selector `case` and add 
    `generate-hosts` just before `provision` line, so it looks somewhat like:

```bash
    -p|--provision)
        generate-hosts
        provision
        shift;;
```

    Diff:

```diff
*** docker-hadoop.sh    2019-07-24 14:26:41.819170800 +0300
--- docker-hadoop2.sh   2019-08-10 16:22:13.249734800 +0300
***************
*** 243,248 ****
--- 243,249 ----
          list
          shift;;
      -p|--provision)
+         generate-hosts
          provision
          shift;;
      -s|--smoke-tests)
```

