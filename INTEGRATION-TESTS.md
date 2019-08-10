# `webhdfs` integration tests

Intergation tests are set up with Apache Bigtop provisioner 3-node cluster. See comments in [`itt.sh`](itt.sh) and `itt.sh --help` for details.

DISCLAIMER: the integration tests were developed on a Windows 10 machine (using Docker Desktop and both Cygwin and WSL). While the best effort was made to ensure Linux and OS X compatibility, the test scripts have not been tested there (yet), and may contain bugs. Sholud you notice any bugs and incompatibilies, please create an issue in the webhdfs Github project.

Steps:

    1. Launch the Bigtop cluster (ensure the supplemental configuration is applied)
    2. Run `itt.sh --run`

Sample output from `itt.sh --run`:

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

