# `webhdfs` integration tests

Intergation tests are set up with Apache Bigtop provisioner vagrant (typically 1-node) or docker (typically 3-node) cluster.
The test process itself runs on the host. See comments in [`itt.sh`](itt.sh) and `itt.sh --help` for details.

DISCLAIMER: the integration tests were developed on a Windows 10 machine (using Docker Desktop and both Cygwin and WSL). While the best effort was made to ensure Linux and OS X compatibility, the test scripts have not been tested there (yet), and may contain bugs. Sholud you notice any bugs and incompatibilies, please create an issue in the webhdfs Github project.

## Preparation

1. Install prerequisites
* either docker or vagrant
* For docker, also install ruby. 
* For vagrant, you will need Virtualbox (other providers are untested). 
* Under Windows you will also need Cygwin or WSL. Cygwin is recommended if Docker is used with Bigtop provisioner 
(because of docker to windows console interop issues), while WSL is recommended for Vagrant.
2. Check out Bigtop. Bigtop Version 1.3 was used (`rel/1.3` tag). Bigtop 1.3 has Hadoop version 2.8.4.
3. Create a file named `itt-config.sh` in the webhdfs source root, and make it `chmod +x`. This is the test configuration script.
4. Set `BIGTOP_ROOT` var to the root path of the cloned Bigtop repo. It must be set in the test config script. 
5. Edit Bigtop provisioner configuration to make guest webhdfs ports 50070 and 50075 accessible from the host, and
guest folder `/test-data` mounted to the folder `test-data` under webhdfs source root. See Sharing ports and folders below for instructions.
6. Using Bigtop tools, bring the cluster up and make sure the shared folder is mounted and the ports are accessble from the host.
7. The test script uses Vagrant by default. To use docker, specify `PROVISIONER=docker` in the test config script.

NOTES:
1. There is a number of other settings that may be specified in `itt-config.sh`. See [`itt.sh`](itt.sh) for details.
2. If you use the number of VMs or containers other than default (1 for vagrant and 3 for docker), the exact number of containers must be 
specified in `N_C` setting in the test config script, otherwise the test might fail.

### Sharing ports and folders: Docker

Go to the `provisioner/docker` folder under Bigtop source root. Edit `docker-container.yaml` to add the following 
(edit the absolute volume part):

```yaml
   ports:
   - "50070"
   - "50075"
   volumes:
   - //c/path/to/test-data : /test-data
```

### Sharing ports and folders: Vagrant

1. Edit bigtop's Vagrantfile in the `provisioner/vagrant` folder under Bigtop source root and add the following in the VM configuration loop:

```ruby
      #=== Intergation test environment settings ========================================================
      if i == 1 then
        config.vm.network "forwarded_port", guest: 50070, guest_ip: "10.10.10.11", host: 51070
        bigtop.vm.synced_folder "C:/devel/src/webhdfs-rs/test-data", "/test-data"
      end
      config.vm.network "forwarded_port", guest: 50075, host: 50075 + i * 1000
      #=== END: Intergation test environment settings ===================================================
```

Sample diff:

```diff
--- Vagrantfile.orig    2019-07-22 17:02:05.929272200 +0300
+++ Vagrantfile 2019-09-14 14:47:58.212960600 +0300
@@ -118,6 +118,14 @@
       # two levels up is the bigtop "home" directory.
       # the current directory has puppet recipes which we need for provisioning.
       bigtop.vm.synced_folder "../../", "/bigtop-home"
+
+      #=== Intergation test environment settings ========================================================
+      if i == 1 then
+        config.vm.network "forwarded_port", guest: 50070, guest_ip: "10.10.10.11", host: 51070
+        bigtop.vm.synced_folder "C:/devel/src/webhdfs-rs/test-data", "/test-data"
+      end
+      config.vm.network "forwarded_port", guest: 50075, host: 50075 + i * 1000
+      #=== END: Intergation test environment settings ===================================================

       # We also add the bigtop-home output/ dir, so that locally built rpms will be available.
       puts "Adding output/ repo ? #{enable_local_repo}"
```

## How the test works

### Read test
The read test consists of a sequence of reads and seeks against a testfile, set up by `READSCRIPT` setting.
The social networking graph data file `soc-pokec-relationships.txt`, available on SNAP, is used as a testflle (any file, large enough, 
may be used; recommended size is 200-400M).
During preparation, SHA-512 checksums are pre-calculated for each of `r:` chunks set up by `READSCRIPT` (chunks are extracted by dd utility).
The program under test is expected to execute the `READSCRIPT` below (actually, `READSCRIPTOUT`) by doing seeks and reads as requested.
Upon each read, the program reads from the testfile (HDFS) then writes the content read to a file specified by 3rd part of `READSCRIPTOUT` 
item. Finally, checksums for newly written chunks are validated against pre-calculated checksums.

### hooks
1. place all local settings in `./itt-config.sh` and make it `chmod a+x`
2. if using a test file other than standard, or using the standard one but pre-downloaded, or using other source, 
    place the test file materialzation command(s) in `./test-data./create-source-script` and make it `chmod a+x`. 
    Note that the script is launched in `./test-data`.
    Note that if the test file is already in `./test-data`, it is used as-is and left intact 
    (if downloaded, the test file is deleted from `./test-data` after preparation).


## Running the test

1. Bring the Bigtop cluster up
2. Run `itt.sh --run`
3. Look for the following terminating line in the output from `itt.sh --run`:

```
==================== TEST SUCCESSFUL ====================
```

## Possible problems and workarounds

1. Note that both Bigtop provisioner variants seemingly do not support VM/container reboot. If a cluster's VM is powered down or a container terminated, the cluster shall be entirely deleted and re-created from scratch. However, a VM could be suspended.

2. 'IOException: Failed to replace a bad datanode on the existing pipeline due to no more good datanodes being available to try' errors during write test: Add the following to `/etc/hadoop/conf/hdfs-site.xml` and restart the datanode services(s):

 ```xml
 <property>
  <name>dfs.client.block.write.replace-datanode-on-failure.policy</name>
  <value>NEVER</value>
</property>
```
3. If port forwarding does not work (Vagrant): this might be consequences of VM's reboot. The following are possible temporary workarounds. 

a) a possible cause is iptables in the VM. Check if it is active; if yes, execute

```
sudo iptables -A IN_public_allow -p tcp --dport 50070 -j ACCEPT
sudo iptables -A IN_public_allow -p tcp --dport 50075 -j ACCEPT
```

b) (NN only) the other possible cause is NN binding address mismatch. Check what IP is the NN bound to and specify that address in port
forwarding settings.

c) Look at `/etc/hosts`, should look as the following. In particular, remove 10.0.*.* entries from the NAT adapter or give this IP a name other than hostname.

```
::1         localhost localhost.localdomain localhost6 localhost6.localdomain6

## vagrant-hostmanager-start
10.10.10.11     bigtop1.vagrant
10.10.10.11     bigtop1.vagrant bigtop1
## vagrant-hostmanager-end
```
