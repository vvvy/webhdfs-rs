#!/bin/bash
# webhdfs integration test tool
# See INTEGRATION-TESTS.md and 'itt.sh --help' for usage details

#Extension points for adding new tests
#Preparation
#prepare
#prepare-hdfs
#clean-test-output (run before test and after successful validation)

#If you need to alter the following settings, the best place to do it is './itt-config.sh'
TESTFILE=soc-pokec-relationships.txt
#A sequence of instructions to test read
READSCRIPT=(r:128m s:0 r:1m r:128m)
#A sequence of instructions to test write
WRITE_SCRIPT=(0 10% 50% 70%)
#Provisioner (vagrant or docker)
PROVISIONER=vagrant
#local directory where test data is maintained
TESTDATA_DIR=./test-data
#container directory TESTDATA_DIR maps to
C_TESTDATA_DIR=/test-data
#HDFS directory where the test file gets copied and all the test data reside.
#Default is /user/vagrant/test-data for vagrant and /user/root/test-data for docker.
#The default is set after itt-config.sh
#HDFS_DIR=...
#local bigtop root
BIGTOP_ROOT=/usr/local/src/bigtop
#webhdfs NN port inside containers
C_WEBHDFS_NN_PORT=50070
#webhdfs DN port inside containers
C_WEBHDFS_DN_PORT=50075
#number of bigtop containers or VMs. By default, this is 3 for docker and 1 for vagrant. 
#The default is set after itt-config.sh.
#N_C=...

[ `uname -o` == "Cygwin" ] && { IS_CYGWIN=true ; DRIVES_CONTAINER=/cygdrive ; }
grep -q Microsoft /proc/version && { IS_WSL=true ; DRIVES_CONTAINER=/mnt ; }

if [ -x ./itt-config.sh ]; then . ./itt-config.sh ; fi

[ -z "$N_C" ] && { [ $PROVISIONER == vagrant ] && N_C=1 || N_C=3 ; }
[ $PROVISIONER == vagrant ] && HADOOP_USER=vagrant || HADOOP_USER=root
[ -z "$HDFS_DIR" ] && HDFS_DIR=/user/$HADOOP_USER/test-data
LOCALHOST=localhost
ENTRYPOINT=$TESTDATA_DIR/entrypoint
NATMAP=$TESTDATA_DIR/natmap
USERFILE=$TESTDATA_DIR/user
#read-write test
TESTFILE_W=$TESTFILE.w
SOURCE=$TESTDATA_DIR/$TESTFILE
SHASUMS=$TESTDATA_DIR/shasums
CHECKSUMFILE=$TESTDATA_DIR/hdfs-checksum
C_CHECKSUMFILE=$C_TESTDATA_DIR/hdfs-checksum
CHAL_CHECKSUMFILE=$TESTDATA_DIR/hdfs-checksum-chal
C_CHAL_CHECKSUMFILE=$C_TESTDATA_DIR/hdfs-checksum-chal
READSCRIPTFILE=$TESTDATA_DIR/readscript
WRITESCRIPTFILE=$TESTDATA_DIR/writescript
SIZEFILE=$TESTDATA_DIR/size
SOURCEPATHFILE=$TESTDATA_DIR/source
TARGETPATHFILE=$TESTDATA_DIR/target
SEGFILE_PREFIX=$TESTDATA_DIR/seg-
WSEGFILE_PREFIX=$TESTDATA_DIR/wseg-
#mkdir/rmdir test
DIR2MK=mkrmdirtest-dir-to-make
DIR2RM=mkrmdirtest-dir-to-remove
DIR2MKFILE=$TESTDATA_DIR/dir-to-make
DIR2RMFILE=$TESTDATA_DIR/dir-to-remove

#=======================================================================================================
#Test system tools

case $PROVISIONER in

vagrant)

vagcmd() {
    (cd $BIGTOP_ROOT/provisioner/vagrant && vagrant "$@")
}

c-up() {
    vagcmd up
}

c-dn() {
    vagcmd suspend
}

c-exec() {
    (cd $BIGTOP_ROOT/provisioner/vagrant && CNO=$1 && shift && vagrant ssh bigtop$CNO -c "$*")
}

c-ssh() {
    vagcmd ssh
}

get-host-port() {
    case "$2" in
        $C_WEBHDFS_NN_PORT)
            expr "51070"
        ;;
        $C_WEBHDFS_DN_PORT)
            expr 50075 + $1 "*" 1000
        ;;
        *)
        echo Invalid port "$2" >&2
        exit 2
        ;;
    esac
}

get-hostname() {
    echo bigtop$1.vagrant
}
;;

docker)

c-up() {
    true
}

c-dn() {
    true
}

c-exec() {
    (cd $BIGTOP_ROOT/provisioner/docker && ./docker-hadoop.sh --exec "$@")
}

c-ssh() {
    echo notsupported
    false
}

get-host-port() {
    (cd $BIGTOP_ROOT/provisioner/docker && docker-compose -p `cat .provision_id` port --index=$1 bigtop $2)
}

get-hostname() {
    #docker inspect --format {{.Config.Hostname}}.{{.Config.Domainname}} ${NODES[0]}
    c-exec $1 hostname -f
}
;;
*)
    echo "Invalid PROVISIONER setting" >&2
    exit 2
esac

#create entrypoint, user, NAT mappings
create-webhdfs-config() {
    echo -n $HADOOP_USER > $USERFILE
    > $NATMAP
    for CN in `seq 1 $N_C` ; do
        C_HOSTNAME=`get-hostname $CN`
        if [ $CN -eq 1 ] ; then #this is namenode
            NN_HOST_PORT=`get-host-port $CN $C_WEBHDFS_NN_PORT`
            if [ -z "$NN_HOST_PORT" ] ; then
                echo Error: port $C_WEBHDFS_NN_PORT @C[$CN] is not mapped to host port space >&2
                exit 2
            fi 
            echo $C_HOSTNAME:$C_WEBHDFS_NN_PORT=$LOCALHOST:${NN_HOST_PORT##0.0.0.0:} >> $NATMAP
            echo -n $LOCALHOST:${NN_HOST_PORT##0.0.0.0:} > $ENTRYPOINT
        fi
        DN_HOST_PORT=`get-host-port $CN $C_WEBHDFS_DN_PORT`
        if [ -z "$DN_HOST_PORT" ] ; then
            echo Error: port $C_WEBHDFS_DN_PORT @C[$CN] is not mapped to host port space >&2
            exit 2
        fi 
        echo $C_HOSTNAME:$C_WEBHDFS_DN_PORT=$LOCALHOST:${DN_HOST_PORT##0.0.0.0:} >> $NATMAP
    done       
}


cleanup-webhdfs-config() {
    rm -f $NATMAP $ENTRYPOINT $USERFILE
}

#=======================================================================================================
# Read/write test

create-source-cmd() {
    if [ -x ./create-source-script ]
    then
        . create-source-script
    else
        curl --output $TESTFILE.gz https://snap.stanford.edu/data/$TESTFILE.gz && gzip -d $TESTFILE.gz
    fi
}

#creates (downloads, unpacks, etc.) the test file
create-source() { 
    if [ ! -f $SOURCE ] ; then
        (cd $TESTDATA_DIR ; create-source-cmd)
        DROP_SOURCE=yes
    fi
    if [ ! -f $SOURCE ] ; then
        echo "Cannot materialize source file" >&2
        exit 2
    fi
}

#removes testfile if it has been downloaded
drop-source() {
    if [ "$DROP_SOURCE" == "yes" ] ; then
        rm $SOURCE
    fi
}

#handles 'k', 'm' and '%'' suffixes
pval() {
    case $1 in
        *k) echo $((${1%%m} * 1024)) ;;
        *m) echo $((${1%%m} * 1024 * 1024)) ;;
        *%) echo $((${1%%%} * $2 / 100)) ;;
        *) echo $1 ;;
    esac
}

#extracts pieces from $1 and calcuates sha512sum on them
#the result is 'shasums' file in the CWD
create-shasums() {
    SZ=`stat -c "%s" $SOURCE`
    POS=0
    FN=0
    > $SHASUMS
    for item in ${READSCRIPT[*]} 
    do 
        case $item in
        s:*)
            W=${item##s:}
            V=`pval $W $SZ`
            #echo Seek=$W/$V
            POS=$V
            ;;
            
        r:*)
            W=${item##r:}
            V=`pval $W $SZ`
            FNAME=$SEGFILE_PREFIX$FN
            #echo Read $W/$V @$POS =\>$FNAME
            dd if=$SOURCE of=$FNAME count=$V skip=$POS iflag=count_bytes,skip_bytes
            sha512sum $FNAME >> $SHASUMS
            rm $FNAME
            FN=$(($FN + 1))
            POS=$(($POS + $V))
            ;;
            
        *)
            echo Invalid program item '$item' >&2
            exit 2
            ;;
        esac
    done
}

#creates write test segments
create-wsegs() {
    SZ=`stat -c "%s" $SOURCE`
    for i in ${!WRITE_SCRIPT[*]} 
    do
        pos=`pval ${WRITE_SCRIPT[$i]} $SZ`
        npos=`pval ${WRITE_SCRIPT[$(($i + 1))]:-$SZ} $SZ`
        len=$((npos - pos))
        dd if=$SOURCE of=$WSEGFILE_PREFIX$i count=$len skip=$pos iflag=count_bytes,skip_bytes
    done
}


#creates misc r/w test files
create-args() {
    FN=0
    for i in ${!READSCRIPT[*]} 
    do 
        case ${READSCRIPT[$i]} in
        s:*)
            READSCRIPTOUT[$i]=${READSCRIPT[$i]}
            ;;          
        r:*)
            READSCRIPTOUT[$i]=${READSCRIPT[$i]}:$SEGFILE_PREFIX$FN
            FN=$(($FN + 1))
            ;;          
        *)
            echo Invalid program item '$item' >&2
            exit 2
            ;;
        esac
    done

    for i in ${!WRITE_SCRIPT[*]} 
    do
        WRITESCRIPTOUT[$i]=$WSEGFILE_PREFIX$i
    done
 
    echo -n ${READSCRIPTOUT[*]} > $READSCRIPTFILE
    echo -n ${WRITESCRIPTOUT[*]} > $WRITESCRIPTFILE
    echo -n $HDFS_DIR/$TESTFILE > $SOURCEPATHFILE
    echo -n $HDFS_DIR/$TESTFILE_W > $TARGETPATHFILE
    echo -n `stat -c "%s" $SOURCE` > $SIZEFILE
}

validate-read() {
    if sha512sum -c $SHASUMS
    then
        rm -f $SEGFILE_PREFIX*
    else
        echo Read: Checksum mismatch >&2
        exit 2
    fi
}

validate-write() {
    local orig=(`cat $CHECKSUMFILE`)
    # "\>" makes redirection happen inside a container/VM
    c-exec 1 hdfs dfs -checksum $HDFS_DIR/$TESTFILE_W \> $C_CHAL_CHECKSUMFILE
    local chal=(`cat $CHAL_CHECKSUMFILE`)
    rm $CHAL_CHECKSUMFILE
    if [ "${orig[1]}" == "${chal[1]}" -a "${orig[2]}" == "${chal[2]}" ]
    then
        echo Write checksums Ok
    else
        echo Write: HDFS Checksum mismatch >&2
        echo Orig: ${orig[*]} >&2
        echo Chal: ${chal[*]} >&2
        exit 2
    fi       
}

#put the test file to HDFS and a checksum file locally
# "\>" makes redirection happen inside a container/VM
upload() {
    c-exec 1 hdfs dfs -mkdir -p $HDFS_DIR
    c-exec 1 hdfs dfs -put -f $C_TESTDATA_DIR/$TESTFILE $HDFS_DIR
    c-exec 1 hdfs dfs -checksum $HDFS_DIR/$TESTFILE \> $C_CHECKSUMFILE
}

cleanup-test-output-rwtest() {
    c-exec 1 hdfs dfs -rm -f -skipTrash $HDFS_DIR/$TESTFILE_W
}

cleanup-rwtest() {
    rm -f $SHASUMS $CHECKSUMFILE $SIZEFILE 
    rm -f $SOURCEPATHFILE $READSCRIPTFILE $TARGETPATHFILE $WRITESCRIPTFILE
    rm -f $SEGFILE_PREFIX* $WSEGFILE_PREFIX*
    c-exec 1 hdfs dfs -rm -f -skipTrash $HDFS_DIR/$TESTFILE
}

prepare-all-rwtest() {
    create-source &&
    create-shasums &&
    create-wsegs &&
    create-args &&
    upload &&
    drop-source
}

prepare-hdfs-rwtest() {
    create-source &&
    upload &&
    drop-source
}

create-test-input-rwtest() { true ; }

validate-rwtest() {
    validate-read && 
    validate-write
}

#=======================================================================================================
# mkdir/rmdir test

prepare-all-mkrmdirtest() { 
    echo -n $HDFS_DIR/$DIR2MK > $DIR2MKFILE
    echo -n $HDFS_DIR/$DIR2RM > $DIR2RMFILE

}
prepare-hdfs-mkrmdirtest() { true ; }
create-test-input-mkrmdirtest() {
    c-exec 1 hdfs dfs -mkdir $HDFS_DIR/$DIR2RM
}
validate-mkrmdirtest() { true; }
cleanup-test-output-mkrmdirtest() {
    c-exec 1 hdfs dfs -rm -r -f -skipTrash $HDFS_DIR/$DIR2MK $HDFS_DIR/$DIR2RM
}

cleanup-mkrmdirtest() {
    rm -f $DIR2MKFILE $DIR2RMFILE
}

#===========================================================================================

prepare-hdfs() {
    create-webhdfs-config &&
    prepare-hdfs-rwtest &&
    prepare-hdfs-mkrmdirtest &&
    #INSERT prepare-hdfs-XXX && above this line
    cleanup-test-output
}

prepare() {
    mkdir -p $TESTDATA_DIR
    if [ "$1" == "--force" -o ! -f $TESTDATA_DIR/.prepared ] ; then 
        create-webhdfs-config &&
        prepare-all-rwtest &&
        prepare-all-mkrmdirtest &&
        #INSERT prepare-all-XXX && above this line
        cleanup-test-output &&
        > $TESTDATA_DIR/.prepared
    fi
}

#this creates volatile test input (the input that gets removed or damaged during test run).
#it is called before each test run
create-test-input() {
    create-test-input-rwtest &&
    create-test-input-mkrmdirtest &&
    #INSERT create-test-input-XXX && above this line
    true
}

cleanup-test-output() {
    cleanup-test-output-rwtest
    cleanup-test-output-mkrmdirtest
    #INSERT cleanup-test-output-XXX above this line
    true
}

cleanup() {
    cleanup-test-output
    cleanup-rwtest
    cleanup-mkrmdirtest
    #INSERT cleanup-XXX above this line
    cleanup-webhdfs-config
    rm -f $TESTDATA_DIR/.prepared
}

validate() {
    validate-rwtest && 
    validate-mkrmdirtest &&
    #INSERT validate-XXX above this line 
    echo "==================== TEST SUCCESSFUL ===================="
}

run-test() {
    prepare &&
    create-test-input &&
    cargo test --test it -- --nocapture &&
    validate &&
    cleanup-test-output
}

cd `dirname $0`
#WSL and Cygwin only
if [ -n "$IS_CYGWIN" -o -n "$IS_WSL" ] ; then
    docker() { docker.exe "$@" | tr -d \\r ; }
    export -f docker
    docker-compose() { docker-compose.exe "$@" | tr -d \\r ; }
    export -f docker-compose
    vagrant() { vagrant.exe "$@" ; }
    cargo() { cargo.exe "$@" ; }
fi

case "$1" in 
    --help) cat <<EOF
Usage
$0 --prepare [--force]
    Uploads the testfile to hdfs, calculates checksums and other necessary data.
$0 --prepare-hdfs
    Does partial preparation for just the Bigtop/HDFS part. Typically used after re-creating bigtop containers.
$0 --create-test-input
    Creates volatile test input
$0 --validate
    Validation step. For rwtest, validates checksums of the files generated by the program being tested, 
    against the reference checksums generated at the preparation step above.
$0 --cleanup-test-output
    Cleans up test output (typically, after a failed test).
$0 --run
    Does --prepare, --create-test-input, then runs the test with cargo, then does --validate, then, if success, --cleanup-test-output
$0 --cleanup
    Cleans up everything.
$0 --c-exec <command>
    Execute command inside 1st VM or container

Additional Vagrant only commands: --c-up, --c-dn, --c-ssh
EOF
        ;;
    --prepare)
        prepare $2
        ;;
    --cleanup)
        cleanup
        ;;
    --validate)
        validate
        ;;
    --run|--test)
        run-test
        ;;
    --prepare-hdfs)
        prepare-hdfs
        ;;
     --create-test-input)
        create-test-input
        ;;
    --cleanup-test-output)
        cleanup-test-output
        ;;    
    --c-up)    c-up ;;
    --c-dn)    c-dn ;;
    --c-exec)
        shift
        c-exec 1 "$*"
        ;;
    --c-ssh)
        c-ssh
        ;;
    *)
        echo Invalid option $1 >&2
        exit 2
        ;;
esac 
