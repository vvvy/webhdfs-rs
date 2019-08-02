#!/bin/bash

#integration test tool
#
#prerequisites:
# 0. (Windows) Cygwin or WSL
# 1. Apache Bigtop version 1.3
#   1.1. Bigtop repo cloned locally (rel/1.3 tag) and BIGTOP_ROOT set to the root of the cloned repo
#   1.2. docker-container.yaml extended with the following settings:
#   ports:
#   - "50070"
#   - "50075"
#   volumes:
#   - //c/path/to/test-data : /test-data
#   1.3. 3-node cluster provisioned via bigtop/provisioner (Windows HOWTO: http://...) with docker-container.yaml extended as above
#
#operation:
# Read test:
# The read test consists of a sequence of reads and seeks against a testfile, set up by PROGRAM below.
# The social networking graph data file 'soc-pokec-relationships.txt', available on SNAP, is used as a testflle (any file, large enough, 
# may be used; recommended size is 200-400M).
# During preparation, SHA-512 checksums are pre-calculated for each of r: chunks set up by PROGRAM (chunks are extracted by dd utility).
# The program under test is expected to execute the PROGRAM below (actually, POUT) by doing seeks and reads as requested.
# Upon each read, the program reads from the testfile (HDFS) then writes the content read to a file specified by 3rd part of POUT item.
# Finally, checksums for newly written chunks are validated against pre-calculated checksums.
#
#hooks:
# 1. place all local settings in './itt-config.sh' and make it 'chmod a+x'
# 2. if using a test file other than standard, or using the standard one but pre-downloaded, or using other source, 
#    place the test file materialzation command(s) in './test-data./create-source-script' and make it 'chmod a+x'. 
#    Note that the script is launched in './test-data'.
#    Note that if the test file is already in './test-data', it is used as-is and left intact 
#    (if downloaded, the test file is deleted from './test-data' after preparation).
#
# See 'itt.sh --help' for usage details


TESTFILE=soc-pokec-relationships.txt
PROGRAM=(r:128m s:0 r:1m r:128m)

#local directory where test data is maintained
TESTDATA_DIR=./test-data
#container directory TESTDATA_DIR maps to
C_TESTDATA_DIR=/test-data
#hdfs directory where test file is copied
HDFS_DIR=/user/root/test-data
#local bigtop root
BIGTOP_ROOT=/usr/local/src/bigtop
#number of bigtop containers
N_C=3
#webhdfs NN port inside containers
C_WEBHDFS_NN_PORT=50070
#webhdfs DN port inside containers
C_WEBHDFS_DN_PORT=50075

if [ -x ./itt-config.sh ]; then . ./itt-config.sh ; fi

LOCALHOST=localhost
SOURCE=$TESTDATA_DIR/$TESTFILE
SHASUMS=$TESTDATA_DIR/shasums
NATMAP=$TESTDATA_DIR/natmap
ENTRYPOINT=$TESTDATA_DIR/entrypoint
PROGRAMFILE=$TESTDATA_DIR/program
SIZEFILE=$TESTDATA_DIR/size
SOURCEPATHFILE=$TESTDATA_DIR/source
SEGFILE_PREFIX=$TESTDATA_DIR/seg-

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


#handles 'k' and 'm' suffixes as multipliers
ival() {
    case $1 in
        *k) echo $((${1%%m} * 1024)) ;;
        *m) echo $((${1%%m} * 1024 * 1024)) ;;
        *) echo $1 ;;
    esac
}

#extracts pieces from $1 and calcuates sha512sum on them
#the result is 'shasums' file in the CWD
create-shasums() {
    POS=0
    FN=0
    > $SHASUMS
    for item in ${PROGRAM[*]} 
    do 
        case $item in
        s:*)
            W=${item##s:}
            V=`ival $W`
            #echo Seek=$W/$V
            POS=$V
            ;;
            
        r:*)
            W=${item##r:}
            V=`ival $W`
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

c-exec() {
    (cd $BIGTOP_ROOT/provisioner/docker && ./docker-hadoop.sh --exec "$@")
}

get-host-port() {
    (cd $BIGTOP_ROOT/provisioner/docker && docker-compose -p `cat .provision_id` port --index=$1 bigtop $2)
}

#create NAT mappings
create-natmap() {
    > $NATMAP
    for CN in `seq 1 $N_C` ; do
        #docker inspect --format {{.Config.Hostname}}.{{.Config.Domainname}} ${NODES[0]}
        C_HOSTNAME=`c-exec $CN hostname -f`
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

#put test file to HDFS
upload() {
    c-exec 1 hdfs dfs -mkdir -p $HDFS_DIR
    c-exec 1 hdfs dfs -put -f $C_TESTDATA_DIR/$TESTFILE $HDFS_DIR
}

prepare() {
    mkdir -p $TESTDATA_DIR
    if [ "$1" == "--force" -o ! -f $TESTDATA_DIR/.prepared ] ; then 
        create-source &&
        create-shasums &&
        create-natmap &&
        create-args &&
        upload &&
        drop-source &&
        > $TESTDATA_DIR/.prepared
    fi
}

create-args() {
    I=0
    FN=0
    for item in ${PROGRAM[*]} 
    do 
        case $item in
        s:*)
            POUT[$I]=$item
            ;;          
        r:*)
            POUT[$I]=$item:$SEGFILE_PREFIX$FN
            FN=$(($FN + 1))
            ;;          
        *)
            echo Invalid program item '$item' >&2
            exit 2
            ;;
        esac
        I=$(($I + 1))
    done
 
    #echo --entrypoint=\"`cat $ENTRYPOINT`\" --source=\"$HDFS_DIR/$TESTFILE\"--program=\"${POUT[*]}\"
    echo -n ${POUT[*]} > $PROGRAMFILE
    echo -n $HDFS_DIR/$TESTFILE > $SOURCEPATHFILE
    echo -n `stat -c "%s" $SOURCE` > $SIZEFILE
}

validate() {
    if sha512sum -c $SHASUMS
    then
        rm -f $SEGFILE_PREFIX*
    else
        echo Checksum mismatch >&2
        exit 2
    fi
}

cd `dirname $0`
#WSL and Cygwin only
if [ `uname -o` == "Cygwin" ] || grep -q Microsoft /proc/version ; then
    docker() { docker.exe "$@" | tr -d \\r ; }
    export -f docker
    docker-compose() { docker-compose.exe "$@" | tr -d \\r ; }
    export -f docker-compose
fi

case "$1" in 
    --help)
        echo Usage
        echo $0 --prepare [--force]
        echo "    Uploads the testfile to hdfs, calculates checksums and other necessary data."
        echo $0 --validate
        echo "    Validates checksums of the files generated by the program being tested, against the reference checksums generated"
        echo "    at the preparation step above."
        echo $0 --create-natmap
        echo "    Only creates NAT mapping file."
        echo $0 --upload
        echo "    Only uploads the testfile."
        ;;
    --prepare)
        prepare $2
        ;;
    --validate)
        validate
        ;;
    --create-natmap)
        create-natmap
        ;;
    --create-args)
        create-args
        ;;
    --upload)
        upload
        ;;
    *)
        echo Invalid option $1 >&2
        exit 2
        ;;
esac 
