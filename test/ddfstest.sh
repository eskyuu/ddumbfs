#!/bin/bash
# ddfstest
 
ulimit -c 100000000

DEBUG=0
FOREGROUND=0
GDB=0

X_TARGET=""
TARGET=/tmp/ddumbfs
PDIR=/tmp/ddumbfs.data
INDEX_FILE=
BLOCK_FILE=

OVERFLOW=1.3
if grep "model name" /proc/cpuinfo | grep -q Pentium ; then
  HASH=SHA1
else
  HASH=TIGER
fi

MOUNT_OPTS=""
BLOCK_SIZE="64k"
BLOCK_FILE_SIZE="50G"
COUNT="4"
VM_NAME="TTY"
VM_FORMAT="thin"
RND_SIZE=1G
RUNTIME=300

BASIC_FILE="/etc/services"
USER="asx"
SQLITE_SRC="sqlite-src-3070800.zip"
MYSQL_SRC="mysql-5.5.28.tar.gz"
LOG_FILE="/tmp/ddumbfs.log"

SCRIPT=$(readlink -f $0)
SCRIPTPATH=`dirname $SCRIPT`
DDPATH=`dirname $SCRIPTPATH`

TESTDDUMBFS=${DDPATH}/src/testddumbfs
IOZONE=iozone

if [ `hostname` == dell360.asxnet.loc ] ; then
    HOST_IP=192.168.23.103
elif [ `hostname` == cos6-x64.asxnet.loc ] ; then
    HOST_IP=192.168.23.30
#    BLOCK_FILE=/l1/blockfile
elif [ `hostname` == pcasx.asxnet.loc ] ; then
    HOST_IP=192.168.23.23
elif [ `hostname` == f11asx.asxnet.loc ] ; then
    HOST_IP=192.168.23.20
    TARGET=/ddumbfs
    PDIR=/opt/ddumbfs
    INDEX_FILE=
    BLOCK_FILE=
    BLOCK_FILE_SIZE="7G"
elif [ `hostname` == dynamic07.dmz.storagedata.nl ] ; then
    HOST_IP=10.10.10.66
    TARGET=/ddumbfs
    PDIR=/data/ddumbfs
    INDEX_FILE=
    BLOCK_FILE=/dev/sda1
    BLOCK_FILE_SIZE="200G"
    RND_SIZE=10G
    IOZONE="/usr/src/iozone3_397/src/current/iozone"
elif [ `hostname` == leeloo ] ; then
    HOST_IP=192.168.23.33
    TARGET=/ddumbfs
    PDIR=/data/ddumbfs
    INDEX_FILE=
    BLOCK_FILE=
    BLOCK_FILE_SIZE="50G"
    RND_SIZE=2G
    IOZONE="/usr/bin/iozone"
elif [ `hostname` == quad.asxnet.loc ] ; then
    HOST_IP=192.168.23.35
    TARGET=/ddumbfs
    PDIR=/data/ddumbfs
    INDEX_FILE=
    BLOCK_FILE=
    BLOCK_FILE_SIZE="200G"
    RND_SIZE=2G
    IOZONE="/usr/bin/iozone"
elif [ `hostname` == max.asxnet.loc -o `hostname` == max ] ; then
    HOST_IP=192.168.23.11
    TARGET=/ddumbfs
    PDIR=/s0/tmp/ddumbfs
    MOUNT_OPTS=nodio
    INDEX_FILE=
    BLOCK_FILE=
    BLOCK_FILE_SIZE="20G"
    RND_SIZE=1G
elif [ `hostname` == i530.asxnet.loc -o `hostname` == i530 ] ; then
    HOST_IP=192.168.23.24
    TARGET=/ddumbfs
    PDIR=/tmp/ddumbfs
    MOUNT_OPTS=nodio
    INDEX_FILE=
    BLOCK_FILE=
    BLOCK_FILE_SIZE="20G"
    RND_SIZE=1G

fi

[ ! -d $TARGET ] && [[ $TARGET == /tmp/* ]] && mkdir $TARGET
[ ! -d $PDIR   ] && [[ $PDIR   == /tmp/* ]] && mkdir $PDIR
 

RESET_FS=0

# sfdisk --force /dev/sdb < dell360--22-2-256.sfdisk

die()
{
  echo $1
  exit 1
}

show()
{
  echo PDIR=$PDIR
  echo TARGET=$TARGET
  echo BLOCK_SIZE=$BLOCK_SIZE
  echo BLOCK_FILE_SIZE=$BLOCK_FILE_SIZE
}

usage()
{
  echo "Usage: `basename $0` -h"
  echo "       `basename $0` [ basic-options ] actions"
  echo
  echo "basic-options:"
  echo "    [ -B BLOCK_SIZE ]      default $BLOCK_SIZE"
  echo "    [ -s BLOCK_FILE_SIZE ] default $BLOCK_FILE_SIZE"
  echo "    [ -H HASH ] default is $HASH, others (SHA1, TIGER160)"
    
  echo "    [ -m MOUNT_OPTS ]      
  echo "        in pool=NUM,[no]lock_index,[no]dio 
  echo 
  echo "    [ -r ] re-format filesysteme"
  echo "    [ -d ] mount in debug mode"
  echo "    [ -f ] mount in foreground"
  echo "    [ -g ] gdb in foreground"
  echo "    [ -T X_TARGET ] where to run the test"
  echo
  echo "actions:"
  echo "    kill:       kill -9 the mount (not a test)"
  echo
  echo "    basic:      basic copy and check"
  echo "    fsx:        run fsx-linux"
  echo "    pjd:        run the POSIX compliance test"
  echo "    reclaim:    reclaim"
  echo "    rnd:        speed test write/2write/read"
  echo "         [ -n COUNT ] default $COUNT" 
  echo "    esx:        speed test write/2write/read"
  echo "         [ -v VMNAME ] default TTY (others: XP)" 
  echo "         [ -i FORMAT ] default thin (others: 2gbsparse)"
  echo "    etc:        copy /etc to ddumbfs"
  echo "    bonnie:     run bonnie++"
  echo "    alter:      alter test"
  echo "         [ -n COUNT ] default $COUNT" 
  echo "    crash:      test how kill -9 ddumbfs works"
  echo "    mark:       regression from Mark"
  echo "         [ -t RUNTIME ] default $RUNTIME" 
  echo "    mysql:      compile and run mysql regression"
  echo "    sqlite:     compile sqlite and run regression"
  echo ""
  echo "    show:       show all possible testd"
            
  exit 0  
}

fs_umount()
{
    for i in 1 2 3 ; do 
        umount $TARGET 2> /dev/null && echo umount $TARGET
        umount $PDIR  2> /dev/null && echo umount $TARGET
        if ! mount | egrep "${TARGET} |${PDIR} " ; then
            break
        fi
        sleep 5
    done
    mount | grep "${TARGET} " && die "$TARGET still mounted"
    mount | grep "${PDIR} " && die "$PARENT still mounted"
    sleep 2
}

fs_reset()
{
  fs_umount

  #dd if=/dev/zero of=/dev/sdb3 bs=1M seek=1 count=1024
  cd ${DDPATH}/src || die "cannot cd in ${DDPATH}/src"
  make mkddumbfs || die "make mkddumbfs failed"
  ./mkddumbfs -s $BLOCK_FILE_SIZE -B $BLOCK_SIZE -i "$INDEX_FILE" -b "$BLOCK_FILE" -o $OVERFLOW -H $HASH $PDIR || die "mkddumbfs failed"
}

fs_mount()
{
    cd ${DDPATH}/src || die "cannot cd in ${DDPATH}/src"
    make ddumbfs || die "make ddumbfs failed"
    [ -n $TARGET ] && rm -rf ${TARGET}/*
    rm -rf ${PDIR}/ddfsroot/core.*
    
    [ -z $MOUNT_OPTS ] && mount_opts="" || mount_opts="-o $MOUNT_OPTS"
    
    echo ./ddumbfs ${TARGET} -o parent=${PDIR} $mount_opts || die "mount failed"

    if [ "$DEBUG" == "1" ] ; then
        ./ddumbfs ${TARGET} -o parent=${PDIR} $mount_opts -o debug -d -f || die "mount failed"
    elif [ "$FOREGROUND" == "1" ] ; then
        ./ddumbfs ${TARGET} -o parent=${PDIR} $mount_opts -f || die "mount failed"
    elif [ "$GDB" == "1" ] ; then
        tmpfile=`mktemp gdbddumbfs.XXXXXXXXXX`
        echo > $tmpfile run ${TARGET} -o parent=${PDIR} $mount_opts -f
        gdb -x $tmpfile ./ddumbfs
    else
        ./ddumbfs ${TARGET} -o parent=${PDIR} $mount_opts || die "mount failed"
    fi 
    echo filesystem mounted on $TARGET
    read_cfg
}    

fs_check()
{
    if [ -z "$X_TARGET" ] ; then 
        mount | grep $TARGET | grep -q $PDIR || ( fs_mount ; sleep 1)
        mount | grep $TARGET | grep -q $PDIR || die "filesystem not mounted"
        if ! touch $TARGET/.test_ready ; then
              echo filesystem stalled, remount 
            fs_umount
            mount | grep $TARGET | grep -q $PDIR || ( fs_mount ; sleep 1)
            mount | grep $TARGET | grep -q $PDIR || die "filesystem not mounted"
            touch $TARGET/.test_ready || die "filesystem not mounted"
        fi
        rm $TARGET/.test_ready || die "filesystem not mounted"
    fi        
    read_cfg
}

read_cfg()
{
    # read $PDIR/ddfs.cfg
    
    [ -f $PDIR/ddfs.cfg ] || die "file not found: $PDIR/ddfs.cfg"
    exec 3<> $PDIR/ddfs.cfg
    while read -u 3 keyvalue ; do
        if [ -z "$keyvalue" ] || [[ "$keyvalue" == \#* ]] ;  then
                continue
        fi
        # use regular expression
        # rem this work on bash3 and bash4
        reg='\s*([^:]*[^[:space:]])\s*:\s*(.*[^[:space:]])\s*'
        if [[ $keyvalue =~ $reg ]] ; then
            eval c_${BASH_REMATCH[1]}="${BASH_REMATCH[2]}"
        fi
    done
    exec 3>&-
}

run_kill()
{
  if mount | grep $TARGET | grep -q $PDIR ; then
      pid=`ps ax | grep "parent=$PDIR" | grep $TARGET | grep ddumbfs | grep -v grep | cut -c -5`
      [ -n $pid ] && kill -9 $pid
      sleep 1
      umount $TARGET
  fi 
}

run_mount()
{
    fs_umount
    sleep 1
    fs_check
}

run_umount()
{
    fs_umount
}

run_basic()
{
    fs_check
    [ ! -f $BASIC_FILE ] && die $BASIC_FILE not found
    filename=`basename $BASIC_FILE`
    rm -f $TARGET/$filename
    cp $BASIC_FILE $TARGET/$filename || die "error copying file $filename"
    cmp $BASIC_FILE $TARGET/$filename && echo ok  test_basic || echo err test_basic
}

run_fsx()
{
    EXTRA="-W -R"
    EXTRA="-N 30000"
    EXTRA=""
    fs_check
    ( cd $TARGET ; ${DDPATH}/examples/fsx-linux ${EXTRA} fsx -P /tmp )
}

run_reclaim()
{
    fs_check
    SIZE=500M
    FSX_CNT=20000
    #SIZE=20M
    #FSX_CNT=2000

    ${TESTDDUMBFS} -o F -S ${SIZE} -s 1 ${TARGET}/file1 & 
    ${TESTDDUMBFS} -o F -S ${SIZE} -s 2 ${TARGET}/file2 & 
    ( cd ${TARGET} ; ${DDPATH}/examples/fsx-linux fsxreclaim -P /tmp -N ${FSX_CNT} ) &

    echo WAIT FOR END OF FILE COPY AND FSX
    wait

    echo COMPARE
    ${TESTDDUMBFS} -o C -S ${SIZE} -s 1 ${TARGET}/file1 || die "compare error for ${TARGET}/file1"  
    ${TESTDDUMBFS} -o C -S ${SIZE} -s 2 ${TARGET}/file2 || die "compare error for ${TARGET}/file2"  

    echo remove file2
    rm -f ${TARGET}/file2
    
    echo start RECLAIM when a file is copied
    ${TESTDDUMBFS} -o F -S 200M -s 4 ${TARGET}/file4 &  
    cat ${TARGET}/.ddumbfs/reclaim > /dev/null

    wait

    echo COPY FILE 2 and 3
    ${TESTDDUMBFS} -o F -S ${SIZE} -s 2 ${TARGET}/file2 
    ${TESTDDUMBFS} -o F -S ${SIZE} -s 3 ${TARGET}/file3

    echo "COMPARE all files" 
    ${TESTDDUMBFS} -o C -S ${SIZE} -s 1 ${TARGET}/file1 || die "compare error for ${TARGET}/file1"  
    ${TESTDDUMBFS} -o C -S ${SIZE} -s 2 ${TARGET}/file2 || die "compare error for ${TARGET}/file2"  
    ${TESTDDUMBFS} -o C -S ${SIZE} -s 3 ${TARGET}/file3 || die "compare error for ${TARGET}/file3"  
    ${TESTDDUMBFS} -o C -S 200M -s 4 ${TARGET}/file4 || die "compare error for ${TARGET}/file4"  

    echo "ok  reclaim"
}

run_rnd()
{
    fs_check
#    ${DDPATH}/examples/rndblock $TARGET $(($c_block_size/1024)) 1024 $COUNT 0 0x0 12R
    ${TESTDDUMBFS} -o 12R -B ${c_block_size} -c ${COUNT} -f -S ${RND_SIZE} -s 0 ${TARGET}   
}

run_esx()
{
    service nfs stop

    fs_check
    sleep 6
    service nfs start
    sleep 4
    
    ssh root@esxi /bin/vim-cmd hostsvc/datastore/nas_create ddumbfs ${HOST_IP} ${TARGET} 0

    pushd ${SCRIPTPATH}/mksbackup
    python2 src/mksbackup -d -v -c mksbackup.ini backup DDFS${VM_NAME}${VM_FORMAT}
    status=$?
    popd
    
    if [ "$status" != "0" ] ; then
        ssh root@esxi /bin/vim-cmd hostsvc/datastore/destroy ddumbfs
        service nfs stop
        die "MKSBackup failed"
    fi
     
    for DIR in ${TARGET}/vmware/*/*2011* ; do true ; done
    if [ ! -d "$DIR" ] ; then 
        ssh root@esxi /bin/vim-cmd hostsvc/datastore/destroy ddumbfs
        service nfs stop
        die "vmware directory not found"
    fi
    
    VMDK=`echo "$DIR"/*[^fs][^l0-9][^a0-9][^t].vmdk`
    VMDK_FLAT=`echo "$DIR"/*-flat.vmdk`
    if [ -f "$VMDK_FLAT" ] ; then
        md5hash=`md5sum "$VMDK_FLAT" | cut -b 1-32`
    elif [ -f "$VMDK" ] ; then 
        RVMDK=/vmfs/volumes/ddumbfs/"${VMDK#/ddumbfs/}"
        ssh root@esxi rm -f /vmfs/volumes/SATA500-0/vmware-flat.vmdk /vmfs/volumes/SATA500-0/vmware.vmdk
        ssh root@esxi /sbin/vmkfstools -i "\"$RVMDK\"" -d thin /vmfs/volumes/SATA500-0/vmware.vmdk 
        md5hash=`ssh root@esxi /vmfs/volumes/SATA500-0/misc/sbin/md5sum /vmfs/volumes/SATA500-0/vmware-flat.vmdk | cut -b 1-32`
    else 
        ssh root@esxi /bin/vim-cmd hostsvc/datastore/destroy ddumbfs
        service nfs stop
        die "no vmdk found: $VMDK, $VMDK_FLAT"
    fi
    
    ssh root@esxi /bin/vim-cmd hostsvc/datastore/destroy ddumbfs
    service nfs stop
    if [ "${md5hash}" != "de353db7e15a7a6162d15ab6a942c75c" -a "${md5hash}" != "a445b7cc57616bac9101be4f2c886e84" ] ; then
        die "ERROR test_esx.sh $* ${md5hash}"
    else
        echo OK $* ${md5hash}
    fi
}

run_etc()
{
    fs_check
    time ( cd / ; tar cf - etc | (cd /$TARGET ; tar xf - ))
}

run_basic2()
{
    fs_check
    echo Write ${TARGET}/file1 
    time ${TESTDDUMBFS} -o F -B $c_block_size -S ${RND_SIZE} -f -m 0x0 -s 1 ${TARGET}/file1  
    echo Compare ${TARGET}/file1 
    time ${TESTDDUMBFS} -o C -B $c_block_size -S ${RND_SIZE} -f -m 0x0 -s 1 ${TARGET}/file1 || die "compare error for ${TARGET}/file1"  
}

run_basic3()
{
    fs_check
    time ${DDPATH}/examples/rndblock $TARGET $(($c_block_size/1024)) 1024 1 0 0x0 1
}

run_bonnie()
{
    [ -d ${TARGET}/${USER} ] || (mkdir ${TARGET}/${USER} ; chown ${USER} ${TARGET}/${USER} )
    bonnie++ -u ${USER} -d ${TARGET}/${USER}
}

run_crash()
{
    echo umount
    fs_umount
    dd if=/dev/null of=$BLOCK_FILE bs=1M count=2048
    fs_reset
    fs_check
    
    echo copying file
    ${TESTDDUMBFS} -o F -B $c_block_size -S ${RND_SIZE} -f -m 0x0 -s 1 ${TARGET}/file1 &
    ${TESTDDUMBFS} -o F -B $c_block_size -S ${RND_SIZE} -f -m 0x0 -s 2 ${TARGET}/file2 &
    ${TESTDDUMBFS} -o F -B $c_block_size -S ${RND_SIZE} -f -m 0x0 -s 3 ${TARGET}/file3 &
    sleep 5
    if false ; then
        echo sync
        sync
    fi
    echo pkill
    pkill -9 ddumbfs
    sleep 5
    echo umount
    fs_umount
    if false ; then  
        echo fsck -n
        ${DDPATH}/src/fsckddumbfs -n ${PDIR}
    fi
    echo check file1
    fs_check

    ls -l ${PDIR}/ddfsroot/file*
    for file in 1 2 3 ; do 
        if ! ${DDPATH}/src/cpddumbfs -c -v ${PDIR}/ddfsroot/file${file} /dev/null 2>&1 | egrep "err|size" ; then 
            echo CORRUPTED BLOCKS file${file}
        fi
        ${TESTDDUMBFS} -o C -B $c_block_size -S ${RND_SIZE} -f -m 0x0 -s ${file} ${TARGET}/file${file}
    done

    echo test consitency of block at next rewrite     
    
    ${TESTDDUMBFS} -o F -B $c_block_size -S ${RND_SIZE} -f -m 0x0 -s 1 ${TARGET}/clone1
    ${TESTDDUMBFS} -o C -B $c_block_size -S ${RND_SIZE} -f -m 0x0 -s 1 ${TARGET}/clone1 || die "compare error for ${TARGET}/clone1"  
    
    ${TESTDDUMBFS} -o F -B $c_block_size -S ${RND_SIZE} -f -m 0x0 -s 2 ${TARGET}/clone2
    ${TESTDDUMBFS} -o C -B $c_block_size -S ${RND_SIZE} -f -m 0x0 -s 2 ${TARGET}/clone2 || die "compare error for ${TARGET}/clone2"
    
    ${TESTDDUMBFS} -o F -B $c_block_size -S ${RND_SIZE} -f -m 0x0 -s 3 ${TARGET}/clone3
    ${TESTDDUMBFS} -o C -B $c_block_size -S ${RND_SIZE} -f -m 0x0 -s 3 ${TARGET}/clone3 || die "compare error for ${TARGET}/clone3"
    
    echo crash test ok

}

run_resize()
{
    BLOCK_FILE_SIZE=500M
    NEW_BLOCK_FILE_SIZE=240M
    BLOCK_SIZE=4k    
    fs_reset
    fs_mount
    fs_check
    echo == Populate
	${TESTDDUMBFS} -o F -B $c_block_size -S 300M -f -m 0x0 -s 2 ${TARGET}/file2 &
	${TESTDDUMBFS} -o F -B $c_block_size -S  50M -f -m 0x0 -s 1 ${TARGET}/file1
	${TESTDDUMBFS} -o F -B $c_block_size -S  50M -f -m 0x0 -s 3 ${TARGET}/file3  
    wait
    echo == Remove one file
    rm -f ${TARGET}/file2                
    fs_umount
    
    echo == Test fs integrity
    ${DDPATH}/src/fsckddumbfs -C $PDIR 2>&1 || die "fsck -C failed"  
    
    echo == Compact
    ${DDPATH}/src/fsckddumbfs -n -v -p -k $PDIR || die "compact failed"
    
    echo == Double check
    ${DDPATH}/src/fsckddumbfs -c -v -p $PDIR || die "compact failed"
        
    echo == Test fs integrity once more
    ${DDPATH}/src/fsckddumbfs -C $PDIR >/dev/null 2>&1 || die "fsck -C failed"
    
    fs_mount
    fs_check
          
    echo == Populate new files
    ${TESTDDUMBFS} -o F -B $c_block_size -S 15M -f -m 0x0 -s 4 ${TARGET}/file4
    ${TESTDDUMBFS} -o F -B $c_block_size -S 15M -f -m 0x0 -s 5 ${TARGET}/file5
	${TESTDDUMBFS} -o F -B $c_block_size -S 50M -f -m 0x0 -s 2 ${TARGET}/file2

    echo == Test files
	${TESTDDUMBFS} -o C -B $c_block_size -S 50M -f -m 0x0 -s 1 ${TARGET}/file1 || die "compare error for ${TARGET}/file1"  
	${TESTDDUMBFS} -o C -B $c_block_size -S 50M -f -m 0x0 -s 2 ${TARGET}/file2 || die "compare error for ${TARGET}/file2"  
	${TESTDDUMBFS} -o C -B $c_block_size -S 50M -f -m 0x0 -s 3 ${TARGET}/file3 || die "compare error for ${TARGET}/file3"  
    ${TESTDDUMBFS} -o C -B $c_block_size -S 15M -f -m 0x0 -s 4 ${TARGET}/file4 || die "compare error for ${TARGET}/file4"  
    ${TESTDDUMBFS} -o C -B $c_block_size -S 15M -f -m 0x0 -s 5 ${TARGET}/file5 || die "compare error for ${TARGET}/file5"  

    echo == pack success
    echo == start migration
    fs_umount
    rm -rf ${PDIR}.save
	mkdir ${PDIR}.save || die "cannot create ${PDIR}.save"
	mv ${PDIR}/* ${PDIR}.save
	BLOCK_FILE_SIZE=${NEW_BLOCK_FILE_SIZE}
    fs_reset
    fs_umount
	${DDPATH}/src/migrateddumbfs -v -p ${PDIR}.save ${PDIR}
    mv ${PDIR}.save/ddfsblocks ${PDIR}/ddfsblocks
    fs_mount
    fs_check
    
    echo == Test files
	${TESTDDUMBFS} -o C -B $c_block_size -S 50M -f -m 0x0 -s 1 ${TARGET}/file1 || die "compare error for ${TARGET}/file1"  
	${TESTDDUMBFS} -o C -B $c_block_size -S 50M -f -m 0x0 -s 2 ${TARGET}/file2 || die "compare error for ${TARGET}/file2"  
	${TESTDDUMBFS} -o C -B $c_block_size -S 50M -f -m 0x0 -s 3 ${TARGET}/file3 || die "compare error for ${TARGET}/file3"  
    ${TESTDDUMBFS} -o C -B $c_block_size -S 15M -f -m 0x0 -s 4 ${TARGET}/file4 || die "compare error for ${TARGET}/file4"  
    ${TESTDDUMBFS} -o C -B $c_block_size -S 15M -f -m 0x0 -s 5 ${TARGET}/file5 || die "compare error for ${TARGET}/file5"  
    echo == Resize SUCCESS

}


run_alter()
{
    BLOCK_FILE_SIZE=512M
    BLOCK_SIZE=4k    
    fs_reset
    fs_mount
    fs_check
    echo "== create files"
    ${TESTDDUMBFS} -o F -B $c_block_size -S 100M -f -m 0x0 -s 1 ${TARGET}/file1  
    ${TESTDDUMBFS} -o F -B $c_block_size -S 100M -f -m 0x0 -s 3 ${TARGET}/file3  
    ${TESTDDUMBFS} -o F -B $c_block_size -S 100M -f -m 0x0 -s 3 ${TARGET}/clone3  # I need this clone to be able to shake file 3
    ${TESTDDUMBFS} -o C -B $c_block_size -S 100M -f -m 0x0 -s 1 ${TARGET}/file1 || die "compare error for ${TARGET}/file1"  
    ${TESTDDUMBFS} -o C -B $c_block_size -S 100M -f -m 0x0 -s 3 ${TARGET}/file3 || die "compare error for ${TARGET}/file3"  
    
    echo "== files are ok, umount" 
    fs_umount
    sleep 1
    echo "== alter filesystem"
    ${DDPATH}/src/alterddumbfs -i $COUNT $PDIR
    ${DDPATH}/src/alterddumbfs -f $PDIR
    ${DDPATH}/src/alterddumbfs -k ${PDIR}/ddfsroot/file3 $PDIR
    sleep 1
    echo "== create .autofsck"
    touch ${PDIR}/.autofsck
    echo remount    
    fs_mount
    
    echo "== create clones and test"
    ${TESTDDUMBFS} -o F -B $c_block_size -S 100M -f -m 0x0 -s 2 ${TARGET}/file2
    ${TESTDDUMBFS} -o F -B $c_block_size -S 100M -f -m 0x0 -s 1 ${TARGET}/clone1
    ${TESTDDUMBFS} -o F -B $c_block_size -S 100M -f -m 0x0 -s 3 ${TARGET}/clone3  
    ${TESTDDUMBFS} -o C -B $c_block_size -S 100M -f -m 0x0 -s 1 ${TARGET}/file1 || die "compare error for ${TARGET}/file1"  
    ${TESTDDUMBFS} -o C -B $c_block_size -S 100M -f -m 0x0 -s 2 ${TARGET}/file2 || die "compare error for ${TARGET}/file2"  
    ${TESTDDUMBFS} -o C -B $c_block_size -S 100M -f -m 0x0 -s 3 ${TARGET}/file3 || die "compare error for ${TARGET}/file3"  
    ${TESTDDUMBFS} -o C -B $c_block_size -S 100M -f -m 0x0 -s 1 ${TARGET}/clone1 || die "compare error for ${TARGET}/clone1"  
    ${TESTDDUMBFS} -o C -B $c_block_size -S 100M -f -m 0x0 -s 3 ${TARGET}/clone3  || die "compare error for ${TARGET}/clone3"  
    echo "== FILES OK" 
    fs_umount
    sleep 1
    ${DDPATH}/src/fsckddumbfs -C $PDIR && die "ERROR FSCK should have failed, but didn't"
    echo $?
    echo "== delete all unrecoverable files"
    rm -f ${PDIR}/ddfsroot/{ff_block_not_found_2,ff_bad_header_long,ff_bad_header_short,ff_already_corrupted,ff_block_not_found_1,ff_bad_zero}
    ${DDPATH}/src/fsckddumbfs -C $PDIR || die "ERROR FSCK Failed"
    echo "ALTER SUCCESSFUL ONCE"
    
    
    echo "== try to shake a file without any clone, will need -R to rebuild"
    echo "== alter and move file2"
    ${DDPATH}/src/alterddumbfs -k ${PDIR}/ddfsroot/file2 $PDIR
    filename=`mktemp`
    cp ${PDIR}/ddfsroot/file2 ${filename}
    rm -f ${PDIR}/ddfsroot/file2
    echo "== fsck and remove any ref to file2 blocks"
    ${DDPATH}/src/fsckddumbfs -n $PDIR || die "FSCK failed"
    
    echo "== bring back file 2"
    cp ${filename} ${PDIR}/ddfsroot/file2
    rm -f ${filename}
    ${DDPATH}/src/fsckddumbfs -C $PDIR && die "ERROR FSCK shoould have failed"
    
    echo "== create .autofsck and remount"
    touch ${PDIR}/.autofsck
    fs_mount

    ${TESTDDUMBFS} -o C -B $c_block_size -S 100M -f -m 0x0 -s 2 ${TARGET}/file2 && die "compare should have failed ${TARGET}/file2"  

    echo "== umount and repair -R"
    fs_umount
    sleep 1
    ${DDPATH}/src/fsckddumbfs -R $PDIR || die "ERROR FSCK failed"

    echo "== mount and check file2"
    fs_mount
	${TESTDDUMBFS} -o C -B $c_block_size -S 100M -f -m 0x0 -s 2 ${TARGET}/file2 ||  die "compare error for ${TARGET}/file2"
    echo "ALTER SUCCESSFUL TWICE"
                
}

run_alter_meta()
{
    BLOCK_FILE_SIZE=256M
    BLOCK_SIZE=4k   
    INDEX_FILE=""
    BLOCK_FILE=""
    
    fs_reset
    fs_mount
    fs_check
    echo create file1
    ${TESTDDUMBFS} -o F -B $c_block_size -S 10M -f -m 0x0 -s 1 ${TARGET}/file1  
    ${TESTDDUMBFS} -o C -B $c_block_size -S 10M -f -m 0x0 -s 1 ${TARGET}/file1 || die "compare error for ${TARGET}/file1"
    echo file1 ok, umount 

    for action in delete truncate empty magic ; do
        fs_umount
        sleep 1
        echo alter INDEX : $action
        ${DDPATH}/src/alterddumbfs -I $action $PDIR
        ${DDPATH}/src/ddumbfs ${TARGET} -o parent=${PDIR} && die "mount should fail"
        echo mount failed ok
        ${DDPATH}/src/fsckddumbfs -f -r ${PDIR}
        fs_mount
        ${TESTDDUMBFS} -o C -B $c_block_size -S 10M -f -m 0x0 -s 1 ${TARGET}/file1 || die "compare error for ${TARGET}/file1"
        echo file1 ok after index $action 
    done

    for action in magic ; do
        fs_umount
        sleep 2
        echo alter BLOCK : $action
        ${DDPATH}/src/alterddumbfs -B $action $PDIR
        ${DDPATH}/src/ddumbfs ${TARGET} -o parent=${PDIR} && die "mount should fail"
        echo mount failed ok
        echo fsck
        ${DDPATH}/src/fsckddumbfs -f -r ${PDIR}
        fs_mount
        ${TESTDDUMBFS} -o C -B $c_block_size -S 10M -f -m 0x0 -s 1 ${TARGET}/file1 || die "compare error for ${TARGET}/file1"
        echo file1 ok after block $action 
    done
    echo "ALTER META SUCCESSFUL"

}

run_sqlite()
{
    which ulockmgr_server 2> /dev/null || die "ulockmgr_server not found in PATH"
	[ -f ${DDPATH}/test/${SQLITE_SRC} ] || die "file not found: ${DDPATH}/test/${SQLITE_SRC}"
    fs_check
    cd ${TARGET}
    chmod a+rx ${TARGET}
    unzip ${DDPATH}/test/${SQLITE_SRC}
    mkdir build
    chown ${USER} build
    cd build
    [ -f /usr/local/lib/tclConfig.sh ] && TCL=/usr/local/lib
    [ -f /usr/lib/tcl8.5/tclConfig.sh ] && TCL=/usr/lib/tcl8.5
    [ -f /usr/lib64/tcl8.5/tclConfig.sh ] && TCL=/usr/lib64/tcl8.5
    [ -z $TCL ] && die "TCLDEV not found"
    su ${USER} -c "../sqlite-src-3070800/configure --with-tcl=${TCL}" 
    [ -f Makefile ] || die "configure failure, Makefile not found"
    su ${USER} -c make
    [ -f sqlite3 ] || die "build failure, sqlite3 not found"
    su ${USER} -c "make test && echo OK"
}

run_mysql()
{
    which ulockmgr_server 2> /dev/null || die "ulockmgr_server not found in PATH"
	[ -f ${DDPATH}/test/${MYSQL_SRC} ] || die "file not found: ${DDPATH}/test/${MYSQL_SRC}"
    fs_check
    cd ${TARGET}
    chmod a+rx ${TARGET}
    tar xvzf ${DDPATH}/test/${MYSQL_SRC}
    cd ${TARGET}/mysql-5.5.28
    cmake .
    make
    [ -f sql/mysqld ] || die "build failure, sql/mysqld not found"
    cd mysql-test
    perl mysql-test-run.pl --force
}

run_pjd()
{
    fs_check
    mkdir ${TARGET}/pjd
    chmod a+rwx ${TARGET}
    cd ${DDPATH}/test/pjd-fstest-20080816
    rm -f fstest
    make 
    cd ${TARGET}/pjd
    prove -r ${DDPATH}/test/pjd-fstest-20080816
}

run_fsx8()
{
    fs_check

        rm /tmp/logfsx*

        # Seconds to run.
        #RUNTIME=3000
        STIME=`date +%s`
        CONCUR=2

        for n in $(seq -w 1 $CONCUR ); do
                mkdir -p ${TARGET}/data/fsx$n
                cd ${TARGET}/data/fsx$n
                ( ${DDPATH}/examples/fsx-linux -d all >/tmp/logfsx$n 2>&1 ; pkill -9 fsx-linux ) &

        done

        error=0
        count=0
        while true
        do
           ETIME=`date +%s`
           ((count++))
           CURTIME=$((ETIME-STIME))
           echo $CURTIME  `ps ax | grep -v grep | grep fsx-linux | wc -l` 
           if [ $CURTIME -gt $RUNTIME ]
              then
                FSXRES=`ps ax | grep -v grep | grep fsx-linux | wc -l`
                if [ $FSXRES != $CONCUR ]
                   then
                   error=1
                   echo failed FSX $FSXRES
                fi
                pkill -9 fsx-linux
                sleep 5
                sync
                break
           fi
           sleep 1
        done
        clear
        if [ $error != 0 ]
           then
            echo "THIS FILESYSTEM HAS FAILED THE TEST"
        else
            echo "THIS FILESYSTEM HAS PASSED THE TEST"
        fi
        sync
}

run_mark()
{
    fs_check
    
    rm /tmp/logfsx*
    rm /tmp/logbonnie*
    rm /tmp/logiozone*
    
    # Seconds to run.
    #RUNTIME=300
    STIME=`date +%s`
    CONCUR=8
    
    for n in $(seq -w 1 $CONCUR ); do
            mkdir -p ${TARGET}/data/fsx$n
            cd ${TARGET}/data/fsx$n
            ${DDPATH}/examples/fsx-linux -d all >/tmp/logfsx$n 2>&1 &
    
            mkdir ${TARGET}/data/bonnie$n
            chown ${USER} ${TARGET}/data/bonnie$n
            bonnie++ -u ${USER} -d ${TARGET}/data/bonnie$n >/tmp/logbonnie$n 2>&1 &
    
            mkdir ${TARGET}/data/iozone$n
            cd ${TARGET}/data/iozone$n
            ${IOZONE} -a >/tmp/logiozone$n 2>&1 &
    done

    mkdir ${TARGET}/pjd
    cd ${TARGET}/pjd
    prove -r ${DDPATH}/test/pjd-fstest-20080816 2>&1 >/tmp/prove.log &
    
    error=0
        error_ioz=0
        error_fsx=0
        error_bon=0
    count=0
    while true
    do
       ETIME=`date +%s`
       ((count++))
       CURTIME=$((ETIME-STIME))
       echo $CURTIME  `ps ax | grep -v grep | grep fsx-linux | wc -l` 
       if [ $CURTIME -gt $RUNTIME ]
          then
            IOZRES=`ps ax | grep -v grep | grep iozone | wc -l`
            FSXRES=`ps ax | grep -v grep | grep fsx-linux | wc -l`
            BONRES=`ps ax | grep -v grep | grep bonnie++ | wc -l`
            if [ $IOZRES != $CONCUR ]
               then
               error=1
               error_ioz=1
            fi
            if [ $FSXRES != $CONCUR ]
               then
               error=1
               error_fsx=1
                   echo failed FSX $FSXRES
            fi
            if [ $BONRES != $CONCUR ]
               then
               error=1
               error_bon=1
            fi 
            pkill -9 iozone
            pkill -9 fsx-linux
            pkill -9 bonnie++
            pkill -9 prove
            sleep 5
            sync
            break
       fi
       sleep 1
    done
    clear
    if [ $error != 0 ]
       then
        echo "THIS FILESYSTEM HAS FAILED THE TEST"
            [ $error_ioz != 0 ] && echo "IOZONE FAILED"
            [ $error_fsx != 0 ] && echo "FSX FAILED"
            [ $error_bon != 0 ] && echo "BONNIE++ FAILED"
    else
        echo "THIS FILESYSTEM HAS PASSED THE TEST"
    fi
    sync
}

run_show()
{
	grep ^run_ ${script_name}
}

run_test()
{
    fs_umount
    sleep 1
    fs_check
    rm -f ${TARGET}/*.bin ; ${TESTDDUMBFS} -c 5 -B ${c_block_size} -S 1G -f -s 0 -o 2 ${TARGET}    
}

while getopts ":hdfgrB:s:n:t:v:i:m:H:T:" opt; do
    case $opt in
        h)
          usage
          ;;
        d)
          DEBUG=1
          MOUNT=1
          ;;
        f)
          FOREGROUND=1
          MOUNT=1
          ;;
        g)
          GDB=1
          MOUNT=1
          ;;
        B)
          BLOCK_SIZE=$OPTARG
          ;;
        H)
          HASH=$OPTARG
          ;;
        s)
          BLOCK_FILE_SIZE=$OPTARG
          ;;
        m)
          MOUNT_OPTS=$OPTARG
          ;;
        r)
          RESET_FS=1
          ;;
        n)
          COUNT="$OPTARG"
          ;;
        t)
          RUNTIME="$OPTARG"
          ;;
        v)
          VM_NAME="$OPTARG"
          ;;
        i)
          VM_FORMAT="$OPTARG"
          ;;
        T)
          X_TARGET="$TARGET"
          TARGET="$OPTARG"
          ;;
        \?)
          echo "Invalid option: -$OPTARG" >&2
          exit 1
          ;;
    esac
done

[ "$RESET_FS" == "1" ] && fs_reset && MOUNT=1
[ "$MOUNT" == "1" ] && fs_mount

script_name=$0

shift $((OPTIND-1))
# now do something with $@
    
for action in $@ ; do
    a="run_"${action}
    $a
done
    
