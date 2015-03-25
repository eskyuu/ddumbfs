.. ddumbfs getstarted


Get started with ddumbfs
========================

To install **ddumbfs**, see :ref:`install`.

Initialisation
--------------

To create a 100Go volume in the *Parent* directory */l0/ddumbfs* 
using 64K blocks, use :doc:`mkddumbfs <man/mkddumbfs>`::

    # mkdir /l0/ddumbfs
    # mkddumbfs -B 64k -s 100G /l0/ddumbfs
    Initialize ddumb filesystem in directory: /l0/ddumbfs
    BlockFile initialized: /l0/ddumbfs/ddfsblocks
    IndexFile initialized: /l0/ddumbfs/ddfsidx
    ddumbfs initialized in /l0/ddumbfs
    file_header_size: 16
    hash: TIGER
    hash_size: 24
    block_size: 65536
    index_block_size: 4096
    node_overflow: 1.30
    reuse_asap: 0
    partition_size: 107374182400
    block_count: 1638400
    addr_size: 3
    node_size: 27
    node_count: 2130678
    node_block_count: 14045
    freeblock_offset: 4096
    freeblock_size: 204800
    node_offset: 208896
    index_size: 57737216
    index_block_count: 14096
    root_directory: ddfsroot
    block_filename: ddfsblocks
    index_filename: ddfsidx

:doc:`cpddumbfs <man/cpddumbfs>`:: can be used to copy files while the 
filesystem is **offline**, for example::  

    # cpddumbfs -v /etc/services /l0/ddumbfs/ddfsroot/
    upload /etc/services /l0/ddumbfs/ddfsroot/
         0      2 439298a8d7fa68d7
         1      3 fc55e04cf6e55203
         2      4 f2eb53074706ddce
         3      5 876023dafe0aa7de
         4      6 231f8566ae45bc3e
         5      7 df175a84680270e3
         6      8 cdf7e0e67d39940b
         7      9 8e63fccc5e7ff37a
         8     10 112e027d492812fb
         9     11 aed5bf3d84c21750

First column is the n° of the block inside the source file, 2nd column is where the
block is copied inside the *Block File*, last column is the first 64bits of the 
block hash. As 2nd column shows, first 2 blocks are already used, block 0 is used 
by the header of the *Block File* and block 1 is full of zeroes for the 
*zero block*.

First mount
-----------

To mount the volume on */ddumbfs*, use :doc:`ddumbfs <man/ddumbfs>`::

    # mkdir /ddumbfs
    # ddumbfs /ddumbfs -o parent=/l0/ddumbfs/
    file_header_size 16
    hash TIGER
    hash_size 24
    block_size 65536
    index_block_size 4096
    node_overflow 1.30
    reuse_asap 0
    partition_size 107374182400
    block_count 1638400
    addr_size 3
    node_size 27
    node_count 2130678
    node_block_count 14045
    freeblock_offset 4096
    freeblock_size 204800
    node_offset 208896
    index_size 57737216
    index_block_count 14096
    root_directory ddfsroot
    block_filename ddfsblocks
    index_filename ddfsidx
    HASH TIGER
    root directory: /l0/ddumbfs/ddfsroot
    blockfile: /l0/ddumbfs/ddfsblocks
    indexfile: /l0/ddumbfs/ddfsidx
    index locked into memory: 55.1Mo


Check using mount, one more time::

    # mount | grep ddumbfs
    /l0/ddumbfs on /ddumbfs type fuse (rw,nosuid,nodev,allow_other)
    
Now the filesystem is up and running !

Copy a new file the the *ddumbfs* :: 
    
    # cp /iso/CentOS-5.2-i386-bin-1of6.iso /ddumbfs

This take some seconds. Now take a look at the underlying filesystem::

    # find /l0/ddumbfs/ddfsroot/ -ls
    14360580    4 drwx------   3 root     root         4096 Sep 18 02:47 /l0/ddumbfs/ddfsroot/
    14401537    4 drwx------   2 root     root         4096 Sep 18 02:44 /l0/ddumbfs/ddfsroot/.ddumbfs
    14401538    0 -r--------   1 root     root            0 Sep 18 02:43 /l0/ddumbfs/ddfsroot/.ddumbfs/stats
    14401539    0 -r--------   1 root     root            0 Sep 18 02:43 /l0/ddumbfs/ddfsroot/.ddumbfs/stats0
    14401540    0 -r--------   1 root     root            0 Sep 18 02:43 /l0/ddumbfs/ddfsroot/.ddumbfs/reclaim
    14401541    4 -rw-rw-rw-   1 root     root           55 Sep 18 02:44 /l0/ddumbfs/ddfsroot/.ddumbfs/ddumbfs.log
    14360582    4 -rw-------   1 root     root          286 Sep 18 02:43 /l0/ddumbfs/ddfsroot/services
    14360584  268 -rw-r--r--   1 root     root       269503 Sep 18 02:47 /l0/ddumbfs/ddfsroot/CentOS-5.2-i386-bin-1of6.iso

And at the filesystem itself::

    # find /ddumbfs -ls
    1      4 drwx------   3 root     root         4096 Sep 18 02:47 /ddumbfs
    3      4 drwx------   2 root     root         4096 Sep 18 02:44 /ddumbfs/.ddumbfs
    4      0 -r--------   1 root     root            0 Sep 18 02:43 /ddumbfs/.ddumbfs/stats
    5      0 -r--------   1 root     root            0 Sep 18 02:43 /ddumbfs/.ddumbfs/stats0
    6      0 -r--------   1 root     root            0 Sep 18 02:43 /ddumbfs/.ddumbfs/reclaim
    7      4 -rw-rw-rw-   1 root     root          118 Sep 18 02:48 /ddumbfs/.ddumbfs/ddumbfs.log
    8    640 -rw-------   1 root     root       651949 Sep 18 02:43 /ddumbfs/services
    2 638784 -rw-r--r--   1 root     root    654061568 Sep 18 02:47 /ddumbfs/CentOS-5.2-i386-bin-1of6.iso

Both are identical except for the file size ! This is how *ddumbfs* works, 
each file exist on the *underlying filesystem* but contains only a list
of pointer to the corresponding blocks stored in the *Block File*. The 
hash of each blocks is also stored with the pointer, it allows to control 
the file integrity.
 
Just to be sure, compare files::

    # md5sum /etc/services /ddumbfs/services
    77a7f18fe1508eec6c0f2b5e15b8804e  /etc/services
    77a7f18fe1508eec6c0f2b5e15b8804e  /ddumbfs/services

Mount at startup
----------------

As soon as the *ddumbfs* binary is in the path, usually in */bin* or */sbin*, you can add a line 
to */etc/fstab* to mount the filesystem at startup::

    -oparent=/l0/ddumbfs/        /ddumbfs        fuse.ddumbfs    defaults 0 0
    
Be sure to have file */sbin/mount.fuse* in place

The special directory
---------------------

The special directory *.ddumbfs* contains some special files:

- **stats**: return :ref:`statistics<statistics>` about how many time some functions have 
  been called, the filesystem usage and some options used at initialization and mount time.
- **stats0**: same as *stats*, but reset statistics to zero.
- **reclaim**: start the :ref:`reclaim procedure<reclaim_gs>` and return a short report.
- **ddumbfs.log**: is the log of the filesystem.

Theses file are not de-duplicated.

.. _statistics:

Get some statistics
-------------------

To retrieve *statistics* from an online filesystem, use::

    # cat /ddumbfs/.ddumbfs/stats
    header_load                            3
    header_save                            1
    hash                                9981
    block_write                         9981
    ghost_write                           86
    block_write_try_next_node             13
    block_write_slide                     10
    getattr                               16
    fgetattr                               2
    create                                 1
    open                                   3
    write                             159683
    flush                                  3
    release                                3
    block_allocated                     9824
    block_free                       1628576
    overflow                            1.30
    direct_io                              1
    lock_index                             1
    hash                               TIGER

Values currently to zero are not displayed. Values above *block_allocated* and 
*block_free* are related to function calls. The last values are options used 
at creation and at mount time.

.. _reclaim_gs:

Reclaim procedure
-----------------

After you have removed some files, you can reclaim the free space by accessing 
the special file *reclaim* inside the special directory *.ddumbfs*.
This will free unused blocks inside the *block file* and make them available
for further *write* but will not reduce the size of the *block file* itself. 

Here is a sample, showing the deletion of a new file and how to monitor 
the allocated blocks::

    # grep block_allocated /ddumbfs/.ddumbfs/stats
    block_allocated                     9824
    # cp /etc/fstab /ddumbfs/
    # grep block_allocated /ddumbfs/.ddumbfs/stats
    block_allocated                     9825
    # rm -f /ddumbfs/fstab 
    # grep block_allocated /ddumbfs/.ddumbfs/stats
    block_allocated                     9825
    # cat  /ddumbfs/.ddumbfs/reclaim 
    == Read used blocks from files in 0.0s
    block_allocated                     9825
    block_not_allocated              1628575
    block_in_use                        9824
    block_not_in_use                 1628576
    hash_not_found                         0
    files                                  2
    block_references                    9908
    fragmentation                       0.0%
    == Index cleanup in 0.2s
    nodes_in_index                      9822
    node_deleted                           1
    # grep block_allocated /ddumbfs/.ddumbfs/stats
    block_allocated                     9824

This procedure is very fast and has been designed to be run even
when the filesystem is online and loaded.
When the filesystem grows up to 90%, blocks are reclaimed 
automatically. When the filesystem is full, every write will return an error.
Any *repair* or *rebuild* done using *fsckddumbfs* also reclaim free space.
If the filesystem has not been cleanly unmounted, blocks are also reclaimed during
the *file check* at startup.  

Unmount
-------

Un-mounting the filesystem can take some seconds to *synchronize* the index to
the disk::

    umount /ddumbfs


Offline accesses
----------------
:doc:`cpddumbfs <man/cpddumbfs>` can be used to *upload* or *download* files when the 
filesystem is offline. It can be used as a **recovery** tools or to
check integrity of specific files. 

Before to mount the filesystem we have used 
it to copy the */etc/services* file, now we copy it back to /tmp::

    # cpddumbfs -c -v /l0/ddumbfs/ddfsroot/services /tmp
    download /l0/ddumbfs/ddfsroot/services /tmp
    0      2 d768fad7a8989243 ok
    1      3 0352e5f64ce055fc ok
    2      4 cedd06470753ebf2 ok
    3      5 dea70afeda236087 ok
    4      6 3ebc45ae66851f23 ok
    5      7 e3700268845a17df ok
    6      8 0b94397de6e0f7cd ok
    7      9 7af37f5eccfc638e ok
    8     10 fb1228497d022e11 ok
    9     11 5017c2843dbfd5ae ok

The last column means all blocks match the stored hash. The *-c* option
calculates and compare the hash of each blocks.
 
To check the consistency of one file, use */dev/null* this way::
  
    # cpddumbfs -c /l0/ddumbfs/ddfsroot/services /dev/null && echo really ok
    OK
    really ok
  
You can copy from *stdin* or to *stdout* using *-* as the source or the 
destination::   

    # cpddumbfs /l0/ddumbfs/ddfsroot/services - | head -n 2
    # /etc/services:
    # $Id: services,v 1.51 2010/11/12 12:45:32 ovasik Exp $


Check and repair
----------------

When a **ddumbfs** *volume* has not been cleanly unmounted, it is automatically 
checked at next startup.

:doc:`fsckddumbfs <man/fsckddumbfs>` 
provides a lot of options to check and repair from any situation.
Because the index is build from files in the *volume*, it can be rebuild
from scratch. 

Using a corrupted index can be disastrous:

- new blocks could overwrite other blocks.
- references to non-existent blocks could be returned.
- blocks could be duplicated.

Because hashes and block addresses are stored inside the files themselves, it is
always possible to know if a file is corrupted or not. 
 
*fsckddumbfs* give you the choice between:

- *check*: just check, don't repair anything.
- *repair*: repair the existing index, using information from files.
- *rebuild*: drop existing index and rebuild a new one using information from 
  files and blocks.

*rebuild* and *repair* also reclaim free space automatically.

When a hash from a file don't match a hash inside the index, the hash is  
re-calculated from the block and the index and the address inside the file 
are corrected when required. 
 
When *checking* or *repairing*, you can choose to re-calculate the hash of all
blocks. This will take more time, but give you the assurance about your index 
integrity. 

When a block is lost, when its hash don't match the referenced block in 
the *Block File*, and the hash cannot be found anywhere else, 
*fsckddumbfs* replace its address by 1 in the file itself.
1 means the block is lost and any access to this block will generate IO error 
like for a bad sector.
You can still download the file using *cpddumbfs* that will replace missing 
blocks by *zeroes*.
 
