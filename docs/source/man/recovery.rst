.. ddumbfs recovery


Recovery 
========

Automatic recovery
------------------

At startup, *ddumbfs* search for the file *.autofsck*. The presence of this file means
that ddumbfs didn't shutdown properly and that the filesystem must be checked.
*ddumbfs* start a file check similar to the *fsckddumbfs* command with option *-r*.
This is fast, and appropriate to handle such situation.


Manual recovery
---------------

**ddumbfs** is released with the powerful **fsckddumbfs**.
This tools do **everything** that is possible to detect and repair errors.
Errors that cannot be repaired are reported to the user through the special file 
*./ddumbfs/corrupted.txt*.   

Informations are stored in 3 main containers:

* **the index**: This is a sparse *hash* table made of *nodes*. *Nodes* are composed by 
  the position and the *hash* of a block in the *block file*. The index manages also a list of free blocks.

* **the files**: This is the sequence of blocks that compose a user file. Each *node* in the
  *file* are identical to *nodes* in the *index*. Information in these *nodes* can be used to rebuild
  the index without accessing the *block file*.

* **the block file**: This is where blocks are stored one after the others.

These structures hold a lot of redundant informations. This redundancy help to improve the
access speed and the reliability of the system. **fsckddumbfs** cross all these informations
to detect anomalies and fix them. The *index* can be quickly and completely rebuild from the *files* *nodes*.
 
**fsckddumbfs** has 3 main modes of operation :

* **check**: check in read only mode.
* **repair**: check and repair the existing *index*.
* **rebuild**: erase the *index* and recreate a new one using data from *file* nodes or from 
  the *block file* itself.


Each of these mode have a *light* (lowercase) and an *heavy* (uppercase) version. The heavy one 
read all the blocks of the *block file* to check, repair or rebuild the index.

 
Read the very complete :doc:`fsckddumbfs <man/fsckddumbfs>` manual for more informations.


How it works
------------
      
I have simulated an unexpected shutdown and show you how *ddumbfs* handle this 
situation.

I was copying 3 files of 1Go to the filesystem when I have powered down the 
virtual machine.

After a reboot, the linux distribution checked and mounted all *conventional* filesystems,
including the *underlying* filesystem used by *ddumbfs*  

I'm now mounting the *crashed* filesystem and explain lines of the log::

      [root@cos6-x64 ddumbfsC6]# src/ddumbfs /ddumbfs/ -o parent=/l0/ddumbfs/
        file_header_size 16
        hash TIGER
        hash_size 24
        block_size 4096
        index_block_size 4096
        node_overflow 1.30
        reuse_asap 0
        partition_size 10737418240
        block_count 2621440
        addr_size 3
        node_size 27
        node_count 3408630
        node_block_count 22469
        freeblock_offset 4096
        freeblock_size 327680
        node_offset 331776
        index_size 92364800
        index_block_count 22550
        root_directory ddfsroot
        block_filename /dev/sdb3
        index_filename /dev/sdb2
    hash: TIGER
    direct_io: 1 enable
    writer pool: 2 cpus
    root directory: /l0/ddumbfs/ddfsroot
    blockfile: /dev/sdb3
    indexfile: /dev/sdb2
    index locked into memory: 88.1Mo
    09:49:11 INF check filesystem /l0/ddumbfs
    
*ddumbfs* has detected the file *.autofsck* in the parent directory and
start automatically the appropriate file system check. ::
    
    09:49:12 INF Repair node order, fixed 0 errors.
    
All nodes in the index are at the expected place. A crash should not disturb
the node order. But all further tests expect some consistency of the index.
Because the index has not been flushed, some data can be on the filesystem 
but not in the index. The autocheck and its manual equivalent *fsckddumbfs* read 
hashes from *all* files and update the index when possible. 
Here a lot off block have to be *hashed* to update the index. ::
     
    09:49:12 INF Update index from files.
    09:49:13 INF calculate hash for block addr=2
    ......
    09:49:13 INF calculate hash for block addr=1625
    09:49:13 INF calculate hash for block addr=1628
    09:49:13 INF Read 3 files in 1.0s.
    09:49:13 INF 1478 blocks used in files.
    09:49:13 INF 1103 blocks have been added to index.

*fsckddumbfs* has registered 1103 blocks that were referenced by
*files* but not yet in the index. The index is hold in memory or
in cache and not flushed to disk to often to increase performance.
But this is not a problem, data can be recovered from file themself. ::

    09:49:13 INF ddfs_load_usedblocks
    09:49:14 INF Check also recently added blocks: 1668.

On the other side some blocks could have been added to the index and 
referenced by *files* but not yet written to the *block file*. 
They are 1668 blocks that have been added since the last checkpoint,
all must be checked. The regulars checkpoint limit the number of blocks
to check at reboot time:: 

    09:49:14 INF 1670 blocks used in nodes.
    09:49:14 INF 1668 suspect blocks in nodes.
    09:49:14 INF Resolve Index conflicts.
    09:49:14 INF 0 nodes fixed.

Everything was ok and now, the index is clean and supposed to match what is in 
the *block file*. 

*fsckddumbfs* now check and update files consistency. :: 

    09:49:14 INF Fix files.
    09:49:14 WAR F s  /l0/ddumbfs/ddfsroot/file1
    09:49:14 WAR F s  /l0/ddumbfs/ddfsroot/file3
    09:49:14 WAR  Cs  /l0/ddumbfs/ddfsroot/file2
    09:49:14 INF Fixed:2  Corrupted:1  Total:3 files in 0.0s.

Two files have an invalid **s**\ ize but *fsckddumbfs* has **F**\ ixed the problem.
The last file is **C**\ orrupted and had a bad **s**\ ize. The size problem can be
fixed but the file is now known as *Corrupted*. See below:: 

    09:49:14 INF Deleted 193 useless nodes.

Some blocks were registered in the index but not yet written nor used by *files*.
These useless nodes can also come from a previous file deletion. Index is not
updated when files are deleted. To free the space you must start a *reclaim* procedure.
These node have been removed::  

    09:49:14 INF blocks in use: 1477   blocks free: 2619963.

This is clear.

Now take a look at what we can see on the filesystem. Files are less than 1Go 
has expected. ::

    [root@cos6-x64 ddumbfsC6]# ll /ddumbfs/
    total 6056
    -rw-r--r--. 1 root root 2015232 Oct 14 09:49 file1
    -rw-r--r--. 1 root root 2015232 Oct 14 09:49 file2
    -rw-r--r--. 1 root root 2170880 Oct 14 09:49 file3

To have a resume of which files are *corrupted* take a look at the *corrupted.txt*
file that will display the same as seen above. ::

    [root@cos6-x64 ddumbfsC6]# cat /ddumbfs/.ddumbfs/corrupted.txt 
    F s  /l0/ddumbfs/ddfsroot/file1
    F s  /l0/ddumbfs/ddfsroot/file3
     Cs  /l0/ddumbfs/ddfsroot/file2


:doc:`cpddumbfs <man/cpddumbfs>` is a tools able to *upload* or *download* files 
from an offline ddumbfs *volume*.
*download* can be used when the filesystem is online without risk for it. 
Don't expect consistent result if you are writing on it at the same time !
Option *-c* can be used to **c**\ heck the file with hash stored in the *ddumbfs* 
filesystem and its consistency. ::   

    [root@cos6-x64 ddumbfsC6]# src/cpddumbfs -c /l0/ddumbfs/ddfsroot/file1 /dev/null 
    OK
    [root@cos6-x64 ddumbfsC6]# src/cpddumbfs -c /l0/ddumbfs/ddfsroot/file3 /dev/null 
    OK
    
**OK** means *file1* and *file3* are consistent. Of course they are incomplete because
of the unexpected shutdown ! :: 
 
    [root@cos6-x64 ddumbfsC6]# src/testddumbfs -o C -B 4096 -S 1024M -f -m 0x0 -s 1 /ddumbfs/file1
    difference in block starting at: 2015232
    [root@cos6-x64 ddumbfsC6]# src/testddumbfs -o C -B 4096 -S 1024M -f -m 0x0 -s 3 /ddumbfs/file3
    difference in block starting at: 2170880

Comparing with the original I can see that the written data match up to the last byte.
**testddumbfs** is tool that generate big random file and then allow to compare 
them. The advantage is to avoid the need to have such big files under the hand for testing.
Trust the syntax and the appropriate usage by the author :-) 

Now the **corrupted** file ! :: 
    
    [root@cos6-x64 ddumbfsC6]# src/cpddumbfs -c /l0/ddumbfs/ddfsroot/file2 /dev/null 
       416      1 e38bf882357c4a0b err
    ERR

**ERR** means some blocks don't match the expected one. Read of the block 416 
at offset 416*4k=1703936 will return full of *zeros*. This is the default behavior.
The reference has been written to the file, but the block was still in cache when the 
server *crashed*.

If you are lucky, you will copy another file, or make another backup that will contains 
and identical block and the next *file check* will re-link the corrupted *node*
to the new block and the file will be removed of the *corrupted list*. 
If not, you must reload the file from the source or delete it.
You can also copy the file, the missing block will be replaced by a block full of zeroes.
But keep in mind that this new file is *corrupted*.
  
