.. ddumbfs performance


Performance
===========


Introduction
------------

Before to be written to disk, a block must be *de-duplicated*.
**ddumbfs** first calculates its signature, usually its **SHA1** or **TIGER**
hash, searches the *index* and, if the hash is not found,
add the block to the *block file*, update the *index* and write the address 
of the block to the underlying file (instead of the data themselves).
If the hash is already in the index, the related address is used and the block
is not written. 

1\ :sup:`st` write, 2\ :sup:`nd` write and read
-------------------------------------------------------

Usually, *read* and *write* speed are used to compare filesystem performances.
But for a **de-duplicated** filesystem, a third speed must be taken in account,
this is **2**\ :sup:`nd` **write** speed. This consist of writing blocks that are already
on the filesystem ! This don't require a disk write and can be a lot faster.
The **writer pool** take advantage of this to drastically increase speed. 


Copy on write
-------------

When a process want to overwrite few byte of an existing blocks, 
**ddumbfs** need to load the block it will partially overwrite, write the new data,
and store the block as a new one, calculates is new HASH, write it in free
block before to update the file with the new block address. 
 
Because **ddumbfs** use bigger blocks than other filesystem, it will suffer more
of these access and the CPUs usage can be higher. 
      
To maximize speed of **backup** tools that often access data sequentially,   
*ddumbfs* include some caching to handle the write of non blocked data without 
loose of performance. 

File fragmentation
------------------
**ddumbfs** try to maximize *write* speed. Simultaneous writes will not reduce 
global performance but blocks will be mixed together and *read* will be slowed down !
If read access speed is important, avoid simultaneous writes !
The re-use of *reclaimed* block can also increase fragmentation.
Most used strategy optimize the file system for read access but 
**ddumbfs** is optimized for write.  


De-fragmentation
----------------
De-duplicated filesystem **cannot** be de-fragmented ! Each block
can be referenced by multiple files and have multiple predecessors and successors 
in multiple sequences. A block can even be used multiple time by the same file ! 

Moving a block inside the filesytem require to update all references to this block. 
This is fare more complicate than in common filesystems (but not impossible) !

Anyway, some *empirical* algorithm could be used to optimize blocks inside the
block file, but this is not done yet.   


The limits
----------

To be more clear and have an idea about the different values, I use the speed
from my *quad core 2.4GHz* server. 

The fuse throughput
^^^^^^^^^^^^^^^^^^^
FUSE communicate with the user-space through a Unix socket that limits
the throughput to about **1000Mo/s**. The FUSE API increase the overhead a little bit
more and reduce this throughput to about **700Mo/s**.
Then it is impossible using FUSE API on this host to go faster than 700Mo/s !
  
The disk speed
^^^^^^^^^^^^^^
The most recent SATA disk I have can read and write at about **90Mo/s**.
Disk can be combined into RAID array to increase IO speed.

The memory speed
^^^^^^^^^^^^^^^^
The processor cache works at about **7.7Go/s** per CPU, about 28.0Go/s for a *quad core*.
But the memory shared by these processors is a lot slower, about **2.0Go/s**.
When accessed by the 4 core at the same time this slow down to about **650Mo/s** per CPU.

Keep in mind that data must be copied between each layer, from user-space to the kernel,
then back to the user-space and finally back to the kernel. Each block is copied at least 
**4 times** ! Then don't abuse of *memcpy()*.
 
The HASH calculation
^^^^^^^^^^^^^^^^^^^^
**TIGER** hash can be calculated at about **290Mo/s** per CPU, this mean about
1150Mo/s on a *quad core*.

The results
^^^^^^^^^^^
Here are some results reading and writing aligned blocks of 128k on this *quad core*: 

.. rst-class:: perf-result
============== ========= =========== ============== =========== ============= =========== 
 Writer pool    1\ :sup:`st` write   2\ :sup:`nd` write                    read          
-------------- --------------------- -------------------------- ------------------------- 
                  Mo/s     cpu %             Mo/s     cpu %            Mo/s     cpu %      
============== ========= =========== ============== =========== ============= =========== 
   4 cpu           72.69       10.67         490.89       58.23         77.84      9.13     
   3 cpu       **74.54**       11.25         502.58       61.65         79.49      9.16    
   2 cpu           73.41       10.83     **525.62**       62.00         78.28      9.20    
   1 cpu           69.53       10.50         267.36       30.24         79.30      7.24    
 disable           66.30        8.54         188.88       18.76         78.37      9.99    
============== ========= =========== ============== =========== ============= =========== 

For reference, the command used to run this test was::

    testddumbfs -S 2G -s 0 -f -c 4 -o 12R

The 1\ :sup:`st` write and read performances are mostly limited by disk.
The *writer pool* don't improve the the *read* in any way and slightly improve
the 1\ :sup:`st` write up to the disk limit. On the other side the
2\ :sup:`nd` write is not limited by the disk throughput and take a big advantage 
of the  *writer pool*  up to near the maximum FUSE API throughput.  
 
Tuning
======


Some ddumbfs parameters can impact the write and read throughput.


Block size
----------

The block size is the most critical parameter ! Authorized values are 
4k, 8k, 16k, 32k, 64k, 128k.
Using big blocks increase the throughput because less operations are required 
to handle the same among of data.
 
Using small blocks has a lot of disadvantages:

* Files will be composed of more blocks:

    * more system call to the *ddumbfs* interface
    * more lookups in the index
    * more updates of the index 
    * more writes to the block file
    * more write to the underlying file
    
* Bigger index
* Require more memory when locking the index into memory
    
And too little advantages:
 
* Better de-duplication. (small win)
* Less space lost in the unused part of the last block.
 

The Block File
--------------

The use of a **block device** to store the blocks eliminate the 
underlying filesystem overhead and insure blocks will not be fragmented
by the filesystem itself. 
Combined with **direct io**, this give the best of what *ddumbfs* can do.
Some systems are very very slow when using **direct io**. On such system try to disable it.  

The Index File
--------------

If your *index* is too big to be locked into memory, then put it 
on your fasted device. SSD drives are fine. Storing the index on a block device
don't give a significant performance improvement and require to pre-calculate its size
to create the partition accordingly. This is a loose of time.


Lock the Index into memory
--------------------------

The biggest speed boost is to **lock the index into memory**. *ddumbfs* try
to do it by default at mount time. Try to keep the index small enough 
to be able to fit into memory. Use big blocks size and a small overflow factor.
If *ddumbfs* cannot lock the index into memory, if will start anyway.
The status can be checked in the :ref:`stats_file`.

If you cannot lock the index into memory, then every access to the index
will require a disk access. 
Thanks to the good balancing of the hash, 99% of the time only one read in required.
Because of the good balancing of the hash access will be done *randomly* on all
the length of the index, making the disk cache useless.
If the hash is not found, the update will be done in the same *page* and not require 
any additional read. 

Even if *ddumbfs* lower the IOs to the minimum, the IOs will be mostly random 
and then very slow regarding the memory speed or even the disk throughput when 
accesses are sequential.

Direct IO
---------

Doing *direct io* means bypass the kernel cache to read and write *directly* 
to the disk.
The goal was not to speed up the disk access but to avoid to
*pollute* the kernel cache with the blocks of data that would eject the 
*meta-data* (here the *index*) out of the cache. 
Most of the time, when doing backups, data are just written and not reused 
after and keeping them in the cache is useless.
This was a big improvement until the author get the idea of locking the 
index into memory. Anyway the use of *direct io* sometime give some improvements.

The use of big blocks, storing blocks on a block device instead of a regular file 
and enable *direct io* usually give a significant speed boost. 
But this can vary from one system to another. 

If you cannot *lock the index into memory* then at least use *direct io*.

*Direct io* is used only for writing not when reading files.  

When storing blocks on a regular file *direct io* looks to be useless. 
If you use **ddumbfs** as a multi-purpose filesystem (not dedicated
to the storage of big files or backups) and use a regular file instead
of a block device then use option *nodio* to disable *direct io*.   

The HASH algorithm
------------------

The two most reliable and fast candidates are SHA1 and TIGER. 
TIGER was designed for efficiency on 64-bit platforms and then performs 
well on *x64*. SHA1 is about 10% faster on old PIV but a lot slower on
modern x64 architecture. *mkddumbfs* use most appropriate HASH by checking the CPU
at volume creation. You can force one or another.

TIGER160 and TIGER128 are truncated version of the TIGER192 hash. They are not
faster but use less memory and then can help to reduce the index size.  

  
The overflow factor
-------------------
Each *hash* is registered and stored into the index at a *calculated* place.
Two or more *hashes* can compete for the same place, and sometime some hashes must
move inside the index to insert new one. To reduce these moves some free space
are allocated all along the index. This is the *overflow factor*. This also 
increase the chance to find each hash at its *optimal* place to avoid
to search the index sequentially.
A factor of 1.3 will make the index 30% bigger. This is the optimal
factor. If you are sure the filesytem will never be filled up, you can use a 
smaller value, 1.2 or even 1.1 and hope to keep good performance. 
Value above 1.3 don't give significant improvements. 

.. _writer_pool:

The writer pool
---------------

This is the number of processor attached to the calculation of hashes 
when writing blocks. If >0, blocks will be written **asynchronously** by multiple
threads. This improve performance a lot. Default is to start as many thread
as available CPUs. If the disk system is slow vs the CPUs, it is useless to have 
too many threads that will all wait for the disks. 

The default is to use as many CPU as available. Anyway it looks like using
a pool of 3 *writers* on a quad core CPU give better performances than 4.
Using more writers than the CPUs available looks to be always counterproductive.
On a single CPU/core using 1 *writer* is recommended.

**Be careful**, when using the *writer pool*, write request are done **asynchronously**,
this mean that requests are accepted before to know if they can be successfully
achieved. Errors are reported to the next *write* operation. Applications can
have unexpected behavior and return erroneous error message ! If error append
to the last write, the error is reported in the *close* statement, 
but most application ignores such errors. Then the error can be ignored ! Be warned !
Such write errors are very unusual and rarely isolated and should not be unnoticed.   

If performance is not a priority, but your are looking for reliability
then disable the pool using **pool=0** at startup.


Space usage
-----------

When their is less than 1000 free blocks available in the *blocks file*, *ddumbfs* stop
accepting write and return error (ENOSPC). This is to avoid that blocks 
that are in the cache cannot be written to disk. This is to avoid silent corruption.
If your applications open more than 1000 files at at time for writing then you
can have such *silent corruption* be warned. 

Conclusion
----------

To maximize performances (in order):

* lock the index into memory, if not, put it on and SSD drive or on a separate drive, or try using *direct io*.
* use the best HASH for your system, or let :doc:`mkddumbfs <man/mkddumbfs>` does it for you. 
* use big blocks.
* use a pool of writers equal to the number of CPUs/cores.
* put your *block file* on a block device.
* use direct io
