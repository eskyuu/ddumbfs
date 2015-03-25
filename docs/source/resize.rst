.. ddumbfs resize

.. highlight:: none

Resizing ddumbfs filesystem
:::::::::::::::::::::::::::

The resizing of the filesystem require multiple operations:

- When shrinking the filesystem, blocks that are allocated too far at the end of 
  the *BlockFile* (if any),   must be relocated at the beginning using the 
  *--pack* option of the *fsckddumbfs* command.
- Create a new empty filesystem having the required size.
- Migrate meta-data to this new filesystem using the *migrateddumbfs* command.
- *connect* the *BlockFile* device to this new filesystem

The advantage of migrating data to a new filesystem is that no data can be lost
if the migration process is interrupted. The only sensitive operation is the *packing*
of the filesystem. But a simple *normal* repair using *fsckddumbfs* can fix any problem.      

*block devices* vs of *regular* files
======================================

*Block devices* are a less flexible to handle than *regular* files. 
Even if your a using LVM the resize operation is a lot more complex. 
Be sure to have a good understanding of how *ddumbfs* works before to 
start resizing.  

Things you have to know
=======================

Optimized address size
----------------------
The size of the block addresses used in the *index* and in the *DataFiles* are optimized.
For example, if the maximum number of block is under 16777216 (16M), the size of 
the address is stored in 3 bytes to save space. If the new size cross such a boundary
the *DataFiles* must be *migrated* too. The *index* is always *migrated* anyway.
This is what *migrateddumbfs* does. 

Resizing the *IndexFile*
-------------------------

The size of the *IndexFile* is defined at creation time and never changes. The
position of every entries in the *index* depend of the total capacity of the filesystem.
Any change of the capacity requires to move the entries inside the *index*.
 

Resizing the *BlockFile*
-------------------------

Increasing the filesystem size don't require to do anything to the *BlockFile*. 
You don't need to *pack* it.

To *shrink* the *BlockFile*, all blocks allocated beyond the new limit must be 
moved at the beginning. Any block move require to update its address in the *index* and in 
each *DataFile*. See :ref:`packing`. 
 
.. _packing:

How does *packing* works 
=========================

*Pack* will swap all the free blocks at the beginning with used blocks
located at the end to finally get all the free space at the end of 
the *BlockFile*. After that the *BlockFile* can be truncated without 
losing data.

 
- First the filesystem must be checked to avoid any corruption.
- The new size is calculated regarding the used space.
- Using the bit list of free blocks, the first allocated block beyond 
  the limit will be moved to the first free space and so on. This is
  probably the longest operation. To reduce the number of un-sequential
  iops, reads and writes are grouped in 1Mo chunk. The first blocks
  to move are read up to fill in a 1Mo buffer, then written to appropriate
  places.
- Then all *DataFiles* are opened, and all *addresses* above the new limit
  are updated. Their is no *translation* table, such a table would take
  too much RAM and cannot be stored to disk without slowing down the process
  too much. The bits list is used as a table. The Nth used block
  above the limit goes to the Nth free block starting at zero. To speed
  up a bit, two indexes of free and unfree blocks are created.
- Finally the *index* is updated using the same logic as the *DataFiles*
  and the bits list is filled with 1 up to the limit and reset to zero beyond.
- If the *BlockFile* is a *regular* file (not a block device) it is truncated
  as much as possible

Here is a sample with 10 used blocks::   
    
              1
    01234567890123456789
    AB C  DE  FG  HIJ       

Block F, G, H, I and J will be moved to free space at the beginning, blocks
A, B, C, D, and E stay at the same place.::

              1
    01234567890123456789
    ABFCGHDEIJ       

Samples
=======

This is part of my *regression* suite. They are lot of *test* operations
that you don't need for a real migration. This show you how it works
and how I test it.
 
I first create a 500Mo *ddumbfs* filesystem in a temporary directory::

    # mkddumbfs -s 500M -B 4k -i '' -b '' -o 1.3 -H TIGER /s0/tmp/ddumbfs
    Initialize ddumb filesystem in directory: /s0/tmp/ddumbfs
    BlockFile initialized: /s0/tmp/ddumbfs/ddfsblocks
    IndexFile initialized: /s0/tmp/ddumbfs/ddfsidx
    ddfs.cfg file initialized: /s0/tmp/ddumbfs/ddfs.cfg
    ddumbfs initialized in /s0/tmp/ddumbfs
    file_header_size: 16
    hash: TIGER
    hash_size: 24
    block_size: 4096
    index_block_size: 4096
    node_overflow: 1.30
    reuse_asap: 0
    auto_buffer_flush: 60
    auto_sync: 120
    partition_size: 524288000
    block_count: 128000
    addr_size: 3
    node_size: 27
    node_count: 167177
    node_block_count: 1102
    freeblock_offset: 65536
    freeblock_size: 16000
    node_offset: 131072
    index_size: 4644864
    index_block_count: 1134
    root_directory: ddfsroot
    block_filename: ddfsblocks
    index_filename: ddfsidx

Then mount it::

    # ddumbfs /ddumbfs -o parent=/s0/tmp/ddumbfs -o nodio
        file_header_size 16
        hash TIGER
        hash_size 24
        block_size 4096
        index_block_size 4096
        node_overflow 1.30
        reuse_asap 0
        auto_buffer_flush 60
        auto_sync 120
        partition_size 524288000
        block_count 128000
        addr_size 3
        node_size 27
        node_count 167177
        node_block_count 1102
        freeblock_offset 65536
        freeblock_size 16000
        node_offset 131072
        index_size 4644864
        index_block_count 1134
        root_directory ddfsroot
        block_filename ddfsblocks
        index_filename ddfsidx
    hash:      TIGER
    direct_io: 0 disable
    reclaim:   95
    writer pool: 2 cpus
    root directory: /s0/tmp/ddumbfs/ddfsroot
    blockfile: /s0/tmp/ddumbfs/ddfsblocks
    indexfile: /s0/tmp/ddumbfs/ddfsidx


Then populate it with some test files. *testddumbfs* creates files with 
random data that can be checked later:: 
    
    # testddumbfs -o F -B 4096 -S 300M -f -m 0x0 -s 2 /ddumbfs/file2 &
    # testddumbfs -o F -B 4096 -S  50M -f -m 0x0 -s 1 /ddumbfs/file1
    # testddumbfs -o F -B 4096 -S  50M -f -m 0x0 -s 3 /ddumbfs/file3
    # wait
    
Process are running in parallel and blocks are mixed in the *BlockFiles*.
Now I remove the big one and umount the filesystem::
    
    # rm -f /ddumbfs/file2
    # umount /ddumbfs

I check the filesystem integrity::

    # fsckddumbfs -C /s0/tmp/ddumbfs
    root directory: /s0/tmp/ddumbfs/ddfsroot
    blockfile: /s0/tmp/ddumbfs/ddfsblocks
    indexfile: /s0/tmp/ddumbfs/ddfsidx
    started 2 hashing thread(s)
    == Check index
    == Checked 167177 nodes in 0.0s
    == Check block hash
    == Checked 102402 blocks in 3.5s
    == Check unmatching 0 block
    == Checked 0 unmatching blocks in 0.0s
    == Read files
    == Read 2 files in 0.1s
    filesystem cleanly shut down: skip 'last recently added' blocks check
    == Summary   
    Used blocks              :     102402
    Free blocks              :      25598
    Last used blocks         :     102401
    Total blocks             :     128000
    Blocks usage             : ********0.
    Index errors             :          0 OK
    Index blocks             :     102402
    Index blocks dup         :          0 OK
    Block errors             :          0 OK
    Files                    :          2
    Files errors             :          0 OK
    Files blocks             :      25602
    Files lost blocks        :          0 OK
    Tested hashes            :          0
    Wrong hashes             :          0 OK
    in index & not in files  :      76800 OK can be reclaimed
    in files & not in index  :          0 OK
    in files & in index      :      25602
    in blocks & not in files :      76800 OK can be reclaimed
    in files & not in blocks :          0 OK
    Filesystem status        : OK blocks can be reclaimed
    
As you can see in the **Blocks usage** line, the first 80% of the 
*BlocFile* is full. **0** means than between 0 and 10% of the 
part is filled. **.** (a dot) means the part is completely empty.
You can also see that 76800 blocks can be reclaimed.

Now I *reclaims* the free space and pack the *BlockFile. 
Any non *read-only* *fsckddumbfs* operation reclaims the free blocks, 
here I choose *-n* for a *normal* repair. The *-k* is for *pack*::

    # fsckddumbfs -n -v -p -k /s0/tmp/ddumbfs
    root directory: /s0/tmp/ddumbfs/ddfsroot
    blockfile: /s0/tmp/ddumbfs/ddfsblocks
    indexfile: /s0/tmp/ddumbfs/ddfsidx
    04:21:25 INF Check and repair node order in index: fixed 0 errors.
    04:21:25 INF Search files for nodes missing in the index.
    04:21:25 INF Read 2 files in 0.0s.
    04:21:25 INF 25602 blocks used in files.
    04:21:25 INF 0 more suspect blocks.
    04:21:25 INF Found 0 duplicate addresses in index.
    04:21:25 INF filesystem cleanly shut down: skip 'last recently added' blocks check
    04:21:25 INF Re-hash all 0 suspect blocks.
    04:21:25 INF Re-hash errors: 0
    04:21:25 INF Fix files.
    04:21:25 INF Calculated 0 hashes.
    04:21:25 INF Fixed:0  Corrupted:0  Total:2 files in 0.0s.
    04:21:25 INF Deleted 76800 useless nodes.
    04:21:25 INF blocks in use: 25602   blocks free: 102398.
    04:21:25 INF 102402 blocks used in nodes.
    04:21:25 INF Resolve Index conflicts and re-hash 0 suspect blocks.
    04:21:25 INF 0 nodes fixed.
    Pack
    == Move 12804 (used=25602, total=128000) blocks to the beginning.
    == Update files
    == Updated 2 files in 0.1s
    Update index
    Update free block list
    OK

You can see that **76800** node have been deleted. 
76800*4k=300Mo, this our 300Mo file that we have deleted.
Rerun *fsckddumbfs* to see what happened::
 
    # fsckddumbfs -c -v -p /s0/tmp/ddumbfs
    root directory: /s0/tmp/ddumbfs/ddfsroot
    blockfile: /s0/tmp/ddumbfs/ddfsblocks
    indexfile: /s0/tmp/ddumbfs/ddfsidx
    == Check index
    == Checked 167177 nodes in 0.0s
    == Read files
    == Read 2 files in 0.1s
    filesystem cleanly shut down: skip 'last recently added' blocks check
    == Summary   
    Used blocks              :      25602
    Free blocks              :     102398
    Last used blocks         :      25601
    Total blocks             :     128000
    Blocks usage             : **0.......
    Index errors             :          0 OK
    Index blocks             :      25602
    Index blocks dup         :          0 OK
    Block errors             :          0 OK
    Files                    :          2
    Files errors             :          0 OK
    Files blocks             :      25602
    Files lost blocks        :          0 OK
    Tested hashes            :          0
    Wrong hashes             :          0 OK
    in index & not in files  :          0 OK
    in files & not in index  :          0 OK
    in files & in index      :      25602
    in blocks & not in files :          0 OK
    in files & not in blocks :          0 OK
    Filesystem status        : OK 
    OK
    
The last 70% of the *BlockFile* are completely empty (line *Blocks usage* ).
I mount the filesystem to check the file integrity and see if *packing*
was fine:: 

    # ddumbfs /ddumbfs -o parent=/s0/tmp/ddumbfs -o nodio
        file_header_size 16
        hash TIGER
        hash_size 24
        block_size 4096
        index_block_size 4096
        node_overflow 1.30
        reuse_asap 0
        auto_buffer_flush 60
        auto_sync 120
        partition_size 524288000
        block_count 128000
        addr_size 3
        node_size 27
        node_count 167177
        node_block_count 1102
        freeblock_offset 65536
        freeblock_size 16000
        node_offset 131072
        index_size 4644864
        index_block_count 1134
        root_directory ddfsroot
        block_filename ddfsblocks
        index_filename ddfsidx
    hash:      TIGER
    direct_io: 0 disable
    reclaim:   95
    writer pool: 2 cpus
    root directory: /s0/tmp/ddumbfs/ddfsroot
    blockfile: /s0/tmp/ddumbfs/ddfsblocks
    indexfile: /s0/tmp/ddumbfs/ddfsidx


First I will add some new files to be sure they will not overwrite
existing data::

    # testddumbfs -o F -B 4096 -S 15M -f -m 0x0 -s 4 /ddumbfs/file4
    # testddumbfs -o F -B 4096 -S 15M -f -m 0x0 -s 5 /ddumbfs/file5
    # testddumbfs -o F -B 4096 -S 50M -f -m 0x0 -s 2 /ddumbfs/file2
    
And then test old and new files::

    # testddumbfs -o C -B 4096 -S 50M -f -m 0x0 -s 1 /ddumbfs/file1
    # testddumbfs -o C -B 4096 -S 50M -f -m 0x0 -s 2 /ddumbfs/file2
    # testddumbfs -o C -B 4096 -S 50M -f -m 0x0 -s 3 /ddumbfs/file3
    # testddumbfs -o C -B 4096 -S 15M -f -m 0x0 -s 4 /ddumbfs/file4
    # testddumbfs -o C -B 4096 -S 15M -f -m 0x0 -s 5 /ddumbfs/file5
    
The *packing* is a success ! Now I start migrating the data to a new filesystem::

    # umount /ddumbfs
    # mv /s0/tmp/ddumbfs /s0/tmp/ddumbfs.save
    # mkdir /s0/tmp/ddumbfs
    
Then I create a smaller filesystem. 240Mo is smaller than 256Mo=65K*4Ko. This 
means that in this tiny filesystem, block addresses are only 2 bytes long::
     
    # mkddumbfs -s 240M -B 4k -i '' -b '' -o 1.3 -H TIGER /s0/tmp/ddumbfs
    Initialize ddumb filesystem in directory: /s0/tmp/ddumbfs
    BlockFile initialized: /s0/tmp/ddumbfs/ddfsblocks
    IndexFile initialized: /s0/tmp/ddumbfs/ddfsidx
    ddfs.cfg file initialized: /s0/tmp/ddumbfs/ddfs.cfg
    ddumbfs initialized in /s0/tmp/ddumbfs
    file_header_size: 16
    hash: TIGER
    hash_size: 24
    block_size: 4096
    index_block_size: 4096
    node_overflow: 1.30
    reuse_asap: 0
    auto_buffer_flush: 60
    auto_sync: 120
    partition_size: 251658240
    block_count: 61440
    addr_size: 2
    node_size: 26
    node_count: 80659
    node_block_count: 512
    freeblock_offset: 65536
    freeblock_size: 7680
    node_offset: 131072
    index_size: 2228224
    index_block_count: 544
    root_directory: ddfsroot
    block_filename: ddfsblocks
    index_filename: ddfsidx

And I migrate the filesystem using the *migrateddumbfs* command::

    # migrateddumbfs -v -p /s0/tmp/ddumbfs.save /s0/tmp/ddumbfs
    mounting destination: /s0/tmp/ddumbfs
        file_header_size 16
        hash TIGER
        hash_size 24
        block_size 4096
        index_block_size 4096
        node_overflow 1.30
        reuse_asap 0
        auto_buffer_flush 60
        auto_sync 120
        partition_size 251658240
        block_count 61440
        addr_size 2
        node_size 26
        node_count 80659
        node_block_count 512
        freeblock_offset 65536
        freeblock_size 7680
        node_offset 131072
        index_size 2228224
        index_block_count 544
        root_directory ddfsroot
        block_filename ddfsblocks
        index_filename ddfsidx
    mounting source: /s0/tmp/ddumbfs.save
    check source and destination compatibility
    == migrating index
    migrate bit list
    migrating free block list
    == migrating file
    == Migrated 5 files in 0.0s
    don't forget to handle the blockfile manually

Now I must manually handle the *BlockFile*::

    # mv /s0/tmp/ddumbfs.save/ddfsblocks /s0/tmp/ddumbfs/ddfsblocks
    
Thats it ! I can mount the filesystem and check once more for the 
filesystem integrity::

    # ddumbfs /ddumbfs -o parent=/s0/tmp/ddumbfs -o nodio
        file_header_size 16
        hash TIGER
        hash_size 24
        block_size 4096
        index_block_size 4096
        node_overflow 1.30
        reuse_asap 0
        auto_buffer_flush 60
        auto_sync 120
        partition_size 251658240
        block_count 61440
        addr_size 2
        node_size 26
        node_count 80659
        node_block_count 512
        freeblock_offset 65536
        freeblock_size 7680
        node_offset 131072
        index_size 2228224
        index_block_count 544
        root_directory ddfsroot
        block_filename ddfsblocks
        index_filename ddfsidx
    hash:      TIGER
    direct_io: 0 disable
    reclaim:   95
    writer pool: 2 cpus
    root directory: /s0/tmp/ddumbfs/ddfsroot
    blockfile: /s0/tmp/ddumbfs/ddfsblocks
    indexfile: /s0/tmp/ddumbfs/ddfsidx
    # testddumbfs -o C -B 4096 -S 50M -f -m 0x0 -s 1 /ddumbfs/file1
    # testddumbfs -o C -B 4096 -S 50M -f -m 0x0 -s 2 /ddumbfs/file2
    # testddumbfs -o C -B 4096 -S 50M -f -m 0x0 -s 3 /ddumbfs/file3
    # testddumbfs -o C -B 4096 -S 15M -f -m 0x0 -s 4 /ddumbfs/file4
    # testddumbfs -o C -B 4096 -S 15M -f -m 0x0 -s 5 /ddumbfs/file5

That's it
 