:orphan:

alterddumbfs manual page
========================

Synopsis
--------

**alterddumbfs** [options] *<parent-directory>*

Description
-----------

**alterddumbfs** create anomalies in an offline ddumbfs filesystem, 
**for testing only**.

The *parent-directory* is the directory containing the *ddfs.cfg* file and 
*ddfsroot* directory.

Options
-------

.. program:: alterddumbfs

.. option:: -h, --help

    show this help message and exit
            
.. option:: -v, --verbose         

    be more verbose and display progress
    
.. option:: -l, --lock_index      

    lock index into memory (increase speed)
    
.. option:: -f, --file            

    create funny file
    
.. option:: -n, --random-seed=<VAL> 

    initialize random seed
    
.. option:: -i, --index=<NUM>       

    alter index

.. option:: -s, --swap-consecutive=<NUM>
            
    swap non empty consecutive nodes in the index

.. option:: -S, --swap-random=<NUM>
    
    swap non empty random nodes in the index

.. option:: -r, --reset-node=<NUM>
    
    reset to zero non empty nodes in the index

.. option:: -d, --duplicate-node=<NUM>
    
    duplicate non empty nodes in empty random place

.. option:: -p, --duplicate-inplace=<NUM>
    
    duplicate non empty nodes just after itself

.. option:: -a, --swap-addr=<NUM>
    
    exchange addresses of two non empty nodes in the index

.. option:: -c, --corrupt-node=<NUM>
    
    write one random byte anywhere in the index

.. option:: -u, --unexpected_shutdown
    
    simulate a crash or a powercut
    
.. option:: -I, --index-ops=<OPS>  

    alter index file
    
        - OPS = delete: delete the file
        - OPS = magic: put MAGIC at blank
        - OPS = truncate: truncate the file at mid size
        - OPS = empty: truncate file
        
 .. option:: -B, --block-ops=<OPS>
 
    alter block file, see **--index-ops** for possible operations
    
Example
-------

For example, to test if **fsckddumbfs** will detect and correct addresses 
the swap of some addresses inside the index::

    # umount /l0/ddumbfs/
    # alterddumbfs -a 2 /l0/ddumbfs/
    # fsckddumbfs -C /l0/ddumbfs
    use asynchronous io
    root directory: /l0/ddumbfs/ddfsroot
    blockfile: /l0/ddumbfs/ddfsblocks
    indexfile: /l0/ddumbfs/ddfsidx
    == Check index
    == Checked 983 nodes in 0.0s
    == Check block hash
    == Checked 12 blocks in 0.0s
    == Check unmatching 2 block
    == Checked 2 unmatching blocks in 0.0s
    == Read files
    == Read 1 files in 0.0s
    == Summary   
    Used blocks          :       12
    Free blocks          :      148
    Index errors         :        0 OK
    Index blocks         :       12
    Index blocks dup     :        0 OK
    Block errors         :        2 ERR
    Files                :        1
    Files errors         :        2 ERR
    Files blocks         :       12
    Files lost blocks    :        0 OK
    Tested hashes        :        0
    Wrong hashes         :        0 OK
    Diff index vs files  :        0 OK
    Diff blocks vs files :        0 OK
    Filesystem status    : ERROR
    # fsckddumbfs -n /l0/ddumbfs
    use asynchronous io
    root directory: /l0/ddumbfs/ddfsroot
    blockfile: /l0/ddumbfs/ddfsblocks
    indexfile: /l0/ddumbfs/ddfsidx
    02:40:45 INF Repair node order, fixed 0 errors.
    02:40:45 INF Update index from files.
    02:40:45 INF calculate hash for block addr=2
    02:40:45 INF calculate hash for block addr=5
    02:40:45 INF Read 1 files in 0.0s.
    02:40:45 INF 12 blocks used in files.
    02:40:45 INF 2 blocks have been added to index.
    02:40:45 INF ddfs_load_usedblocks
    02:40:45 INF 12 blocks used in nodes.
    02:40:45 INF 2 suspect blocks in nodes.
    02:40:45 INF Resolve Index conflicts.
    02:40:45 INF 2 nodes fixed.
    02:40:45 INF Fix files.
    02:40:45 INF Fixed:0  Corrupted:0  Total:1 files in 0.0s.
    02:40:45 INF Deleted 0 useless nodes.
    02:40:45 INF blocks in use: 12   blocks free: 148.
    # fsckddumbfs -C /l0/ddumbfs
    use asynchronous io
    root directory: /l0/ddumbfs/ddfsroot
    blockfile: /l0/ddumbfs/ddfsblocks
    indexfile: /l0/ddumbfs/ddfsidx
    == Check index
    == Checked 983 nodes in 0.0s
    == Check block hash
    == Checked 12 blocks in 0.0s
    == Check unmatching 0 block
    == Checked 0 unmatching blocks in 0.0s
    == Read files
    == Read 1 files in 0.0s
    == Check 'unexpected shutdown' blocks: 0.
    == Summary   
    Used blocks          :       12
    Free blocks          :      148
    Index errors         :        0 OK
    Index blocks         :       12
    Index blocks dup     :        0 OK
    Block errors         :        0 OK
    Files                :        1
    Files errors         :        0 OK
    Files blocks         :       12
    Files lost blocks    :        0 OK
    Tested hashes        :        0
    Wrong hashes         :        0 OK
    Diff index vs files  :        0 OK


See also
--------

:manpage:`ddumbfs(1)`, :manpage:`fsckddumbfs(8)`


Author
------

Alain Spineux <alain.spineux@gmail.com>

