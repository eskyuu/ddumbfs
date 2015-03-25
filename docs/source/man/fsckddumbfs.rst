:orphan:

fsckddumbfs manual page
=======================

Synopsis
--------

**fsckddumbfs** [options] *parent-directory*

Description
-----------

**fsckddumbfs** check and repair a ddumbfs filesystem

The *parent-directory* is the directory containing the *ddfs.cfg* file and 
*ddfsroot* directory.
Operations inside the **fsckddumbfs* process are done in a way that any 
unexpected interruption can be repaired by re-running the  same command. 

Options
-------

.. program:: fsckddumbfs

.. option:: -h, --help

    Show help message and exit.

.. option:: -v, --verbose

    Be more verbose. Be careful this can slow down the process when you are 
    working remotely through a slow link and that *fsckddumbfs* has a lot 
    of problem to display.  

.. option:: -p, --progress

    Display progress second by second of some part of the file system check.

.. option:: -l, --lock_index

    Lock index into memory (increase speed)

.. option:: -f, --force

    Bypass some guardrail. 
    Continue even if the *block file* don't start with the appropriate 
    *magic* signature.
    
.. option:: -c, --check

    Read-only check. (very fast, only few seconds) 
    Search any conflicting information in the index and in the files nodes. 
    This is the right option to get an idea if their is any problem. To check
    block hashes, use option **-C** bellow.

.. option:: -C, --check-block

    read-only check with block re-hashing: (slow, depend of disk usage)
    Search any conflicting information in the index and in the files nodes,
    but also re-hash all block referenced by the index and the files nodes.
    This give you an exhaustive status of the filesystem. 

.. option:: -n, --repair

    Automatically repair. (fast, this is the **recommended** option.)
    Search any conflicting information in the index and in the files nodes
    and fix them by calculating the blocks hashes when needed. Fix the index, and all
    files nodes. 
 
.. option:: -N, --repair-relaxed

    Automatically repair. (fastest, use it only if you understand what it does).
    Identical to the *-n* option above but don't check new entries from files. 
    This is useful if you have deleted your index and want to quickly rebuild it
    from files nodes without re-hashing blocks. 
    

.. option:: -r, --rebuild

    Rebuild a new index using only known used blocks. (slow, depend of disk usage) 
    Drop index and re-hash all known used blocks to re-build a new index 
    from scratch. Only blocks having a reference in files and in the used block 
    list are re-hashed. 

.. option:: -R, --rebuild-block

    Rebuild a new index by re-hashing all blocks. (slowest)
    Drop index and re-hash ALL blocks from the block file.
    Use it if some files are *corrupted* and you are desperate, maybe some 
    unallocated space in the block file could still contain the missing blocks.
    Don't have too much hope.
  
.. option:: -k, --pack

    Pack the *blockfile*. **Warning**, be sure to understand what it does
    and what it implies before to use it.
    
    This operation can slow down further sequential read of the existing data.
    Further writes can be faster.    
    
    Blocks at the end of the *blockfile* will be moved at the beginning. 
    The *index* and the *files* will be updated to match the new addresses
    of the moved blocks. If the *blockfile* is a regular file, it will be
    truncated to free unused space.
     
    The filesystem must be clean, You must use it in combination with 
    switches *-n*, *-N*, *-r* or *-R*.  


Examples
--------

Check ddumbfs filesystem located in */l0/ddumbfs*. The read-only check can
even be done on an online filesystem, but the result could be
inaccurate. To increase speed, use *-l* to lock the index in memory.::

    fsckddumbfs -c -l /l0/ddumbfs

Repair an offline ddumbfs filesystem located in */l0/ddumbfs*::

    fsckddumbfs -n -l /l0/ddumbfs


Every fix is logged in the log file (here in */l0/ddumbfs/.ddumbfs/ddumbs.log*)
and corrupted files are logged in file */l0/ddumbfs/.ddumbfs/corrupted.txt*.
 
See also
--------

:manpage:`ddumbfs(1)`, :manpage:`mkddumbfs(8)`, :manpage:`cpddumbfs(1)`


Author
------

Alain Spineux <alain.spineux@gmail.com>
