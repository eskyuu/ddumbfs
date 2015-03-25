:orphan:

cpddumbfs manual page
=======================

Synopsis
--------

**cpddumbfs** [options] *source* *target*

Description
-----------

**cpddumbfs** copy from and to an offline ddumbfs filesystem

One and only one of the *source* or *target* must be inside the *ddfsroot* directory.
    
Options
-------

.. program:: cpddumbfs

.. option:: -h, --help

    show this help message and exit
    
.. option:: -v, --verbose

    display addresses and hashes from file (download only)
    
.. option:: -l, --lock_index

    lock index into memory (increase speed for large file)

.. option:: -c, --check

    check file integrity in exit code (download only)

.. option:: -f, --force
    
    download a file even if the header is corrupted or if *bad blocks*
    are found.


Example
-------

usage::

    $ cpddumbfs -l /data/ddumbfs/ddfsroot/data /tmp
    $ cpddumbfs /tmp/data /data/ddumbfs/ddfsroot
    
Remarks
-------

*cpddumbfs* is single threaded. If you have a multi-core CPU, *cpddumbfs*
will be slower than copying files to the online filesystem. By default 
*ddumbfs* mount the filesystem with a pool of thread used to calculate
block hash in parallel. 
When downloading file from the offline filesystem, *cpddumbfs* is 
as fast as the mounted filesystem.    

See also
--------

:manpage:`ddumbfs(1)`, :manpage:`mkddumbfs(8)`


Author
------

Alain Spineux <alain.spineux@gmail.com>

                        