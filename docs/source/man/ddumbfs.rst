:orphan:

ddumbfs manual page
=======================

Synopsis
--------

**ddumbfs** [-C] -o <mount options> <mount-point>

Description
-----------

**ddumbfs** mount a *ddumbfs* filesystem

*ddumbfs* is a filesystem that performs inline data deduplication.
*ddumbfs* uses the SHA1 or TIGER hash algorithm to compare the blocks of data

The *parent-directory* is the directory containing the *ddfs.cfg* file and 
*ddfsroot* directory.

Options
-------
.. program:: fsckddumbfs

.. option:: -h, --help

    Show help message and exit.

.. option:: -C

    Do a filesystem check and quit without mounting the filesystem.

.. option:: -o <opt[,opt...]>

    mount options:

        *parent=<parent-directory>*
       
            **parent-directory** is the directory where are the *ddfs.cfg* 
            file and *ddfsroot* directory. This option is **mandatory**

        *pool=<num>*
       
            **num** is the number of CPUs to allocate to the pool of *writers*.
            Use **num=0** to *disable* the pool and let each process write data itself.
            
            If **num>0** this the the number of writer to start. Don't use value
            bigger than 2*CPUs.
            
            If **num<0** then the formula -**num** /100*N°CPUs is used.
             
            For example if you have a *quad core*:
            
            =====  ========  =====  ========
             num   N° cpus    num   N° cpus
            =====  ========  =====  ========
             -25   5*0.25=1   -100  4*1.00=4
             -50   4*0.50=2   -150  4*1.50=6
            =====  ========  =====  ========
            
            default is -100              

        *[no]lock_index*
        
            Default is to lock the index in memory. This will speed up the 
            filesystem, because access to the index don't require access to the disk.
            If the index to too big to stay in memory, the lock will fail but
            the mount will continue. Read special file */.ddumbfs/stats* to
            get the status of the mounted filesystem.
              
        *[no]dio*
        
            Enable or disable *diretc io* access to the *Block File*. Use of *Direct io* 
            can give a small performance improvement when *Block File* is a block device,
            and often give **poor** performance when it is a regular file.
            Default is to enable *direct io* when the *Block File* is a block device. 
            The idea is to avoid to pollute the cache with data that will probably be used 
            only once. This is good for file archiving and backups. 
            When blocks are small ( 4k ), disable *direct io* will increase performance. 
            
        *reclaim=<num>*
       
            **num** is the disk usage in % above what a *reclaim* is managed.
            If disk usage continues to increase then a *reclaim* is managed
            every *half way* up to 100%. Default is 90.  

        *check*
        
            force a filesystem check at startup

    Read the fuse documentation for other fuse related options.

Examples
--------

    Mounting a ddumbfs is very simple::
  
        ddumbfs -o parent=/data/ddfs /ddumbfs


    To reclaim free space after file deletion, you have to start the reclaim 
    procedure. Just access the **special** *reclaim* file::

        cat /ddumbfs/.ddumbfs/reclaim

    You can display statistics::

        cat /ddumbfs/.ddumbfs/stats


See also
--------

:manpage:`fsckddumbfs(1)`, :manpage:`mkddumbfs(8)`, :manpage:`cpddumbfs(1)`

Author
------

Alain Spineux <alain.spineux@gmail.com>
                                       