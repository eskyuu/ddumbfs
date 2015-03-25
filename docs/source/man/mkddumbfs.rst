:orphan:

mkddumbfs manual page
=====================

Synopsis
--------

**mkddumbfs** [options] *parent-directory*

Description
-----------

**mkddumbfs** initialize a ddumbfs filesystem on a existing
filesystem, inside the *parent-directory*.

The *parent-directory* will contain the *ddfs.cfg* file and *ddfsroot* 
directory and eventualy the *Index File* and the *Block File*.

Options
-------

.. option:: -h, --help

    Show help message and exit.

.. option:: -f, --force

    Reply yes to any interactive question.

.. option:: -a, --asap

    Re-use reclaimed blocks as soon as possible. This help to keep the *block file*
    small but without warranty.  
    This is mostly useless when storing the *block file* on a block device.

.. option:: -i <INDEXFILE>, --index=<INDEXFILE>

    Specify the index filename or device. The index file can be
    located somewhere else than in the *parent-directory*, event on
    another device. It can also be a block device.

.. option:: -b <BLOCKFILE>, --block=<BLOCKFILE>

    Specify the block filename or device. The block file can be
    located somewhere else than in the *parent-directory*, event on
    another device. It can also be a block device.

.. option:: -H <HASH>, --hash=<HASH>

    Specify the hash. Supported hash are SHA1, TIGER128, TIGER160, TIGER.
    SHA1 is 160bits and performs better on old PIV. TIGER is 192 bits. TIGER128
    is not faster than other TIGER* but just requires less space in the index. 

.. option:: -s <SIZE>, --size=<SIZE>

    The size of the block file. If the block file is a block device and this
    option is omitted or set to 0, the size of the device will be used instead.

.. option:: -B <BLOCK_SIZE>, --block-size=<BLOCK_SIZE>

    The block size (default is 128k). Between 4k and 128k, a power of 2.

.. option:: -o <OVERFLOW>, --overflow=<OVERFLOW>

    The overflow factor (default is 1.3). This is the extra space
    allocated to the hash table in the index to handle collision.
    Value must be between 1.1 and 2.0

Examples
--------
Initialize a ddumbfs filesystem of 50G in */l0/ddumbfs*::
    
    mkddumbfs -s 50G /l0/ddumbfs

Idem bud select a different block size, 64k instead of 128k::
    
    mkddumbfs -B 64k -s 50G /l0/ddumbfs

Initialize a ddumbfs filesystem in */l0/ddumfs* using a block
devices to store the blocks. The size of the block device
will be used as the size of the filesystem::

    mkddumbfs -b /dev/sdb3 /l0/ddumbfs

See also
--------

:manpage:`ddumbfs(1)`, :manpage:`fsckddumbfs(8)`, :manpage:`cpddumbfs(1)`


Author
------

Alain Spineux <alain.spineux@gmail.com>

