:orphan:

migrateddumbfs manual page
=======================

Synopsis
--------

**migrateddumbfs** [options] *source-parent-directory* *destination-parent-directory*

Description
-----------

**migrateddumbfs** used to resize a ddumbfs filesystem.

The *source-parent-directory* and *destination-parent-directory* are directory containing 
the *ddfs.cfg* file and *ddfsroot* directory. *destination-parent-directory* must exists, 
must be empty, must have been created with the same *block_size* and *hash* and 
must be big enough to receive the data of the source. The *block file* is not modified
and not copied. You have to manually copy/move data of the *block file*.  

Options
-------

.. program:: migrateddumbfs

.. option:: -h, --help

    Show help message and exit.

.. option:: -v, --verbose

    Be more verbose.

.. option:: -p, --progress

    Display progress second by second of some part of the file system check.

.. option:: -f, --force

    bypass some guardrail
    
.. option:: -m, --migrate

    Default option for now, use it to avoid compatibility problems with
    future release that could have more features


Examples
--------

Create a new ddumbfs filesystem and migrate data to it.:: 

    fsckddumbfs -r /data/ddsource
    mkddumbfs -s 100G /data/dddestination
    migrateddumbfs -m /data/ddsource /data/ddestination
    rm /data/dddestination/ddfsblocks
    mv /data/ddsource/ddfsblocks /data/dddestination/ddfsblocks

 
See also
--------

:manpage:`ddumbfs(1)`, :manpage:`mkddumbfs(8)`, :manpage:`cpddumbfs(1)`


Author
------

Alain Spineux <alain.spineux@gmail.com>
