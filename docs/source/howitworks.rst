.. ddumbfs howitworks


How it works
============

De-duplication
--------------

**ddumbfs** do de-duplication at block level. Before to write a block of data, 
**ddumbfs** check if an identical block was already saved on the disk.
It calculate the SHA1 or the TIGER *hash* of the block and search the index
for it. If the *hash* is found, the reference to the
already stored block is used, else the block is saved and the index updated.


Filesystem in Userspace
-----------------------

**ddumbfs** use `FUSE <http://fuse.sourceforge.net>`_ 
because it was easy to write the first prototypes and it has proved
to be reliable. The plan is to write a *kernel module*.

Underlying filesystem
---------------------

**ddumbfs** use an *underlying filesystem* to handle the directory 
tree and filenames. Create a directory somewhere on your disk and **ddumbfs** will 
use it as the **Parent** directory to store its files and directory tree. 
You'r not supposed to access these files yourself, you must access them through 
the mounted filesystem or using the **cpddumbfs** utility when the filesystem is offline.

A file in the *UFS* (for short) is a list of the addresses and hashes of the
blocks composing the file. The size of the file is stored at the beginning 
of the file in its 16bytes header. 

.. image:: /_static/file_format.png

**ERRATUM**: the header is 16bytes long and start with the 8bytes "DDUMBSFF",
then come the *size* in the remaining 8bytes.

You can access the *UFS* when the *volume* is offline or even online to make 
some simple tasks (this is not recommended, but I explain *how it works* here):     

- Create, remove or rename a directory.
- Remove, rename or move a file **inside** the *volume*.
- Copy a file **inside** the *volume* (not across another filesystem).
- Create an empty file (using *touch* for example) .

**DON'T** copy files across filesystems ! Its content would be useless and
could even disturb **ddumbfs** if accessed.  

Here is the structure of the **Parent** directory and the 
first level of the **root** directory::

    --- Parent
        |-- ddfs.cfg     the configuration file, with parameters of the filesystem
        |-- .autofsck    If present at mount time, the filesystem is fully checked
        |-- ddfsblocks   the Block file (default location)
        |-- ddfsidx      the Index file (default location)
        | 
        `-- ddfsroot     The root of the file system
            |
            |-- .ddumbfs     The system directory to communicate with the program 
            |    |-- stats          a read gets stats from the fs
            |    |-- stats0         a read gets stats and reset them
            |    `-- reclaim        a read reclaims free block in the volume
            |
            |-- your files          where your files and directories live
            `-- your directories 
        
    
The Block file
--------------

The *block file* is usually stored into the *parent* directory 
of the *UFS* as **ddfsblocks**, but it can be 
stored somewhere else or even use a block device to improve speed.

Blocks are stored in the *blocks file* in sequential order, next free block is used.

After file deletion, blocks must be *reclaimed* using the **reclaim** procedure,
to make them available to store new data. A block can be **relaimed** only 
if it is not referenced by any files.

Block size are from 4k up to 128k by power of 2. The two first block are reserved.
 

The Index file
--------------

The Index file is in 3 parts:

- The header, starting with magic string **DDUMBFSI** is 4K width.
- The free block list. One bits per blocks in the *block file*.
- The node table.
  
A *node*, links the *hash* of a block to its *address* inside the *block file*.
The hash is usually a *SHA1* (160bits = 20bytes) or a *TIGER* ( 192bits = 24bytes).
The address size can vary depending the number of block inside the *block file*.
Usually *addresses* are 3 or 4bytes long.
The *hash function* uses the first bits of the hash multiplied by an *overflow* 
factor to lay out some free space and reduce collision. In case of collision
hash are ordered using the usual *binary* order.

Because of the good balancing of the *hash* and of the *overflow* factor,
a hash can be searched in only one read (99% of the time) and the
index can be updated in only one write. 
Despite these IOs limited to the minimum, their are mostly *random* and would
made a *caching* system crazy and useless if it cannot fit entirely in it or 
at least the biggest part ! **ddumbfs** try to lock the index in memory
at startup to maximize the speed. 

As you can see their is not reference counter, this is why **ddumbfs** is unable 
to free the space when block are deleted or modified. The *reclaim* procedure
must be started when needed. 
 
For example, if you use 128K blocks then the size of the index is about 390Mo
per Tera byte of storage.

     
The files
---------

User's files are stored on the *UFS* as a list of nodes (hash and address).
The hash is not required but allows to verify the file integrity and help 
a lot in the process of recovery. :doc:`cpddumbfs <man/cpddumbfs>` can be used 
to test the integrity of one particular file. The *address* allows to read 
the block directly from the *block file* without the help of the index.
 

.. _reclaim:

Reclaim free space
------------------
Removing files, doesn't free blocks because **ddumbfs** don't maintains a
reference counter and blocks can be used by multiple files.
To free blocks, you must run the **reclaim** procedure.

This procedure inspects all files to make a list of all used blocks. 
Block that are not used anymore are removed from the index and marked as free
in the list of free blocks. This procedure is light fast and requires less than one minute 
(and maybe few seconds on fast system) per Tera bytes. 
The **reclaim** can be done when the filesystem is online and even when writing on it.

The *reclaim* procedure impact the file system performance as any other
process would do when writing to the filesystem at the same time. 
 

Filesystem Sizing
-----------------

This paragraph is boring, if you don't need it, skip it !

This calculation is done automatically by :doc:`mkddumbfs <man/mkddumbfs>`
at volume creation, but this can help you to understand *how it works*,
and help when partitioning your disk.

The size you allocate to the filesystem is split into 3 parts:

- The *Blocks file*, holding data blocks. The biggest part.
- The *Index file*, holding indexes and other stuff to manage the filesystem.
- The *UFS*, owning the directory tree and filenames.

Here is a sample to get you an idea about how to size the filesystem parts.
I have a 300Go hard disk, in fact 300090728448 bytes and then only 279Go.
I need to split this space in 3:

The blocks file
^^^^^^^^^^^^^^^
I'll allocate 256Go for the *Blocks file* because this is a power of 2 (and easier for calculation).

The index
^^^^^^^^^
I want to use 64K blocks and an overflow factor of 1.3. 

-256Go / 64K = 4M blocks. To address theses block I need 22bits (2^22=4M) 
and I can store these in 3bytes (24bits).
-Then a node requires 23bytes = 20bytes (for the SHA1 hash) + 3bytes (for the block address).
-The node index requires about 120Mb = 4M * 1.3 * 23b.
-The free block list use 1bits per block and requires 512Kb = 4M / 8.
-The index will need about 121Mb = 512Kb + 120Mb.

The Underlying filesystem
^^^^^^^^^^^^^^^^^^^^^^^^^^

I want to backup my 50Go server and hope to keep up to 100 days of archives.
Wbadmin generate about 20 files per backup, then the *UFS* must 
be able to hold about 2000 files.

Each file requires at least one inode (256bytes) and one block for the data (4Kb), 
say 5Kb to take care of the directory structure. Each block address need 3bytes 
and could be linked up to 100 times.

==========   ========================================   ===========   
==========   ========================================   ===========   
Files         2000 files * 5k                              10Mo
Blocks        4M block * (3+20)bytes * 100 references      1.2Go
**Total**     for the underlying filesystem              **1.3Go**
==========   ========================================   ===========   

