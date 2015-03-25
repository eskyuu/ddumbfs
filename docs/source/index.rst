.. ddumbfs documentation master file, created by
   sphinx-quickstart on Fri Sep 16 20:15:25 2011.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

ddumbfs's homepage
==================

**ddumbfs** is a fast inline **deduplication** filesystem for Linux. Based on  
`FUSE <http://fuse.sourceforge.net>`_  and released under the GNU GPL. 
Deduplication is a technique to avoid data duplication on disks and to increase its virtual capacity. 

Documentation
-------------
    
* :doc:`Getting started <getstarted>`
* :doc:`How it works <howitworks>`
* :doc:`Performances and tuning <performance>`
* :doc:`Recovery <recovery>`
* :doc:`Regression suite <regression_suite>`
* :doc:`Resizing <resize>`
* :ref:`man_pages`
* :doc:`FAQ <faq>`
* *How to backup a* **ddumbfs** *volume* (next to come)

     
Features
--------

* Works on Linux
* ddumbfs is a userspace filesystem using `FUSE <http://fuse.sourceforge.net>`_
* Increase virtual disk capacity using online-deduplication 
* deduplication at block level, from 4k up to 128k
* Fast, thanks to it very simple index design
* Not limited in the size, can dedup Peta bytes 
* VMware support: can be exported through NFS and be used to run or backup VMs
* Index can be locked into memory for maximum performance, or stored on SSD drive   
    
Requirements
------------

**ddumbfs** requires **fuse** and **mhash** libraries.

*fuse*>=2.7 is required. 
If you plan to export your filesystem through **NFS** then fuse>=2.8 and linux>=2.6.29
are recommended.  

To compile **ddumbfs** you need as usual: **make** and **gcc**, the headers for
**fuse** and **mhash** library and **pkg-config**.

Here are the corresponding package for **redhat** and **debian** based distributions:

    ==============  =============================   ===================================     
    Distribution    To run                          To install from source
    ==============  =============================   ===================================     
    **redhat**      fuse fuse-libs mhash            fuse-devel mhash-devel pkgconfig
    **debian**      libfuse2 fuse-utils libmhash2   libfuse-dev libmhash-dev pkg-config 
    ==============  =============================   ===================================     

RedHat 6 don't include *mhash* in the default repository anymore. You can find them in the
**EPEL** repository. 
You can the *EPEL* repository using:: 

    # rpm -Uvh http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-7.noarch.rpm


Install from source
-------------------

To install from source, extract the sources, *cd* into directory 
*ddumbfs-X.X* and run the commands::

    ./configure
    make
    make install

Download
--------

Sources can be found `here <http://www.magiksys.net/download/ddumbfs>`_

In a near future RPM packages for last Fedora and Centos should be available.

Limits
-------

The data structure is not limited. The size of the block addresses is set at 
filesystem creation time. Size can be 3, 4, 5 bytes width or even more, their is no limit.

Anyway current architectures use 64bits (8bytes) integers. These addresses
are stored and handled in such 64bits integers. 

The result of the multiplication of two 32 bits values requires at max 64bits.
Then, it is not always possible to use value above 32bits on 64bits architecture.
Hopefully most of the operations are additions or bit shift that should support 
addresses bigger than 32bits. And for the few critical parts where 64bits
would not be enough for intermediate results, it should be possible to tweaks 
the code to make it works.

Then with few modifications it should be easy to handle addresses bigger than 32bits.
It is even maybe already possible now, because I take care to order operations to
minimize overflows. But I have not tested.
  
Using 32bits addresses: 

    ============  =====================  ==============
    block size    Max *BlockFile* Size   Max Index Size
    ============  =====================  ==============
    4K            16To                   146Go
    128k          512To                  146Go
    ============  =====================  ==============
    

Even with 4K block and a dedup factor of 10, it would be possible to
store up to 160To in one single filesystem. This means 4 days to fill
the volume at 512Mo/s constant throughput. 
    
Having 40bits addresses (5bytes) would allow to go beyond the PetaBytes,
but where will you store the 37To of the *index* ?  
   

A filesystem suitable for backups
----------------------------------

Even if **ddumbfs** is fast and reliable and could be used as any multi-purpose
filesystem, it has been designed with *backups* in mind. 

A **backup** share 99% of identical data with previous and next backups. 
*Differential* techniques exist to reduce the amount of data to save, 
but complicates backup handling and restore procedure. 
The idea of **ddumbfs** is to do the difference at the filesystem level 
to allow backup tools to take benefit of space saving without constraints.

Moreover accesses to the backup are less demanding 
than to the online data. Most backup tools generate big archive files 
in one continuous write. Read access to these file are infrequent 
and random read/write are really uncommon. 
Usually backups are done during the night and increase of 
the backup time to increase the retention period is valuable 
until the backup finishes before morning. 
Regarding these backup requirements, we can imaging to increase 
disk capacity at cost of some flexibility shortage. 
The proof, is we have used and still use magnetic tapes 
for backup despite of its lack of flexibility !

Modern filesystems looks overkill to just store backups or archives ! 
This is where a dedicated filesystem like **ddumbfs** could bring more to 
backup applications !

With it :ref:`writer_pool` and asynchronous features, **ddumbfs** can even
transcend the speed of other filesystems ib througput, not in iops. 

How safe is it
--------------

Everything as been done to make **ddumbfs** as reliable as possible:

* *ddumbfs* succeeds all the tests from the doc:`regression suite <regression_suite>`.
* *ddumbfs* detect when it was uncleanly unmounted and run a quick *filesystem check* at startup.
* *ddumbfs* is released with :doc:`fsckddumbfs <man/fsckddumbfs>` that can deeply check, repair and report all detected, corrected and unrecoverable errors.  


How **ddumbfs** is different
----------------------------

Other de-duplication filesystem alternatives_ exist.

- minimalist, it does de-duplication only and does it well.
- optimized to works without any memory cache, but works faster with caching or SSD drives. 
- **fast**, very fast, even faster !
- tools to access an offline *volume*.
- tools to repair, rebuild and test the filesystem integrity.
- access time doesn't increase with disk usage.
- no kind of garbage collector that *freeze* the filesystem at runtime.
- reliable, at lot of tests in the :doc:`Regression suite <regression_suite>` to proof it.


.. _man_pages:

MAN pages
---------

    :doc:`ddumbfs <man/ddumbfs>`: mount a ddumbfs filesystem
         
    :doc:`mkddumbfs <man/mkddumbfs>`: initialize a ddumbfs filesystem 
      
    :doc:`fsckddumbfs <man/fsckddumbfs>`: check and repair a ddumbfs filesystem 
        
    :doc:`cpddumbfs <man/cpddumbfs>`: upload and download file from a offline ddumbfs filesystem
    
    :doc:`migrateddumbfs <man/migrateddumbfs>`: resize an offline ddumbfs filesystem
    
    :doc:`alterddumbfs <man/alterddumbfs>`: create anomalies in an offline ddumbfs filesystem, **for testing only**
    
    testddumbfs: to do some integrity and performance tests
    
    queryddumbfs: allow to query the filesystem about hashes, blocks and files.

Download
--------

You can download sources and binaries at `here <http://www.magiksys.net/download/ddumbfs>`__.

**RPM** packages are available for CentOS 6 and Fedora 17.
**DEB** packages  are available Ubuntu 12.04 and 12.10.  


Contact
-------

Ask your questions in this `forum <http://forum.magiksys.net/viewforum.php?f=17>`__.

.. _alternatives:

DE-Duplication alternatives
---------------------------

*  `lessfs <http://www.lessfs.com>`_ : include a lot of features: compression, replication.  
*  `SDFS <http://www.opendedup.org>`_ : works on windows too, very good marketing.  

License
-------

   Copyright (C) 2011 Alain Spineux

   You can redistribute **ddumbfs** and/or modify it under the terms of either
   (1)  the  GNU  General Public License as published by the Free Software
   Foundation; or (2)  obtain  a  commercial  license  by  contacting  the
   Author.   You  should  have  received  a copy of the **GNU General Public
   License**   along    with     this     program.      If     not,     see
   http://www.gnu.org/licenses/.

   **ddumbfs**  is distributed in the hope that it will be useful, but WITHOUT
   ANY WARRANTY; without even the implied warranty of  MERCHANTABILITY  or
   FITNESS  FOR  A PARTICULAR PURPOSE.  See the GNU General Public License
   for more details.


Contents:

.. toctree::
    :maxdepth: 2

    getstarted
    performance
    recovery
    regression_suite
    howitworks
    
Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

