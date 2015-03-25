.. ddumbfs regression_suite

The regression suite
====================

To ensure that **ddumbfs** is reliable, some automated test are run during the 
development and before each release of a new version.  

You can find the test script *ddfstest.sh* in the source file in directory *test*.
Use option -h to get some help:: 

    # ./ddfstest.sh -h
    Usage: ddfstest.sh -h
           ddfstest.sh [ basic-options ] actions
    
    basic-options:
        [ -B BLOCK_SIZE ]      default 64k
        [ -s BLOCK_FILE_SIZE ] default 50G
        [ -H HASH ] default is TIGER, others (SHA1, TIGER160)
        [ -m MOUNT_OPTS ]      
      echo  in pool=NUM,[no]lock_index,[no]dio
    
        [ -r ] re-format filesysteme
        [ -d ] mount in debug mode
        [ -f ] mount in foreground
        [ -g ] gdb in foreground
        [ -T X_TARGET ] where to run the test
    
    actions:
        kill:       kill -9 the mount (not a test)
    
        basic:      basic copy and check
        fsx:        run fsx-linux
        pjd:        run the POSIX compliance test
        reclaim:    reclaim
        rnd:        speed test write/2write/read
             [ -n COUNT ] default 4
        esx:        speed test write/2write/read
             [ -v VMNAME ] default TTY (others: XP)
             [ -i FORMAT ] default thin (others: 2gbsparse)
        etc:        copy /etc to ddumbfs
        bonnie:     run bonnie++
        alter:      alter test
             [ -n COUNT ] default 4
        crash:      test how kill -9 ddumbfs works
        mark:       regression from Mark
             [ -t RUNTIME ] default 300
        mysql:      compile and run mysql regression
        sqlite:     compile sqlite and run regression
    
        show:       show all possible tests

The script is not well tested and not well documented.
To run a small test, be sure to be root and run:: 

    # ./ddfstest.sh -r basic
    
*-r* means create new filesystem, and *basic* the name of the test.
This will create two directories in /tmp, create a filesystem in the first one
and mount it in the second one, then copy a file into the filesystem.
Look into the mounted filesystem::
 
    # ls /tmp/ddumbfs.data
    services
       

Some tests
==========

fsx
---

`fsx <http://codemonkey.org.uk/projects/fsx/>`_ is a popular tool (for file system developer) 
that make random reads, writes and truncates operations on a file. 
*fsx* access also files using mmap facility, this generate asynchronous accesses 
(the kernel flush modified data to disk at unpredictable time) that make the test 
very interesting like if multiple processes where accessing the same file 
at the same time.

*fsx* insure that data are written and read to and from the good place,
nothing more. It help to find bugs, but cannot give any warranty.


POSIX compliance test 
---------------------

This test is done using the Pawel Jakub Dawidek’s POSIX file system test suite
released by `TUXERA <http://www.tuxera.com/community/posix-test-suite/>`_.
**ddumbfs** succeed all tests. This is the consequence of the use of the *underlying filesystem" 
and some *FUSE options*. 

This test concern mostly the *access right*.

.. _sqlite_regression_suite:

The sqlite regression suite
---------------------------

Even if *ddumbfs* is not designed to run database application, having them
successfully running on it is meaningful.
The test is more than running the *test suite*, it also include the entire 
build of *sqlite* itself.

This test, tests deeply concurrent access to files.   

   
The mysql regression suite
--------------------------
Similar to :ref:`sqlite_regression_suite` but using the mysql one. 


NFS and VMware backup
---------------------

One of the goal of ddumbfs was to be able to backup VMWare virtual machine 
from an ESX(i) server using the well known 
`ghettovcb.sh <http://communities.vmware.com/docs/DOC-8760>`_ and 
`MKSBackup <http://www.magikmon.com/mksbackup/ghettovcb.en.html>`_. 
The backup is done through NFS by *vmkfstool* that open multiple connections
and write in parallel at multiple place in the same file. 
This make it very fast but make it very stress full to and the author spend a 
lot of time to make it works.

Now it's part of the regression test.


Reclaim test
------------

The reclaim procedure reclaim blocks no more in use. The process can be run 
when the filesystem is online. To test it, files are copied and deleted before, 
meanwhile and after the reclaim. 
At the end all remaining file are compared with the source.

The reclaim procedure can also be run at regular interval when other test are
running.

.. _crash_test:

Crash test
----------

The *ddumbfs daemon* is killed using a SIGKILL signal while files are copied 
to the volume. The test check if written data and further written data are valid.

This test give a quick overview about how **ddumbfs** will behave when
an unexpected shutdown occur.


Unexpected shutdown 
-------------------

This test consist of running **ddumbfs** inside a virtual machine and poweroff
the machine while file are written to the volume. This test is identical 
to the :ref:`crash_test` above.

This test still requires some manual operations and is not run too often !  

fsck test
---------

The *alterddumbfs* tools allows to corrupt the index. Multiple type of corruption are
possible. Some are recoverable and other not.  The test corrupts the index
and let the built-in recovery run at **ddumbfs** startup repairs the errors.
( identical to a *fsckddumbfs -n* ) Some later tests check if the volume 
perform as expected after the recovery.    

The ability of *fsckddumbfs* to handle *Unrecoverable error* is evaluated manually.  

speed test
----------

The *testddumbfs* tool can generate an infinity number of random but repeatable 
files and compare the content with original random data. 
It also performs speed test for first write and second write and 
generate nice statistics.

Windows ntbackup and wbadmin test
---------------------------------

This test start a ntbackup or a wbadmin backup from
a windows host to a *ddumbfs* volume through a *samba* share.
The backup are then restored and content is compared with original data. 

This test is not yet fully automated, because of the lack of a linux version
of psexec.    

Resizing test
--------------

The resizing test is fully described in the :doc:`resize <resize>` page.
 


