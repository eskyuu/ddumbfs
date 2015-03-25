AM_CFLAGS = $(libfuse_CFLAGS) -Wall
# put libraries in LDADD instead of LDFLAGS 
# http://wiki.debian.org/ToolChain/DSOLinking#Only_link_with_needed_libraries
#AM_LDFLAGS = $(libfuse_LIBS) -lulockmgr -lmhash
LDADD = $(libfuse_LIBS) -lulockmgr -lmhash

bin_PROGRAMS = ddumbfs mkddumbfs cpddumbfs fsckddumbfs migrateddumbfs
noinst_PROGRAMS = alterddumbfs testddumbfs queryddumbfs

ddumbfs_SOURCES = ddumbfs.c ddfschkrep.h ddfschkrep.c ddfslib.c ddfslib.h bits.h bits.c xlog.h xlog.c

mkddumbfs_SOURCES = mkddumbfs.c ddfslib.c ddfslib.h bits.h bits.c xlog.h xlog.c

cpddumbfs_SOURCES = cpddumbfs.c ddfslib.c ddfslib.h bits.h bits.c xlog.h xlog.c

migrateddumbfs_SOURCES = migrateddumbfs.c ddfslib.c ddfslib.h bits.h bits.c xlog.h xlog.c

fsckddumbfs_SOURCES = fsckddumbfs.c ddfschkrep.h ddfschkrep.c ddfslib.c ddfslib.h bits.h bits.c xlog.h xlog.c

alterddumbfs_SOURCES = alterddumbfs.c ddfslib.c ddfslib.h bits.h bits.c xlog.h xlog.c

queryddumbfs_SOURCES = queryddumbfs.c ddfslib.c ddfslib.h bits.h bits.c xlog.h xlog.c

testddumbfs_SOURCES = testddumbfs.c
