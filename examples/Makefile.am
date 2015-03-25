exampledir = $(datarootdir)/doc/@PACKAGE@-@VERSION@

AM_CFLAGS = -g

EXTRA_DIST = gprof-helper.c
noinst_HEADERS =  gsha1.h
noinst_PROGRAMS = cmphash rndblock fsx-linux cmpspinmutex cmpmem
noinst_DATA = gprof-helper.so

cmphash_SOURCES = cmphash.c gsha1.c gsha1.h
cmphash_LDADD = -lmhash

rndblock_SOURCES = rndblock.c

fsx_linux_SOURCES = fsx-linux.c

cmpspinmutex_SOURCES = cmpspinmutex.c
cmpspinmutex_LDADD = -lpthread

cmpmem_SOURCES = cmpmem.c

gprof-helper.so: gprof-helper.c
	gcc -shared -fPIC gprof-helper.c -o gprof-helper.so -lpthread -ldl
 
