# -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.63])
AC_INIT([ddumbfs], [1.1], [alain.spineux@gmail.com])
AC_CONFIG_AUX_DIR([build-aux])
AM_INIT_AUTOMAKE([foreign -Wall -Werror])
AC_CONFIG_SRCDIR([src/ddumbfs.c])

AC_PROG_CC
AM_PROG_CC_C_O

AC_CONFIG_HEADERS([config.h])
AC_CONFIG_FILES([Makefile src/Makefile doc/Makefile examples/Makefile man/Makefile scripts/Makefile rpmbuild/Makefile rpmbuild/ddumbfs.spec debbuild/Makefile])
AC_C_BIGENDIAN
AC_CHECK_HEADER(mhash.h,
                [AC_DEFINE([HAVE_MHASH_H], [], [MHASH provide SHA1 and TIGER hash])],
                [AC_MSG_ERROR([Please install mhash])])
AC_CHECK_HEADERS([syslog.h mhash.h fuse.h])

PKG_CHECK_MODULES([libfuse], [fuse >= 2.7.0] )

AC_OUTPUT

