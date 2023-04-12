#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.67])
AC_INIT([abt-io], [0.5.1], [],[],[])
AC_CONFIG_MACRO_DIRS([m4])
LT_INIT

AC_CANONICAL_TARGET
AC_CANONICAL_SYSTEM
AC_CANONICAL_HOST

AM_INIT_AUTOMAKE([foreign subdir-objects -Wall])

# we should remove this soon, only needed for automake 1.10 and older
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AC_CONFIG_SRCDIR([README.md])
AC_CONFIG_HEADERS([abt-io-config.h])

# Checks for programs.
AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_CXX
AC_PROG_CXXCPP

AC_PROG_MKDIR_P

AC_REQUIRE_CPP

AC_CHECK_SIZEOF([long int])

AC_MSG_CHECKING([for openssl])
AX_CHECK_OPENSSL(
AC_MSG_RESULT(yes)
,
AC_MSG_ERROR([Could not find openssl]))

dnl
dnl Verify pkg-config
dnl
PKG_PROG_PKG_CONFIG
if test "x$PKG_CONFIG" == "x"; then
   AC_MSG_ERROR([Could not find pkg-config utility!])
fi

PKG_CHECK_MODULES([ARGOBOTS],[argobots],[],
   [AC_MSG_ERROR([Could not find working argobots installation!])])
LIBS="$ARGOBOTS_LIBS $LIBS"
CPPFLAGS="$ARGOBOTS_CFLAGS $CPPFLAGS"
CFLAGS="$ARGOBOTS_CFLAGS $CFLAGS"

PKG_CHECK_MODULES([JSONC],[json-c],[],
   [AC_MSG_ERROR([Could not find working json-c installation!])])
LIBS="$JSONC_LIBS $LIBS"
dnl
dnl Note that pkg-config may report an include path that contains a
dnl "/json-c" component.  If so, strip it out.  We prefer to use an explicit
dnl subdir path in the source to to avoid potential header name conflicts
dnl with other json libraries.
dnl
JSONC_CFLAGS=`echo $JSONC_CFLAGS | sed 's/\/include\/json-c/\/include/g'`
CPPFLAGS="$JSONC_CFLAGS $CPPFLAGS"
CFLAGS="$JSONC_CFLAGS $CFLAGS"

AC_MSG_CHECKING([for fallocate])
AC_TRY_COMPILE([
#define _GNU_SOURCE
#include <fcntl.h>
], [
int ret = fallocate(0, 0, 0, 0);
],
AC_MSG_RESULT(yes)
AC_DEFINE([HAVE_FALLOCATE], [], [Define if fallocate available])
,
AC_MSG_RESULT(no))

NONCOMPLIANT_IO=""

AC_MSG_CHECKING([for O_DIRECT])
AC_TRY_COMPILE([
#define _GNU_SOURCE
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
], [
int fd = open(NULL, O_DIRECT);
], 
AC_MSG_RESULT(yes)
AC_DEFINE([HAVE_ODIRECT], [], [Define if able to open files with O_DIRECT])
,
NONCOMPLIANT_IO=1
AC_MSG_RESULT(no))

AC_MSG_CHECKING([for mkostemp])
AC_TRY_COMPILE([
#define _GNU_SOURCE
#include <stdlib.h>
], [
int fd = mkostemp(NULL, 0);
], 
AC_MSG_RESULT(yes)
AC_DEFINE([HAVE_MKOSTEMP], [], [Define if mkostemp available])
,
NONCOMPLIANT_IO=1
AC_MSG_RESULT(no))

# zlib is used for benchmark output file
CHECK_ZLIB

build_liburing=no
AC_ARG_ENABLE(liburing,
        [AS_HELP_STRING([--enable-liburing],[Enable liburing library support @<:@default=no@:>@])],
        [case "${enableval}" in
         yes) enable_liburing="yes" ;;
         no) enable_liburing="no" ;;
         *) AC_MSG_ERROR(bad value ${enableval} for --enable-liburing) ;;
 esac],
 [enable_liburing="no"]
 )
AM_CONDITIONAL(ENABLE_LIBURING, test x$enable_liburing = xyes)
if test "$enable_liburing" = "yes"; then
        PKG_CHECK_MODULES(LIBURING, liburing)
        AC_DEFINE(USE_LIBURING, 1, [LIBURING support enabled.])
        USE_LIBURING=1
        CPPFLAGS="$LIBURING_CFLAGS $CPPFLAGS"
        CFLAGS="$LIBURING_CFLAGS $CFLAGS"
        LIBS="$LIBURING_LIBS $LIBS"
        build_liburing=yes

        # see if this system also has IOSQE_ASYNC
        AC_MSG_CHECKING([for IOSQE_ASYNC])
        AC_TRY_COMPILE([
        #include <liburing.h>
        ], [
        int flag = IOSQE_ASYNC;
        ],
        AC_MSG_RESULT(yes)
        AC_DEFINE([HAVE_IOSQE_ASYNC], [], [Define if IOSQE_ASYNC is present])
        ,
        AC_MSG_RESULT(no))
else
        USE_LIBURING=0
fi
AC_SUBST(USE_LIBURING)
AM_CONDITIONAL([BUILD_LIBURING], [test "x${build_liburing}" = xyes])

AC_CONFIG_FILES([Makefile maint/abt-io.pc])
AC_OUTPUT

if test "x$NONCOMPLIANT_IO" = "x1" ; then
    AC_MSG_WARN([This platform lacks O_DIRECT and/or mkostemp().  All code
    should still compile and pass make check tests, but behavior and
    performance results in example programs may not match other platforms.])
fi
