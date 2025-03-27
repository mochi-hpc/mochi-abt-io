#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.67])
AC_INIT([abt-io], [0.8.0], [],[],[])
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

dnl
dnl Verify pkg-config
dnl
PKG_PROG_PKG_CONFIG
if test "x$PKG_CONFIG" == "x"; then
   AC_MSG_ERROR([Could not find pkg-config utility!])
fi

AC_ARG_ENABLE(coverage,
              [AS_HELP_STRING([--enable-coverage],[Enable code coverage @<:@default=no@:>@])],
              [case "${enableval}" in
                yes) enable_coverage="yes" ;;
                no) enable_coverage="no" ;;
                *) AC_MSG_ERROR(bad value ${enableval} for --enable-coverage) ;;
               esac],
              [enable_coverage="no"]
)

if test "$enable_coverage" = "yes" ; then
    CPPFLAGS="$CPPFLAGS --coverage -O0"
    LDFLAGS="--coverage -lgcov"
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

AC_ARG_ENABLE(bedrock,
    [AS_HELP_STRING([--enable-bedrock],[Enable bedrock support @<:@default=no@:>@])],
    [case "${enableval}" in
     yes) enable_bedrock="yes" ;;
     no) enable_bedrock="no" ;;
     *) AC_MSG_ERROR(bad value ${enableval} for --enable-bedrock) ;;
     esac],
    [enable_bedrock="no"]
)
AM_CONDITIONAL(ENABLE_BEDROCK, test x$enable_bedrock = xyes)
if test "$enable_bedrock" = "yes"; then
    PKG_CHECK_MODULES(BEDROCK, bedrock-module-api)
    AC_DEFINE(ABTIO_USE_BEDROCK, 1, [Bedrock support enabled.])
    ABTIO_USE_BEDROCK=1
    CPPFLAGS="$BEDROCK_CPPFLAGS $CPPFLAGS"
    CXXFLAGS="$BEDROCK_CXXFLAGS $BEDROCK_CFLAGS -std=c++17 $CXXFLAGS"
    CFLAGS="$BEDROCK_CFLAGS $CFLAGS"
else
    ABTIO_USE_BEDROCK=0
fi
AC_SUBST(ABTIO_USE_BEDROCK)


AC_MSG_CHECKING([for fallocate])
AC_TRY_LINK([
#define _GNU_SOURCE
#include <fcntl.h>
], [
int ret = fallocate(0, 0, 0, 0);
],
AC_MSG_RESULT(yes)
AC_DEFINE([HAVE_FALLOCATE], [], [Define if fallocate available])
CFLAGS="-D_GNU_SOURCE $CFLAGS"
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
CFLAGS="-D_GNU_SOURCE $CFLAGS"
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

        # see if liburing includes a probe operation (mandatory; we'll rely
        # on to check kernel capabilities at runtime)
        #AC_MSG_CHECKING([for probe support in liburing])
        AC_CHECK_FUNC([io_uring_get_probe],[],
         [AC_SEARCH_LIBS([io_uring_get_probe], [uring],
          [], [AC_MSG_ERROR(mochi-abt-io requires a version of liburing with io_uring_get_probe support)])]
        )
        PC_REQUIRES="argobots json-c liburing"
else
        PC_REQUIRES="argobots json-c"
        USE_LIBURING=0
fi
AC_SUBST([PC_REQUIRES], ["$PC_REQUIRES"])
AC_SUBST(USE_LIBURING)
AM_CONDITIONAL([BUILD_LIBURING], [test "x${build_liburing}" = xyes])

AC_CONFIG_FILES([Makefile maint/abt-io.pc])
AC_OUTPUT

if test "x$NONCOMPLIANT_IO" = "x1" ; then
    AC_MSG_WARN([This platform lacks O_DIRECT and/or mkostemp().  All code
    should still compile and pass make check tests, but behavior and
    performance results in example programs may not match other platforms.])
fi
