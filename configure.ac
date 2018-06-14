#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.67])
AC_INIT([abt-io], [0.1], [],[],[]) 
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

AC_CONFIG_FILES([Makefile maint/abt-io.pc])
AC_OUTPUT
 
if test "x$NONCOMPLIANT_IO" = "x1" ; then
    AC_MSG_WARN([This platform lacks O_DIRECT and/or mkostemp().  All code
    should still compile and pass make check tests, but behavior and
    performance results in example programs may not match other platforms.])
fi
