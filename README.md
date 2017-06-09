# abt-io
abt-io is a library that provides Argobots-aware wrappers to common POSIX I/O
functions.  The wrappers behave identically to their POSIX counterparts from
a caller's point of view, but internally they delegate I/O system calls to a
dedicated Argobots pool.  The caller is suspended until the system call
completes so that other concurrent ULTs can make progress in the mean time.

##  Dependencies

* argobots (origin/master):
  (git://git.mcs.anl.gov/argo/argobots.git)
* abt-snoozer (https://xgitlab.cels.anl.gov/sds/abt-snoozer)

## Building Argobots (dependency)

Example configuration:

    ../configure --prefix=/home/pcarns/working/install 

## Building abt-snoozer (dependency)

See abt-snoozer README.md

## Building

Example configuration:

    ../configure --prefix=/home/pcarns/working/install \
        PKG_CONFIG_PATH=/home/pcarns/working/install/lib/pkgconfig \
        CFLAGS="-g -Wall"
