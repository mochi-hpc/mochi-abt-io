# abt-io
abt-io is a library that provides Argobots bindings to common POSIX I/O
functions.

##  Dependencies

* argobots (origin/master):
  (git://git.mcs.anl.gov/argo/argobots.git)
* abt-snoozer (https://xgitlab.cels.anl.gov/sds/abt-snoozer)

## Building Argobots (dependency)

Example configuration:

    ../configure --prefix=/home/pcarns/working/install --enable-perf-opt \
     --disable-thread-cancel --disable-task-cancel --disable-migration \
     --enable-affinity

## Building abt-snoozer (dependency)

See abt-snoozer README.md

## Building

Example configuration:

    ../configure --prefix=/home/pcarns/working/install \
        PKG_CONFIG_PATH=/home/pcarns/working/install/lib/pkgconfig \
        CFLAGS="-g -Wall"
