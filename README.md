# abt-io
abt-io is a library that provides Argobots bindings to common POSIX I/O
functions.

##  Dependencies

* argobots (argobots-review repo, dev/opts-test branch):
  (git://git.mcs.anl.gov/argo/argobots-review.git)
* abt-snoozer (https://xgitlab.cels.anl.gov/sds/abt-snoozer)

## Building Argobots (dependency)

Example configuration:

    ../configure --prefix=/home/pcarns/working/install --enable-perf-opt \
     --enable-aligned-alloc --enable-single-alloc --enable-mem-pool \
     --enable-mmap-hugepage --enable-opt-struct-thread --enable-opt-struct-task \
     --enable-take-fcontext --enable-ult-join-opt --disable-thread-cancel \
     --disable-task-cancel --disable-migration --enable-affinity

## Building abt-snoozer (dependency)

See abt-snoozer README.md

## Building

Example configuration:

    ../configure --prefix=/home/pcarns/working/install \
        PKG_CONFIG_PATH=/home/pcarns/working/install/lib/pkgconfig \
        CFLAGS="-g -Wall"
