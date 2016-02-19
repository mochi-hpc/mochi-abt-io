# abt-io
abt-io is a library that provides Argobots bindings to common POSIX I/O
functions.

##  Dependencies

* argobots (argobots-review repo, dev/opts-test branch):
  (git://git.mcs.anl.gov/argo/argobots-review.git)
* abt-snoozer (https://xgitlab.cels.anl.gov/sds/abt-snoozer)

## Building

Example configuration:

    ../configure --prefix=/home/pcarns/working/install \
        PKG_CONFIG_PATH=/home/pcarns/working/install/lib/pkgconfig \
        CFLAGS="-g -Wall"
