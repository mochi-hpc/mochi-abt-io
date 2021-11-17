# abt-io

abt-io provides Argobots-aware wrappers to common POSIX I/O
functions.

Argobots (https://github.com/pmodels/argobots) is a threading package
that provides light-weight _non-preemptable_ user-level threads.  This
means that if an Argobots thread executes a blocking I/O system call
directly, it will block forward progress for all threads on that execution
stream until that blocking I/O system call completes.

The abt-io library addresses this problem by providing a set of wrapper
functions that delegate blocking I/O system calls to a dedicated set
of execution streams (i.e. pthreads) that perform the I/O operation on
behalf of the caller.  User level threads that invoke these wrappers
will yield control until the I/O operation completes, allowing other
user-level threads to continue execution.

If multiple I/O operations are issued at the same time, then they will make
progress concurrently according to the number of execution streams assigned
to abt-io.

This library is a companion to the Margo library (which provides similar
capability, but for the Mercury RPC library).  When used with Margo it
prevents RPC handlers (callbacks) from blocking on I/O operations, thereby
improving RPC throughput.

Margo: https://github.com/mochi-hpc/mochi-margo

##  Dependencies

* argobots (https://github.com/pmodels/argobots)

## Building Argobots (dependency)

Example configuration:

    ../configure --prefix=/home/pcarns/working/install

## Building

Example configuration:

    ../configure --prefix=/home/pcarns/working/install \
        PKG_CONFIG_PATH=/home/pcarns/working/install/lib/pkgconfig \
        CFLAGS="-g -Wall"

## Design details

![abt-io architecture](doc/fig/abt-io-diagram.png)

abt-io provides Argobots-aware wrappers to common POSIX I/O functions
like open(), pwrite(), and close().  The wrappers behave identically to
their POSIX counterparts from a caller's point of view, but internally
they delegate blocking I/O system calls to a dedicated Argobots pool.
The caller is suspended until the system call completes so that other
concurrent ULTs can make progress in the mean time.

The delegation step is implemented by spawning a new tasklet that
coordinates with the calling ULT via an eventual construct. The tasklets
are allowed to block on system calls because they are executing on a
dedicated pool that has been designated for that purpose. This division
of responsibility between a request servicing pool and an I/O system
call servicing pool can be thought of as a form of I/O forwarding.

The use of multiple execution streams in the abt-io pool means that
multiple I/O operations can make asynchronous progress concurrently.
The caller can issue abt\_io calls from an arbitrary number of ULTs,
but the actual underying operations will be issued to the local file
system with a level of concurrency determined by the number of execution
streams in the abt-io pool.  This is similar to aio functionality but with a
simpler interface and less serialization.

Additional details and performance analysis can be found in
https://ieeexplore.ieee.org/document/8082139.

## Tracing
If you enable tracing, either by setting the ` "trace_io":true,` in the json
config, or by setting the environment variable `ABT_IO_TRACE_IO`, abt-io will log
to stderr the i/o operations it is doing in Darshan "DXT format": for example


```
% ./tests/basic abt.json
#Module Rank    Op      Segment Offset  Length  Start(s)        End(s)
X_ABTIO -1      mkostemp        -1      0       0       0.025596        0.025698
X_ABTIO -1      pwrite  -1      0       4       0.025736        0.025801
X_ABTIO -1      fallocate       -1      4       4       0.025843        0.026262
X_ABTIO -1      truncate        -1      0       1024    0.026305        0.026336
X_ABTIO -1      close   -1      0       0       0.026460        0.026494
X_ABTIO -1      stat    -1      0       0       0.026542        0.026568
X_ABTIO -1      statfs  -1      0       0       0.026603        0.026608
X_ABTIO -1      unlink  -1      0       0       0.026662        0.026737
```
