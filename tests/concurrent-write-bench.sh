#!/bin/bash

set -x

dat1=/tmp/concurrent-write-bench-$$.dat

examples/concurrent-write-bench 4096 16 5 $dat1 0
if [ $? -ne 0 ]; then
    exit 1
fi

exit 0
