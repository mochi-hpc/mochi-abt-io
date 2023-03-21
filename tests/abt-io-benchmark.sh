#!/bin/bash

set -x

if [ -z $srcdir ]; then
    echo srcdir variable not set.
    exit 1
fi

examples/abt-io-benchmark -j $srcdir/tests/abt-io-benchmark-example.json -o /tmp/output-$$.gz
if [ $? -ne 0 ]; then
    exit 1
fi

exit 0
