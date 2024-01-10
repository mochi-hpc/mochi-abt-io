#!/bin/bash

set -x

if [ -z $srcdir ]; then
    echo srcdir variable not set.
    exit 1
fi

tests/basic $srcdir/tests/abt-io-uring-cfg-example.json
if [ $? -ne 0 ]; then
    exit 1
fi

exit 0
