#!/bin/bash

set -x

echo "{\"engine\":\"liburing\"}" > /tmp/basic-liburing-$$.json

tests/basic /tmp/basic-liburing-$$.json
if [ $? -ne 0 ]; then
    exit 1
fi

exit 0
