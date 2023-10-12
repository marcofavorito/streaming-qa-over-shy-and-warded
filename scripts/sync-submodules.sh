#!/usr/bin/env bash

set -e

git submodule update --init --recursive third_party/chasebench
git submodule update --init --recursive third_party/Llunatic

if ! git submodule update --init --recursive third_party/vadalog-engine ; then
    echo "******************************"
    echo "Vadalog could not be cloned, please check you have the right of access to the repository. Vadalog is available under request to the authors."
    echo "******************************"
fi
