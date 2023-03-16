#!/usr/bin/env bash

set -e

docker run --rm -v$(pwd):/home/default/workdir -it vadalog-dlv-benchmarking /bin/bash
