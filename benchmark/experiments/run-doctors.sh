#!/usr/bin/env bash


./benchmark/experiments/run-experiment    \
    --dataset doctors                     \
    --tool dlve                           \
    --tool vadalog                        \
    --tool vadalog-parsimonious-aggregate \
    --output-dir results/doctors          \
    --timeout 600.0                       \
    --nb-runs 3
