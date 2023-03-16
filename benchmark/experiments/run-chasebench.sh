#!/usr/bin/env bash


./benchmark/experiments/run-experiment    \
    --dataset ontology-256                \
    --dataset stb-128                     \
    --tool vadalog                        \
    --tool vadalog-parsimonious-aggregate \
    --output-dir results/chasebench       \
    --timeout 600.0                       \
    --nb-runs 3
