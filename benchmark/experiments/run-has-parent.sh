#!/usr/bin/env bash


./benchmark/experiments/run-experiment               \
    --dataset has-ancestor                           \
    --tool vadalog                                   \
    --tool vadalog-parsimonious-aggregate            \
    --tool vadalog-resumption                        \
    --tool vadalog-parsimonious-aggregate-resumption \
    --output-dir results/has-parent                  \
    --timeout 300.0                                  \
    --nb-runs 3
