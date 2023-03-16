#!/usr/bin/env bash


./benchmark/experiments/run-experiment               \
    --dataset dbpedia-stronglink2                    \
    --tool dlve                                      \
    --tool vadalog                                   \
    --tool vadalog-parsimonious-aggregate            \
    --tool vadalog-resumption                        \
    --tool vadalog-parsimonious-aggregate-resumption \
    --output-dir results/stronglink                  \
    --timeout 600.0                                  \
    --nb-runs 3
