#!/usr/bin/env bash

set -e

cd third_party/original_datasets

echo "Downloading LUBM dataset..."
rm -fr lubm
./generate_LUBM_data.sh
mkdir -p lubm
mv 01k 001 010 100 lubm/

echo "Downloading Ontology-256 dataset..."
rm -fr ontology-256
./generate_Ontology-256_data.sh
mkdir -p ontology-256
mv data/* ontology-256
rm -r data

echo "Downloading STB-128 dataset..."
rm -fr stb-128
./generate_STB-128_data.sh
mkdir -p stb-128
mv data/* stb-128
rm -r data

echo "Downloading Doctors dataset..."
rm -fr doctors
./generate_doctors_data.sh
mkdir -p doctors
mv 10k 100k 500k 1m doctors

echo "Downloading Doctors-FD dataset..."
rm -fr doctors-fd
./generate_doctors-fd_data.sh
mkdir -p doctors-fd
mv 10k 100k 500k 1m doctors-fd

cd ../../