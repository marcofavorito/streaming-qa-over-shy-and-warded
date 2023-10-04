# Vadalog-DLV^E Benchmarking

This repository contains code and datasets for benchmarking Datalog^E reasoners.

We tested the software on Ubuntu 22.04.

## Preliminaries

- Clone the repository:
```
git clone git@github.com:bancaditalia/vadalog-dlv-benchmarking.git
```

- Sync the submodules:
```
./scripts/sync-submodules.sh
```

Note: you might not have access to the Vadalog repository. Please contact the authors if you want to reproduce the 
experiments for that system. 

- Make sure you have Python 3.10 installed.

- Install [Pipenv](https://pipenv.pypa.io/en/latest/)
```
pip install pipenv
```

- Create a virtual environment and install the dependencies
```
pipenv shell --python=python3.10
pipenv install
```

- Add the current directory to PYTHONPATH (this has to be done whenever a new terminal is spawned):
```
export PYTHONPATH=${PYTHONPATH:+$PYTHONPATH:}$(pwd)
```

- Install JDK 15. Using [sdkman](https://sdkman.io/):
```
sdk install java 15.0.2-open
sdk use java 15.0.2-open
```

- Install Maven 3+
```
sdk install maven 3.8.4
sdk use maven 3.8.4
```

- Build the Vadalog project:
```
cd third_party/vadalog-engine-bankitalia
mvn -f pom.xml package
```

## Optional: use Docker

Instead of running the following commands on the host machine,
you could use the Docker image. To build:
```
./scripts/build-docker.sh
```

To run a shell inside the container:
```
./scripts/run-docker.sh
```


## Run all

To run all experiments:

```
./benchmark/experiments/run-all.sh
```

## Parse and plot results

```
./benchmark/plots/scalability-plot.py all-results/stronglink --dataset dbpedia-stronglink2 --output-dir plots/stronglink
./benchmark/plots/scalability-plot.py all-results/chasebench --dataset doctors --output-dir plots/doctors
./benchmark/plots/histogram-plot.py --stb-128-dir all-results/chasebench/stb-128 --ontology-256-dir all-results/chasebench/ontology-256 --output-dir plots/chasebench
```
