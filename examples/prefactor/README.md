# Prefactor workflow
This workflow is a redo of [prefactor-cwl](https://github.com/EOSC-LOFAR/prefactor-cwl) using pumpkin. This example is based on Docker which is assumed already installed on the system. 

## Installation
```sh
git clone -b prefactor-pmk https://github.com/recap/pumpkin.git
cd pumpkin/examples/prefactor
make small
make docker
```
At this stage a dataset should have been copied to data/ in pumpkin/examples/prefactor and a docker image named kernsuite/prefactor-pmk is created.

## Run the example
```sh
make run
```
This will start the workflow within a container. The final output will be in pumpkin/examples/prefactor/output


