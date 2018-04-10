.PHONY: clean run small docker
all: data docker
SHELL=bash
RUN := $(PWD)/runs/run_$(shell date +%F-%H-%M-%S)
ARCHIVE=ftp://ftp.astron.nl/outgoing/EOSC/datasets/
TINY=L591513_SB000_uv_delta_t_4.MS
PULSAR=GBT_Lband_PSR.fil
SMALL=L570745_SB000_uv_first10.MS

# archive name is different from file name
SMALL_ARCHIVE=L570745_uv_first10.MS.tar.xz

TMPMOUNT=/tmp/pmk-tmp-mount

dirs:
	[ -d output ] || mkdir ./output
	[ -d data ] || mkdir ./data
	[ -d $(TMPMOUNT) ] || mkdir $(TMPMOUNT)

data/$(PULSAR): dirs
	cd data && wget $(ARCHIVE)$(PULSAR)

data/$(TINY)/: dirs
	cd data && wget $(ARCHIVE)$(TINY).tar.xz && tar Jxvf $(TINY).tar.xz

tiny: data/$(TINY)/
	echo "data/$(TINY)/ is downloaded"

data/$(SMALL_ARCHIVE): dirs 
	cd data && wget $(ARCHIVE)$(SMALL_ARCHIVE)

data/$(SMALL)/: data/$(SMALL_ARCHIVE)
	cd data && tar Jxvf $(SMALL_ARCHIVE)

small: data/$(SMALL)/
	echo "data/$(SMALL)/ is downloaded"
	
split_tasks:
	[ -d tasks1 ] || mkdir tasks1
	[ -d tasks2 ] || mkdir tasks2
	cp tasks/* tasks1/
	cp tasks/ndppp.py tasks2/
	cp tasks/ampl.py tasks2/
	cp tasks/calibcalc.py tasks2/

run: dirs
	docker run  --volume=$(PWD)/data/:/data/ --volume=$(PWD)/output/:/output/ --volume=/tmp/pmk-tmp-mount:/tmp/ --volume=$(PWD)/tasks/:/tasks kernsuite/prefactor-pmk /root/pumpkin/bin/pmk --taskdir /tasks/ --endpoints \"inqueue://*\"

run_one: dirs split_tasks
	docker run  --volume=$(PWD)/data/:/data/ --volume=$(PWD)/output/:/output/ --volume=/tmp/pmk-tmp-mount:/tmp/ --volume=$(PWD)/tasks1/:/tasks kernsuite/prefactor-pmk /root/pumpkin/bin/pmk --taskdir /tasks/ --endpoints \"tcp://*:*\" --broadcast

run_two: dirs split_tasks
	docker run  --volume=$(PWD)/data/:/data/ --volume=$(PWD)/output/:/output/ --volume=/tmp/pmk-tmp-mount:/tmp/ --volume=$(PWD)/tasks2/:/tasks kernsuite/prefactor-pmk /root/pumpkin/bin/pmk --taskdir /tasks/ --endpoints \"tcp://*:*\" --broadcast

test:
	echo $(PWD)

stop-docker:
	docker ps | grep prefactor-pmk | awk '{print $$1}' > docker_id
	if [ -f docker_id ]; then cat docker_id | xargs docker kill; fi
	rm docker_id

docker:
	docker build docker/ -t kernsuite/prefactor-pmk

