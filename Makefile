WORKDIR=$(shell pwd)
IMAGE?=plugin-wxt530

default:
	@echo ${WORKDIR}


build:
	docker build --pull -f Dockerfile -t ${IMAGE}:latest .


rm: 
	docker rm -f ${IMAGE}


deploy:
	docker run -d --rm --name ${IMAGE} \
	       --device=/dev/ttyUSB0 \
	       --entrypoint '/bin/sh' ${IMAGE} -c '/bin/sleep infinity'

interactive:
	docker exec -it ${IMAGE} bash
