.PHONY: build add

DOCKER_COMPOSE_PATH := /usr/local/bin/docker-compose
DOCKER_COMPOSE_CONFIG := ~/DS_Assgn/DS_ASSIGNMENT_1/docker-compose.yml

build:
#	cd Server && sudo docker build -t flaskserver . && cd ..
	sudo docker compose up

add:
	curl -X POST -H "Content-Type: application/json" -d '{"n": 3, "hostnames": ["S1","S2","S3"]}' http://127.0.0.1:5000/add

clean:
	sudo docker rm  -f $$(sudo docker ps -aq)
	sudo docker rmi lb_image
	sudo docker rmi flaskserver