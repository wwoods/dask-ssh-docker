
run: build
	../../bin/dask-ssh-docker -v localhost localhost --image dask-ssh-docker-test:latest -- test.py

build:
	docker build -f ./Dockerfile . -t dask-ssh-docker-test:latest
