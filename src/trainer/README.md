# improve-trainer


Docker secrets are supported with docker BuildKit. The build kit is installed along standard 
docker installation for docker versions 18.09+.

Source: https://docs.docker.com/develop/develop-images/build_enhancements/

### How to build and push to ECR with ./tools/build_and_push.sh script

**_Free:_**

- ./tools/build_and_push.sh --stage dev --tag latest
- ./tools/build_and_push.sh -s dev -t latest

**_Pro:_**

- ./tools/build_and_push.sh --pro --stage dev --tag latest
- ./tools/build_and_push.sh --pro -s dev -t latest



### How to build image directly with docker build command

**_Free:_**

DOCKER_BUILDKIT=1 docker buildx build --platform linux/amd64 -t v8_trainer:latest . --build-arg subscription=free

**_Pro:_**

DOCKER_BUILDKIT=1 docker buildx build --platform linux/amd64 -t v8_trainer:latest . --build-arg subscription=pro

**_Without specifying pro / free version within --build.arg the container will not build_**

### Usage

TBD

### License

TBD

