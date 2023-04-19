# improve-trainer

### How to build image directly with docker build command

DOCKER_BUILDKIT=1 docker buildx build --platform linux/amd64 -t v8_trainer:latest .

### Automatic build and push along serverless deployment

serverless should automatically build and push image to private ECR when tracker is being deployed