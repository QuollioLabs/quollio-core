include .env

.PHONY: help
help:
		@grep "^[a-zA-Z\-]*:" Makefile | grep -v "grep" | sed -e 's/^/make /' | sed -e 's/://'

.PHONY: test
test:
		python3 -m unittest discover tests

.PHONY: fmt
fmt:
		isort .
		black .

.PHONY: lint
lint:
		pflake8 .

.PHONY: install
install:
		flit install --deps production

.PHONY: install-local
install-local:
		flit install --deps develop

.PHONY: build
build:
		docker build -t $(IMAGE_NAME) . -f ./Dockerfile --platform=linux/amd64

.PHONY: buildnc
buildnc:
		docker build --no-cache -t $(IMAGE_NAME) . -f ./Dockerfile --platform=linux/amd64

.PHONY: build-local
build-local:
		docker build -t $(IMAGE_NAME) . -f ./Dockerfile.local --platform=linux/amd64

.PHONY: buildnc-local
buildnc-local:
		docker build --no-cache -t $(IMAGE_NAME) . -f ./Dockerfile.local --platform=linux/amd64

.PHONY: push
push:
		aws ecr get-login-password --region $(AWS_REGION) --profile $(AWS_PROFILE) | docker login --username AWS --password-stdin $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com
		docker tag $(IMAGE_NAME):latest $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(IMAGE_NAME):latest
		docker push $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(IMAGE_NAME):latest

.PHONY: run-local
devrun:
		docker run --name $(IMAGE_NAME) -v $(PWD)/:/home/app/dev -w /home/app/dev --rm -itd --platform=linux/amd64 $(IMAGE_NAME) /bin/bash

.PHONY: kill
kill:
		docker kill $(IMAGE_NAME)

.PHONY: sso
sso:
		aws sso login --profile $(AWS_PROFILE)
