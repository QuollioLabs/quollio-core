.PHONY: help
help:
		@grep "^[a-zA-Z\-]*:" Makefile | grep -v "grep" | sed -e 's/^/make /' | sed -e 's/://'

.PHONY: test
test:
		python3 -m unittest discover tests

.PHONY: fmt
fmt:
		isort . --check --diff
		black . --check

.PHONY: lint
lint:
		pflake8 .

.PHONY: install
install:
		flit install
