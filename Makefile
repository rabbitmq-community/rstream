all: test

install:
	pip3 install poetry
	poetry install


format:
	which poetry
	poetry --version
	poetry run isort .
	poetry run black --exclude=venv .
	poetry run flake8 --exclude=venv,local_tests,docs/examples --max-line-length=120 --ignore=E203,W503,E701,E704
	poetry run mypy .

test: format
	poetry run pytest .
help:
	cat Makefile