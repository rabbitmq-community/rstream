
all: test

install:
	pip3 install --upgrade pip
	pip3 install packaging
	pip3 install poetry
	poetry install
	pip3 install isort black mypy pytest flake8
	pip3 install flake8 --upgrade


format:
	which poetry
	poetry run isort --skip venv .
	poetry run black --exclude=venv .
	poetry run flake8 --exclude=venv,local_tests,docs/examples --max-line-length=120 --ignore=E203,W503
	poetry run mypy  --skip venv .

test: format
	poetry run pytest .
help:
	cat Makefile
