all: test

install:
	pip3 install poetry
	poetry install


format:
	which poetry
	poetry --version
	poetry run isort .
	poetry run black --exclude=venv .
	poetry run flake8 --exclude=venv,local_tests,docs/examples --max-line-length=120 --ignore=E203,W503,E701,E704,E131
	poetry run mypy .

rabbitmq-ha-proxy:
	cd compose/ha_tls; rm -rf tls-gen;
	cd compose/ha_tls; git clone https://github.com/michaelklishin/tls-gen tls-gen; cd tls-gen/basic; make
	mv compose/ha_tls/tls-gen/basic/result/server_*_certificate.pem compose/ha_tls/tls-gen/basic/result/server_certificate.pem
	mv compose/ha_tls/tls-gen/basic/result/server_*key.pem compose/ha_tls/tls-gen/basic/result/server_key.pem
	cd compose/ha_tls; docker build -t haproxy-rabbitmq-cluster  .
	cd compose/ha_tls; docker compose down
	cd compose/ha_tls; docker compose up

test: format
	poetry run pytest . -s -v
help:
	cat Makefile