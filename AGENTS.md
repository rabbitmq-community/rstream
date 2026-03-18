# Agent guidance for rstream

This file helps AI agents work effectively on the **rstream** codebase.

## Project overview

**rstream** is a Python asyncio client for [RabbitMQ Streams](https://www.rabbitmq.com/stream.html). It provides producers, consumers, super streams, and single active consumer support. The codebase is in the [rabbitmq-community](https://github.com/rabbitmq-community/rstream) org.

## Tech stack and layout

- **Language**: Python 3.9+
- **Runtime**: asyncio
- **Package manager**: Poetry (`pyproject.toml`)
- **Source**: `rstream/` (e.g. `producer.py`, `consumer.py`, `superstream_consumer.py`, `amqp.py`, `client.py`, `schema.py`)
- **Tests**: `tests/` (pytest, pytest-asyncio)
- **Examples**: `docs/examples/` (basic producers/consumers, super streams, SAC, TLS, etc.)

## Running the project

- **Install**: `poetry install`
- **Tests**: `poetry run pytest` — requires a running RabbitMQ Stream server (see README for Docker setup)
- **Linting/formatting**: black, isort, flake8, mypy (see `pyproject.toml` for config)

## Code style and conventions

- **Formatting**: Black (line length 110, no string normalization), isort (profile: black)
- **Linting**: flake8 (max line length 120; extend-ignore: E203, W503)
- **Types**: mypy with `ignore_missing_imports = true`; exclude `venv`, `.venv`, `compose`
- Prefer async/await; keep APIs consistent with existing producer/consumer patterns.

## Key concepts

- **Codecs**: AMQP 1.0 (default, interoperable with other clients) and Binary (Python-to-Python). See README “Client Codecs”.
- **Recovery**: Producers and consumers support auto-reconnect via `recovery_strategy` (e.g. `BackOffRecoveryStrategy`).
- **Breaking changes**: Document in `CHANGELOG.md` under a version heading with “Breaking changes” when changing or removing public APIs.

## When editing

- Run tests and linters after changes.
- Update `CHANGELOG.md` for user-facing or breaking changes.
- Keep docstrings and README/examples in sync with API changes.
- For new features, consider adding or updating an example under `docs/examples/`.
