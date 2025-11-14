# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - 2025-11-14

### Features

- Update tag pattern and skip tags regex

## [0.1.1] - 2025-11-14

### Miscellaneous Tasks

- Update version format in bump_main.yaml

## [0.1.0] - 2025-11-14

### Features

- Update readme
- Add license
- Add AMQP test
- Add AMQP support
- Add ci
- Add ci steps
- Update test.yaml
- Add license and version to client properties (#27)

Add license an version to client properties
- Update test.yaml to use Ubuntu 22.04
- Update README.md (#68)
- Add the example for super stream (#70)

* Add the example for super stream
---------
Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Added sub_entry_batch example, modified README linking to the example… (#88)

* added sub_entry_batch example, modified README linking to the example and bumping the version

* Formatting (#89)

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

---------

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
Co-authored-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Add performances documentation (#91)

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Update documentation [skip ci] (#95)

* update documentation [skip ci]

---------

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Add build and test documentation (#106)

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Add documentation [skip ci] (#116)

* add documentation [skip ci]

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Update to 0.12.0 (#120)

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Add External configuration  (#145)

* Add external Auth Configuration
---------

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Update README.md (#189)
- Add cleanup in unsubscribe (#199)

* add cleanup in unsubscribe

* cleanup subscriber_task in client during unsubscribing

* adding unit test

---------

Co-authored-by: Daniele <daniele@MacBook-Air-di-Daniele.local>
- Update pip release with token (#218)

* update pip release with token
---------

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Update pip release with token

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Update ga

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Add random name to the publish reference  (#222)

* Add a random name to the publisher reference. The reference is now unique across the cluster. Two publishers with the same ID and the same stream could enable involuntarily deduplication. With this fix, the name will be random.
* update version to 0.20.9

---------
Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Update deduplication example (#223)

* udpate deduplication example

---------

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Update README.md

Fixes: https://github.com/rabbitmq-community/rstream/issues/226
- Update to 0.21.0 (#230)

* update to 0.21.0
---------

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Update dependencies  (#243)

This PR updates project dependencies to their latest versions, including upgrading pytest-asyncio, flake8, mypy, pytest, black, and other development dependencies. Additionally, it updates the RabbitMQ Docker image and Poetry version in the CI workflow.

Key changes:

- Updates major dependency versions (pytest 7→8, flake8 3→7, mypy 0.910→1.18.2, pytest-asyncio 0.15.1→1.2.0)
- Migrates test fixtures from @pytest.fixture to @pytest_asyncio.fixture for async compatibility
- Temporarily disables flake8 and mypy checks in CI workflow


---------

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- Adds CHANGELOG and git-cliff configuration
- *(ci)* Add workflows for release bumping and PR conventional commit validation
- Update commit parsers to prioritize GitHub PR labels for changelog grouping
- Update git-cliff setup in workflow

### Bug Fixes

- Fix project name
- Fix Consumer handles subscribe offsets of NEXT and LAST
- Fix ci integration
- Fix circular imports
- Fix tests
- Fix publish pypi action (#51)
- Fixed a bug in offset computation (#71)
- Fixing a bug in send_sub_entry_batching (#93)

* fixing a bug in send_sub_entry_batching

* change sub entry example

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

* bump to 0.10.2

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

---------

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
Co-authored-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Fixing publish confirmation for send_batch (#107)

* fixing publish confirmation for send_batch

* some print progress

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

* bump to 0.11.1

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

---------

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
Co-authored-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- *(client)* Fix cleanup of Client._waiters dict (#129)
- Fix the overflow error (#154)

The publisher_id is an unsigned  byte

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Fixing examples, updated doc and bumping version (#167)
- Fixed a bug when, after unsubscribing, the next subscriber with the same subscriber name used a first subscriber callback and ignore second subscriber callback. (#204)

* Actualized build and test section at README.md like ci.

* Fixed a bug when, after unsubscribing, the next subscriber with the same subscriber name used a first subscriber callback and ignore second subscriber callback.
- Fixed a bug when in single_active_consumer mode and deliver frame with more than one message, the result of the consumer_update_listener function was ignored. (#206)
- Fixed a bug when subscriber frames not clears after unsubscribe (#211)
- Fixed a bug when used `consumer_update_listener` callback from another subscription in single active consumer mode. (#210)
- Fixed bugs of exception logging (#215)
- Fix for re-applying subscription filter on reconnect (#229)

* Fix for re-applying subscription filter on reconnect

When the reconnect_stream function runs a subscriber is recreated with
the existing subscriber setup. This did not include the original
filter configuration leading to the filtering quietly disappearing on
reconnect.

Added filter_input to the `_Subscriber` object to track this configuration
and passed to the `create_subscriber` during `reconnect_stream`
- Fix `sasl_configuration_mechanism` for ClientPool. (#238)

Existing implementation ignored setting of sasl_configuration_mechanism for ClientPool. Modified code to use `SlasMechanism.MechanismPlain` as default matching the existing functionality but to default to the provided value at the constructor for the new(), get(), and _resolve_broker() methods.
- Fix encoding long values with AMQP 1.0 encoder. (#240)

* Fix encoding long values with AMQP 1.0 encoder.

Adjusted integer encoding in `encode_unknown()` to encode a python int as either an int or long depending on whether the value is within range of AMQP 1.0 standard 32-bit signed integer.

* Refactor: Use constants for 32-bit integer bounds in encoding logic.

* Reorder INT32_MAX and INT32_MIN to be lexicographically sorted

* chore: clarified comment that we're referring to **signed** 32-bit integers
- Fix subscribers list (#246)

Fixes: https://github.com/rabbitmq-community/rstream/issues/241


This PR refactors the consumer/subscriber system to use numeric IDs instead of string names for internal tracking, addressing issues with subscriber list management. The changes enhance performance and eliminate bugs associated with string-based subscriber identification.

- Converts subscriber tracking from string-based names to integer-based IDs for better performance and reliability
- Updates MessageContext and EventContext to include stream information directly instead of requiring lookups
- Adds validation for max_subscribers_by_connection limits and introduces new exception types


Breaking changes
======

1. Subscriber returns not the `subscriber_id`, but instead the`reference`

The bug was here [Tag 0.3.1]:
https://github.com/rabbitmq-community/rstream/blob/654a9ef23118d96098fe00861b3d661da7886030/rstream/consumer.py#L197-L203 

Given two references with the same name the  `subscriber = self._subscribers[reference]`  is not consistent.

With this PR the `_subscribers` is `[int, _Subscriber]`
```python
self._subscribers: dict[int, _Subscriber] = {}
```

Where the int is the subscriber id that _must_ be unique for connection by protocol.

2. remove the `get_stream` function `message_context.consumer.get_stream(message_context.subscriber_name)` 

The `get_stream` is not needed anymore since the `stream` is now passed on the `message_context` and also `event_context` 

3. `subscribe_name` is now optional
```python
class MessageContext:
    consumer: Consumer
    stream: str
    subscriber_id: int
    subscriber_name: Optional[str]
    offset: int
    timestamp: int
```
---------

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Fix typos and enhance README clarity

Update for 0.40.0
- Correct task_types format in PR Conventional Commit Validation workflow

### Performance

- Improve callback handlers (#134)

* making callback asynchronous

* making listener handlers async

* implementing core review suggestions

* update: create a thread for every subscriber

* removing handlers from frames structure

* updating with last code review suggestions

* improving _run_delivery_handlers

---------

Co-authored-by: DanielePalaia <dpalaia@pivotal.io>
Co-authored-by: Daniele Palaia <dpalaia@dpalaiaYMD6R.vmware.com>
- Improve reconnection with  metadata update handling (#252)

This PR improves reconnection and metadata update handling across the rstream library by implementing a recovery strategy pattern and refactoring connection lifecycle management. The changes enhance reliability for producers and consumers when dealing with connection failures and stream metadata updates.

Key changes:

- Implements IReliableEntity interface and BackOffRecoveryStrategy with configurable retry logic for automatic reconnection
- Refactors metadata update and connection closed handlers to use a unified recovery mechanism
- Updates test assertions to be more resilient to timing variations in distributed scenarios---------

----
Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>

### Refactoring

- Change the example to AMQP 1.0

rename a typo
- Change QPID to Azure AMQP
- Remove work in progress (#76)

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Change repo link

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Change type annotation from `list` to `Sequence` in `send_batch` and `send_sub_entry` methods to make variable covariant https://mypy.readthedocs.io/en/stable/generics.html#variance-of-generic-types (#219)
- Remove requests from main depenencies (#234)

This package is only used as part of the test suite and can be part of
dev dependencies

The version in main dependencies strictly pinned a version, preventing
security updates for library users
- Remove tag

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

### Testing

- Test index issue
- Test index issue
- Test PR checks (#28)

add PR checks
- Test fixing a timeout in test (#193)

### Attestations

- False


