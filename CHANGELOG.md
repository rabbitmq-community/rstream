# Changelog

All notable changes to this project will be documented in this file.

## [0.31.0] - 2025-09-02

### Bug Fixes

- Fix `sasl_configuration_mechanism` for ClientPool. (#238)

Existing implementation ignored setting of sasl_configuration_mechanism for ClientPool. Modified code to use `SlasMechanism.MechanismPlain` as default matching the existing functionality but to default to the provided value at the constructor for the new(), get(), and _resolve_broker() methods.
- Fix encoding long values with AMQP 1.0 encoder. (#240)

* Fix encoding long values with AMQP 1.0 encoder.

Adjusted integer encoding in `encode_unknown()` to encode a python int as either an int or long depending on whether the value is within range of AMQP 1.0 standard 32-bit signed integer.

* Refactor: Use constants for 32-bit integer bounds in encoding logic.

* Reorder INT32_MAX and INT32_MIN to be lexicographically sorted

* chore: clarified comment that we're referring to **signed** 32-bit integers

### Refactoring

- Remove requests from main depenencies (#234)

This package is only used as part of the test suite and can be part of
dev dependencies

The version in main dependencies strictly pinned a version, preventing
security updates for library users

## [0.30.0] - 2025-06-03

### Features

- Update README.md

Fixes: https://github.com/rabbitmq-community/rstream/issues/226
- Update to 0.21.0 (#230)

* update to 0.21.0
---------

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

### Bug Fixes

- Fix for re-applying subscription filter on reconnect (#229)

* Fix for re-applying subscription filter on reconnect

When the reconnect_stream function runs a subscriber is recreated with
the existing subscriber setup. This did not include the original
filter configuration leading to the filtering quietly disappearing on
reconnect.

Added filter_input to the `_Subscriber` object to track this configuration
and passed to the `create_subscriber` during `reconnect_stream`

## [0.29.0] - 2025-03-31

### Features

- Add random name to the publish reference  (#222)

* Add a random name to the publisher reference. The reference is now unique across the cluster. Two publishers with the same ID and the same stream could enable involuntarily deduplication. With this fix, the name will be random.
* update version to 0.20.9

---------
Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Update deduplication example (#223)

* udpate deduplication example

---------

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

### Refactoring

- Change type annotation from `list` to `Sequence` in `send_batch` and `send_sub_entry` methods to make variable covariant https://mypy.readthedocs.io/en/stable/generics.html#variance-of-generic-types (#219)

## [0.28.0] - 2024-12-04

### Features

- Update pip release with token (#218)

* update pip release with token
---------

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Update pip release with token

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Update ga

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

### Refactoring

- Change repo link

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

### Attestations

- False

## [0.20.7] - 2024-11-27

### Bug Fixes

- Fixed bugs of exception logging (#215)

## [0.20.6] - 2024-10-16

### Bug Fixes

- Fixed a bug when subscriber frames not clears after unsubscribe (#211)
- Fixed a bug when used `consumer_update_listener` callback from another subscription in single active consumer mode. (#210)

## [0.20.5] - 2024-10-02

### Bug Fixes

- Fixed a bug when in single_active_consumer mode and deliver frame with more than one message, the result of the consumer_update_listener function was ignored. (#206)

## [0.20.4] - 2024-09-18

### Bug Fixes

- Fixed a bug when, after unsubscribing, the next subscriber with the same subscriber name used a first subscriber callback and ignore second subscriber callback. (#204)

* Actualized build and test section at README.md like ci.

* Fixed a bug when, after unsubscribing, the next subscriber with the same subscriber name used a first subscriber callback and ignore second subscriber callback.

## [0.20.3] - 2024-09-03

## [0.20.2] - 2024-08-28

### Features

- Add cleanup in unsubscribe (#199)

* add cleanup in unsubscribe

* cleanup subscriber_task in client during unsubscribing

* adding unit test

---------

Co-authored-by: Daniele <daniele@MacBook-Air-di-Daniele.local>

## [0.20.1] - 2024-06-11

## [0.20.0] - 2024-05-28

### Testing

- Test fixing a timeout in test (#193)

## [0.19.1] - 2024-05-02

### Features

- Update README.md (#189)

## [0.19.0] - 2024-04-23

## [0.18.0] - 2024-04-17

## [0.17.1] - 2024-04-10

## [0.17.0] - 2024-02-19

## [0.16.0] - 2024-01-18

### Bug Fixes

- Fixing examples, updated doc and bumping version (#167)

## [0.15.0] - 2023-12-05

## [0.14.2] - 2023-11-15

### Bug Fixes

- Fix the overflow error (#154)

The publisher_id is an unsigned  byte

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

## [0.14.1] - 2023-11-13

## [0.14.0] - 2023-11-06

### Features

- Add External configuration  (#145)

* Add external Auth Configuration
---------

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

## [0.13.0] - 2023-10-30

## [0.12.1] - 2023-10-11

### Bug Fixes

- *(client)* Fix cleanup of Client._waiters dict (#129)

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

## [0.12.0] - 2023-09-18

### Features

- Add documentation [skip ci] (#116)

* add documentation [skip ci]

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
- Update to 0.12.0 (#120)

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

## [0.11.1] - 2023-07-18

### Features

- Add build and test documentation (#106)

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

### Bug Fixes

- Fixing publish confirmation for send_batch (#107)

* fixing publish confirmation for send_batch

* some print progress

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

* bump to 0.11.1

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

---------

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
Co-authored-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

## [0.11.0] - 2023-07-18

## [0.10.3] - 2023-07-11

### Features

- Update documentation [skip ci] (#95)

* update documentation [skip ci]

---------

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

## [0.10.2] - 2023-07-10

### Features

- Add performances documentation (#91)

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

### Bug Fixes

- Fixing a bug in send_sub_entry_batching (#93)

* fixing a bug in send_sub_entry_batching

* change sub entry example

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

* bump to 0.10.2

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

---------

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
Co-authored-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

## [0.10.1] - 2023-07-07

### Features

- Added sub_entry_batch example, modified README linking to the example… (#88)

* added sub_entry_batch example, modified README linking to the example and bumping the version

* Formatting (#89)

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

---------

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>
Co-authored-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

## [0.10.0] - 2023-07-04

### Refactoring

- Remove work in progress (#76)

Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

## [0.9.0] - 2023-06-20

## [0.8.1] - 2023-06-16

### Features

- Update README.md (#68)
- Add the example for super stream (#70)

* Add the example for super stream
---------
Signed-off-by: Gabriele Santomaggio <G.santomaggio@gmail.com>

### Bug Fixes

- Fixed a bug in offset computation (#71)

## [0.8] - 2023-06-15

## [0.7] - 2023-05-22

## [0.6] - 2023-04-12

### Features

- Update test.yaml to use Ubuntu 22.04

### Bug Fixes

- Fix publish pypi action (#51)

## [0.5] - 2023-03-08

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

### Bug Fixes

- Fix project name
- Fix Consumer handles subscribe offsets of NEXT and LAST
- Fix ci integration
- Fix circular imports
- Fix tests

### Refactoring

- Change the example to AMQP 1.0

rename a typo
- Change QPID to Azure AMQP

### Testing

- Test index issue
- Test index issue
- Test PR checks (#28)

add PR checks


