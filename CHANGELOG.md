# Changelog

All notable changes to this project will be documented in this file.

## [[0.40.1](https://github.com/rabbitmq-community/rstream/releases/tag/v0.40.1)]

This release includes bug fixes and improvements to connection handling.

## 0.40.1 - 2025-01-22
- [Release 0.40.1](https://github.com/rabbitmq-community/rstream/releases/tag/v0.40.1) 

### Fixed
- Bugfix cleanup logic in `_maybe_clean_up_during_lost_connection` method by @dbotwinick in [#262](https://github.com/rabbitmq-community/rstream/pull/262)

- Divide the locator connection from the Consumer connection by @Gsantomaggio in [#261](https://github.com/rabbitmq-community/rstream/pull/261)

## [[0.40.0](https://github.com/rabbitmq-community/rstream/releases/tag/v0.40.0)]

This release focuses on improving stability during reconnection.

## 0.40.0 - 2025-28-10
- [Release 0.40.0](https://github.com/rabbitmq-community/rstream/releases/tag/v0.40.0) 

### Added
- Implement Auto recovery connection for consumer by @Gsantomaggio in [#250](https://github.com/rabbitmq-community/rstream/pull/250)
- Implement producer recovery connection by @Gsantomaggio in [#251](https://github.com/rabbitmq-community/rstream/pull/251)
- improve reconnection with metadata update handling by @Gsantomaggio in [#252](https://github.com/rabbitmq-community/rstream/pull/252)

### Fixed
- Fix subscribers list by @Gsantomaggio in [#246](https://github.com/rabbitmq-community/rstream/pull/246)
- Restore flake8 test by @Gsantomaggio in [#244](https://github.com/rabbitmq-community/rstream/pull/244)

### Changed
- Refactor the producers list replace the reference with the id as map key by @Gsantomaggio in [#249](https://github.com/rabbitmq-community/rstream/pull/249)
- Update dependencies by @Gsantomaggio in [#243](https://github.com/rabbitmq-community/rstream/pull/243)

### Removed
- Removed `reconnect_stream`
- Removed `message_context.consumer.get_stream(message_context.subscriber_name)` in favour of `message_context.stream`

### Breaking changes
- The `reconnect_stream` function has been removed. Use the auto-recovery feature instead.
- The `message_context.consumer.get_stream(message_context.subscriber_name)` method has been removed.
- Use `message_context.stream` to access the stream directly.
- The subscribe function now returns the `subscription_id` that you can use to unsubscribe.
