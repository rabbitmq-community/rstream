# RabbitMQ Stream Python Client

A Python asyncio-based client for [RabbitMQ Streams](https://www.rabbitmq.com/stream.html)


The RabbitMQ stream plug-in is required. See the [documentation](https://www.rabbitmq.com/stream.html#enabling-plugin) for enabling it.


# Table of Contents


- [Installation](#installation)
- [Examples](#examples)
- [Client Codecs](#client-codecs)
    * [AMQP 1.0 codec  vs Binary](#amqp-10-codec--vs-binary)
- [Publishing messages](#publishing-messages)
    * [Publishing with confirmation](#publishing-with-confirmation)
- [Sub-Entry Batching and Compression](#sub-entry-batching-and-compression)
- [Deduplication](#deduplication)
- [Consuming messages](#consuming-messages)
    * [Server-side offset tracking](#server-side-offset-tracking)
- [Super Streams](#superstreams)
- [Single Active Consumer](#single-active-consumer)
- [Connecting with SSL](#connecting-with-ssl)
- [Sasl Mechanisms](#sasl-mechanisms)
- [Managing disconnections](#managing-disconnections)
	* [Reconnect](#reconnect)
- [Load Balancer](#load-balancer)
- [Client Performances](#client-performances)
   * [Test case](#test-case)
- [Build and Test](#build-and-test)
- [Project Notes](#project-notes)


## Installation

The RabbitMQ stream plug-in is required. See the [documentation](https://www.rabbitmq.com/stream.html#enabling-plugin) for enabling it.


The client is distributed via [`PIP`](https://pypi.org/project/rstream/):
```bash
 pip install rstream
```

## Examples

[Here](https://github.com/qweeze/rstream/blob/master/docs/examples/) you can find different examples.


## Client Codecs
Before start using the client is important to read this section.
The client supports two codecs to store the messages to the server:
 - `AMQP 1.0`
 - `Binary`

By default you should use `AMQP 1.0` codec:
```python
   amqp_message = AMQPMessage(
    body=bytes("hello: {}".format(i), "utf-8"),
  )
```
 
#### AMQP 1.0 codec  vs Binary

You need to use the `AMQP 1.0` codec to exchange messages with other stream clients like
[Java](https://github.com/rabbitmq/rabbitmq-stream-java-client), [.NET](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client), [Rust](https://github.com/rabbitmq/rabbitmq-stream-rust-client), [Go](https://github.com/rabbitmq/rabbitmq-stream-go-client) or if you want to use the `AMQP 0.9.1` clients. 

You can use the `Binary` version if you need to exchange messages from Python to Python. 

<b>Note</b>: The messages stored in `Binary` are not compatible with the other clients and with AMQP 0.9.1 clients. <br /> 
Once the messages are stored to the server, you can't change them. 

Read also the [Client Performances](#client-performances) section 


## Publishing messages

You can publish messages with four different methods:

* `send`: asynchronous, messages are automatically buffered internally and sent at once after a timeout expires.
* `send_batch`: synchronous, the user buffers the messages and sends them. This is the fastest publishing method.
* `send_wait`: synchronous, the caller wait till the message is confirmed. This is the slowest publishing method.
* `send_sub_entry`: asynchronous. See [Sub-entry batching and compression](#sub-entry-batching-and-compression).

On the [examples](https://github.com/qweeze/rstream/blob/master/docs/examples/) directory you can find diffent way to send the messages:
- [producer using send](https://github.com/qweeze/rstream/blob/master/docs/examples/basic_producers/producer_send.py)
- [producer using send_wait](https://github.com/qweeze/rstream/blob/master/docs/examples/basic_producers/producer_send_wait.py)
- [producer using send_batch](https://github.com/qweeze/rstream/blob/master/docs/examples/basic_producers/producer_send_batch.py)
- [producer using sub_entry_batch](https://github.com/qweeze/rstream/blob/master/docs/examples/sub_entry_batch/producer_sub_entry_batch.py)


### Publishing with confirmation

The Send method takes as parameter an handle function that will be called asynchronously when the message sent will be notified from the server to have been published.

Example:
- [producer using send and handling confirmation](https://github.com/qweeze/rstream/blob/master/docs/examples/producers_with_confirmations/send_with_confirmation.py)
- [producer using send_batch and handling confirmation](https://github.com/qweeze/rstream/blob/master/docs/examples/producers_with_confirmations/send_batch_with_confirmation.py)

With `send_wait` instead will wait until the confirmation from the server is received.


## Sub-Entry Batching and Compression
RabbitMQ Stream provides a special mode to publish, store, and dispatch messages: sub-entry batching. This mode increases throughput at the cost of increased latency and potential duplicated messages even when deduplication is enabled. It also allows using compression to reduce bandwidth and storage if messages are reasonably similar, at the cost of increasing CPU usage on the client side.

Sub-entry batching consists in squeezing several messages – a batch – in the slot that is usually used for one message. This means outbound messages are not only batched in publishing frames, but in sub-entries as well.

```python

  # sending with compression
   await producer.send_sub_entry(
        STREAM, compression_type=CompressionType.Gzip, sub_entry_messages=messages
   )
```
[Full example producer using sub-entry batch](https://github.com/qweeze/rstream/blob/master/docs/examples/sub_entry_batch/producer_sub_entry_batch.py)

Consumer side is automatic, so no need configurations. 

The client is shipped with No Compression (`CompressionType.No`) and Gzip Compression (`CompressionType.Gzip`) the other compressions (`Snappy`, `Lz4`, `Zstd`) can be used implementing the `ICompressionCodec` class. 



## Deduplication

RabbitMQ Stream can detect and filter out duplicated messages, based on 2 client-side elements: the producer name and the message publishing ID.
All the producer methods to send messages (send, send_batch, send_wait) takes a publisher_name parameter while the message publishing id can be set in the AMQP message.

Example:
- [producer with deduplication](https://github.com/qweeze/rstream/blob/master/docs/examples/deduplication/producer_ded.py)

## Consuming messages

See [consumer examples](https://github.com/qweeze/rstream/blob/master/docs/examples/basic_consumers)  for basic consumer and consumers with different offsets.


### Server-side offset tracking

RabbitMQ Streams provides server-side offset tracking for consumers. This features allows a consuming application to restart consuming where it left off in a previous run.
You can use the store_offset (to store an offset in the server) and query_offset (to query it) methods of the consumer class like in this example:
- [server side offset tracking](https://github.com/qweeze/rstream/blob/master/docs/examples/manual_server_offset_tracking/consumer.py)


## Superstreams

A super stream is a logical stream made of individual, regular streams. It is a way to scale out publishing and consuming with RabbitMQ Streams: a large logical stream is divided into partition streams, splitting up the storage and the traffic on several cluster nodes.

See the [blog post](https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-super-streams/) for more info.

You can use `superstream_producer` and `superstream_consumer` classes which internally uses producers and consumers to operate on the componsing streams.


See the [Super Stream example](https://github.com/qweeze/rstream/tree/master/docs/examples/super_stream)

### Single Active Consumer

Single active consumer provides exclusive consumption and consumption continuity on a stream. <br /> 
See the [blog post](https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-single-active-consumer-for-streams) for more info.
See examples in: 

See the [single active consumer example](https://github.com/qweeze/rstream/blob/master/docs/examples/single_active_consumer/)

### Filtering

Filtering is a new streaming feature enabled from RabbitMQ 3.13 based on Bloom filter.
RabbitMQ Stream provides a server-side filtering feature that avoids reading all the messages of a stream and filtering 
only on the client side. This helps to save network bandwidth when a consuming application needs only a subset of 
messages.

https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/#filtering 

See the [filtering examples](https://github.com/qweeze/rstream/blob/master/docs/examples/filtering/)

### Connecting with SSL

You can enable ssl/tls.
See example here:
[tls example](https://github.com/qweeze/rstream/blob/master/docs/examples/tls/producer.py)

### Sasl Mechanisms

You can use the following sasl mechanisms:
- PLAIN
- EXTERNAL

The client uses `PLAIN` mechanism by default.

The `EXTERNAL` mechanism is used to authenticate a user based on a certificate presented by the client.
Example:
```python
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    # put the root certificate of the ca
    ssl_context.load_verify_locations("certs/ca_certificate.pem")
    ssl_context.load_cert_chain(
        "certs/client_HOSTNAME_certificate.pem",
        "certs/client_HOSTNAME_key.pem",
    )

    async with Producer(
        "HOSTNAME",
        username="not_important",
        password="not_important",
        port=5551,
        ssl_context=ssl_context,
        sasl_configuration_mechanism=SlasMechanism.MechanismExternal ## <--- here EXTERNAL configuration
```
The plugin `rabbitmq_auth_mechanism_ssl` needs to be enabled on the server side, and `ssl_options.fail_if_no_peer_cert` needs to set to `true`
config example:
```
auth_mechanisms.3 = PLAIN
auth_mechanisms.2 = AMQPLAIN
auth_mechanisms.1 = EXTERNAL

ssl_options.cacertfile = certs/ca_certificate.pem
ssl_options.certfile = certs/server_certificate.pem
ssl_options.keyfile = certs/server_key.pem
listeners.ssl.default = 5671
stream.listeners.ssl.default = 5551
ssl_options.verify               = verify_peer
ssl_options.fail_if_no_peer_cert = true
```

### Managing disconnections

The client supports auto-reconnect for all the entities: Producer, Consumer, SuperstreamProducer, SuperstreamConsumer.

When the TCP connection is disconnected unexpectedly the entities try to reconnect automatically.
```python
async def on_connection_closed(disconnection_info: OnClosedErrorInfo) -> None:
    print(
        "connection has been closed from stream: "
        + str(disconnection_info.streams)
        + " for reason: "
        + str(disconnection_info.reason)
    )

consumer = Consumer(
..        
on_close_handler=on_connection_closed,
)
```

### Reconnect
It is possible to configure the reconnect behaviour using `recovery_strategy` parameter
By default, the clients uses `BackOffRecoveryStrategy`<br/>
Example:
```python
   consumer = Consumer(
             ...
            recovery_strategy=BackOffRecoveryStrategy(),
        )
```

Please take a look at the complete, reliable client example [here](https://github.com/qweeze/rstream/blob/master/docs/examples/reliable_client/)

## Load Balancer

In order to handle load balancers, you can use the `load_balancer_mode` parameter for producers and consumers. This will always attempt to create a connection via the load balancer, discarding connections that are inappropriate for the client type.

Producers must connect to the leader node, while consumers can connect to any, prioritising replicas if available.


## Client Performances

The RabbitMQ Stream queues can handle high throughput. Currently, the client cannot reach the server's maximum throughput. 

We found some bottlenecks; one of them is the current AMQP 1.0 marshal/unmarshal message format. 

This one:
```python
 for i in range(1_000_000):
            amqp_message = AMQPMessage(
                body=bytes("hello: {}".format(i), "utf-8"),
            )
            # send is asynchronous
            await producer.send(stream=STREAM, message=amqp_message)
```

is more or less ~55% slower than:
```python
 for i in range(1_000_000):
            # send is asynchronous
            await producer.send(stream=STREAM, message=b"hello")
```

You can use the `batch_send` to test the performance.

We are evaluating rewriting the `AMQP 1.0 codec` to optimise it for the stream use case.

### Test case
- Linux Ubuntu 4 cores and 8 GB of Ram
- RabbitMQ installed to the server 

- Send batch with AMQP 1.0 codec:
```python
$  python3 docs/examples/basic_producers/producer_send_batch.py
Sent 1.000.000 messages in 9.3218 seconds. 107.275,5970 messages per second
```

- Send batch with binary codec:
```python
$ python3 docs/examples/basic_producers/producer_send_batch_binary.py
Sent 1.000.000 messages in 2.9930 seconds. 334.116,5639 messages per second
```


## Build and Test

To run the tests, you need to have a running RabbitMQ Stream server. 
You can use the Docker official image.

Run the server with the following command:
```bash
docker run -it --rm --name rabbitmq -p 5552:5552 -p 5672:5672 -p 15672:15672 \
    -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS='-rabbitmq_stream advertised_host localhost' \
    rabbitmq:3.13.1-management
```

enable the plugin:
```bash
docker exec rabbitmq rabbitmq-plugins enable rabbitmq_stream rabbitmq_stream_management rabbitmq_amqp1_0
```

and run the tests:
```bash
 poetry run pytest
```
