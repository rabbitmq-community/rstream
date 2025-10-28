Complete example of Reliable Client
---

This is an example of how to use the client reliably. By following these best practices, you can ensure that your
application will be able to recover from network failures, metadata updates and other issues.

The client takes in input some parameters listed in appsettings.json, like connection parameters, how many streams, and if we want 
To run the example in stream and super-stream mode, and the possibility to use a load_balancer.

The example uses the asynchronous send() method with a confirmation callback to check for confirmations.

### Disconnection management.

The client supports auto-reconnect for Producer and Consumer.
It is possible to configure the recovery strategy via `recovery_strategy` field:
```python
 consumer = Consumer(
        .....
         recovery_strategy=BackOffRecoveryStrategy(True),
)
```

The lib automatically tries to reconnect.

Set `BackOffRecoveryStrategy(False)` to disable the reconnections.

### Metadata Update
If the streams topology changes (ex:Stream deleted or add/remove follower), the client receives a MetadataUpdate event.
The server removes the producers and consumers linked to the stream before sending the Metadata update event.
Similarly to the disconnection scenario, the lib  automatically reconnects.


### Management of connections
You can specify the parameters: MaxPublishersByConnection and MaxSubscribersByConnection in appsettings.json to specify how many maximum publishers and subscribers can 
share a connection.
These parameters are then passed to the Producer and Consumer constructors.
