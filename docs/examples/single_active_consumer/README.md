Single Active Consumer example
---

[Super Streams Documentation](https://www.rabbitmq.com/streams.html#super-streams) for more details.
[Single active consumer blog post](https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-single-active-consumer-for-streams/)

Step 1: Run the Super stream producer:

    $ python3 sac_super_stream_producer.py

Step 2: Run multiple super stream consumers:

    $ python3 single_active_consumer.py


You will see that running the first consumer will read from all the partitions, 
while when all the 3 consumers will be up everyone will read from its own partition.
The forth consumer if you run it will wait.