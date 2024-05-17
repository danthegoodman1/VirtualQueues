# VirtualQueues
Virtual queues on top of the Kafka API.

## Topics

Three topics must be created in order to use VirtualQueues.

### Data topic

The data topic is where the records for the virtual queues live.

You will want to create this with a relatively high partition count, and ensure that the offset topic has the same number of partitions.

### Offset topic

The offset topic is used to track the virtual offsets of the virtual consumers of virtual queues.

The offset topic (env `KAFKA_OFFSET_TOPIC`) should have **the exact same number of partitions as the data topic always**.

### Partitions topic

A topic for partition assignment must be created. This topic is used so that consumers can simply discover the current assignment of partitions. This allows consumers to cache the partition assignment map on change to provide to consumers.

This topic is very low throughput, and should have a single partition.

## Consumers

### Consumer-aware partitioning

Unlike other distributed systems, VirtualQueues will NOT forward requests to the correct node. Consumers should be partition-aware by downloading the partition assignment map on boot and intervals (and when ever a 409 conflict is returned), and should always go direct to the respective node for a given virtual queue. This means that consumers need to support the same partitioning function (murmur2 with a specific seed) that is used here. An example in Go is provided in `utils.Go#GetPartition`.

Consumer-aware partitioning massively reduces median and tail latencies, especially under high load.

### Named consumers

You can optionally use named consumers when reading queues. Each named consumer will have its offset tracked.

You can always manually read from a provided offset.

A consumer can reset its offset by providing an `Offset` and `AllowRewind` at ack time. Without the `AllowRewind` option, the server will reject an offset that is below the previously tracked offset.

A named consumer is responsible for coordination consumption: That is to say, if two processes use the same named consumer they will frequently both miss records (due to acks) and process duplicate records (no exclusive assignment).

Named consumers should also be aware when to delete themselves, as their offsets will be retained both in memory and in the offset queue.

## Queues

Queues are created on demand when the first record is written to them.

### Deleting a queue

Queues can be deleted when they have completed processing. This is useful to restrict the working set on disk from growing.

Deleting a queue will also automatically delete all tracked consumers for that queue.

## Transactional queueing suggestion

Since this is a different system, you can run into issues with duplicate processing if you enqueue within a transaction (or missed processing if you enqueue after). Each have their own risks.

At least once (queue during txn) is the preferred solution, but user-invoked transactions (e.g. api endpoint) are far more likely to restart than internally managed transactions (e.g. background task).

Combining a transactional outbox pattern (e.g. Apple's QuiCK) with VirtualQueues greatly reduces this risk. It still provides at-least-once semantics, but greatly reduces the chance that a transaction restarts and a message is enqueued multiple times. This is because the job to enqueue to the VirtualQueue is transactionally secure, and the execution of that job is very quick (in comparison to being part of a larger transaction).