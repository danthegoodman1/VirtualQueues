# VirtualQueues
Virtual queues on top of the Kafka API.

## Topics

Three topics must be created in order to use VirtualQueues.

### Data topic

The data topic is where the records for the virtual queues live.

You will want to create this with a relatively high partition count, and ensure that the offset topic has the same number of partitions.

### Offset topic

The offset topic (env `KAFKA_OFFSET_TOPIC`) should have **the exact same number of partitions as the data topic always**.

### Partitions topic

A topic for partition assignment must be created. This topic is used so that consumers can simply discover the current assignment of partitions. This allows consumers to cache the partition assignment map on change to provide to consumers.

This topic is very low throughput, and should have a single partition.

## Consumers

### Consumer-aware partitioning

Unlike other distributed systems, VirtualQueues will NOT forward requests to the correct node. Consumers should be partition-aware by downloading the partition assignment map on boot and intervals (and when ever a 409 conflict is returned), and should always go direct to the respective node for a given virtual queue. This means that consumers need to support the same partitioning function (murmur2 with a specific seed) that is used here. An example in Go is provided in `utils.Go#GetPartition`.

Consumer-aware partitioning massively reduces median and tail latencies, especially under high load.