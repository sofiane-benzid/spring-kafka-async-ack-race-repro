# spring-kafka async-ack race reproducer

With out-of-order commits (`asyncAcks=true`), `KafkaMessageListenerContainer.ListenerConsumer#removeOffsetsInBatch` modifies the per-partition `LinkedList`s in `offsetsInThisBatch` / `deferredOffsets` without the `ListenerConsumer` monitor, while `ackInOrder` modifies the same lists from the ack-completion thread under that monitor. The list can be corrupted, the partition entry never drains, and the consumer stays paused (`pausedForAsyncAcks`) until the partition is revoked.

## 1. Embedded-Kafka reproducer (real spring-kafka)

Requires JDK 21 and Maven.

```bash
mvn test                                          # spring-kafka 4.1.1, 15 minutes
mvn test -Dspring-kafka.version=4.1.2-SNAPSHOT    # a snapshot build
mvn test -Drepro.minutes=30 -Drepro.taskMs=90     # tune the timing
```

A stall fails the test with `STALL REPRODUCED ...` and a dump of the container's `pausedForAsyncAcks`, `offsetsInThisBatch` and `deferredOffsets`, including whether each `LinkedList`'s size field matches its node chain.

| Property | Default | Meaning |
|---|---|---|
| `repro.poolMax` | 1 | worker threads; the pool has no queue, so a busy pool rejects |
| `repro.taskMs` / `repro.taskJitter` | 95 / 12 | async work time per record (ms) |
| `repro.backOffMs` | 100 | `DefaultErrorHandler` fixed back-off |
| `repro.rate` | 300 | records produced per second |
| `repro.minutes` | 15 | how long to run before declaring "no stall" |
| `repro.stallSeconds` | 30 | no acks for this long + `pausedForAsyncAcks=true` = stall |

The race needs record N's ack to land while the consumer thread is handling the retry of record N+1, so `taskMs` should be a little below `backOffMs`. Logging is kept at WARN on purpose: per-record logging slows the threads enough to hide it.

## 2. Model of the race (no dependencies)

`model/LinkedListRaceModel.java` copies the list logic of `captureOffsets`, `ackInOrder` and `removeOffsetsInBatch` and races the last two from two threads.

```bash
cd model && javac LinkedListRaceModel.java
java LinkedListRaceModel unsync 300000   # current behaviour
java LinkedListRaceModel sync   300000   # removeOffsetsInBatch synchronized
```
