package example;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Reproducer: with {@code asyncAcks=true} (out-of-order commits), a record listener that hands
 * work to a bounded executor, and a {@link DefaultErrorHandler} doing blocking retries, a single
 * partition can stay paused forever with no error.
 *
 * <p>Setup:
 * <ul>
 * <li>one topic, one partition; a producer writes at a steady rate</li>
 * <li>the listener submits each record to a {@link ThreadPoolExecutor} with no queue
 * ({@link SynchronousQueue}); the worker sleeps ~{@code taskMs} and then calls
 * {@code Acknowledgment.acknowledge()} from the worker thread</li>
 * <li>when the pool is busy, {@code execute()} throws {@link RejectedExecutionException}
 * synchronously; {@link DefaultErrorHandler} backs off ~{@code backOffMs}, seeks, and throws
 * {@code RecordInRetryException}, so the container calls {@code removeOffsetsInBatch} on the
 * consumer thread</li>
 * </ul>
 * With {@code taskMs} just under {@code backOffMs}, the previous record's ack (completion thread,
 * {@code ackInOrder}, holds the {@code ListenerConsumer} monitor) lands at the same time as
 * {@code removeOffsetsInBatch} (consumer thread, no monitor). Both modify the same per-partition
 * {@code LinkedList}. When it is corrupted, the partition's entry never empties and the consumer
 * stays paused ({@code pausedForAsyncAcks}) until the partition is revoked.
 *
 * <p>The test fails with "STALL REPRODUCED" when acks stop for {@code stallSeconds} while the
 * producer is ahead and the container reports {@code pausedForAsyncAcks=true}.
 */
class AsyncAckRaceReproTest {

	private static final String TOPIC = "async-ack-race";

	private static final String GROUP = "async-ack-race-group";

	@Test
	void partitionShouldNotStayPausedForever() throws Exception {
		int poolMax = Integer.getInteger("repro.poolMax", 1);
		long taskMs = Long.getLong("repro.taskMs", 95);
		int taskJitter = Integer.getInteger("repro.taskJitter", 12);
		int rate = Integer.getInteger("repro.rate", 300);
		long minutes = Long.getLong("repro.minutes", 15);
		long backOffMs = Long.getLong("repro.backOffMs", 100);
		long stallSeconds = Long.getLong("repro.stallSeconds", 30);
		System.out.printf("repro: poolMax=%d taskMs=%d±%d rate=%d/s backOff=%dms minutes=%d%n",
				poolMax, taskMs, taskJitter, rate, backOffMs, minutes);

		EmbeddedKafkaKraftBroker broker = new EmbeddedKafkaKraftBroker(1, 1, TOPIC);
		broker.afterPropertiesSet();

		AtomicLong sent = new AtomicLong();
		AtomicLong invocations = new AtomicLong();
		AtomicLong rejections = new AtomicLong();
		AtomicLong acked = new AtomicLong();
		AtomicLong lastAckNanos = new AtomicLong(System.nanoTime());
		AtomicReference<Throwable> firstAckError = new AtomicReference<>();
		AtomicBoolean producing = new AtomicBoolean(true);

		// No queue: a busy pool rejects immediately (default AbortPolicy).
		ThreadPoolExecutor executor = new ThreadPoolExecutor(poolMax, poolMax, 0L, TimeUnit.MILLISECONDS,
				new SynchronousQueue<>());

		AcknowledgingMessageListener<Integer, String> listener = (rec, ack) -> {
			invocations.incrementAndGet();
			try {
				executor.execute(() -> {
					long jitter = taskJitter > 0 ? ThreadLocalRandom.current().nextLong(-taskJitter, taskJitter + 1) : 0;
					LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(Math.max(0, taskMs + jitter)));
					try {
						ack.acknowledge(); // completion thread -> ackInOrder (synchronized)
						acked.incrementAndGet();
						lastAckNanos.set(System.nanoTime());
					}
					catch (Throwable t) {
						firstAckError.compareAndSet(null, t);
					}
				});
			}
			catch (RejectedExecutionException ex) {
				rejections.incrementAndGet();
				throw ex; // -> DefaultErrorHandler -> seek -> RecordInRetryException -> removeOffsetsInBatch
			}
		};

		Map<String, Object> consumerProps = new HashMap<>();
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20);
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		ContainerProperties containerProps = new ContainerProperties(TOPIC);
		containerProps.setAckMode(ContainerProperties.AckMode.MANUAL);
		containerProps.setAsyncAcks(true);
		containerProps.setMessageListener(listener);

		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(new DefaultKafkaConsumerFactory<>(consumerProps), containerProps);
		DefaultErrorHandler errorHandler =
				new DefaultErrorHandler(new FixedBackOff(backOffMs, FixedBackOff.UNLIMITED_ATTEMPTS));
		errorHandler.setLogLevel(KafkaException.Level.DEBUG);
		container.setCommonErrorHandler(errorHandler);

		Map<String, Object> producerProps = new HashMap<>();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 0);
		KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);

		Thread producerThread = new Thread(() -> {
			long periodNanos = TimeUnit.SECONDS.toNanos(1) / rate;
			long next = System.nanoTime();
			int i = 0;
			while (producing.get()) {
				producer.send(new ProducerRecord<>(TOPIC, 0, i, "value-" + i));
				sent.incrementAndGet();
				i++;
				next += periodNanos;
				long wait = next - System.nanoTime();
				if (wait > 0) {
					LockSupport.parkNanos(wait);
				}
			}
		}, "repro-producer");

		try {
			container.start();
			ContainerTestUtils.waitForAssignment(container, 1);
			producerThread.start();

			long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(minutes);
			long lastReport = System.nanoTime();
			while (System.nanoTime() < deadline) {
				Thread.sleep(1000);
				if (firstAckError.get() != null) {
					throw new AssertionError("acknowledge() threw on the completion thread", firstAckError.get());
				}
				// the producer never stops, so a long gap between acks means the consumer is not progressing
				long sinceAck = System.nanoTime() - lastAckNanos.get();
				if (sinceAck > TimeUnit.SECONDS.toNanos(stallSeconds)) {
					String state = dumpListenerConsumer(container);
					if (state.contains("pausedForAsyncAcks=true")) {
						throw new AssertionError(String.format(
								"STALL REPRODUCED: no ack for %ds; sent=%d invocations=%d acked=%d rejections=%d%n%s",
								TimeUnit.NANOSECONDS.toSeconds(sinceAck), sent.get(), invocations.get(),
								acked.get(), rejections.get(), state));
					}
				}
				if (System.nanoTime() - lastReport > TimeUnit.SECONDS.toNanos(30)) {
					lastReport = System.nanoTime();
					System.out.printf("progress: sent=%d invocations=%d acked=%d rejections=%d%n",
							sent.get(), invocations.get(), acked.get(), rejections.get());
				}
			}
			System.out.printf("no stall in %d min: sent=%d invocations=%d acked=%d rejections=%d%n",
					minutes, sent.get(), invocations.get(), acked.get(), rejections.get());
		}
		finally {
			producing.set(false);
			producerThread.join(5000);
			producer.close(Duration.ofSeconds(5));
			container.stop();
			executor.shutdownNow();
			broker.destroy();
		}
	}

	/**
	 * Diagnostic only: reads ListenerConsumer's private out-of-order-commit state.
	 */
	private static String dumpListenerConsumer(KafkaMessageListenerContainer<?, ?> container) {
		StringBuilder sb = new StringBuilder("ListenerConsumer state:");
		try {
			Field lcField = KafkaMessageListenerContainer.class.getDeclaredField("listenerConsumer");
			lcField.setAccessible(true);
			Object lc = lcField.get(container);
			synchronized (lc) { // same monitor ackInOrder/captureOffsets use, so we read a quiescent view
				for (String name : List.of("pausedForAsyncAcks", "offsetsInThisBatch", "deferredOffsets")) {
					Field f = lc.getClass().getDeclaredField(name);
					f.setAccessible(true);
					Object value = f.get(lc);
					sb.append("\n  ").append(name).append('=');
					if (value instanceof Map<?, ?> map) {
						sb.append('{');
						map.forEach((k, v) -> sb.append(k).append(" -> ").append(describeList(v)).append("; "));
						sb.append('}');
					}
					else {
						sb.append(value);
					}
				}
			}
		}
		catch (ReflectiveOperationException | RuntimeException ex) {
			sb.append("\n  (could not read state: ").append(ex).append(')');
		}
		return sb.toString();
	}

	/**
	 * Compares LinkedList's size field with the nodes actually reachable (needs
	 * --add-opens java.base/java.util=ALL-UNNAMED, set in the pom).
	 */
	private static String describeList(Object list) {
		if (!(list instanceof java.util.LinkedList<?> ll)) {
			return String.valueOf(list);
		}
		try {
			Field size = java.util.LinkedList.class.getDeclaredField("size");
			Field first = java.util.LinkedList.class.getDeclaredField("first");
			size.setAccessible(true);
			first.setAccessible(true);
			Object node = first.get(ll);
			StringBuilder items = new StringBuilder("[");
			int walked = 0;
			while (node != null && walked < 1000) {
				Field item = node.getClass().getDeclaredField("item");
				Field next = node.getClass().getDeclaredField("next");
				item.setAccessible(true);
				next.setAccessible(true);
				items.append(walked == 0 ? "" : ", ").append(item.get(node));
				node = next.get(node);
				walked++;
			}
			items.append(']');
			int sizeField = size.getInt(ll);
			return "LinkedList sizeField=" + sizeField + " nodesWalked=" + walked + " items=" + items
					+ (sizeField != walked ? " (CORRUPTED)" : "");
		}
		catch (ReflectiveOperationException | RuntimeException ex) {
			return "LinkedList (internals not readable: " + ex.getClass().getSimpleName() + ")";
		}
	}

}
