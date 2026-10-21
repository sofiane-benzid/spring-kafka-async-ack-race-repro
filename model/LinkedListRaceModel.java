import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Dependency-free model of the out-of-order-commit bookkeeping in spring-kafka's
 * {@code KafkaMessageListenerContainer.ListenerConsumer}. It is NOT spring-kafka code; it
 * mirrors the list operations of {@code captureOffsets}, {@code ackInOrder} (incl.
 * {@code containsUnackedOffsets}) and {@code removeOffsetsInBatch} as of main (Oct 2026),
 * to show what happens when the last one runs without the {@code ListenerConsumer} monitor.
 *
 * <p>Each trial: one poll of N records on one partition. Records [0, k) were handed to async
 * work and are acked in random order from a "completion" thread (like
 * {@code ConsumerAcknowledgment.acknowledge()} -> {@code ackInOrder}, which holds the monitor).
 * Records [k, N) failed synchronously, so the "consumer" thread removes them (like the
 * {@code RecordInRetryException} path -> {@code removeOffsetsInBatch}). When both are done, the
 * partition entry must be gone; if it is still there the real container would stay paused
 * ({@code pausedForAsyncAcks}) until the partition is revoked.
 *
 * <pre>
 * javac LinkedListRaceModel.java
 * java LinkedListRaceModel unsync 300000   # removeOffsetsInBatch without the lock (current code)
 * java LinkedListRaceModel sync   300000   # removeOffsetsInBatch synchronized (proposed fix)
 * </pre>
 */
public class LinkedListRaceModel {

	static final int PARTITION = 0;

	final Map<Integer, List<Long>> offsetsInThisBatch = new ConcurrentHashMap<>();

	final Map<Integer, List<Long>> deferredOffsets = new ConcurrentHashMap<>();

	final boolean lockRemoval;

	LinkedListRaceModel(boolean lockRemoval) {
		this.lockRemoval = lockRemoval;
	}

	// mirrors captureOffsets (synchronized in spring-kafka)
	synchronized void captureOffsets(int n) {
		this.offsetsInThisBatch.clear();
		this.deferredOffsets.clear();
		LinkedList<Long> offs = new LinkedList<>();
		for (long i = 0; i < n; i++) {
			offs.add(i);
		}
		this.offsetsInThisBatch.put(PARTITION, offs);
		this.deferredOffsets.put(PARTITION, new LinkedList<>());
	}

	// mirrors ackInOrder (synchronized in spring-kafka); processAck is a no-op here
	synchronized void ackInOrder(long offset) {
		List<Long> offs = this.offsetsInThisBatch.get(PARTITION);
		if (offs == null || offs.isEmpty()) {
			throw new IllegalStateException("Unexpected ack for " + offset + "; offsets list is empty");
		}
		List<Long> deferred = this.deferredOffsets.get(PARTITION);
		if (offs.get(0) == offset) {
			offs.remove(0);
			long recordToAck = offset;
			if (deferred != null && !deferred.isEmpty()) {
				deferred.sort(Comparator.naturalOrder());
				while (!deferred.isEmpty() && !containsUnackedOffsets(offs, recordToAck, deferred.get(0))) {
					recordToAck = deferred.remove(0);
					offs.remove(0);
				}
			}
			if (offs.isEmpty()) {
				this.deferredOffsets.remove(PARTITION);
				this.offsetsInThisBatch.remove(PARTITION);
			}
		}
		else if (offset < offs.get(0)) {
			throw new IllegalStateException("stale ack " + offset + ", head " + offs.get(0));
		}
		else if (deferred != null) {
			deferred.add(offset);
		}
	}

	private static boolean containsUnackedOffsets(List<Long> unacked, long start, long end) {
		for (Long o : unacked) {
			if (o > start && o < end) {
				return true;
			}
		}
		return false;
	}

	// mirrors removeOffsetsInBatch (NOT synchronized in spring-kafka today)
	void removeOffsetsInBatch(List<Long> records) {
		if (this.lockRemoval) {
			synchronized (this) {
				doRemove(records);
			}
		}
		else {
			doRemove(records);
		}
	}

	private void doRemove(List<Long> records) {
		for (Long offset : records) {
			List<Long> offsets = this.offsetsInThisBatch.get(PARTITION);
			if (offsets == null || offsets.isEmpty()) {
				continue;
			}
			offsets.remove(offset); // remove(Object), same as offsets.remove(record.offset()) boxing
			List<Long> deferred = this.deferredOffsets.get(PARTITION);
			if (deferred != null) {
				deferred.removeIf(d -> d.longValue() == offset);
			}
			if (offsets.isEmpty()) {
				this.offsetsInThisBatch.remove(PARTITION);
				this.deferredOffsets.remove(PARTITION);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		boolean lock = args.length > 0 && args[0].equals("sync");
		int trials = args.length > 1 ? Integer.parseInt(args[1]) : 200_000;
		int n = 8;

		AtomicInteger stuck = new AtomicInteger();
		AtomicInteger exceptions = new AtomicInteger();
		String firstStuck = null;
		String firstException = null;

		for (int t = 0; t < trials; t++) {
			LinkedListRaceModel c = new LinkedListRaceModel(lock);
			c.captureOffsets(n);
			ThreadLocalRandom rnd = ThreadLocalRandom.current();
			int k = 1 + rnd.nextInt(n - 1); // [0,k) async-acked, [k,n) retried
			List<Long> acks = new ArrayList<>();
			for (long i = 0; i < k; i++) {
				acks.add(i);
			}
			Collections.shuffle(acks);
			List<Long> retry = new ArrayList<>();
			for (long i = k; i < n; i++) {
				retry.add(i);
			}

			CountDownLatch go = new CountDownLatch(1);
			Throwable[] err = new Throwable[2];
			Thread completion = new Thread(() -> {
				try {
					go.await();
					for (Long o : acks) {
						c.ackInOrder(o);
					}
				}
				catch (Throwable e) {
					err[0] = e;
				}
			});
			Thread consumer = new Thread(() -> {
				try {
					go.await();
					c.removeOffsetsInBatch(retry);
				}
				catch (Throwable e) {
					err[1] = e;
				}
			});
			completion.start();
			consumer.start();
			go.countDown();
			completion.join();
			consumer.join();

			if (err[0] != null || err[1] != null) {
				exceptions.incrementAndGet();
				if (firstException == null) {
					Throwable e = err[0] != null ? err[0] : err[1];
					firstException = (err[0] != null ? "completion thread: " : "consumer thread: ") + e;
				}
			}
			else if (c.offsetsInThisBatch.containsKey(PARTITION)) {
				stuck.incrementAndGet();
				if (firstStuck == null) {
					List<Long> l = c.offsetsInThisBatch.get(PARTITION);
					firstStuck = "acks=" + acks + " retry=" + retry + " -> offsetsInThisBatch size()="
							+ l.size() + " contents=" + safeToString(l) + " deferred="
							+ safeToString(c.deferredOffsets.get(PARTITION));
				}
			}
		}

		System.out.printf("mode=%s trials=%d stuck(partition never drains)=%d exceptions=%d%n",
				lock ? "sync" : "unsync", trials, stuck.get(), exceptions.get());
		if (firstStuck != null) {
			System.out.println("first stuck trial:     " + firstStuck);
		}
		if (firstException != null) {
			System.out.println("first exception trial: " + firstException);
		}
	}

	private static String safeToString(List<Long> l) {
		if (l == null) {
			return "null";
		}
		try {
			return l.toString();
		}
		catch (RuntimeException e) {
			return "<" + e.getClass().getSimpleName() + " while printing>";
		}
	}

}
