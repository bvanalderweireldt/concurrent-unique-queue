package com.hybhub.util.concurrent;

import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class SetBlockingQueueMultiThreadTest {

	public void testTwoThreads() throws InterruptedException {
		//Arrange
		Queue<UUID> queue = new SetBlockingQueue<>();
		QueueConsume addToQueue = new QueueConsume(queue, 10, 10, QueueConsume.OFFER_TO_QUEUE);
		QueueConsume pollFromQueue = new QueueConsume(queue, 10, 500, 10, QueueConsume.POLL_FROM_QUEUE);
		ExecutorService exec = Executors.newFixedThreadPool(2);

		//Act
		exec.execute(addToQueue);
		exec.execute(pollFromQueue);
		exec.shutdown();
		exec.awaitTermination(10, TimeUnit.SECONDS);

		//Test
		Assert.assertTrue(queue.isEmpty());
	}

	private static class QueueConsume implements Runnable {

		private Queue<UUID> queue;
		private long wait;
		private long initialWait;
		private int max;
		private Consumer<Queue> consume;
		private final Random random = new Random();
		private static final Consumer<Queue> OFFER_TO_QUEUE = (queue) -> queue.offer(UUID.randomUUID());
		private static final Consumer<Queue> POLL_FROM_QUEUE = (queue) -> queue.poll();

		public QueueConsume(final Queue<UUID> queue, final long wait, final int max, final Consumer<Queue> consume) {
			this(queue,wait,0,max,consume);
		}

		public QueueConsume(final Queue<UUID> queue, final long wait, final long initialWait, final int max, final Consumer<Queue> consume) {
			this.queue = queue;
			this.wait = wait;
			this.initialWait = initialWait;
			this.max = max;
			this.consume = consume;
		}

		@Override
		public void run() {
			try {
				Thread.sleep(initialWait);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			for (int i = 0 ; i < max ; i++){
				consume.accept(queue);
				try {
					Thread.sleep(wait + (long)random.nextInt(1000));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
