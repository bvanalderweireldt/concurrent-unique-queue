package com.hybhub.util.concurrent;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Test
public class ConcurrentSetBlockingQueueMultiThreadTest {

	private static class OfferGiver implements Callable<Boolean> {
		private BlockingQueue<UUID> queue;

		public OfferGiver(final BlockingQueue<UUID> queue) {
			this.queue = queue;
		}

		@Override
		public Boolean call()  {
			for ( int ignored : IntStream.range(0,1_000).toArray()) {
				queue.offer(UUID.randomUUID());
			}
			return Boolean.TRUE;
		}
	}

	private static class PutGiver implements Callable<Boolean> {
		private BlockingQueue<UUID> queue;

		public PutGiver(final BlockingQueue<UUID> queue) {
			this.queue = queue;
		}

		@Override
		public Boolean call() throws InterruptedException {
			for ( int ignored : IntStream.range(0,1_000).toArray()) {
				queue.put(UUID.randomUUID());
			}
			return Boolean.TRUE;
		}
	}

	private static class Consumer implements Callable<Boolean> {
		private BlockingQueue<UUID> queue;

		public Consumer(final BlockingQueue<UUID> queue) {
			this.queue = queue;
		}

		@Override
		public Boolean call() throws InterruptedException {
			for ( int ignored : IntStream.range(0,8_000).toArray()) {
				queue.take();
			}
			return Boolean.TRUE;
		}
	}

	public void testTwoThreadsOfferTake() throws InterruptedException {
		//Arrange
		BlockingQueue<UUID> queue = new ConcurrentSetBlockingQueue<>();
		ExecutorService exec = Executors.newFixedThreadPool(16);

		//Act
		exec.invokeAll((Collection<? extends Callable<Boolean>>) Stream.of(
				new OfferGiver(queue),
				new OfferGiver(queue),
				new OfferGiver(queue),
				new OfferGiver(queue),
				new OfferGiver(queue),
				new OfferGiver(queue),
				new OfferGiver(queue),
				new PutGiver(queue),
				new Consumer(queue))
				.collect(Collectors.toCollection(ArrayList::new)));

		exec.awaitTermination(5, TimeUnit.SECONDS);
		exec.shutdown();

		//Test
		System.out.println(queue.size());
		Assert.assertTrue(queue.isEmpty());
	}

	public void testTenThreadsOffer() throws InterruptedException {
		//Arrange
		BlockingQueue<UUID> queue = new ConcurrentSetBlockingQueue<>();
		List<Callable<Object>> consumers = new ArrayList<>();
		for ( int ignored : IntStream.range(0,10).toArray() ){
			consumers.add(() -> {
				for ( int ignored2 : IntStream.range(0,15).toArray() ) {
					queue.offer(UUID.randomUUID());
				}
				return Boolean.TRUE;
			});
		}
		ExecutorService exec = Executors.newFixedThreadPool(5);

		//Act
		exec.invokeAll(consumers);
		exec.shutdown();
		exec.awaitTermination(1, TimeUnit.SECONDS);

		//Test
		Assert.assertEquals(queue.size(), 150);
		Assert.assertFalse(queue.isEmpty());
	}

	public void testOfferWithTimeout() throws InterruptedException {
		//Arrange
		BlockingQueue<UUID> queue = new ConcurrentSetBlockingQueue<>(1);
		List<Callable<Object>> consumers = new ArrayList<>();
		for ( int ignored : IntStream.range(0,20).toArray() ){
			consumers.add(() -> queue.offer(UUID.randomUUID(), 10, TimeUnit.SECONDS) && queue.offer(UUID.randomUUID(), 10, TimeUnit.SECONDS));
		}
		consumers.add(() -> { IntStream.range(0,40).parallel().forEach((i) -> {
			try {
				queue.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		return Boolean.TRUE; }
		);
		ExecutorService exec = Executors.newFixedThreadPool(21);

		//Act
		exec.invokeAll(consumers);
		exec.shutdown();
		exec.awaitTermination(1, TimeUnit.SECONDS);

		//Test
		Assert.assertEquals(queue.size(), 0);
		Assert.assertTrue(queue.isEmpty());
	}

	public void testFiftyThreadsPutPoll() throws InterruptedException {
		//Arrange
		BlockingQueue<UUID> queue = new ConcurrentSetBlockingQueue<>(30);
		List<Callable<Object>> consumers = new ArrayList<>();
		for (int ignored : IntStream.range(0, 20).toArray()) {
			consumers.add(() -> {
				queue.put(UUID.randomUUID());
				queue.put(UUID.randomUUID());
				return Boolean.TRUE;
			});
			consumers.add(queue::poll);
			consumers.add(queue::poll);
		}
		ExecutorService exec = Executors.newFixedThreadPool(50);

		//Act
		final List<Future<Object>> results = exec.invokeAll(consumers);
		exec.shutdown();
		exec.awaitTermination(1, TimeUnit.SECONDS);

		//Test
		final long failedPolls = results.stream().filter((o) -> {
			try {
				return o.get() == null;
			} catch (Exception e) {
				return true;
			}
		}).count();
		Assert.assertTrue((failedPolls == 0) == queue.isEmpty());
		Assert.assertEquals(failedPolls, queue.size());
	}

}
