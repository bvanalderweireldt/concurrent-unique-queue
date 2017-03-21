package com.hybhub.util.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class ConcurrentSetBlockingQueueMultiThreadTest {

	public void testTwoThreadsOfferTake() throws InterruptedException {
		//Arrange
		BlockingQueue<UUID> queue = new ConcurrentSetBlockingQueue<>();
		ExecutorService exec = Executors.newFixedThreadPool(2);

		//Act
		exec.submit(() -> {
			wait(10);
			for ( int ignored : IntStream.range(0,10).toArray()) {
				queue.offer(UUID.randomUUID());
				wait(10);
			}
			return Boolean.TRUE;
		});
		exec.submit(() -> {
			wait(100);
			for ( int ignored : IntStream.range(0,10).toArray() ) {
				queue.take();
				wait(10);
			}
			return Boolean.TRUE;
		});
		exec.shutdown();
		exec.awaitTermination(1, TimeUnit.SECONDS);

		//Test
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
