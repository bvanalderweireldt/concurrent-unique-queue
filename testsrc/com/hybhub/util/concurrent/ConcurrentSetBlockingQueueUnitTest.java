package com.hybhub.util.concurrent;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class ConcurrentSetBlockingQueueUnitTest {

	public void testRemainingCapacity() throws InterruptedException {
		//Arrange
		final BlockingQueue<Integer> queue = new ConcurrentSetBlockingQueue<>(5);

		//Act
		final int initialCapacity = queue.remainingCapacity();
		queue.offer(1);
		final int capacityOne = queue.remainingCapacity();
		queue.offer(1);
		final int capacityOneRepeated = queue.remainingCapacity();
		queue.offer(2);
		final int capacityTwo = queue.remainingCapacity();
		queue.peek();
		final int capacityThree = queue.remainingCapacity();
		queue.put(3);
		final int capacityFour = queue.remainingCapacity();
		queue.remove(3);
		final int capacityFive = queue.remainingCapacity();
		queue.add(4);
		final int capacitySix = queue.remainingCapacity();
		queue.element();
		final int capacitySeven = queue.remainingCapacity();
		queue.remove();
		final int capacityEight = queue.remainingCapacity();
		queue.take();
		final int capacityNine = queue.remainingCapacity();
		queue.offer(5, 1, TimeUnit.SECONDS);
		final int capacityTen = queue.remainingCapacity();
		queue.drainTo(new ArrayList<>());
		final int capacityEleven = queue.remainingCapacity();

		//Test
		Assert.assertEquals(initialCapacity, 5);
		Assert.assertEquals(capacityOne, 4);
		Assert.assertEquals(capacityOneRepeated, 4);
		Assert.assertEquals(capacityTwo, 3);
		Assert.assertEquals(capacityThree, 3);
		Assert.assertEquals(capacityFour, 2);
		Assert.assertEquals(capacityFive, 3);
		Assert.assertEquals(capacitySix, 2);
		Assert.assertEquals(capacitySeven, 2);
		Assert.assertEquals(capacityEight, 3);
		Assert.assertEquals(capacityNine, 4);
		Assert.assertEquals(capacityTen, 3);
		Assert.assertEquals(capacityEleven, 5);
	}

}
