package com.hybhub.util.concurrent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class SetBlockingQueueUnitTest {

	public void testOfferThenPoll(){
		//Arrange
		final Queue<String> queue = new ConcurrentSetBlockingQueue<>();
		final List<String> dataSet = Arrays.asList("1","3","6","10","4");

		//Act
		for(String s : dataSet){
			queue.offer(s);
		}

		//Test
		for(String s : dataSet){
			Assert.assertEquals(queue.poll(), s, "Polled String is incorrect");
		}
		Assert.assertTrue(queue.isEmpty());
	}

	public void testOfferDuplicates(){
		//Arrange
		final Queue<Integer> queue = new ConcurrentSetBlockingQueue<>();

		//Act
		final boolean firstOffer = queue.offer(new Integer(1));
		final boolean secondOffer = queue.offer(new Integer(1));

		//Test
		Assert.assertTrue(firstOffer);
		Assert.assertFalse(secondOffer);
	}

	public void testRemainingCapacity() throws InterruptedException {
		//Arrange
		final BlockingQueue<Integer> queue = new ConcurrentSetBlockingQueue<>(5);

		//Act
		final int initialCapacity = queue.remainingCapacity();
		queue.offer(new Integer(1));
		final int capacityOne = queue.remainingCapacity();
		queue.offer(new Integer(1));
		final int capacityOneRepeated = queue.remainingCapacity();
		queue.offer(new Integer(2));
		final int capacityTwo = queue.remainingCapacity();
		queue.peek();
		final int capacityThree = queue.remainingCapacity();
		queue.put(new Integer(3));
		final int capacityFour = queue.remainingCapacity();
		queue.remove(new Integer(3));
		final int capacityFive = queue.remainingCapacity();
		queue.add(new Integer(4));
		final int capacitySix = queue.remainingCapacity();
		queue.element();
		final int capacitySeven = queue.remainingCapacity();
		queue.remove();
		final int capacityEight = queue.remainingCapacity();
		queue.take();
		final int capacityNine = queue.remainingCapacity();
		queue.offer(new Integer(5), 1, TimeUnit.SECONDS);
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

	public void testFullCapacity() throws InterruptedException {
		//Arrange
		final BlockingQueue<Integer> queue = new ConcurrentSetBlockingQueue<>(1);

		//Act
		final boolean acceptedOffer = queue.offer(new Integer(1));
		final boolean rejectedOffer1 = queue.offer(new Integer(2));
		final boolean rejectedOffer2 = queue.offer(new Integer(2), 1, TimeUnit.SECONDS);
		boolean rejectedException = false;
		try{
			queue.add(new Integer(2));
		}
		catch (IllegalStateException ex){
			rejectedException = true;
		}

		//Test
		Assert.assertTrue(acceptedOffer);
		Assert.assertFalse(rejectedOffer1);
		Assert.assertFalse(rejectedOffer2);
		Assert.assertTrue(rejectedException);
	}
}
