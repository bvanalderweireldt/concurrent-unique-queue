package com.hybhub.util.concurrent;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class ConcurrentSetQueueUnitTest {

	public void testFullCapacityOffer() throws InterruptedException {
		//Arrange
		final Queue<Integer> queue = new ConcurrentSetBlockingQueue<>(1);

		//Act
		final boolean acceptedOffer = queue.offer(1);
		final boolean rejectedOffer1 = queue.offer(2);
		final boolean rejectedOffer2 = queue.offer(2);
		boolean rejectedException = false;
		try{
			queue.add(2);
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

	public void testOfferThenPoll(){
		//Arrange
		final Queue<String> queue = new ConcurrentSetBlockingQueue<>();
		final List<String> dataSet = Arrays.asList("1","3","6","10","4");

		//Act
		dataSet.forEach(queue::offer);

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
		final boolean firstOffer = queue.offer(1);
		final boolean secondOffer = queue.offer(1);

		//Test
		Assert.assertTrue(firstOffer);
		Assert.assertFalse(secondOffer);
	}

	public void testPeek(){
		//Arrange
		final Queue<Integer> queue = new ConcurrentSetBlockingQueue<>();

		//Act
		final Integer shouldBeNull = queue.peek();
		final boolean shouldBeTrue = queue.offer(1);
		final Integer shouldBeNonNull = queue.peek();
		final int shouldBeOne = queue.size();

		//Test
		Assert.assertNull(shouldBeNull);
		Assert.assertTrue(shouldBeTrue);
		Assert.assertNotNull(shouldBeNonNull);
		Assert.assertEquals(1, shouldBeOne);
	}

	public void testRemove(){
		//Arrange
		final Queue<Integer> queue = new ConcurrentSetBlockingQueue<>();

		//Act
		Exception expectedException = null;
		try {
			queue.remove();
		}
		catch (NoSuchElementException noSuchElementException){
			expectedException = noSuchElementException;
		}
		final boolean addedFirst = queue.offer(3);
		final boolean addedSecond = queue.offer(1);
		final boolean addedThird = queue.offer(2);
		final Integer integerRemoved = queue.remove();
		final int finalQueueSize = queue.size();

		//Test
		Assert.assertNotNull(expectedException);
		Assert.assertTrue(addedFirst);
		Assert.assertTrue(addedSecond);
		Assert.assertTrue(addedThird);
		Assert.assertEquals(integerRemoved, new Integer(3));
		Assert.assertEquals(finalQueueSize, 2);
	}

	public void testElement(){
		//Arrange
		final Queue<Integer> queue = new ConcurrentSetBlockingQueue<>();

		//Act
		Exception expectedException = null;
		try {
			queue.element();
		}
		catch (NoSuchElementException noSuchElementException){
			expectedException = noSuchElementException;
		}
		final boolean addedFirst = queue.offer(4);
		final boolean addedSecond = queue.offer(1);
		final Integer shouldBeNonNull = queue.element();
		final int shouldBeTwo = queue.size();

		//Test
		Assert.assertNotNull(expectedException);
		Assert.assertTrue(addedFirst);
		Assert.assertTrue(addedSecond);
		Assert.assertNotNull(shouldBeNonNull);
		Assert.assertEquals(2, shouldBeTwo);
	}
}
