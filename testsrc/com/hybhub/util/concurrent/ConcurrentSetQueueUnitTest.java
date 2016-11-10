package com.hybhub.util.concurrent;

import java.util.Arrays;
import java.util.List;
import java.util.Queue;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class ConcurrentSetQueueUnitTest {

	public void testFullCapacityOffer() throws InterruptedException {
		//Arrange
		final Queue<Integer> queue = new ConcurrentSetBlockingQueue<>(1);

		//Act
		final boolean acceptedOffer = queue.offer(new Integer(1));
		final boolean rejectedOffer1 = queue.offer(new Integer(2));
		final boolean rejectedOffer2 = queue.offer(new Integer(2));
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
		final boolean firstOffer = queue.offer(new Integer(1));
		final boolean secondOffer = queue.offer(new Integer(1));

		//Test
		Assert.assertTrue(firstOffer);
		Assert.assertFalse(secondOffer);
	}

	public void testPeek(){
		//Arrange
		final Queue<Integer> queue = new ConcurrentSetBlockingQueue<>();

		//Act
		final Integer shouldBeNull = queue.peek();
		final boolean shouldBeTrue = queue.offer(new Integer(1));
		final Integer shouldBeNonNull = queue.peek();
		final int shouldBeOne = queue.size();

		//Test
		Assert.assertNull(shouldBeNull);
		Assert.assertTrue(shouldBeTrue);
		Assert.assertNotNull(shouldBeNonNull);
		Assert.assertEquals(1, shouldBeOne);
	}

}
