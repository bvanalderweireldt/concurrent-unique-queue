package com.hybhub.util.concurrent;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Test
public class ConcurrentSetBlockingQueueThreadPoolExecutorTest {

    /**
     * IMPORTANT : The ThreadPoolExecutor won't add the first Runnable inside the queue, that's the reason why I execute it with Integer 1 three times first.
     * @throws InterruptedException
     */
    @Test
    public void testBLockingQueueInsideThreadPoolExecutor() throws InterruptedException {
        //Arrange
        final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1,1,20, TimeUnit.SECONDS,
                new ConcurrentSetBlockingQueue<>(100), new ThreadPoolExecutor.DiscardPolicy());

        //Act
        threadPoolExecutor.execute(new SimpleRunnable(1));
        final int shouldBeZero = threadPoolExecutor.getQueue().size();

        threadPoolExecutor.execute(new SimpleRunnable(1));
        final int shouldBeOne = threadPoolExecutor.getQueue().size();

        threadPoolExecutor.execute(new SimpleRunnable(1));
        final int shouldBeOneStill = threadPoolExecutor.getQueue().size();

        threadPoolExecutor.execute(new SimpleRunnable(2));
        final int shouldBeTwo = threadPoolExecutor.getQueue().size();

        threadPoolExecutor.execute(new SimpleRunnable(1));
        final int shouldBeTwoStill = threadPoolExecutor.getQueue().size();

        System.out.println("Waiting....");
        threadPoolExecutor.awaitTermination(4, TimeUnit.SECONDS);
        Thread.sleep(2_000);
        final long shouldBeThree = threadPoolExecutor.getCompletedTaskCount();

        //Test
        Assert.assertEquals(shouldBeZero, 0);
        Assert.assertEquals(shouldBeOne, 1);
        Assert.assertEquals(shouldBeOneStill, 1);
        Assert.assertEquals(shouldBeTwo, 2);
        Assert.assertEquals(shouldBeTwoStill, 2);
        Assert.assertEquals(shouldBeThree, 3);
    }

    private class SimpleRunnable implements Runnable {

        private Integer uid;

        public SimpleRunnable(final Integer uid) {
            this.uid = uid;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public int hashCode() {
            return this.uid.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof SimpleRunnable){
                return this.uid.equals(((SimpleRunnable) obj).uid);
            }
            return false;
        }
    }

}
