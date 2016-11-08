package com.hybhub.util.concurrent;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class SetBlockingQueue<E> implements BlockingQueue<E>, Queue<E> {

	private static final int DEFAULT_MAX_CAPACITY = Integer.MAX_VALUE;

	private final int capacity;

	/** Current number of elements */
	private final AtomicInteger count = new AtomicInteger();

	/** Lock held by take, poll, etc */
	private final ReentrantLock takeLock = new ReentrantLock();

	/** Wait queue for waiting takes */
	private final Condition notEmpty = takeLock.newCondition();

	/** Lock held by put, offer, etc */
	private final ReentrantLock putLock = new ReentrantLock();

	/** Wait queue for waiting puts */
	private final Condition notFull = putLock.newCondition();

	private final Set<E> set;

	/**
	 * Signals a waiting take. Called only from put/offer (which do not
	 * otherwise ordinarily lock takeLock.)
	 */
	private void signalNotEmpty() {
		final ReentrantLock takeLock = this.takeLock;
		takeLock.lock();
		try {
			notEmpty.signal();
		} finally {
			takeLock.unlock();
		}
	}

	/**
	 * Signals a waiting put. Called only from take/poll.
	 */
	private void signalNotFull() {
		final ReentrantLock putLock = this.putLock;
		putLock.lock();
		try {
			notFull.signal();
		} finally {
			putLock.unlock();
		}
	}

	/**
	 * Locks to prevent both puts and takes.
	 */
	void fullyLock() {
		putLock.lock();
		takeLock.lock();
	}

	/**
	 * Unlocks to allow both puts and takes.
	 */
	void fullyUnlock() {
		takeLock.unlock();
		putLock.unlock();
	}

	public SetBlockingQueue(final int capacity) {
		super();
		this.set = new LinkedHashSet<E>();
		this.capacity = capacity;
	}

	public SetBlockingQueue() {
		this(DEFAULT_MAX_CAPACITY);
	}

	@Override
	public int remainingCapacity() {
		return capacity - count.get();
	}

	@Override
	public boolean remove(final Object o) {
		if (o == null) return false;
		fullyLock();
		try {
			if(set.contains(o)){
				set.remove(o);
				count.decrementAndGet();
				return true;
			}
			return false;
		} finally {
			fullyUnlock();
		}
	}

	@Override
	public boolean containsAll(final Collection<?> c) {
		for (Object o : c){
			if(!this.contains(o)){
				return false;
			}
		}

		return true;
	}

	@Override
	public boolean addAll(final Collection<? extends E> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll(final Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(final Collection<?> c) {
		return false;
	}

	@Override
	public void clear() {
		fullyLock();
		try {
			set.clear();
		}
		finally {
			fullyUnlock();
		}

	}

	@Override
	public int size() {
		return count.get();
	}

	@Override
	public boolean isEmpty() {
		return count.get() == 0;
	}

	@Override
	public boolean contains(final Object o) {
		if(o == null) return false;

		fullyLock();
		try {
			return set.contains(o);
		}
		finally {
			fullyUnlock();
		}
	}

	@Override
	public Iterator<E> iterator() {
		return null;
	}

	@Override
	public Object[] toArray() {
		fullyLock();
		try {
			Object[] objects = new Object[count.get()];
			return set.toArray();
		}
		finally {
			fullyUnlock();
		}
	}

	@Override
	public <T> T[] toArray(final T[] a) {
		return null;
	}

	@Override
	public boolean add(final E e) {
		if (this.offer(e))
			return true;
		else
			throw new IllegalStateException("Queue full");
	}

	@Override
	public boolean offer(final E e) {
		if (e == null) throw new NullPointerException();
		final AtomicInteger count = this.count;
		if (count.get() == capacity)
			return false;
		int c = -1;
		final ReentrantLock putLock = this.putLock;
		putLock.lock();
		try {
			if (count.get() < capacity && ! this.contains(e)) {
				set.add(e);
				c = count.getAndIncrement();
				if (c + 1 < capacity)
					notFull.signal();
			}
		} finally {
			putLock.unlock();
		}
		if (c == 0)
			signalNotEmpty();
		return c >= 0;
	}

	@Override
	public E remove() {
		E x = poll();
		if (x != null)
			return x;
		else
			throw new NoSuchElementException();

	}

	@Override
	public E poll() {
		final AtomicInteger count = this.count;
		if (count.get() == 0)
			return null;
		E x = null;
		int c = -1;
		final ReentrantLock takeLock = this.takeLock;
		takeLock.lock();
		try {
			if (count.get() > 0) {
				x = set.iterator().next();
				set.remove(x);
				c = count.getAndDecrement();
				if (c > 1)
					notEmpty.signal();
			}
		} finally {
			takeLock.unlock();
		}
		if (c == capacity)
			signalNotFull();
		return x;	}

	@Override
	public E element() {
		E x = peek();
		if (x != null)
			return x;
		else
			throw new NoSuchElementException();
	}

	@Override
	public E peek() {
		if (count.get() == 0)
			return null;
		final ReentrantLock takeLock = this.takeLock;
		takeLock.lock();
		try {
			return set.iterator().next();
		} finally {
			takeLock.unlock();
		}
	}

	@Override
	public void put(final E e) throws InterruptedException {
		if (e == null) throw new NullPointerException();

		int c = -1;
		final ReentrantLock putLock = this.putLock;
		final AtomicInteger count = this.count;
		putLock.lockInterruptibly();
		try {
			if (set.contains(e)) return;

			while (count.get() == capacity) {
				notFull.await();
			}
			set.add(e);
			c = count.getAndIncrement();
			if (c + 1 < capacity)
				notFull.signal();
		} finally {
			putLock.unlock();
		}
		if (c == 0)
			signalNotEmpty();
	}

	@Override
	public boolean offer(final E e, final long timeout, final TimeUnit unit) throws InterruptedException {
		if (e == null) throw new NullPointerException();
		long nanos = unit.toNanos(timeout);
		int c = -1;
		final ReentrantLock putLock = this.putLock;
		final AtomicInteger count = this.count;
		putLock.lockInterruptibly();
		try {
			if (set.contains(e)) return false;

			while (count.get() == capacity) {
				if (nanos <= 0)
					return false;
				nanos = notFull.awaitNanos(nanos);
			}
			set.add(e);
			c = count.getAndIncrement();
			if (c + 1 < capacity)
				notFull.signal();
		} finally {
			putLock.unlock();
		}
		if (c == 0)
			signalNotEmpty();
		return true;
	}

	@Override
	public E take() throws InterruptedException {
		E x;
		int c = -1;
		final AtomicInteger count = this.count;
		final ReentrantLock takeLock = this.takeLock;
		takeLock.lockInterruptibly();
		try {
			while (count.get() == 0) {
				notEmpty.await();
			}
			x = set.iterator().next();
			set.remove(x);
			c = count.getAndDecrement();
			if (c > 1)
				notEmpty.signal();
		} finally {
			takeLock.unlock();
		}
		if (c == capacity)
			signalNotFull();
		return x;
	}

	@Override
	public E poll(final long timeout, final TimeUnit unit) throws InterruptedException {
		E x = null;
		int c = -1;
		long nanos = unit.toNanos(timeout);
		final AtomicInteger count = this.count;
		final ReentrantLock takeLock = this.takeLock;
		takeLock.lockInterruptibly();
		try {
			while (count.get() == 0) {
				if (nanos <= 0)
					return null;
				nanos = notEmpty.awaitNanos(nanos);
			}
			x = set.iterator().next();
			set.remove(x);
			c = count.getAndDecrement();
			if (c > 1)
				notEmpty.signal();
		} finally {
			takeLock.unlock();
		}
		if (c == capacity)
			signalNotFull();
		return x;
	}

	@Override
	public int drainTo(final Collection<? super E> c) {
		return drainTo(c, Integer.MAX_VALUE);
	}

	@Override
	public int drainTo(final Collection<? super E> c, final int maxElements) {
		if (c == null)
			throw new NullPointerException();
		if (c == this)
			throw new IllegalArgumentException();
		if (maxElements <= 0)
			return 0;
		boolean signalNotFull = false;
		final ReentrantLock takeLock = this.takeLock;
		takeLock.lock();
		try {
			int n = Math.min(maxElements, count.get());
			int i = 0;
			try {
				while (i < n) {
					E e = set.iterator().next();
					set.remove(e);
					c.add(e);
					++i;
				}
				return n;
			} finally {
				if (i > 0) {
					signalNotFull = (count.getAndAdd(-i) == capacity);
				}
			}
		} finally {
			takeLock.unlock();
			if (signalNotFull)
				signalNotFull();
		}
	}

}
