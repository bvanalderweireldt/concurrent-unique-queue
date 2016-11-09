package com.hybhub.util.concurrent;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public abstract class ConcurrentSetQueue<E> implements Queue<E> {

	protected final int capacity;

	/** Current number of elements */
	protected final AtomicInteger count = new AtomicInteger();

	/** Lock held by take, poll, etc */
	protected final ReentrantLock takeLock = new ReentrantLock();

	/** Wait queue for waiting takes */
	protected final Condition notEmpty = takeLock.newCondition();

	/** Lock held by put, offer, etc */
	protected final ReentrantLock putLock = new ReentrantLock();

	/** Wait queue for waiting puts */
	protected final Condition notFull = putLock.newCondition();

	protected final Set<E> set;

	public ConcurrentSetQueue(final int capacity) {
		this.capacity = capacity;
		this.set = new LinkedHashSet<E>();
	}

	/**
	 * Signals a waiting take. Called only from put/offer (which do not
	 * otherwise ordinarily lock takeLock.)
	 */
	protected void signalNotEmpty() {
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
	protected void signalNotFull() {
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
	protected void fullyLock() {
		putLock.lock();
		takeLock.lock();
	}

	/**
	 * Unlocks to allow both puts and takes.
	 */
	protected void fullyUnlock() {
		takeLock.unlock();
		putLock.unlock();
	}

	@Override
	public int size() {
		return count.get();
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
	public boolean isEmpty() {
		return count.get() == 0;
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
	public boolean containsAll(final Collection<?> c) {
		for (Object o : c){
			if(!this.contains(o)){
				return false;
			}
		}
		return true;
	}

	@Override
	public Iterator<E> iterator() {
		return new CopyOnWriteArrayList<E>(set).iterator();
	}

	@Override
	public Object[] toArray() {
		fullyLock();
		try {
			return set.toArray();
		}
		finally {
			fullyUnlock();
		}
	}

	@Override
	public <T> T[] toArray(final T[] a) {
		fullyLock();
		try {
			return set.toArray(a);
		}
		finally {
			fullyUnlock();
		}
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
	public boolean add(final E e) {
		if (this.offer(e))
			return true;
		else
			throw new IllegalStateException("Queue full");
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
	public E remove() {
		E x = poll();
		if (x != null)
			return x;
		else
			throw new NoSuchElementException();

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
		throw new UnsupportedOperationException();
	}
}
