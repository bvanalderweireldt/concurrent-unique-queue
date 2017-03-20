package com.hybhub.util.concurrent;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract concurrent class holding a set and methods to handle concurrent locks.
 * The {@link java.util.Set} is a {@link LinkedHashSet}, it guaranties unity,
 * and is aware of the order of insertion inside the {@link java.util.Set},
 * so we are able to simulate a "first in first out" strategy.
 * Inspired by {@link java.util.concurrent.LinkedBlockingQueue}
 * @param <E>
 */
public abstract class AbstractConcurrentSet<E> {

	/** Maximum capacity of the queue */
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

	/** Set backing the queue */
	protected final Set<E> set;

	/**
	 * Instantiate a concurrent set with a maximum capacity of capacity
	 * @param capacity
	 */
	public AbstractConcurrentSet(final int capacity) {
		this.capacity = capacity;
		this.set = new LinkedHashSet<E>();
	}

	/**
	 * Signals a waiting take. Called only from put/offer (which do not
	 * otherwise ordinarily lock takeLock.)
	 */
	protected void signalNotEmpty() {
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
}
