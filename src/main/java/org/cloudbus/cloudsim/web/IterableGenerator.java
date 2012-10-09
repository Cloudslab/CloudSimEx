package org.cloudbus.cloudsim.web;

import java.util.Iterator;

public class IterableGenerator<T> implements
		IGenerator<T> {

	private Iterator<T> iterator;
	private T peeked;

	public IterableGenerator(Iterable<T> collection) {
		this.iterator = collection.iterator();
	}

	public T peek() {
		peeked = peeked == null ? iterator.next() : peeked;
		return peeked;
	}

	public T poll() {
		T result = peeked;
		if (peeked != null) {
			peeked = null;
		} else {
			result = iterator.next();
		}
		return result;
	}

	public boolean isEmpty() {
		return peeked == null && !iterator.hasNext();
	}

	@Override
	public void prefetch(final double time) {
		// Do nothing
	}

}
