package org.cloudbus.cloudsim.web;

import java.util.Iterator;

import org.cloudbus.cloudsim.Cloudlet;

public class IterableGenerator<T> implements
		ICloudLetGenerator<T> {

	private Iterator<T> iterator;
	private T peeked;

	public IterableGenerator(Iterable<T> collection) {
		super();
		this.iterator = collection.iterator();
	}

	public T peek() {
		peeked = peeked == null ? iterator.next() : peeked;
		return peeked;
	}

	public T pop() {
		if (peeked == null) {
			peeked = null;
			return peeked;
		} else {
			return iterator.next();
		}

	}

	public boolean isEmpy() {
		return peeked == null && !iterator.hasNext();
	}


}
