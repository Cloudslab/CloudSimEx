package org.cloudbus.cloudsim.incubator.web;


public interface IGenerator<T> {

	T peek();
	
	T poll();

	boolean isEmpty();
	
	void prefetch(final double time);
}
