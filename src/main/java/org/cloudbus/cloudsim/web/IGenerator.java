package org.cloudbus.cloudsim.web;


public interface IGenerator<T> {

	T peek();
	
	T poll();

	boolean isEmpty();
	
	void prefetch(final double time);
}
