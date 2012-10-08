package org.cloudbus.cloudsim.web;


public interface ICloudLetGenerator<T> {

	T peek();
	
	T pop();

	boolean isEmpy();
}
