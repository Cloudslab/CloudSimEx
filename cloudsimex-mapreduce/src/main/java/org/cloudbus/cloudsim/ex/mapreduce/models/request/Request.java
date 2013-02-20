package org.cloudbus.cloudsim.ex.mapreduce.models.request;

public class Request {
	public Double budget;
	public int deadline;
	public String jobFile;//This should not job file, this should be a new class called MapReduceJob with a list of Map and Reduce objects
	public UserClass userClass;
}
