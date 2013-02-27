package org.cloudbus.cloudsim.ex.mapreduce.policy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VMType;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Requests;

public abstract class Policy {
	//<Task ID, VM ID>
	Map<Integer, Integer> schedulingPlan;
	
	public Policy() {
		
		schedulingPlan = new HashMap<Integer, Integer>();
	}
	
	public abstract List<VMType> runAlgorithm(Cloud cloud, Request request);
	
	public Map<Integer, Integer> getSchedulingPlan() {
		return schedulingPlan;
	}
	public void setSchedulingPlan(Map<Integer, Integer> schedulingPlan) {
		this.schedulingPlan = schedulingPlan;
	}
	
	
	
}
