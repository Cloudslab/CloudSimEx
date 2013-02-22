package org.cloudbus.cloudsim.ex.mapreduce.policy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Requests;

public abstract class Policy {
	
	Cloud cloud;
	Requests requests;
	
	List<Integer> selectedVMIds;
	
	//<Task ID, VM ID>
	Map<Integer, Integer> schedulingPlan;
	
	public Policy(Cloud cloud, Requests requests) {
		this.cloud = cloud;
		this.requests = requests;
		
		selectedVMIds = new ArrayList<Integer>();
		schedulingPlan = new HashMap<Integer, Integer>();
	}
	
	public abstract void runAlgorithm();
	
	public Cloud getCloud() {
		return cloud;
	}
	public void setCloud(Cloud cloud) {
		this.cloud = cloud;
	}
	public Requests getRequests() {
		return requests;
	}
	public void setRequests(Requests requests) {
		this.requests = requests;
	}
	public List<Integer> getSelectedVMIds() {
		return selectedVMIds;
	}
	public void setSelectedVMIds(List<Integer> selectedVMIds) {
		this.selectedVMIds = selectedVMIds;
	}
	public Map<Integer, Integer> getSchedulingPlan() {
		return schedulingPlan;
	}
	public void setSchedulingPlan(Map<Integer, Integer> schedulingPlan) {
		this.schedulingPlan = schedulingPlan;
	}
	
	
	
}
