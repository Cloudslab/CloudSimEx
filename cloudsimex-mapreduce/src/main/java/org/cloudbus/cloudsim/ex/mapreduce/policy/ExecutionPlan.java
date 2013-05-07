package org.cloudbus.cloudsim.ex.mapreduce.policy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.ex.mapreduce.PredictionEngine;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Job;

public class ExecutionPlan {
	public Map<Integer, Integer> schedulingPlan = new HashMap<Integer, Integer>(); //<Task ID, VM ID>
	public double ExecutionTime = 0;
	public double Cost = 0;
	
	public ExecutionPlan(Map<Integer, Integer> schedulingPlan, List<VmInstance> nVMs, Job job)
	{
		this.schedulingPlan = schedulingPlan;
		double[] predictedExecutionTimeAndCost = PredictionEngine.predictExecutionTimeAndCostFromScheduleingPlan(schedulingPlan, nVMs, job);
		ExecutionTime = predictedExecutionTimeAndCost[0];
		Cost = predictedExecutionTimeAndCost[1];
	}
}
