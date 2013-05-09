package org.cloudbus.cloudsim.ex.mapreduce;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Job;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.MapTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.ReduceTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Task;

public class PredictionEngine
{
    /***
     * 
     * @param schedulingPlanInput
     * @param nVMs
     * @return [Execution Time, Cost]
     */
    public static double[] predictExecutionTimeAndCostFromScheduleingPlan(Map<Integer, Integer> schedulingPlanInput,
	    List<VmInstance> nVMs, Job job)
    {

	ArrayList<ArrayList<VmInstance>> provisioningPlans = Request
		.getProvisioningPlan(schedulingPlanInput, nVMs, job);

	// Get the mapPhaseFinishTime
	double mapPhaseFinishTime = 0;
	for (ArrayList<VmInstance> BothMapAndReduceAndReduceOnlyVms : provisioningPlans) {
	    for (VmInstance vm : BothMapAndReduceAndReduceOnlyVms) {
		ArrayList<Task> tasks = new ArrayList<Task>();
		for (Entry<Integer, Integer> schedulingPlan : schedulingPlanInput.entrySet()) {
		    if (schedulingPlan.getValue() == vm.getId())
			tasks.add(Request.getTaskFromId(schedulingPlan.getKey(), job));
		}

		double totalExecutionTimeInVmForMapOnly = getTotalExecutionTimeForMapsOnlyOnVm(tasks, vm);
		if (totalExecutionTimeInVmForMapOnly > mapPhaseFinishTime)
		    mapPhaseFinishTime = totalExecutionTimeInVmForMapOnly;
	    }
	}

	// Now get the totalCost and maxExecutionTime
	double maxExecutionTime = 0;
	double totalCost = 0;

	for (ArrayList<VmInstance> BothMapAndReduceAndReduceOnlyVms : provisioningPlans) {
	    for (VmInstance mapAndReduceVm : BothMapAndReduceAndReduceOnlyVms) {
		ArrayList<Task> tasks = new ArrayList<Task>();
		for (Entry<Integer, Integer> schedulingPlan : schedulingPlanInput.entrySet()) {
		    if (schedulingPlan.getValue() == mapAndReduceVm.getId())
			tasks.add(Request.getTaskFromId(schedulingPlan.getKey(), job));
		}

		double totalExecutionTimeInVm = getTotalExecutionTimeOnVm(tasks, mapAndReduceVm, mapPhaseFinishTime);
		if (totalExecutionTimeInVm > maxExecutionTime)
		    maxExecutionTime = totalExecutionTimeInVm;
		totalCost += getTotalCostOnVm(tasks, mapAndReduceVm, mapPhaseFinishTime);
	    }
	}

	return new double[] { maxExecutionTime, totalCost };
    }

    private static double getTotalExecutionTimeForMapsOnlyOnVm(ArrayList<Task> tasks, VmInstance vm)
    {
	double totalExecutionTime = 0;
	for (Task task : tasks)
	{
	    if (task instanceof MapTask)
		totalExecutionTime += task.mi / vm.getMips();
	}
	return totalExecutionTime;
    }

    private static double getTotalExecutionTimeOnVm(ArrayList<Task> tasks, VmInstance vm, double mapPhaseFinishTime)
    {
	double totalReducePhaseExecutionTime = 0;

	for (Task task : tasks)
	{
	    if (task instanceof ReduceTask)
		totalReducePhaseExecutionTime += task.mi / vm.getMips();
	}

	return mapPhaseFinishTime + totalReducePhaseExecutionTime;
    }

    private static double getTotalCostOnVm(ArrayList<Task> tasks, VmInstance vm, double mapPhaseFinishTime)
    {
	double dataTransferCostFromTheDataSource = 0;// DC-in
	double vmCost = 0;// VMC
	double dataTransferCostToReduceVms = 0;// DC-out

	for (Task task : tasks) {
	    if (task instanceof MapTask)
	    {
		MapTask mapTask = (MapTask) task;
		dataTransferCostFromTheDataSource += mapTask.dataTransferCostFromTheDataSource();
		dataTransferCostToReduceVms += mapTask.dataTransferCostToAllReducers(vm);
	    }
	}

	vmCost = Math.ceil(getTotalExecutionTimeOnVm(tasks, vm, mapPhaseFinishTime) / 3600.0) * vm.vmCostPerHour;

	return dataTransferCostFromTheDataSource + vmCost + dataTransferCostToReduceVms;
    }

}
