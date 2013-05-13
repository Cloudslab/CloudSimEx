package org.cloudbus.cloudsim.ex.mapreduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Job;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.MapTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.ReduceTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Task;
import org.cloudbus.cloudsim.ex.util.CustomLog;

public class PredictionEngine
{
    /***
     * 
     * @param schedulingPlanInput
     * @param nVMs
     * @return [Execution Time, Cost]
     */
    public double[] predictExecutionTimeAndCostFromScheduleingPlan(Map<Integer, Integer> schedulingPlanInput,
	    List<VmInstance> nVMs, Job job)
    {

	ArrayList<ArrayList<VmInstance>> provisioningPlans = getProvisioningPlan(schedulingPlanInput, nVMs, job);

	// Get the mapPhaseFinishTime
	double mapPhaseFinishTime = 0;
	for (ArrayList<VmInstance> BothMapAndReduceAndReduceOnlyVms : provisioningPlans) {
	    for (VmInstance vm : BothMapAndReduceAndReduceOnlyVms) {
		// Get a list of all map tasks that scheduled to run in this vm
		ArrayList<Task> mapTasks = new ArrayList<Task>();
		for (Entry<Integer, Integer> schedulingPlan : schedulingPlanInput.entrySet()) {
		    if (schedulingPlan.getValue() == vm.getId())
		    {
			Task task = Request.getTaskFromId(schedulingPlan.getKey(), job);
			if (task instanceof MapTask)
			    mapTasks.add(task);
		    }
		}

		double totalExecutionTimeInVmForMapOnly = getTotalExecutionTimeForTasksOnVm(mapTasks, vm);
		if (totalExecutionTimeInVmForMapOnly > mapPhaseFinishTime)
		    mapPhaseFinishTime = totalExecutionTimeInVmForMapOnly;
	    }
	}

	// Now get the totalCost and maxExecutionTime
	double maxExecutionTime = mapPhaseFinishTime;
	double totalCost = 0;

	for (ArrayList<VmInstance> mapAndReduce_andReduceOnlyVms : provisioningPlans) {
	    for (VmInstance vm : mapAndReduce_andReduceOnlyVms) {
		// Get a list of all reduce tasks that scheduled to run in this
		// vm
		ArrayList<Task> reduceTasks = new ArrayList<Task>();
		// Get a list of all map tasks that scheduled to run in this
		// vm
		ArrayList<MapTask> mapTasks = new ArrayList<MapTask>();
		for (Entry<Integer, Integer> schedulingPlan : schedulingPlanInput.entrySet()) {
		    if (schedulingPlan.getValue() == vm.getId())
		    {
			Task task = Request.getTaskFromId(schedulingPlan.getKey(), job);
			if (task instanceof ReduceTask)
			    reduceTasks.add(task);
			if (task instanceof MapTask)
			    mapTasks.add((MapTask) task);
		    }
		}

		double totalExecutionTimeInVm = mapPhaseFinishTime
			+ getTotalExecutionTimeForTasksOnVm(reduceTasks, vm);
		if (totalExecutionTimeInVm > maxExecutionTime)
		    maxExecutionTime = totalExecutionTimeInVm;
		totalCost += getTotalCostOnVm(mapTasks, vm, totalExecutionTimeInVm);
	    }
	}

	return new double[] { maxExecutionTime, totalCost };
    }

    private double getTotalExecutionTimeForTasksOnVm(ArrayList<Task> tasks, VmInstance vm)
    {
	double totalExecutionTime = 0.0;
	for (Task task : tasks)
	    totalExecutionTime += (double) task.mi / vm.getMips();
	return totalExecutionTime;
    }

    private double getTotalCostOnVm(ArrayList<MapTask> mapTasks, VmInstance vm, double totalExecutionTimeInVm)
    {
	double dataTransferCostFromTheDataSource = 0;// DC-in
	double vmCost = 0;// VMC
	double dataTransferCostToReduceVms = 0;// DC-out

	for (Task task : mapTasks) {
	    MapTask mapTask = (MapTask) task;
	    dataTransferCostFromTheDataSource += mapTask.dataTransferCostFromTheDataSource();
	    dataTransferCostToReduceVms += mapTask.dataTransferCostToAllReducers(vm);
	}

	vmCost = Math.ceil(totalExecutionTimeInVm / 3600.0) * vm.vmCostPerHour;

	return dataTransferCostFromTheDataSource + vmCost + dataTransferCostToReduceVms;
    }

    /***
     * Get VM provisioning plan from a scheduling plan
     */
    public ArrayList<ArrayList<VmInstance>> getProvisioningPlan(Map<Integer, Integer> schedulingPlan,
	    List<VmInstance> nVMs, Job job)
    {
	ArrayList<ArrayList<VmInstance>> provisioningPlans = new ArrayList<ArrayList<VmInstance>>(2); // To
												      // remove
												      // the
												      // temporary
												      // VMs
	// Index 0 for: mapAndReduceVmProvisionList
	provisioningPlans.add(new ArrayList<VmInstance>());
	// Index 1 for: reduceOnlyVmProvisionList
	provisioningPlans.add(new ArrayList<VmInstance>());

	for (Map.Entry<Integer, Integer> entry : schedulingPlan.entrySet()) {
	    Task task = job.getTask(entry.getKey());
	    if (task instanceof MapTask)
		for (VmInstance vm : nVMs) {
		    if (entry.getValue() == vm.getId())
			if (!provisioningPlans.get(0).contains(vm) && !provisioningPlans.get(1).contains(vm))
			    provisioningPlans.get(0).add(vm);
		}
	}

	for (Map.Entry<Integer, Integer> entry : schedulingPlan.entrySet()) {
	    Task task = job.getTask(entry.getKey());
	    if (task instanceof ReduceTask)
		for (VmInstance vm : nVMs) {
		    if (entry.getValue() == vm.getId())
			if (!provisioningPlans.get(0).contains(vm) && !provisioningPlans.get(1).contains(vm))
			    provisioningPlans.get(1).add(vm);
		}
	}

	return provisioningPlans;
    }
    
    public Map<Integer, Integer> vectorToScheduleingPlan(Integer[] res, List<VmInstance> nVMs, List<Task> rTasks)
    {
	Map<Integer, Integer> scheduleingPlan = new HashMap<Integer, Integer>();

	for (int i = 0; i < res.length; i++)
	{
	    int TaskId = rTasks.get(i).getCloudletId();
	    int VmId = nVMs.get(res[i] - 1).getId();
	    scheduleingPlan.put(TaskId, VmId);
	}

	return scheduleingPlan;
    }

}
