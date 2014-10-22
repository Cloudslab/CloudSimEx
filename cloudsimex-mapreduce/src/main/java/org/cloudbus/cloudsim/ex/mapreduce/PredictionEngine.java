package org.cloudbus.cloudsim.ex.mapreduce;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Job;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.MapTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.ReduceTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Task;

public class PredictionEngine
{
    Request request;
    Cloud cloud;
    DecimalFormat df = new DecimalFormat(".00");

    public PredictionEngine(Request request, Cloud cloud)
    {
	this.request = request.clone();
	this.cloud = cloud;
    }

    /***
     * 
     * @param schedulingPlanInput
     * @param nVMs
     * @return [Execution Time, Cost]
     */
    public double[] predictExecutionTimeAndCostFromScheduleingPlan(Map<Integer, Integer> schedulingPlanInput,
	    List<VmInstance> nVMs)
    {
	ArrayList<ArrayList<VmInstance>> provisioningPlans = getProvisioningPlan(schedulingPlanInput, nVMs, request.job);

	// 1- Provisioning
	request.mapAndReduceVmProvisionList = provisioningPlans.get(0);
	request.reduceOnlyVmProvisionList = provisioningPlans.get(1);

	// 2- Scheduling
	request.schedulingPlan = schedulingPlanInput;

	// Get the mapPhaseFinishTime
	double mapPhaseFinishTime = 0;
	for (ArrayList<VmInstance> mapAndReduce_andReduceOnlyVms : provisioningPlans) {
	    for (VmInstance vm : mapAndReduce_andReduceOnlyVms) {
		// Get a list of all map tasks that scheduled to run in this vm
		ArrayList<Task> mapTasks = new ArrayList<Task>();
		for (Entry<Integer, Integer> schedulingPlan : schedulingPlanInput.entrySet()) {
		    if (schedulingPlan.getValue() == vm.getId())
		    {
			Task task = Request.getTaskFromId(schedulingPlan.getKey(), request.job);
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
			Task task = Request.getTaskFromId(schedulingPlan.getKey(), request.job);
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

	return new double[] { Double.valueOf(df.format(maxExecutionTime)), Double.valueOf(df.format(totalCost)) };
    }

    private double getTotalExecutionTimeForTasksOnVm(ArrayList<Task> tasks, VmInstance vm)
    {
	double totalExecutionTime = 0.0;
	for (Task task : tasks)
	    if (task instanceof MapTask)
		totalExecutionTime += dataTransferTimeFromTheDataSource((MapTask) task)
			+ ((double) task.mi / vm.getMips()) + dataTransferTimeToAllReducers((MapTask) task);
	    else
		totalExecutionTime += (double) task.mi / vm.getMips();
	return totalExecutionTime;
    }

    public double dataTransferTimeFromTheDataSource(MapTask task)
    {
	VmInstance vm = request.getProvisionedVmFromTaskId(task.getCloudletId());

	String currentVmTypeName = vm.name;

	for (List<?> throughputs_vm_ds : cloud.throughputs_ds_vm)
	{
	    String source_dataSource = (String) throughputs_vm_ds.get(0);
	    String destination_vm = (String) throughputs_vm_ds.get(1);
	    double throughputInMegaBit = (Double) throughputs_vm_ds.get(2);

	    if (destination_vm.equals(currentVmTypeName) && source_dataSource.equals(request.job.dataSourceName))
		return task.dSize / (throughputInMegaBit / 8.0);
	}

	try
	{
	    throw new Exception("Throughputs between " + currentVmTypeName + " and " + request.job.dataSourceName
		    + " could not be found in Cloud.yaml");
	} catch (Exception e)
	{
	    e.printStackTrace();
	}
	return 0.0;
    }

    public double dataTransferTimeToAllReducers(MapTask mapTask)
    {
	double transferTime = 0.0;
	VmInstance vm = request.getProvisionedVmFromTaskId(mapTask.getCloudletId());

	// For each reduce get the transfer Time
	for (ReduceTask reduceTask : request.job.reduceTasks)
	{
	    if (request.schedulingPlan.containsKey(reduceTask.getCloudletId()))
		transferTime += dataTransferTimeToOneReducer(reduceTask, mapTask, vm);
	}

	return transferTime;
    }

    private double dataTransferTimeToOneReducer(ReduceTask reduceTask, MapTask mapTask, VmInstance vm)
    {
	String currentVmTypeName = vm.name;

	if (mapTask.intermediateData.containsKey(reduceTask.name))
	{
	    int intermediateDataSizeInMegaByte = mapTask.intermediateData.get(reduceTask.name);

	    VmInstance reduceVm = request.getProvisionedVmFromTaskId(reduceTask.getCloudletId());
	    String reduceVmTypeName = reduceVm.name;

	    // if the Reduce vm is in the same Map vm, the Transfer Time is zero
	    if (currentVmTypeName.equals(reduceVmTypeName))
		return 0;

	    for (List<?> throughputs_vm_vm : cloud.throughputs_vm_vm)
	    {
		String source_vm = (String) throughputs_vm_vm.get(0);
		String destination_vm = (String) throughputs_vm_vm.get(1);
		double throughputInMegaBit = (Double) throughputs_vm_vm.get(2);

		if (source_vm.equals(currentVmTypeName) && destination_vm.equals(reduceVmTypeName))
		{
		    return intermediateDataSizeInMegaByte / (throughputInMegaBit / 8.0);
		}
	    }
	    try
	    {
		throw new Exception("Throughputs between " + currentVmTypeName + " and " + reduceVmTypeName
			+ " could not be found in Cloud.yaml");
	    } catch (Exception e)
	    {
		e.printStackTrace();
	    }
	}
	return 0;
    }

    private double getTotalCostOnVm(ArrayList<MapTask> mapTasks, VmInstance vm, double totalExecutionTimeInVm)
    {
	double dataTransferCostFromTheDataSource = 0;// DC-in
	double vmCost = 0;// VMC
	double dataTransferCostToReduceVms = 0;// DC-out

	for (MapTask mapTask : mapTasks) {
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

    public Map<Integer, Integer> vectorToScheduleingPlan(Integer[] vector, List<VmInstance> nVMs, List<Task> rTasks)
    {
	Map<Integer, Integer> scheduleingPlan = new HashMap<Integer, Integer>();

	for (int i = 0; i < vector.length; i++)
	{
	    int TaskId = rTasks.get(i).getCloudletId();
	    int VmId = nVMs.get(vector[i] - 1).getId();
	    scheduleingPlan.put(TaskId, VmId);
	}

	return scheduleingPlan;
    }

    public Map<Integer, Integer> vectorToScheduleingPlan(Integer[] vector, List<Task> rTasks)
    {
	Map<Integer, Integer> scheduleingPlan = new HashMap<Integer, Integer>();

	for (int i = 0; i < vector.length; i++)
	{
	    int TaskId = rTasks.get(i).getCloudletId();
	    int VmId = vector[i];
	    scheduleingPlan.put(TaskId, VmId);
	}

	return scheduleingPlan;
    }

}
