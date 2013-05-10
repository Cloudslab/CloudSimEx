package org.cloudbus.cloudsim.ex.mapreduce.policy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ex.mapreduce.PredictionEngine;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.MapTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.ReduceTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Task;
import org.cloudbus.cloudsim.ex.mapreduce.policy.Policy.CloudDeploymentModel;

public class Backtracking {

    public enum BacktrackingSorts {
	Cost, Performance;
    }

    private List<VmInstance> nVMs = new ArrayList<VmInstance>();
    private List<Task> rTasks = new ArrayList<Task>();
    private Request request;
    private BacktrackingSorts backtrackingSort;

    public Boolean runAlgorithm(Cloud cloud, Request request, BacktrackingSorts backtrackingSort) {
	this.request = request;
	this.backtrackingSort = backtrackingSort;
	CloudDeploymentModel cloudDeploymentModel = request.getCloudDeploymentModel();

	// Fill nVMs
	int numTasks = request.job.mapTasks.size() + request.job.reduceTasks.size();
	nVMs = Policy.getAllVmInstances(cloud, request, cloudDeploymentModel, numTasks);
	if (nVMs.size() == 0)
	    return false;

	if (backtrackingSort == BacktrackingSorts.Cost)
	{
	    // Sort nVMs by cost
	    Collections.sort(nVMs, new Comparator<VmInstance>() {
		public int compare(VmInstance VmInstance1, VmInstance VmInstance2) {
		    return Double.compare(VmInstance1.vmCostPerHour, VmInstance2.vmCostPerHour);
		}
	    });
	}

	if (backtrackingSort == BacktrackingSorts.Performance)
	{
	    // Sort nVMs by mips (performance)
	    Collections.sort(nVMs, new Comparator<VmInstance>() {
		public int compare(VmInstance VmInstance1, VmInstance VmInstance2) {
		    return Double.compare(VmInstance2.getMips(), VmInstance1.getMips());
		}
	    });
	}

	// Fill rTasks
	for (MapTask mapTask : request.job.mapTasks)
	    rTasks.add(mapTask);
	for (ReduceTask reduceTask : request.job.reduceTasks)
	    rTasks.add(reduceTask);

	// Selected SchedulingPlan from backtracking
	Map<Integer, Integer> selectedSchedulingPlan = getFirstSolutionOfBackTracking(nVMs.size(), rTasks.size());
	if (selectedSchedulingPlan == null)
	    return false;

	// 1- Provisioning
	ArrayList<ArrayList<VmInstance>> provisioningPlans = Request.getProvisioningPlan(selectedSchedulingPlan, nVMs,
		request.job);
	request.mapAndReduceVmProvisionList = provisioningPlans.get(0);
	request.reduceOnlyVmProvisionList = provisioningPlans.get(1);

	// 2- Scheduling
	request.schedulingPlan = selectedSchedulingPlan;

	return true;
    }

    private Map<Integer, Integer> getFirstSolutionOfBackTracking(int n, int r) {
	int[] res = new int[] { 1 };
	// for (int i = 0; i < res.length; i++) {
	// res[i] = 1;
	// }
	boolean done = false;
	while (!done) {
	    // Convert int[] to Integer[]
	    Integer[] resObj = new Integer[res.length];
	    for (int i = 0; i < res.length; i++) {
		resObj[i] = res[i];
	    }
	    Map<Integer, Integer> schedulingPlan = vectorToScheduleingPlan(resObj);
	    double[] executionTimeAndCost = PredictionEngine.predictExecutionTimeAndCostFromScheduleingPlan(
		    schedulingPlan, nVMs, request.job);
	    //if((System.currentTimeMillis()/5000) % 5 == 0)
		//Log.printLine(Arrays.toString(resObj) + "->"+(r-res.length)+" : " + Arrays.toString(executionTimeAndCost));
	    if(backtrackingSort == BacktrackingSorts.Performance && executionTimeAndCost[0] > request.getDeadline())
		return null;
	    if(backtrackingSort == BacktrackingSorts.Cost && executionTimeAndCost[1] > request.getBudget())
		return null;
	    if (executionTimeAndCost[0] <= request.deadline && executionTimeAndCost[1] <= request.budget)
	    {
		if (res.length == r)
		    return schedulingPlan;
		else
		    res = goDeeper(res, n);
	    }
	    else
	    {
		if (res.length == r && res[res.length - 1] < n)
		    res[res.length - 1]++;
		else
		    done = (res = goBack(res, n, r)) == null ? true : false;
	    }
	}

	return null;
    }

    private int[] goDeeper(int[] num, int n) {
	int[] res = new int[num.length + 1];
	for (int i = 0; i < res.length - 1; i++) {
	    res[i] = num[i];
	}
	res[res.length - 1] = 1;
	return res;
    }

    private int[] goBack(int[] num, int n, int r) {
	do {
	    int[] res = new int[num.length - 1];
	    if (res.length == 0)
		return null;
	    for (int i = 0; i < res.length; i++)
		res[i] = num[i];
	    res[res.length - 1]++;
	    num = res;
	} while (num[num.length - 1] > n);
	return num;

    }

    private Map<Integer, Integer> vectorToScheduleingPlan(Integer[] res)
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
