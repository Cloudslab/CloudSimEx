package org.cloudbus.cloudsim.ex.mapreduce.policy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ex.mapreduce.PredictionEngine;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.PrivateCloudDatacenter;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.PublicCloudDatacenter;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmType;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Job;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.MapTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.ReduceTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Task;
import org.cloudbus.cloudsim.ex.mapreduce.policy.Policy.CloudDeploymentModel;

public class BruteForce
{
    public enum BruteForceSorts {
	Cost, Performance;
    }

    private List<VmInstance> nVMs = new ArrayList<VmInstance>();
    private List<Task> rTasks = new ArrayList<Task>();

    public Boolean runAlgorithm(Cloud cloud, Request request, BruteForceSorts bruteForceSort)
    {
	CloudDeploymentModel cloudDeploymentModel = request.getCloudDeploymentModel();

	// Fill nVMs
	nVMs = Policy.getAllVmInstances(cloud, request, cloudDeploymentModel);
	if (nVMs.size() == 0)
	    return false;

	// Fill rTasks
	for (MapTask mapTask : request.job.mapTasks)
	    rTasks.add(mapTask);
	for (ReduceTask reduceTask : request.job.reduceTasks)
	    rTasks.add(reduceTask);

	// Get permutations
	List<Integer[]> nPr = getPermutations(nVMs.size(), rTasks.size());

	Map<Integer, Integer> selectedSchedulingPlan = new HashMap<Integer, Integer>();

	if (bruteForceSort == BruteForceSorts.Performance)
	{
	    // Select the fastest
	    double fastest = -1;
	    for (Integer[] one_nPr : nPr)
	    {
		Map<Integer, Integer> schedulingPlan = vectorToScheduleingPlan(one_nPr);
		ExecutionPlan executionPlan = new ExecutionPlan(schedulingPlan, nVMs, request.job);
		if (fastest == -1)
		    fastest = executionPlan.ExecutionTime;
		else if (executionPlan.ExecutionTime < fastest)
		    fastest = executionPlan.ExecutionTime;
	    }

	    // Collect the candidate ExecutionPlans, where they are close to the
	    // fastest value
	    double margin = 0.0;
	    List<ExecutionPlan> candidateExecutionPlans = new ArrayList<ExecutionPlan>();
	    for (Integer[] one_nPr : nPr)
	    {
		Map<Integer, Integer> schedulingPlan = vectorToScheduleingPlan(one_nPr);
		ExecutionPlan executionPlan = new ExecutionPlan(schedulingPlan, nVMs, request.job);
		if (executionPlan.ExecutionTime - margin <= fastest)
		    candidateExecutionPlans.add(executionPlan);
	    }

	    // Select the cheapest from the fastest
	    ExecutionPlan cheapestFastestExecutionPlan = candidateExecutionPlans.get(0);
	    for (ExecutionPlan executionPlan : candidateExecutionPlans)
		if (executionPlan.Cost < cheapestFastestExecutionPlan.Cost)
		    cheapestFastestExecutionPlan = executionPlan;

	    // Selected SchedulingPlan
	    selectedSchedulingPlan = cheapestFastestExecutionPlan.schedulingPlan;
	}
	if (bruteForceSort == BruteForceSorts.Cost)
	{
	    // Select the cheapest
	    double chapest = -1;
	    for (Integer[] one_nPr : nPr)
	    {
		Map<Integer, Integer> schedulingPlan = vectorToScheduleingPlan(one_nPr);
		ExecutionPlan executionPlan = new ExecutionPlan(schedulingPlan, nVMs, request.job);
		if (chapest == -1)
		    chapest = executionPlan.Cost;
		else if (executionPlan.Cost < chapest)
		    chapest = executionPlan.Cost;
	    }

	    // Collect the candidate ExecutionPlans, where they are close to the
	    // fastest value
	    double margin = 0.0;
	    List<ExecutionPlan> candidateExecutionPlans = new ArrayList<ExecutionPlan>();
	    for (Integer[] one_nPr : nPr)
	    {
		Map<Integer, Integer> schedulingPlan = vectorToScheduleingPlan(one_nPr);
		ExecutionPlan executionPlan = new ExecutionPlan(schedulingPlan, nVMs, request.job);
		if (executionPlan.Cost - margin <= chapest)
		    candidateExecutionPlans.add(executionPlan);
	    }

	    // Select the fastest from the cheapest
	    ExecutionPlan fastestCheapestExecutionPlan = candidateExecutionPlans
		    .get(0);
	    for (ExecutionPlan executionPlan : candidateExecutionPlans)
		if (executionPlan.ExecutionTime < fastestCheapestExecutionPlan.ExecutionTime)
		    fastestCheapestExecutionPlan = executionPlan;

	    // Selected SchedulingPlan
	    selectedSchedulingPlan = fastestCheapestExecutionPlan.schedulingPlan;
	}

	// 1- Provisioning
	ArrayList<ArrayList<VmInstance>> provisioningPlans = Request.getProvisioningPlan(selectedSchedulingPlan, nVMs,
		request.job);
	request.mapAndReduceVmProvisionList = provisioningPlans.get(0);
	request.reduceOnlyVmProvisionList = provisioningPlans.get(1);

	// 2- Scheduling
	request.schedulingPlan = selectedSchedulingPlan;

	return true;
    }

    private List<Integer[]> getPermutations(int n, int r)
    {
	List<Integer[]> permutations = new ArrayList<Integer[]>();

	int[] res = new int[r];
	for (int i = 0; i < res.length; i++)
	{
	    res[i] = 1;
	}
	boolean done = false;
	while (!done)
	{
	    // Convert int[] to Integer[]
	    Integer[] resObj = new Integer[r];
	    for (int i = 0; i < res.length; i++)
	    {
		resObj[i] = res[i];
	    }
	    permutations.add(resObj);
	    // Log.printLine(Arrays.toString(resObj));
	    done = getNext(res, n, r);
	}

	// int count = 1;
	// for (Integer[] i : nCr) {
	// System.out.println(count++ + Arrays.toString(i));
	// }

	return permutations;
    }

    // ///////

    private boolean getNext(final int[] num, final int n, final int r)
    {
	int target = r - 1;
	num[target]++;
	if (num[target] > n)
	{
	    // Carry the One
	    while (num[target] >= n)
	    {
		target--;
		if (target < 0)
		{
		    break;
		}
	    }
	    if (target < 0)
	    {
		return true;
	    }
	    num[target]++;
	    for (int i = target + 1; i < num.length; i++)
	    {
		num[i] = 1;
	    }
	}
	return false;
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

class ExecutionPlan {
    public Map<Integer, Integer> schedulingPlan = new HashMap<Integer, Integer>(); // <Task
										   // ID,
										   // VM
										   // ID>
    public double ExecutionTime = 0;
    public double Cost = 0;

    public ExecutionPlan(Map<Integer, Integer> schedulingPlan, List<VmInstance> nVMs, Job job)
    {
	this.schedulingPlan = schedulingPlan;
	double[] predictedExecutionTimeAndCost = PredictionEngine.predictExecutionTimeAndCostFromScheduleingPlan(
		schedulingPlan, nVMs, job);
	ExecutionTime = predictedExecutionTimeAndCost[0];
	Cost = predictedExecutionTimeAndCost[1];
    }
}
