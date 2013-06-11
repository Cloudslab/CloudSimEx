package org.cloudbus.cloudsim.ex.mapreduce.policy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.ex.mapreduce.PredictionEngine;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Job;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.MapTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.ReduceTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Task;
import org.cloudbus.cloudsim.ex.mapreduce.policy.Policy.CloudDeploymentModel;

public class BT
{
    public enum BruteForceSorts {
	Cost, Performance;
    }

    private List<VmInstance> nVMs = new ArrayList<VmInstance>();
    private List<Task> rTasks = new ArrayList<Task>();
    PredictionEngine predictionEngine;
    private long forceToExitTimeMillis = 3 * 60 * 1000;// 3 min
    private Request request;
    private long startTime;

    public Boolean runAlgorithm(Cloud cloud, Request request, BruteForceSorts bruteForceSort)
    {
	CloudDeploymentModel cloudDeploymentModel = request.getCloudDeploymentModel();
	this.request = request;
	predictionEngine = new PredictionEngine(request, cloud);

	// Fill nVMs
	int numTasks = request.job.mapTasks.size() + request.job.reduceTasks.size();
	nVMs = Policy.getAllVmInstances(cloud, request, cloudDeploymentModel, numTasks);
	if (nVMs.size() == 0)
	    return false;

	// Fill rTasks
	for (MapTask mapTask : request.job.mapTasks)
	    rTasks.add(mapTask);
	for (ReduceTask reduceTask : request.job.reduceTasks)
	    rTasks.add(reduceTask);

	startTime = System.currentTimeMillis();
	// Get permutations
	ExecutionPlan selectedExecutionPlan = geExecutionPlan(nVMs.size(), rTasks.size());

	Map<Integer, Integer> selectedSchedulingPlan = selectedExecutionPlan.schedulingPlan;

	// 1- Provisioning
	ArrayList<ArrayList<VmInstance>> provisioningPlans = predictionEngine.getProvisioningPlan(
		selectedSchedulingPlan, nVMs,
		request.job);
	request.mapAndReduceVmProvisionList = provisioningPlans.get(0);
	request.reduceOnlyVmProvisionList = provisioningPlans.get(1);

	// 2- Scheduling
	request.schedulingPlan = selectedSchedulingPlan;

	return true;
    }

    private ExecutionPlan geExecutionPlan(int n, int r)
    {
	ExecutionPlan selectedExecutionPlan = null;

	Integer[] res = new Integer[r];
	for (int i = 0; i < res.length; i++)
	{
	    res[i] = 1;
	}
	boolean done = false;
	while (!done)
	{
	    Map<Integer, Integer> schedulingPlan = vectorToScheduleingPlan(res);
	    ExecutionPlan executionPlan = new ExecutionPlan(schedulingPlan, nVMs, request.job);
	    if (selectedExecutionPlan == null || executionPlan.Cost < selectedExecutionPlan.Cost)
		selectedExecutionPlan = executionPlan;

	    if (request.getAlgoFirstSoulationFoundedTime() == null
		    && executionPlan.Cost <= request.getBudget()
		    && executionPlan.ExecutionTime <= request.getDeadline())
	    {
		Long algoStartTime = request.getAlgoStartTime();
		Long currentTime = System.currentTimeMillis();
		request.setAlgoFirstSoulationFoundedTime((currentTime - algoStartTime));
	    }

	    long currentRunningTime = System.currentTimeMillis();
	    if (currentRunningTime - startTime >= forceToExitTimeMillis)
		break;

	    done = getNext(res, n, r);
	}

	return selectedExecutionPlan;
    }

    private boolean getNext(final Integer[] num, final int n, final int r)
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
	    double[] predictedExecutionTimeAndCost = predictionEngine.predictExecutionTimeAndCostFromScheduleingPlan(
		    schedulingPlan, nVMs);
	    ExecutionTime = predictedExecutionTimeAndCost[0];
	    Cost = predictedExecutionTimeAndCost[1];
	}
    }

}
