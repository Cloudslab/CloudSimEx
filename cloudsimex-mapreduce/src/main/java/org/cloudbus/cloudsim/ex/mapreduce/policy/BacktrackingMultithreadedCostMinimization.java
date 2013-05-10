package org.cloudbus.cloudsim.ex.mapreduce.policy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ex.mapreduce.PredictionEngine;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.MapTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.ReduceTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Task;
import org.cloudbus.cloudsim.ex.mapreduce.policy.Backtracking.BacktrackingSorts;
import org.cloudbus.cloudsim.ex.mapreduce.policy.Policy.CloudDeploymentModel;

public class BacktrackingMultithreadedCostMinimization extends Policy {

    private static List<Task> rTasks = new ArrayList<Task>();
    private Request request;
    private static Random rand = new Random();

    public Boolean runAlgorithm(Cloud cloud, Request request) {
	this.request = request;
	CloudDeploymentModel cloudDeploymentModel = request.getCloudDeploymentModel();

	// Fill nVMs
	int numTasks = request.job.mapTasks.size() + request.job.reduceTasks.size();
	List<VmInstance> nVMs = Policy.getAllVmInstances(cloud, request, cloudDeploymentModel, numTasks);
	if (nVMs.size() == 0)
	    return false;

	// Fill rTasks
	for (MapTask mapTask : request.job.mapTasks)
	    rTasks.add(mapTask);
	for (ReduceTask reduceTask : request.job.reduceTasks)
	    rTasks.add(reduceTask);

	/**
	 * Run the cost tree
	 */
	// Selected SchedulingPlan from backtracking
	BackTrackingCostTree backTrackingCostTree = new BackTrackingCostTree(nVMs);
	Thread backTrackingCostTreeThread = new Thread(backTrackingCostTree);
	backTrackingCostTreeThread.start();

	/**
	 * Run the performance tree
	 */
	// Selected SchedulingPlan from backtracking
	BackTrackingPerfTree backTrackingPerfTree = new BackTrackingPerfTree(nVMs);
	Thread backTrackingPerfTreeThread = new Thread(backTrackingPerfTree);
	backTrackingPerfTreeThread.start();

	/**
	 * Wait for any of the two trees to finish
	 */
	Map<Integer, Integer> selectedSchedulingPlan;
	while (true)
	{
	    try {
		Thread.currentThread().sleep(2000);
		if (!backTrackingCostTreeThread.isAlive())
		{
		    backTrackingPerfTreeThread.interrupt();
		    selectedSchedulingPlan = backTrackingCostTree.solution;
		    break;
		}
		if (!backTrackingPerfTreeThread.isAlive())
		{
		    backTrackingCostTreeThread.interrupt();
		    selectedSchedulingPlan = backTrackingPerfTree.solution;
		    break;
		}
	    } catch (InterruptedException e) {
		e.printStackTrace();
	    }
	}
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

    public class BackTrackingCostTree implements Runnable {
	Map<Integer, Integer> solution = null;
	private List<VmInstance> nVMs = new ArrayList<VmInstance>();
	private int mostVmValue = -1;

	public BackTrackingCostTree(List<VmInstance> nVMs)
	{
	    this.nVMs = nVMs;
	}

	public void run() {
	    // Sort nVMs by cost per mips
	    Collections.sort(nVMs, new Comparator<VmInstance>() {
		public int compare(VmInstance VmInstance1, VmInstance VmInstance2) {
		    double vmInstance1Cost = VmInstance1.transferringCost + VmInstance1.vmCostPerHour
			    / VmInstance1.getMips();
		    double vmInstance2Cost = VmInstance2.transferringCost + VmInstance2.vmCostPerHour
			    / VmInstance2.getMips();
		    // Log.printLine(VmInstance1.name
		    // +"("+vmInstance1Cost+") vs "+VmInstance2.name+" ("+vmInstance2Cost+")");
		    return Double.compare(vmInstance1Cost, vmInstance2Cost);
		}
	    });

	    solution = getFirstSolutionOfBackTracking(nVMs.size(), rTasks.size());
	    request.setLogMessage("By Cost Tree");
	}

	private Map<Integer, Integer> getFirstSolutionOfBackTracking(int n, int r) {
	    int[] res = new int[] { 1 };
	    boolean done = false;
	    while (!done) {
		// Convert int[] to Integer[]
		Integer[] resObj = new Integer[res.length];
		for (int i = 0; i < res.length; i++) {
		    resObj[i] = res[i];
		}
		Map<Integer, Integer> schedulingPlan = vectorToScheduleingPlan(resObj, nVMs);
		double[] executionTimeAndCost = PredictionEngine.predictExecutionTimeAndCostFromScheduleingPlan(
			schedulingPlan, nVMs, request.job);
		// Log.printLine(Arrays.toString(resObj) + "->" + (r -
		// res.length) + " : "
		// + Arrays.toString(executionTimeAndCost));
		if (executionTimeAndCost[1] - (executionTimeAndCost[1] * 0.05) > request.getBudget())
		{
		    request.setLogMessage("Very low budget!");
		    return null;
		}
		if (executionTimeAndCost[0] <= request.deadline)
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
		    {
			double deadlineViolationPercentage = 1.0 - (request.getDeadline() / executionTimeAndCost[0]);
			done = (res = goBack(res, n, r, deadlineViolationPercentage)) == null ? true : false;
		    }
		}
	    }
	    request.setLogMessage("No Solution!");
	    return null;
	}

	private int[] goBack(int[] num, int n, int r, double deadlineViolationPercentage) {
	    do {
		int[] res;
		if (deadlineViolationPercentage > 0.2)
		{
		    int mostVmDuplicates = 0;
		    int mostVmLastIndex = -1;
		    for (int i = 0; i < num.length; i++)
		    {
			int vmDuplicate = 0;
			int vmLastIndex = -1;
			for (int j = 0; j < num.length; j++)
			    if (num[i] == num[j])
			    {
				vmDuplicate++;
				vmLastIndex = j;
			    }
			if (vmDuplicate > (mostVmDuplicates*1.2) && vmLastIndex != -1)
			{
			    mostVmDuplicates = vmDuplicate;
			    mostVmLastIndex = vmLastIndex;
			    mostVmValue = num[i];
			}
		    }
		    if (mostVmLastIndex == -1)
			res = new int[num.length];
		    else
			res = new int[mostVmLastIndex];
		}
		else
		    res = new int[num.length - 1];
		if (res.length == 0)
		    return null;
		for (int i = 0; i < res.length; i++)
		    res[i] = num[i];
		res[res.length - 1]++;
		num = res;
	    } while (num[num.length - 1] > n);
	    return num;

	}

	private int[] goDeeper(int[] num, int n) {
	    int[] res = new int[num.length + 1];
	    for (int i = 0; i < res.length - 1; i++) {
		res[i] = num[i];
	    }
	    if (mostVmValue == -1)
		res[res.length - 1] = 1;
	    else
	    {
		int value = 1;
		while (value <= n)
		{
		    if (value != mostVmValue)
		    {
			res[res.length - 1] = value;
			break;
		    }
		    else
			value++;
		}
		if (value > n)
		    res[res.length - 1] = 1;
	    }
	    return res;
	}

    }

    public class BackTrackingPerfTree implements Runnable {
	Map<Integer, Integer> solution = null;
	private List<VmInstance> nVMs = new ArrayList<VmInstance>();
	private int mostVmValue = -1;

	public BackTrackingPerfTree(List<VmInstance> nVMs)
	{
	    this.nVMs = nVMs;
	}

	public void run() {
	    // Sort nVMs by mips (performance)
	    Collections.sort(nVMs, new Comparator<VmInstance>() {
		public int compare(VmInstance VmInstance1, VmInstance VmInstance2) {
		    return Double.compare(VmInstance2.getMips(), VmInstance1.getMips());
		}
	    });

	    solution = getFirstSolutionOfBackTracking(nVMs.size(), rTasks.size());
	    request.setLogMessage("By Performance Tree");
	}

	private Map<Integer, Integer> getFirstSolutionOfBackTracking(int n, int r) {
	    int[] res = new int[] { 1 };
	    boolean done = false;
	    while (!done) {
		// Convert int[] to Integer[]
		Integer[] resObj = new Integer[res.length];
		for (int i = 0; i < res.length; i++) {
		    resObj[i] = res[i];
		}
		Map<Integer, Integer> schedulingPlan = vectorToScheduleingPlan(resObj, nVMs);
		double[] executionTimeAndCost = PredictionEngine.predictExecutionTimeAndCostFromScheduleingPlan(
			schedulingPlan, nVMs, request.job);
		//Log.printLine(Arrays.toString(resObj) + "->" + (r - res.length) + " : "
		//	+ Arrays.toString(executionTimeAndCost));
		if (res[0] > 1)
		{
		    request.setLogMessage("Very short deadline!");
		    return null;
		}
		if (executionTimeAndCost[1] <= request.getBudget() && executionTimeAndCost[0] <= request.getDeadline())
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
		    {
			double deadlineViolationPercentage = 1.0 - (request.getDeadline() / (executionTimeAndCost[0]));
			done = (res = goBack(res, n, r, deadlineViolationPercentage)) == null ? true : false;
		    }
		}
	    }
	    request.setLogMessage("No Solution!");
	    return null;
	}

	private int[] goBack(int[] num, int n, int r, double deadlineViolationPercentage) {
	    do {
		int[] res;
		if (deadlineViolationPercentage > 0.05)
		{
		    int mostVmDuplicates = 0;
		    int mostVmLastIndex = -1;
		    for (int i = 0; i < num.length; i++)
		    {
			int vmDuplicate = 0;
			int vmLastIndex = -1;
			for (int j = 0; j < num.length; j++)
			    if (num[i] == num[j])
			    {
				vmDuplicate++;
				vmLastIndex = j;
			    }
			if (vmDuplicate > (mostVmDuplicates*1.2) && vmLastIndex != -1)
			{
			    mostVmDuplicates = vmDuplicate;
			    mostVmLastIndex = vmLastIndex;
			    mostVmValue = num[i];
			}
		    }
		    if (mostVmLastIndex == -1)
			res = new int[num.length];
		    else
			res = new int[mostVmLastIndex];
		}
		else
		    res = new int[num.length - 1];
		if (res.length == 0)
		    return null;
		for (int i = 0; i < res.length; i++)
		    res[i] = num[i];
		res[res.length - 1]++;
		num = res;
	    } while (num[num.length - 1] > n);
	    return num;

	}

	private int[] goDeeper(int[] num, int n) {
	    int[] res = new int[num.length + 1];
	    for (int i = 0; i < res.length - 1; i++) {
		res[i] = num[i];
	    }
	    if (mostVmValue == -1)
		res[res.length - 1] = 1;
	    else
	    {
		int value = 1;
		while (value <= n)
		{
		    if (value != mostVmValue)
		    {
			res[res.length - 1] = value;
			break;
		    }
		    else
			value++;
		}
		if (value > n)
		    res[res.length - 1] = 1;
	    }
	    return res;
	}

    }

    // ///// Helper functions

    private static Map<Integer, Integer> vectorToScheduleingPlan(Integer[] res, List<VmInstance> nVMs)
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
