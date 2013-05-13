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
import org.cloudbus.cloudsim.ex.util.CustomLog;

public class MostDuplicatedBacktracking extends Policy {

    private Request request;
    public boolean isMostDuplicatedEnabled = true;

    public Boolean runAlgorithm(Cloud cloud, Request request) {
	this.request = request;
	CloudDeploymentModel cloudDeploymentModel = request.getCloudDeploymentModel();

	// Fill nVMs
	int numTasks = request.job.mapTasks.size() + request.job.reduceTasks.size();
	List<VmInstance> nVMs = Policy.getAllVmInstances(cloud, request, cloudDeploymentModel, numTasks);
	if (nVMs.size() == 0)
	    return false;

	// Fill rTasks
	List<Task> rTasks = new ArrayList<Task>();
	for (MapTask mapTask : request.job.mapTasks)
	    rTasks.add(mapTask);
	for (ReduceTask reduceTask : request.job.reduceTasks)
	    rTasks.add(reduceTask);

	/**
	 * Run the cost tree
	 */
	// Selected SchedulingPlan from backtracking
	BackTrackingCostTree backTrackingCostTree = new BackTrackingCostTree(nVMs, rTasks);
	Thread backTrackingCostTreeThread = new Thread(backTrackingCostTree);
	backTrackingCostTreeThread.start();

	/**
	 * Run the performance tree
	 */
	// Selected SchedulingPlan from backtracking
	BackTrackingPerfTree backTrackingPerfTree = new BackTrackingPerfTree(nVMs, rTasks);
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
		if (backTrackingPerfTree.solution != null)
		{
		    backTrackingCostTree.setPerfTreeSolution(backTrackingPerfTree.solution);
		    backTrackingCostTree.setPerfTreeSolution(backTrackingPerfTree.perfTreeSolutionCost);
		}
	    } catch (InterruptedException e) {
		e.printStackTrace();
	    }
	}
	if (selectedSchedulingPlan == null)
	    return false;

	// 1- Provisioning
	ArrayList<ArrayList<VmInstance>> provisioningPlans = new PredictionEngine().getProvisioningPlan(
		selectedSchedulingPlan, nVMs,
		request.job);
	request.mapAndReduceVmProvisionList = provisioningPlans.get(0);
	request.reduceOnlyVmProvisionList = provisioningPlans.get(1);

	// 2- Scheduling
	request.schedulingPlan = selectedSchedulingPlan;

	return true;
    }

    /**
     * 
     * @author Mohammed Alrokayan
     * 
     */
    public class BackTrackingCostTree implements Runnable {
	PredictionEngine predictionEngine = new PredictionEngine();
	Map<Integer, Integer> solution = null;
	private List<VmInstance> nVMs;
	private List<Task> rTasks;
	private BackTrackingAlgorithm backTrackingAlgorithm = new BackTrackingAlgorithm();
	private double DeadlineViolationPercentge = 0.2;
	private Map<Integer, Integer> perfTreeSolution = null;
	private double perfTreeSolutionCost;
	private int logCounter = 0;
	private int logEvery = 100000;

	public BackTrackingCostTree(List<VmInstance> nVMs, List<Task> rTasks)
	{
	    this.nVMs = new ArrayList<VmInstance>(nVMs);
	    this.rTasks = rTasks;
	}

	public synchronized void setPerfTreeSolution(Map<Integer, Integer> perfTreeSolution) {
	    this.perfTreeSolution = perfTreeSolution;
	}

	public synchronized void setPerfTreeSolution(double perfTreeSolutionCost) {
	    this.perfTreeSolutionCost = perfTreeSolutionCost;
	}

	public void run() {
	    // Sort nVMs by cost per mips
	    Collections.sort(nVMs, new Comparator<VmInstance>() {
		public int compare(VmInstance VmInstance1, VmInstance VmInstance2) {
		    double vmInstance1Cost = VmInstance1.transferringCost + VmInstance1.vmCostPerHour
			    / VmInstance1.getMips();
		    double vmInstance2Cost = VmInstance2.transferringCost + VmInstance2.vmCostPerHour
			    / VmInstance2.getMips();
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
		Map<Integer, Integer> schedulingPlan = predictionEngine.vectorToScheduleingPlan(resObj, nVMs, rTasks);
		double[] executionTimeAndCost = predictionEngine.predictExecutionTimeAndCostFromScheduleingPlan(
			schedulingPlan, nVMs, request.job);
		if (logCounter >= logEvery)
		//if (true)
		{
		    CustomLog.printLine("Cost " + Arrays.toString(resObj) + "->"
			    + (r - res.length) + " : " + Arrays.toString(executionTimeAndCost));
		    logCounter = 0;
		}
		if (perfTreeSolution != null
			&& perfTreeSolutionCost <= executionTimeAndCost[1] + (executionTimeAndCost[1] * 0.05))
		{
		    request.setLogMessage("Accepted Perf Tree Solution!");
		    return perfTreeSolution;
		}
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
			res = backTrackingAlgorithm.goDeeper(res, n);
		}
		else
		{
		    if (res.length == r && res[res.length - 1] < n)
			res[res.length - 1]++;
		    else
		    {
			double deadlineViolationPercentage = 1.0 - (request.getDeadline() / executionTimeAndCost[0]);
			if (deadlineViolationPercentage > DeadlineViolationPercentge)
			    backTrackingAlgorithm.doChangMostVmValue = true;
			done = (res = backTrackingAlgorithm.goBack(res, n, r)) == null ? true : false;
		    }
		}
		logCounter++;
	    }
	    request.setLogMessage("No Solution!");
	    return null;
	}

    }

    /**
     * 
     * @author Mohammed Alrokayan
     * 
     */
    public class BackTrackingPerfTree implements Runnable {
	PredictionEngine predictionEngine = new PredictionEngine();
	Map<Integer, Integer> solution = null;
	private List<VmInstance> nVMs;
	private List<Task> rTasks = new ArrayList<Task>();
	private BackTrackingAlgorithm backTrackingAlgorithm = new BackTrackingAlgorithm();
	double perfTreeSolutionCost;
	private int logCounter = 0;
	private int logEvery = 250000;
	private double DeadlineViolationPercentge = 0.0025;

	public BackTrackingPerfTree(List<VmInstance> nVMs, List<Task> rTasks)
	{
	    this.rTasks = rTasks;
	    this.nVMs = new ArrayList<VmInstance>(nVMs);
	}

	public void run() {
	    // Sort nVMs by mips (performance)
	    Collections.sort(nVMs, new Comparator<VmInstance>() {
		public int compare(VmInstance VmInstance1, VmInstance VmInstance2) {
		    // TODO Add data trasfere time from data source + out from
		    // VM
		    MapTask anyMapTask = request.job.mapTasks.get(0);
		    double vmInstance1Perf = VmInstance1.getMips() + VmInstance1.bootTime
			    + anyMapTask.dataTransferTimeFromTheDataSource(VmInstance1);
		    double vmInstance2Perf = VmInstance2.getMips() + VmInstance2.bootTime
			    + anyMapTask.dataTransferTimeFromTheDataSource(VmInstance2);
		    ;
		    return Double.compare(vmInstance2Perf, vmInstance1Perf);

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
		Map<Integer, Integer> schedulingPlan = predictionEngine.vectorToScheduleingPlan(resObj, nVMs, rTasks);
		double[] executionTimeAndCost = predictionEngine.predictExecutionTimeAndCostFromScheduleingPlan(
			schedulingPlan, nVMs, request.job);
		if (logCounter >= logEvery)
		//if (true)
		{
		    CustomLog.printLine("Perf " + Arrays.toString(resObj) + "->" + (r - res.length) + " : "
			    + Arrays.toString(executionTimeAndCost));
		    logCounter = 0;
		}
		if (res[0] > 1)
		{
		    request.setLogMessage("Very short deadline!");
		    return null;
		}
		if (solution != null
			&& executionTimeAndCost[0] - (executionTimeAndCost[0] * 0.05) > request.getDeadline())
		{
		    return schedulingPlan;
		}
		boolean budgetViolationOnLeaf = executionTimeAndCost[1] > request.getBudget() && res.length == r;
		if (executionTimeAndCost[0] <= request.getDeadline() && !budgetViolationOnLeaf)
		{
		    if (res.length == r)
		    {
			solution = schedulingPlan;
			perfTreeSolutionCost = executionTimeAndCost[1];
		    }
		    else
			res = backTrackingAlgorithm.goDeeper(res, n);
		}
		else
		{
		    if (res.length == r && res[res.length - 1] < n)
			res[res.length - 1]++;
		    else
		    {
			double deadlineViolationPercentage = 1.0 - (request.getDeadline() / (executionTimeAndCost[0]));
			if (deadlineViolationPercentage > DeadlineViolationPercentge)
			    backTrackingAlgorithm.doChangMostVmValue = true;
			done = (res = backTrackingAlgorithm.goBack(res, n, r)) == null ? true : false;
		    }
		}
		logCounter++;
	    }
	    request.setLogMessage("No Solution!");
	    return null;
	}

    }

    public class BackTrackingAlgorithm {
	private boolean doChangMostVmValue = false;

	private int[] goBack(int[] num, int n, int r) {
	    do {
		int[] res;
		if (isMostDuplicatedEnabled && doChangMostVmValue)
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
			if (vmDuplicate > (mostVmDuplicates * 1.2) && vmLastIndex != -1)
			{
			    mostVmDuplicates = vmDuplicate;
			    mostVmLastIndex = vmLastIndex;
			}
		    }
		    if (mostVmLastIndex == -1)
			res = new int[num.length];
		    else
			res = new int[mostVmLastIndex + 1];
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
	    if (!doChangMostVmValue)
		res[res.length - 1] = 1;
	    else
	    {
		res[res.length - 1]++;
		if (res[res.length - 1] > n)
		    res[res.length - 1] = 1;
	    }
	    return res;
	}

    }
}
