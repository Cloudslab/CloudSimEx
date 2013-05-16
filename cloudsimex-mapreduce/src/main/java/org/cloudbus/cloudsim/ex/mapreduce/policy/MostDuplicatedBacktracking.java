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
	Map<Integer, Integer> selectedSchedulingPlan = null;

	try {
	    while (true)
	    {
		if (!backTrackingCostTreeThread.isAlive())
		{
		    selectedSchedulingPlan = backTrackingCostTree.solution;
		    break;
		}
		if (!backTrackingPerfTreeThread.isAlive()) {

		    selectedSchedulingPlan = backTrackingPerfTree.solution;
		    break;
		}
		/*
		 * if (backTrackingPerfTree.solution != null) {
		 * backTrackingCostTree
		 * .setPerfTreeSolution(backTrackingPerfTree.solution);
		 * backTrackingCostTree
		 * .setPerfTreeSolution(backTrackingPerfTree.
		 * perfTreeSolutionCost); }
		 */

		Thread.currentThread().sleep(500);
	    }
	} catch (InterruptedException e) {
	    e.printStackTrace();
	} finally
	{
	    backTrackingCostTreeThread.stop();
	    backTrackingPerfTreeThread.stop();
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
	private double deadlineViolationPercentageLimit = 0.025;
	private Map<Integer, Integer> perfTreeSolution = null;
	private double perfTreeSolutionCost;

	public BackTrackingCostTree(List<VmInstance> nVMs, List<Task> rTasks)
	{
	    this.nVMs = nVMs;
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
	    // here will be the cheapest solution after the 1st major branch to
	    // compare it with the solution from PerfTree
	    Map<Integer, Integer> costTreeBestSolutionSoFar = null;
	    double costTreeBestCostSoFar = Double.MAX_VALUE;

	    // The first 1 is for the number of VMs, the rest is for each task
	    // in which VM
	    int subN = 1;
	    while (subN <= n)
	    {
		boolean isBudgetViolatedInLeaf = false;
		int logCounter = 250000;
		int logEvery = 250000;

		Integer[] res = new Integer[] { 1 };
		boolean done;
		do {
		    done = false;
		    // Get the execution time and cost of current node (res)
		    // without the first element, because the first element is
		    // for determine the number of VMs.
		    Map<Integer, Integer> schedulingPlan = predictionEngine.vectorToScheduleingPlan(res, nVMs,
			    rTasks);
		    double[] executionTimeAndCost = predictionEngine.predictExecutionTimeAndCostFromScheduleingPlan(
			    schedulingPlan, nVMs, request.job);
		    // Logging
		    if (logCounter >= logEvery)
		    {
			CustomLog.printLine("Cost n=" + subN + " :" + Arrays.toString(res) + "->"
				+ (r - res.length) + " : "
				+ Arrays.toString(executionTimeAndCost));
			logCounter = 0;
		    }
		    // If what PerfTree found is very close (5% margin) to the
		    // cheapest complete solution (leaf) after finishing the
		    // first major branch, then accept it.
		    if (subN > 1 && res.length == r
			    && (costTreeBestSolutionSoFar == null || executionTimeAndCost[1] < costTreeBestCostSoFar))
		    {
			costTreeBestCostSoFar = executionTimeAndCost[1];
			costTreeBestSolutionSoFar = schedulingPlan;
		    }
		    if (perfTreeSolution != null
			    && perfTreeSolutionCost <= costTreeBestCostSoFar + (costTreeBestCostSoFar * 0.05))
		    {
			request.setLogMessage("Accepted Perf Tree Solution!");
			return perfTreeSolution;
		    }
		    // Record that there is a budget violation on a leaf, so we
		    // don't terminate the tree and return "very low budget!"
		    if (!isBudgetViolatedInLeaf && res.length == r && executionTimeAndCost[1] > request.getBudget())
			isBudgetViolatedInLeaf = true;

		    // If the budget and deadline are not violated
		    if (executionTimeAndCost[0] <= request.getDeadline()
			    && executionTimeAndCost[1] <= request.getBudget())
		    {
			if (res.length == r)
			{
			    CustomLog.printLine("Cost n=" + subN + " :"
				    + Arrays.toString(backTrackingAlgorithm.getSubRes(res)) + "->"
				    + (r - res.length) + " : " + Arrays.toString(executionTimeAndCost)
				    + " is the selected solution");
			    return schedulingPlan;
			}
			else
			    res = backTrackingAlgorithm.goDeeper(res, subN);
		    }
		    // Come here if the node does violate the budget and/or the
		    // deadline
		    else
		    {
			do {
			    if (res[res.length - 1] < subN)
				res[res.length - 1]++;
			    else
			    {
				double deadlineViolationPercentage = 1.0 - (request.getDeadline() / executionTimeAndCost[0]);
				if (deadlineViolationPercentage > deadlineViolationPercentageLimit)
				    backTrackingAlgorithm.doChangMostVmValue = true;
				done = (res = backTrackingAlgorithm.goBack(res, subN, r)) == null ? true : false;
			    }
			    // if the new subRes has been scanned by previous
			    // major branch; just skip it, and go next.
			} while (!done && Arrays.asList(res).contains(subN));
		    }
		    logCounter++;
		} while (!done);
		if (isBudgetViolatedInLeaf)
		{
		    request.setLogMessage("Very low budget!");
		    return null;
		}
		// Increase the number of VMs to look into
		subN++;
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
	private double deadlineViolationPercentageLimit = 0.0025;

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
	    // The first 1 is for the number of VMs, the rest is for each task
	    // in which VM
	    int subN = 1;
	    while (subN <= n)
	    {
		boolean isDeadlineViolatedInLeaf = false;
		int logCounter = 250000;
		int logEvery = 250000;

		Integer[] res = new Integer[] { 1 };
		boolean done;
		do {
		    done = false;
		    // Get the execution time and cost of current node (res)
		    // without the first element, because the first element is
		    // for determine the number of VMs.
		    Map<Integer, Integer> schedulingPlan = predictionEngine.vectorToScheduleingPlan(res, nVMs,
			    rTasks);
		    double[] executionTimeAndCost = predictionEngine.predictExecutionTimeAndCostFromScheduleingPlan(
			    schedulingPlan, nVMs, request.job);
		    // Logging
		    if (logCounter >= logEvery)
		    {
			CustomLog.printLine("Perf n=" + subN + " :" + Arrays.toString(res) + "->"
				+ (r - res.length) + " : "
				+ Arrays.toString(executionTimeAndCost));
			logCounter = 0;
		    }
		    // If there is a solution, and the current execution time
		    // violate the deadline (5% margin) then just take that
		    // solution and send it to the CostTree to compare it with
		    // what they have so far
		    if (solution != null
			    && executionTimeAndCost[0] - (executionTimeAndCost[0] * 0.05) > request.getDeadline())
			return schedulingPlan;
		    // Record that there a deadline violation on one of the
		    // leafs, so we don't terminate the tree and return
		    // "very short deadline!"
		    if (res.length == r && executionTimeAndCost[0] > request.getDeadline())
			isDeadlineViolatedInLeaf = true;

		    if (res.length != r)
			res = backTrackingAlgorithm.goDeeper(res, subN);
		    else
		    {
			if (solution == null || executionTimeAndCost[1] < perfTreeSolutionCost)
			{
			    solution = schedulingPlan;
			    perfTreeSolutionCost = executionTimeAndCost[1];
			    CustomLog.printLine("Perf n=" + subN + " :" + Arrays.toString(res) + " : "
				    + Arrays.toString(executionTimeAndCost) + " is a soulation");

			}
			do {
			    if (res[res.length - 1] < subN)
				res[res.length - 1]++;
			    else
			    {
				double deadlineViolationPercentage = 1.0 - (request.getDeadline() / executionTimeAndCost[0]);
				if (deadlineViolationPercentage > deadlineViolationPercentageLimit)
				    backTrackingAlgorithm.doChangMostVmValue = true;
				done = (res = backTrackingAlgorithm.goBack(res, subN, r)) == null ? true : false;
			    }
			    // if the new subRes has been scanned by previous
			    // major branch; just skip it, and go next.
			} while (!done && Arrays.asList(res).contains(subN));
		    }
		    logCounter++;
		} while (!done);
		if (isDeadlineViolatedInLeaf)
		{
		    request.setLogMessage("Very short deadline!");
		    return null;
		}
		// Increase the number of VMs to look into
		subN++;
	    }
	    request.setLogMessage("No Solution!");
	    return null;
	}

    }

    public class BackTrackingAlgorithm {
	private boolean doChangMostVmValue = false;

	private Integer[] goBack(Integer[] num, int n, int r) {
	    do {
		Integer[] res;
		if (isMostDuplicatedEnabled && doChangMostVmValue)
		{
		    doChangMostVmValue = false;
		    int mostVmDuplicates = 1;
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
			res = new Integer[num.length - 1];
		    else
			res = new Integer[mostVmLastIndex + 1];
		}
		else
		    res = new Integer[num.length - 1];
		if (res.length == 0)
		    return null;
		for (int i = 0; i < res.length; i++)
		    res[i] = num[i];
		res[res.length - 1]++;
		num = res;
	    } while (num[num.length - 1] > n);
	    return num;

	}

	public Integer[] getRes(Integer indexZeroValue, Integer[] subRes) {
	    Integer[] res = new Integer[subRes.length + 1];
	    res[0] = indexZeroValue;
	    for (int i = 1; i < res.length; i++)
	    {
		res[i] = subRes[i - 1];
	    }
	    return res;
	}

	public Integer[] getSubRes(Integer[] res) {
	    Integer[] resObj = new Integer[res.length - 1];
	    for (int i = 0; i < resObj.length; i++) {
		resObj[i] = res[i + 1];
	    }
	    return resObj;
	}

	private Integer[] goDeeper(Integer[] num, int n) {
	    Integer[] res = new Integer[num.length + 1];
	    for (int i = 0; i < res.length - 1; i++) {
		res[i] = num[i];
	    }
	    res[res.length - 1] = 1;
	    return res;
	}

    }
}
