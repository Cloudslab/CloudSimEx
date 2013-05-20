package org.cloudbus.cloudsim.ex.mapreduce.policy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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
import org.cloudbus.cloudsim.ex.util.CustomLog;

public class MostDuplicatedBacktracking extends Policy {
    
    public enum BacktrackingSorts {
	Cost, Performance;
    }

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
	long costTreeRunningTime = System.currentTimeMillis();
	BackTrackingTree backTrackingCostTree = new BackTrackingTree(nVMs, rTasks, BacktrackingSorts.Cost,0.025,250000);
	Thread backTrackingCostTreeThread = new Thread(backTrackingCostTree);
	backTrackingCostTreeThread.start();

	/**
	 * Run the performance tree
	 */
	// Selected SchedulingPlan from backtracking
	boolean checkPerfTree = false;
	BackTrackingTree backTrackingPerfTree = new BackTrackingTree(nVMs, rTasks, BacktrackingSorts.Performance,0.0025,250000);
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
		if (!checkPerfTree)
		{
		    long currentRunningTime = System.currentTimeMillis();
		    if (currentRunningTime-costTreeRunningTime >= 20000)
			if(backTrackingCostTree.solution != null)
			{
			    selectedSchedulingPlan = backTrackingCostTree.solution;
			    request.setLogMessage("CostTree forced to accept a solution");
			    break;
			}
			else
			    checkPerfTree = true;
		}
		if (checkPerfTree)
		{
		    if (backTrackingCostTree.solution != null)
		    {
			selectedSchedulingPlan = backTrackingCostTree.solution;
			request.setLogMessage("CostTree forced to accept a solution");
			break;
		    }
		    else if (!backTrackingPerfTreeThread.isAlive())
		    {
			selectedSchedulingPlan = backTrackingPerfTree.solution;
			break;
		    }
		}
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
    public class BackTrackingTree implements Runnable {
	private PredictionEngine predictionEngine;
	private BackTrackingAlgorithm backTrackingAlgorithm;
	private List<VmInstance> nVMs;
	private List<Task> rTasks;
	private BacktrackingSorts sort;
	private double deadlineViolationPercentageLimit;
	private Map<Integer, Integer> solution;
	private double solutionCost = Double.MAX_VALUE;
	private Integer[] solutionVector;
	private int logginCounter;
	private int loggingFrequent;
	
	public BackTrackingTree(List<VmInstance> nVMs, List<Task> rTasks, BacktrackingSorts sort, double deadlineViolationPercentageLimit, int loggingFrequent)
	{
	    predictionEngine = new PredictionEngine();
	    backTrackingAlgorithm = new BackTrackingAlgorithm();
	    this.nVMs = new ArrayList<VmInstance>(nVMs);
	    this.rTasks = rTasks;
	    this.sort = sort;
	    this.deadlineViolationPercentageLimit = deadlineViolationPercentageLimit;
	    logginCounter = loggingFrequent;
	    this.loggingFrequent = loggingFrequent;
	}

	public Map<Integer, Integer> getSolution() {
	    return solution;
	}

	public void setSolution(Map<Integer, Integer> solution) {
	    this.solution = solution;
	}

	public void run() {
	    if(sort == BacktrackingSorts.Cost)
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
	    if(sort == BacktrackingSorts.Performance)
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

	    if(sort == BacktrackingSorts.Cost)
		Log.print("Cost Tree Progress [");
	    solution = getFirstSolutionOfBackTracking(nVMs.size(), rTasks.size());
	    if(sort == BacktrackingSorts.Cost)
		Log.printLine("]");
	    request.setLogMessage("By "+sort+" Tree");
	}

	private Map<Integer, Integer> getFirstSolutionOfBackTracking(int n, int r) {
	    int subN = 1;
	    while (subN <= n)
	    {
		if(sort == BacktrackingSorts.Cost)
		    Log.print("-");
		//If cost: the violation will be the budget,
		//but if perf: the violation will be deadline.
		boolean isQoSViolationInLeaf = true;
		boolean isLeafReached = false;

		Integer[] res = new Integer[] { 1 };
		boolean done;
		do {
		    done = false;
		    // Get the execution time and cost of current node (res)
		    Map<Integer, Integer> schedulingPlan = predictionEngine.vectorToScheduleingPlan(res, nVMs,
			    rTasks);
		    double[] executionTimeAndCost = predictionEngine.predictExecutionTimeAndCostFromScheduleingPlan(
			    schedulingPlan, nVMs, request.job);
		    // Logging
		    //if (logginCounter >= loggingFrequent)
		    if(sort == BacktrackingSorts.Cost)
		    {
			CustomLog.printLine("Cost n=" + subN + " :" + Arrays.toString(res) + "->"
				+ (r - res.length) + " : "
				+ Arrays.toString(executionTimeAndCost));
			logginCounter = 0;
		    }
		    // Record that there is a QoS violation on a leaf, so we
		    // terminate the tree and return "very low budget!" or "very short deadline!"
		    if(!isLeafReached && res.length == r)
			isLeafReached = true;
		    if (isQoSViolationInLeaf && res.length == r)
		    {
			if(sort == BacktrackingSorts.Cost && executionTimeAndCost[1] <= request.getBudget())
			    isQoSViolationInLeaf = false;
			if(sort == BacktrackingSorts.Performance && executionTimeAndCost[0] <= request.getDeadline())
			    isQoSViolationInLeaf = false;
		    }
		    
		    //If this is a new major branch, and we have a solution from the previous major branch, just take it.
		    if(solution != null && Arrays.equals(res, new Integer[]{ 1 }))
		    {
			CustomLog.printLine(sort+" n=" + subN + " :"
				    + Arrays.toString(solutionVector) + "->"
				    + (r - solutionVector.length) + " : "
				    + Arrays.toString(executionTimeAndCost)
				    + " is the selected solution");
			return solution;
		    }
		    
		    //Save the solution if we are in the leaf and it does not violate the deadline and budget, and the found one is better than the previous one (if any)
		    if (executionTimeAndCost[1] <= request.getBudget()
			    && executionTimeAndCost[0] <= request.getDeadline()
			    && res.length == r
			    && (solution == null || executionTimeAndCost[1] < solutionCost))
		    {
			CustomLog.printLine(sort + " n=" + subN + " :"
				+ Arrays.toString(res) + "->"
				+ (r - res.length) + " : " + Arrays.toString(executionTimeAndCost)
				+ " is a solution");
			solution = schedulingPlan;
			solutionCost = executionTimeAndCost[1];
			solutionVector = res;
			if(sort == BacktrackingSorts.Cost)
				Log.print("C");
			if(sort == BacktrackingSorts.Performance)
				Log.print("P");
		    }
		    
		    // If the budget and deadline are not violated, and we are not in a leaf -> go deep
		    if (executionTimeAndCost[1] <= request.getBudget()
			    && executionTimeAndCost[0] <= request.getDeadline()
			    && res.length != r)
			res = backTrackingAlgorithm.goDeeper(res, subN);
		    // Come here if the node does violate the budget and/or the
		    // deadline, or we are in the leaf
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
			} while (!done && !Arrays.asList(res).contains(subN) && res.length == r);
		    }
		    logginCounter++;
		} while (!done);
		if (isLeafReached && isQoSViolationInLeaf)
		{
		    if(sort == BacktrackingSorts.Cost)
			request.setLogMessage("Very low budget!");
		    if(sort == BacktrackingSorts.Performance)
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
	//private Map<Integer, Integer> perfTreeSolution = null;
	//private double perfTreeSolutionCost;

	public BackTrackingCostTree(List<VmInstance> nVMs, List<Task> rTasks)
	{
	    this.nVMs = nVMs;
	    this.rTasks = rTasks;
	}

	//public synchronized void setPerfTreeSolution(Map<Integer, Integer> perfTreeSolution) {
	//    this.perfTreeSolution = perfTreeSolution;
	//}

	//public synchronized void setPerfTreeSolution(double perfTreeSolutionCost) {
	//    this.perfTreeSolutionCost = perfTreeSolutionCost;
	//}

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
	    //Map<Integer, Integer> costTreeBestSolutionSoFar = null;
	    //double costTreeBestCostSoFar = -1.0;

	    // The first 1 is for the number of VMs, the rest is for each task
	    // in which VM
	    int subN = 1;
	    while (subN <= n)
	    {
		boolean isBudgetViolatedInLeaf = true;
		boolean isLeafReached = false;
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
		    //if (subN > 1 && res.length == r
			//    && (costTreeBestSolutionSoFar == null || executionTimeAndCost[1] < costTreeBestCostSoFar))
		    //{
			//costTreeBestCostSoFar = executionTimeAndCost[1];
			//costTreeBestSolutionSoFar = schedulingPlan;
		   // }
		    //if (perfTreeSolution != null && costTreeBestSolutionSoFar != null
			//    && perfTreeSolutionCost <= costTreeBestCostSoFar + (costTreeBestCostSoFar * 0.05))
		    //{
			//CustomLog.printLine("Cost n=" + subN + " :"
			//	    + Arrays.toString(backTrackingAlgorithm.getSubRes(res)) + "->"
			//	    + (r - res.length) + " : " + Arrays.toString(executionTimeAndCost)
			//	    + " is the aceepted solution from the PerfTree");
			//request.setLogMessage("Accepted Perf Tree Solution!");
			//return perfTreeSolution;
		    //}
		    // Record that there is a budget violation on a leaf, so we
		    // terminate the tree and return "very low budget!"
		    if(!isLeafReached && res.length == r)
			isLeafReached = true;
		    if (isBudgetViolatedInLeaf && res.length == r && executionTimeAndCost[1] <= request.getBudget())
			isBudgetViolatedInLeaf = false;

		    // If the budget and deadline are not violated
		    if (executionTimeAndCost[0] <= request.getDeadline()
			    && executionTimeAndCost[1] <= request.getBudget())
		    {
			if (res.length == r)
			{
			    CustomLog.printLine("Cost n=" + subN + " :"
				    + Arrays.toString(res) + "->"
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
			} while (!done && !Arrays.asList(res).contains(subN) && res.length == r);
		    }
		    logCounter++;
		} while (!done);
		if (isLeafReached && isBudgetViolatedInLeaf)
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
	//double perfTreeSolutionCost;
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
		boolean isDeadlineViolatedInLeaf = true;
		boolean isLeafReached = false;
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
		    //if (solution != null
			//    && executionTimeAndCost[0] - (executionTimeAndCost[0] * 0.05) > request.getDeadline())
			//return schedulingPlan;
		    // Record that there is a deadline violation on a leaf, so we
		    // terminate the tree and return "very short deadline!!"
		    if(!isLeafReached && res.length == r)
			isLeafReached = true;
		    if (isDeadlineViolatedInLeaf && res.length == r && executionTimeAndCost[0] <= request.getDeadline())
			isDeadlineViolatedInLeaf = false;

		    /*
		    if (res.length != r && executionTimeAndCost[0] <= request.getDeadline())
			res = backTrackingAlgorithm.goDeeper(res, subN);
		    else
		    {
			//if (solution == null || executionTimeAndCost[1] < perfTreeSolutionCost)
			if (executionTimeAndCost[0] <= request.getDeadline()
				    && executionTimeAndCost[1] <= request.getBudget())
			{
			    return schedulingPlan;
			    //solution = schedulingPlan;
			    //perfTreeSolutionCost = executionTimeAndCost[1];
			    //CustomLog.printLine("Perf n=" + subN + " :" + Arrays.toString(res) + " : "
				//    + Arrays.toString(executionTimeAndCost) + " is a soulation");

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
			} while (!done && !Arrays.asList(res).contains(subN) && res.length == r);
			
		    }*/
		    
		 // If the budget and deadline are not violated
		    if (executionTimeAndCost[0] <= request.getDeadline()
			    && executionTimeAndCost[1] <= request.getBudget())
		    {
			if (res.length == r)
			{
			    CustomLog.printLine("Perf n=" + subN + " :"
				    + Arrays.toString(res) + "->"
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
			} while (!done && !Arrays.asList(res).contains(subN) && res.length == r);
		    }
		    
		    logCounter++;
		} while (!done);
		if (isLeafReached && isDeadlineViolatedInLeaf)
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
