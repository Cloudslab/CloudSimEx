package org.cloudbus.cloudsim.ex.mapreduce.policy;

import java.text.DecimalFormat;
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
import org.cloudbus.cloudsim.ex.util.CustomLog;

public class Backtracking {

    public enum BacktrackingSorts {
	Cost, Performance;
    }

    public enum BacktrackingType {
	Full, Decision;
    }

    private Request request;
    private Cloud cloud;
    private boolean enableProgressBar = true;
    private int loggingFrequent = 250000;
    
    public Boolean runAlgorithm(Cloud cloud, Request request, int numCostTrees, boolean enablePerfTree,
	    long forceToAceeptAnySolutionTimeMillis, long forceToExitTimeMillis, BacktrackingType backtrackingType) {
	this.request = request;
	this.cloud = cloud;
	CloudDeploymentModel cloudDeploymentModel = request.getCloudDeploymentModel();

	// Fill nVMs
	int numTasks = request.job.mapTasks.size() + request.job.reduceTasks.size();
	List<VmInstance> nVMs = Policy.getAllVmInstances(cloud, request, cloudDeploymentModel, numTasks);
	if (nVMs.size() == 0)
	    return false;

	// Sort nVMs by cost per mips - Perf Tree will have copy it and re-sort
	// it
	Collections.sort(nVMs, new Comparator<VmInstance>() {
	    public int compare(VmInstance VmInstance1, VmInstance VmInstance2) {
		double vmInstance1Cost = VmInstance1.transferringCost + VmInstance1.vmCostPerHour
			/ VmInstance1.getMips();
		double vmInstance2Cost = VmInstance2.transferringCost + VmInstance2.vmCostPerHour
			/ VmInstance2.getMips();
		return Double.compare(vmInstance1Cost, vmInstance2Cost);
	    }
	});

	// Fill rTasks
	List<Task> rTasks = new ArrayList<Task>();
	for (MapTask mapTask : request.job.mapTasks)
	    rTasks.add(mapTask);
	for (ReduceTask reduceTask : request.job.reduceTasks)
	    rTasks.add(reduceTask);

	/**
	 * Run the cost trees
	 */
	// Get SchedulingPlan from backtracking
	long costTreeRunningTime = System.currentTimeMillis();
	List<BackTrackingTree> backTrackingCostTrees = new ArrayList<BackTrackingTree>();
	List<Thread> backTrackingCostTreeThreads = new ArrayList<Thread>();
	int numBranchesInEachTree = (int) Math.floor((double) nVMs.size() / numCostTrees);
	int lastBranchUsed = 0;
	for (int i = 1; i <= numCostTrees; i++)
	{
	    BackTrackingTree backTrackingCostTree;
	    if (i == numCostTrees)
		backTrackingCostTree = new BackTrackingTree(nVMs, rTasks, BacktrackingSorts.Cost,
			loggingFrequent, lastBranchUsed + 1, nVMs.size());
	    backTrackingCostTree = new BackTrackingTree(nVMs, rTasks, BacktrackingSorts.Cost,
		    loggingFrequent, lastBranchUsed + 1, lastBranchUsed + numBranchesInEachTree);
	    if (i == 1 && enableProgressBar)
	    {
		Log.print("All " + (numCostTrees + 1) + " Trees Progress [");
	    }
	    backTrackingCostTrees.add(backTrackingCostTree);
	    Thread backTrackingCostTreeThread = new Thread(backTrackingCostTree);
	    backTrackingCostTreeThread.setName("C" + i);
	    backTrackingCostTreeThread.start();
	    backTrackingCostTreeThreads.add(backTrackingCostTreeThread);
	    lastBranchUsed += numBranchesInEachTree;
	}

	/**
	 * Run the performance tree
	 */
	// Get SchedulingPlan from backtracking
	boolean checkPerfTree = false;
	BackTrackingTree backTrackingPerfTree = null;
	Thread backTrackingPerfTreeThread = null;
	if (enablePerfTree)
	{
	    backTrackingPerfTree = new BackTrackingTree(new ArrayList<VmInstance>(nVMs), rTasks,
		    BacktrackingSorts.Performance, loggingFrequent, 1, nVMs.size());
	    backTrackingPerfTreeThread = new Thread(backTrackingPerfTree);
	    backTrackingPerfTreeThread.setName("P");
	    backTrackingPerfTreeThread.start();
	}

	/**
	 * Wait for any of the two trees to finish
	 */
	Integer[] solutionVector = null;
	boolean forceToAceeptAnySolution = false;
	try {
	    while (true)
	    {
		long currentRunningTime = System.currentTimeMillis();
		if (!backTrackingCostTreeThreads.get(0).isAlive())
		{
		    solutionVector = checkCostTrees(backTrackingCostTrees);
		    if (solutionVector != null)
			break;
		    else
			forceToAceeptAnySolution = true;
		}
		if (!checkPerfTree && enablePerfTree)
		{
		    if (forceToAceeptAnySolution
			    || currentRunningTime - costTreeRunningTime >= forceToAceeptAnySolutionTimeMillis)
		    {
			solutionVector = checkCostTrees(backTrackingCostTrees);
			if (solutionVector != null)
			{
			    request.setLogMessage("Forced to accept a solution");
			    break;
			}
			checkPerfTree = true;
		    }
		}
		if (checkPerfTree)
		{
		    solutionVector = checkCostTrees(backTrackingCostTrees);
		    if (solutionVector != null)
		    {
			request.setLogMessage("Forced to accept a solution");
			break;
		    }
		    if (!backTrackingPerfTreeThread.isAlive())
		    {
			solutionVector = backTrackingPerfTree.getSolutionVector();
			request.setLogMessage("Forced to accept a solution");
			break;
		    }
		}

		if (currentRunningTime - costTreeRunningTime >= forceToExitTimeMillis)
		{
		    solutionVector = checkCostTrees(backTrackingCostTrees);
		    if (solutionVector == null && backTrackingPerfTree != null
			    && backTrackingPerfTree.solutionVector != null)
			solutionVector = backTrackingPerfTree.solutionVector;
		    request.setLogMessage("Force To Exit");
		    break;
		}

		Thread.currentThread().sleep(500);
	    }
	} catch (InterruptedException e) {
	    e.printStackTrace();
	} finally
	{
	    for (int i = 0; i < backTrackingCostTreeThreads.size(); i++)
		backTrackingCostTreeThreads.get(i).stop();
	    if (enablePerfTree)
		backTrackingPerfTreeThread.stop();
	    if (enableProgressBar)
		Log.printLine("]");
	}
	if (solutionVector == null)
	{
	    CustomLog.printLine("Solution: Could't find a solution");
	    return false;
	}
	PredictionEngine predictionEngine = new PredictionEngine(request, cloud);
	Map<Integer, Integer> selectedSchedulingPlan = predictionEngine.vectorToScheduleingPlan(solutionVector, nVMs,
		rTasks);
	double[] executionTimeAndCost = predictionEngine.predictExecutionTimeAndCostFromScheduleingPlan(
		selectedSchedulingPlan, nVMs);
	CustomLog.printLine("Solution: " + Arrays.toString(solutionVector) + " : "
		+ Arrays.toString(executionTimeAndCost));

	// 1- Provisioning
	ArrayList<ArrayList<VmInstance>> provisioningPlans = predictionEngine.getProvisioningPlan(selectedSchedulingPlan, nVMs, request.job);
	request.mapAndReduceVmProvisionList = provisioningPlans.get(0);
	request.reduceOnlyVmProvisionList = provisioningPlans.get(1);

	// 2- Scheduling
	request.schedulingPlan = selectedSchedulingPlan;

	return true;
    }

    private Integer[] checkCostTrees(List<BackTrackingTree> backTrackingCostTrees) {
	for (int i = 0; i < backTrackingCostTrees.size(); i++)
	{
	    if (backTrackingCostTrees.get(i).getSolutionVector() != null)
	    {
		double minCost = backTrackingCostTrees.get(i).getSolutionCost();
		Integer[] solutionVector = backTrackingCostTrees.get(i).getSolutionVector();
		for (int j = i + 1; j < backTrackingCostTrees.size(); j++)
		{
		    if (backTrackingCostTrees.get(j).getSolutionCost() < minCost)
		    {
			minCost = backTrackingCostTrees.get(j).getSolutionCost();
			solutionVector = backTrackingCostTrees.get(j).getSolutionVector();
		    }
		}
		return solutionVector;
	    }
	}
	return null;
    }

    public class BackTrackingDecisionTree implements Runnable {
	private PredictionEngine predictionEngine;
	private List<VmInstance> nVMs;
	private List<Task> rTasks;
	// Support only Cost
	// private BacktrackingSorts sort;
	private Map<Integer, Integer> solution = null;
	private double solutionCost = Double.MAX_VALUE;
	private Integer[] solutionVector = null;
	private int logginCounter;
	private int loggingFrequent;

	// Support only one tree
	// private int minN;
	// private int maxN;

	public BackTrackingDecisionTree(List<VmInstance> nVMs, List<Task> rTasks, int loggingFrequent)
	{
	    predictionEngine = new PredictionEngine(request, cloud);
	    this.nVMs = nVMs;
	    this.rTasks = rTasks;
	    logginCounter = loggingFrequent;
	    this.loggingFrequent = loggingFrequent;
	}

	public synchronized double getSolutionCost() {
	    return solutionCost;
	}

	public synchronized Integer[] getSolutionVector() {
	    return solutionVector;
	}

	public synchronized void setSolutionCost(double solutionCost) {
	    this.solutionCost = solutionCost;
	}

	public synchronized void setSolutionVector(Integer[] solutionVector) {
	    this.solutionVector = solutionVector.clone();
	}

	@Override
	public void run() {
	    solution = getFirstSolutionOfBackTrackingDecision();
	    request.setLogMessage("By Decision Tree");
	}

	private Map<Integer, Integer> getFirstSolutionOfBackTrackingDecision() {
	    // TODO Auto-generated method stub
	    return null;
	}

    }

    /**
     * 
     * @author Mohammed Alrokayan
     * 
     */
    public class BackTrackingTree implements Runnable {
	private PredictionEngine predictionEngine;
	private List<VmInstance> nVMs;
	private List<Task> rTasks;
	private BacktrackingSorts sort;
	private double solutionCost = Double.MAX_VALUE;
	private Integer[] solutionVector = null;
	private int logginCounter;
	private int loggingFrequent;

	private int minN;
	private int maxN;

	public BackTrackingTree(List<VmInstance> nVMs, List<Task> rTasks, BacktrackingSorts sort, int loggingFrequent,
		int minN, int maxN)
	{
	    predictionEngine = new PredictionEngine(request, cloud);
	    this.nVMs = nVMs;
	    this.rTasks = rTasks;
	    this.sort = sort;
	    logginCounter = loggingFrequent;
	    this.loggingFrequent = loggingFrequent;
	    this.minN = minN;
	    this.maxN = maxN;
	}

	public synchronized double getSolutionCost() {
	    return solutionCost;
	}

	public synchronized Integer[] getSolutionVector() {
	    return solutionVector;
	}

	public synchronized void setSolutionCost(double solutionCost) {
	    this.solutionCost = solutionCost;
	}

	public synchronized void setSolutionVector(Integer[] solutionVector) {
	    this.solutionVector = solutionVector.clone();
	}

	public void run() {
	    if (sort == BacktrackingSorts.Performance)
		// Sort nVMs by mips (performance)
		Collections.sort(nVMs, new Comparator<VmInstance>() {
		    public int compare(VmInstance VmInstance1, VmInstance VmInstance2) {
			// TODO Add data trasfere time from data source + out
			// from
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

	    getFirstSolutionOfBackTracking();
	    request.setLogMessage("By " + sort + " Tree");
	}

	private void getFirstSolutionOfBackTracking() {
	    int r = rTasks.size();
	    int subN = minN;
	    while (subN <= maxN)
	    {
		if (enableProgressBar)
		    Log.print("-");
		// If cost: the violation will be the budget,
		// but if perf: the violation will be deadline.
		boolean isQoSViolationInLeaf = true;
		boolean isLeafReached = false;

		Integer[] currentVector = new Integer[] { 1 };
		boolean done;
		do {
		    done = false;
		    // Get the execution time and cost of current node (res)
		    Map<Integer, Integer> schedulingPlan = predictionEngine.vectorToScheduleingPlan(currentVector, nVMs,
			    rTasks);
		    double[] executionTimeAndCost = predictionEngine.predictExecutionTimeAndCostFromScheduleingPlan(
			    schedulingPlan, nVMs);
		    // Logging
		    if (logginCounter >= loggingFrequent && currentVector.length == r)
		    // if(Thread.currentThread().getName().equals("C1"))
		    {
			CustomLog.printLine(Thread.currentThread().getName() + " n=" + subN + " :"
				+ Arrays.toString(currentVector) + "->"
				+ (r - currentVector.length) + " : "
				+ Arrays.toString(executionTimeAndCost));
			logginCounter = 0;
		    }
		    // Record that there is a QoS violation on a leaf, so we
		    // terminate the tree and return "very low budget!" or
		    // "very short deadline!"
		    if (!isLeafReached && currentVector.length == r)
			isLeafReached = true;
		    if (isQoSViolationInLeaf && currentVector.length == r)
		    {
			if (sort == BacktrackingSorts.Cost && executionTimeAndCost[1] <= request.getBudget())
			    isQoSViolationInLeaf = false;
			if (sort == BacktrackingSorts.Performance && executionTimeAndCost[0] <= request.getDeadline())
			    isQoSViolationInLeaf = false;
		    }

		    // If this is a new major branch, and we have a solution
		    // from the previous major branch, just take it.
		    if (getSolutionVector() != null && Arrays.equals(currentVector, new Integer[] { 1 }))
		    {
			CustomLog.printLine(Thread.currentThread().getName() + " n=" + subN + " :"
				+ Arrays.toString(getSolutionVector()) + "->"
				+ (r - getSolutionVector().length) + " : "
				+ Arrays.toString(executionTimeAndCost)
				+ " is the returned solution");
			return;
		    }

		    // Save the solution if we are in the leaf and it does not
		    // violate the deadline and budget, and the found one is
		    // better than the previous one (if any)
		    if (executionTimeAndCost[1] <= request.getBudget()
			    && executionTimeAndCost[0] <= request.getDeadline()
			    && currentVector.length == r
			    && (getSolutionVector() == null || executionTimeAndCost[1] < solutionCost))
		    {
			CustomLog.printLine(Thread.currentThread().getName() + " n=" + subN + " :"
				+ Arrays.toString(currentVector) + "->"
				+ (r - currentVector.length) + " : " + Arrays.toString(executionTimeAndCost)
				+ " is a solution");
			solutionCost = executionTimeAndCost[1];
			setSolutionVector(currentVector);
			Log.print(Thread.currentThread().getName());
		    }

		    boolean forceNextOrBack = false;
		    // If the budget and deadline are not violated, and we are
		    // not in a leaf -> go deep
		    if (executionTimeAndCost[1] <= request.getBudget()
			    && executionTimeAndCost[0] <= request.getDeadline()
			    && currentVector.length != r)
			currentVector = goDeeper(currentVector, subN);
		    // Come here if the node does violate the budget and/or the
		    // deadline, or we are in the leaf
		    else
			forceNextOrBack = true;
		    // if the new subRes has been scanned by previous
		    // major branch; just skip it, and go next.
		    while (forceNextOrBack || (!done && !Arrays.asList(currentVector).contains(subN) && currentVector.length == r))
		    {
			forceNextOrBack = false;

			if (currentVector[currentVector.length - 1] < subN)
			    currentVector[currentVector.length - 1]++;
			else
			    done = (currentVector = goBack(currentVector, subN, r)) == null ? true : false;
		    }

		    logginCounter++;
		} while (!done);
		if (isLeafReached && isQoSViolationInLeaf)
		{
		    if (sort == BacktrackingSorts.Cost)
			request.setLogMessage("Very low budget!");
		    if (sort == BacktrackingSorts.Performance)
			request.setLogMessage("Very short deadline!");
		    Log.print(Thread.currentThread().getName() + "x");
		    return;
		}
		// Increase the number of VMs to look into
		subN++;
	    }
	    Log.print(Thread.currentThread().getName() + "x");
	    request.setLogMessage("No Solution!");
	    return;
	}

	private Integer[] goBack(Integer[] num, int n, int r) {
	    do {
		Integer[] res;
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
