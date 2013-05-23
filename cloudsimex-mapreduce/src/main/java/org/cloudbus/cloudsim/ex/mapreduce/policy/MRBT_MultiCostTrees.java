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

public class MRBT_MultiCostTrees extends Policy {

    public enum BacktrackingSorts {
	Cost, Performance;
    }

    private Request request;
    protected boolean isMostDuplicatedEnabled = true;
    protected int numCostTrees = 5;
    private long forceToAceeptAnySolutionTimeMillis = 20*1000;//20 sec
    private long forceToExitTimeMillis = 2*60*1000;// 2 min
    private boolean enableProgressBar = true;
    protected boolean enablePerfTree = true;
    
    private double deadlineViolationPercentageLimitCostTree = 0.025;
    private double deadlineViolationPercentageLimitPerfTree = 0.0025;
    private int loggingFrequent = 250000;
    

    public Boolean runAlgorithm(Cloud cloud, Request request) {
	this.request = request;
	CloudDeploymentModel cloudDeploymentModel = request.getCloudDeploymentModel();

	// Fill nVMs
	int numTasks = request.job.mapTasks.size() + request.job.reduceTasks.size();
	List<VmInstance> nVMs = Policy.getAllVmInstances(cloud, request, cloudDeploymentModel, numTasks);
	if (nVMs.size() == 0)
	    return false;
	
	// Sort nVMs by cost per mips - Perf Tree will have copy it and re-sort it
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
		backTrackingCostTree = new BackTrackingTree(nVMs, rTasks, BacktrackingSorts.Cost, deadlineViolationPercentageLimitCostTree,
			loggingFrequent, lastBranchUsed + 1, nVMs.size());
	    backTrackingCostTree = new BackTrackingTree(nVMs, rTasks, BacktrackingSorts.Cost, deadlineViolationPercentageLimitCostTree,
		    loggingFrequent, lastBranchUsed + 1, lastBranchUsed + numBranchesInEachTree);
	    if (i == 1 && enableProgressBar)
	    {
		Log.print("All Trees Progress [");
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
	if(enablePerfTree)
	{
	    backTrackingPerfTree = new BackTrackingTree(new ArrayList<VmInstance>(nVMs), rTasks,
		    BacktrackingSorts.Performance, deadlineViolationPercentageLimitPerfTree, loggingFrequent, 1, nVMs.size());
	    backTrackingPerfTreeThread = new Thread(backTrackingPerfTree);
	    backTrackingPerfTreeThread.setName("P");
	    backTrackingPerfTreeThread.start();
	}

	/**
	 * Wait for any of the two trees to finish
	 */
	Map<Integer, Integer> selectedSchedulingPlan = null;

	try {
	    while (true)
	    {
		long currentRunningTime = System.currentTimeMillis();
		if (!backTrackingCostTreeThreads.get(0).isAlive())
		{
		    selectedSchedulingPlan = checkCostTrees(backTrackingCostTrees);
		    if (selectedSchedulingPlan != null)
			break;
		}
		if (!checkPerfTree && enablePerfTree)
		{
		    if (currentRunningTime - costTreeRunningTime >= forceToAceeptAnySolutionTimeMillis)
		    {
			selectedSchedulingPlan = checkCostTrees(backTrackingCostTrees);
			if (selectedSchedulingPlan != null)
			    break;
			checkPerfTree = true;
		    }
		}
		if (checkPerfTree)
		{
		    selectedSchedulingPlan = checkCostTrees(backTrackingCostTrees);
		    if (selectedSchedulingPlan != null)
			break;
		    if (!backTrackingPerfTreeThread.isAlive())
		    {
			selectedSchedulingPlan = backTrackingPerfTree.solution;
			break;
		    }
		}

		if (currentRunningTime - costTreeRunningTime >= forceToExitTimeMillis)
		{
		    selectedSchedulingPlan = checkCostTrees(backTrackingCostTrees);
		    if (selectedSchedulingPlan == null && backTrackingPerfTree != null && backTrackingPerfTree.solution != null)
			selectedSchedulingPlan = backTrackingPerfTree.solution;
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
	    if(enablePerfTree)
		backTrackingPerfTreeThread.stop();
	    if (enableProgressBar)
		Log.printLine("]");
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

    private Map<Integer, Integer> checkCostTrees(List<BackTrackingTree> backTrackingCostTrees) {
	for (int i = 0; i < backTrackingCostTrees.size(); i++)
	{
	    if (backTrackingCostTrees.get(i).solution != null)
	    {
		double minCost = backTrackingCostTrees.get(i).solutionCost;
		Map<Integer, Integer> solution = backTrackingCostTrees.get(i).solution;
		for (int j = 0; j < backTrackingCostTrees.size(); j++)
		{
		    if (backTrackingCostTrees.get(j).solutionCost < minCost)
		    {
			minCost = backTrackingCostTrees.get(j).solutionCost;
			solution = backTrackingCostTrees.get(j).solution;
		    }
		}
		request.setLogMessage("CostTree forced to accept a solution");
		return solution;
	    }
	}
	return null;
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
	private double deadlineViolationPercentageLimit;
	private Map<Integer, Integer> solution = null;
	public double solutionCost = Double.MAX_VALUE;
	private Integer[] solutionVector = null;
	private int logginCounter;
	private int loggingFrequent;

	private int minN;
	private int maxN;

	public BackTrackingTree(List<VmInstance> nVMs, List<Task> rTasks, BacktrackingSorts sort,
		double deadlineViolationPercentageLimit, int loggingFrequent, int minN, int maxN)
	{
	    predictionEngine = new PredictionEngine();
	    this.nVMs = nVMs;
	    this.rTasks = rTasks;
	    this.sort = sort;
	    this.deadlineViolationPercentageLimit = deadlineViolationPercentageLimit;
	    logginCounter = loggingFrequent;
	    this.loggingFrequent = loggingFrequent;
	    this.minN = minN;
	    this.maxN = maxN;
	}

	public Map<Integer, Integer> getSolution() {
	    return solution;
	}

	public void setSolution(Map<Integer, Integer> solution) {
	    this.solution = solution;
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

	    solution = getFirstSolutionOfBackTracking(rTasks.size());
	    request.setLogMessage("By " + sort + " Tree");
	}

	private Map<Integer, Integer> getFirstSolutionOfBackTracking(int r) {
	    int subN = minN;
	    while (subN <= maxN)
	    {
		if (enableProgressBar)
		    Log.print("-");
		// If cost: the violation will be the budget,
		// but if perf: the violation will be deadline.
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
		    if (logginCounter >= loggingFrequent)
		    // if(Thread.currentThread().getName().equals("C1"))
		    {
			CustomLog.printLine(Thread.currentThread().getName() + " n=" + subN + " :"
				+ Arrays.toString(res) + "->"
				+ (r - res.length) + " : "
				+ Arrays.toString(executionTimeAndCost));
			logginCounter = 0;
		    }
		    // Record that there is a QoS violation on a leaf, so we
		    // terminate the tree and return "very low budget!" or
		    // "very short deadline!"
		    if (!isLeafReached && res.length == r)
			isLeafReached = true;
		    if (isQoSViolationInLeaf && res.length == r)
		    {
			if (sort == BacktrackingSorts.Cost && executionTimeAndCost[1] <= request.getBudget())
			    isQoSViolationInLeaf = false;
			if (sort == BacktrackingSorts.Performance && executionTimeAndCost[0] <= request.getDeadline())
			    isQoSViolationInLeaf = false;
		    }

		    // If this is a new major branch, and we have a solution
		    // from the previous major branch, just take it.
		    if (solution != null && Arrays.equals(res, new Integer[] { 1 }))
		    {
			CustomLog.printLine(sort + " n=" + subN + " :"
				+ Arrays.toString(solutionVector) + "->"
				+ (r - solutionVector.length) + " : "
				+ Arrays.toString(executionTimeAndCost)
				+ " is the selected solution");
			return solution;
		    }

		    // Save the solution if we are in the leaf and it does not
		    // violate the deadline and budget, and the found one is
		    // better than the previous one (if any)
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
			Log.print(Thread.currentThread().getName());
		    }

		    // If the budget and deadline are not violated, and we are
		    // not in a leaf -> go deep
		    if (executionTimeAndCost[1] <= request.getBudget()
			    && executionTimeAndCost[0] <= request.getDeadline()
			    && res.length != r)
			res = goDeeper(res, subN);
		    // Come here if the node does violate the budget and/or the
		    // deadline, or we are in the leaf
		    else
		    {
			do {
			    if (res[res.length - 1] < subN)
				res[res.length - 1]++;
			    else
			    {
				boolean doChangMostVmValue = false;
				double deadlineViolationPercentage = 1.0 - (request.getDeadline() / executionTimeAndCost[0]);
				if (deadlineViolationPercentage > deadlineViolationPercentageLimit)
				    doChangMostVmValue = true;
				done = (res = goBack(res, subN, r, doChangMostVmValue)) == null ? true : false;
			    }
			    // if the new subRes has been scanned by previous
			    // major branch; just skip it, and go next.
			} while (!done && !Arrays.asList(res).contains(subN) && res.length == r);
		    }
		    logginCounter++;
		} while (!done);
		if (isLeafReached && isQoSViolationInLeaf)
		{
		    if(solution!=null)
			return solution;
		    if (sort == BacktrackingSorts.Cost)
			request.setLogMessage("Very low budget!");
		    if (sort == BacktrackingSorts.Performance)
			request.setLogMessage("Very short deadline!");
		    return null;
		}
		// Increase the number of VMs to look into
		subN++;
	    }
	    if(solution!=null)
		return solution;
	    request.setLogMessage("No Solution!");
	    return null;
	}

	private Integer[] goBack(Integer[] num, int n, int r, boolean doChangMostVmValue) {
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
