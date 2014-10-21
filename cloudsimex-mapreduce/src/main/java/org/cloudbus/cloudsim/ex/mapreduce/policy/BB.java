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
import org.cloudbus.cloudsim.ex.mapreduce.policy.Policy.CloudDeploymentModel;
import org.cloudbus.cloudsim.ex.util.CustomLog;

public class BB {

    public enum BacktrackingSorts {
	Cost, Performance;
    }

    public enum BacktrackingType {
	Standard, Decision;
    }

    @SuppressWarnings("unused")
    private Request request;
    @SuppressWarnings("unused")
    private Cloud cloud;
    private int loggingFrequent = 250000;

    @SuppressWarnings("deprecation")
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
	    @Override
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

	Integer[] solutionVector = null;
	if (backtrackingType == BacktrackingType.Decision)
	{
	    BBDecisionAlgorithm backTrackingDecisionAlgorithm = new BBDecisionAlgorithm(request,
		    cloud, nVMs, rTasks, loggingFrequent, forceToAceeptAnySolutionTimeMillis, forceToExitTimeMillis);
	    backTrackingDecisionAlgorithm.getBestSolutionOfBackTrackingDecision();
	    solutionVector = backTrackingDecisionAlgorithm.getSolutionVector();
	}
	if (backtrackingType == BacktrackingType.Standard)
	{

	    /**
	     * Run the cost trees
	     */
	    // Get SchedulingPlan from backtracking
	    long costTreeRunningTime = System.currentTimeMillis();
	    List<StandardTree> backTrackingCostTrees = new ArrayList<StandardTree>();
	    List<Thread> backTrackingCostTreeThreads = new ArrayList<Thread>();
	    int numBranchesInEachTree = (int) Math.floor((double) nVMs.size() / numCostTrees);
	    int lastBranchUsed = 0;
	    for (int i = 1; i <= numCostTrees; i++)
	    {
		StandardTree backTrackingCostTree;
		if (i == numCostTrees)
		    backTrackingCostTree = new StandardTree(request, cloud, nVMs, rTasks, BacktrackingSorts.Cost,
			    loggingFrequent, lastBranchUsed + 1, nVMs.size());
		backTrackingCostTree = new StandardTree(request, cloud, nVMs, rTasks, BacktrackingSorts.Cost,
			loggingFrequent, lastBranchUsed + 1, lastBranchUsed + numBranchesInEachTree);
		if (i == 1 && StandardTree.enableProgressBar)
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
	    StandardTree backTrackingPerfTree = null;
	    Thread backTrackingPerfTreeThread = null;
	    if (enablePerfTree)
	    {
		backTrackingPerfTree = new StandardTree(request, cloud, new ArrayList<VmInstance>(nVMs), rTasks,
			BacktrackingSorts.Performance, loggingFrequent, 1, nVMs.size());
		backTrackingPerfTreeThread = new Thread(backTrackingPerfTree);
		backTrackingPerfTreeThread.setName("P");
		backTrackingPerfTreeThread.start();
	    }

	    /**
	     * Wait for any of the two trees to finish
	     */
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
				&& backTrackingPerfTree.getSolutionVector() != null)
			    solutionVector = backTrackingPerfTree.getSolutionVector();
			request.setLogMessage("Force To Exit");
			break;
		    }

		    Thread.currentThread();
		    Thread.sleep(500);
		}
	    } catch (InterruptedException e) {
		e.printStackTrace();
	    } finally
	    {
		for (int i = 0; i < backTrackingCostTreeThreads.size(); i++)
		    backTrackingCostTreeThreads.get(i).stop();
		if (enablePerfTree)
		    backTrackingPerfTreeThread.stop();
		if (StandardTree.enableProgressBar)
		    Log.printLine("]");
	    }
	}
	if (solutionVector == null)
	{
	    CustomLog.printLine("Solution: Could't find a solution");
	    return false;
	}
	PredictionEngine predictionEngine = new PredictionEngine(request, cloud);
	Map<Integer, Integer> selectedSchedulingPlan = null;
	if (backtrackingType == BacktrackingType.Decision)
	    selectedSchedulingPlan = predictionEngine.vectorToScheduleingPlan(solutionVector, rTasks);
	if (backtrackingType == BacktrackingType.Standard)
	    selectedSchedulingPlan = predictionEngine.vectorToScheduleingPlan(solutionVector, nVMs, rTasks);

	double[] executionTimeAndCost = predictionEngine.predictExecutionTimeAndCostFromScheduleingPlan(
		selectedSchedulingPlan, nVMs);
	CustomLog.printLine("Selected Solution: " + Arrays.toString(solutionVector) + " : "
		+ Arrays.toString(executionTimeAndCost));

	// 1- Provisioning
	ArrayList<ArrayList<VmInstance>> provisioningPlans = predictionEngine.getProvisioningPlan(
		selectedSchedulingPlan, nVMs, request.job);
	request.mapAndReduceVmProvisionList = provisioningPlans.get(0);
	request.reduceOnlyVmProvisionList = provisioningPlans.get(1);

	// 2- Scheduling
	request.schedulingPlan = selectedSchedulingPlan;

	return true;
    }

    private Integer[] checkCostTrees(List<StandardTree> backTrackingCostTrees) {
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
}
