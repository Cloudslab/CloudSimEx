package org.cloudbus.cloudsim.ex.mapreduce.policy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ex.mapreduce.PredictionEngine;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Task;
import org.cloudbus.cloudsim.ex.util.CustomLog;

public class DecisionTree implements Runnable
{
    private Integer vmId;

    private Request request;
    @SuppressWarnings("unused")
    private Cloud cloud;
    private PredictionEngine predictionEngine;
    private List<VmInstance> nVMs;
    private ArrayList<ArrayList<VmInstance>> vmGroupByType;
    private List<Task> rTasks;
    private double solutionCost;
    private double solutionExecutionTime;
    private Integer[] solutionVector = null;
    private int logginCounter;
    private int loggingFrequent;

    private long startRunningTime;
    private long forceToAceeptAnySolutionTimeMillis;
    private long forceToExitTimeMillis;

    public DecisionTree(Integer vmTypeId, Request request, Cloud cloud, List<VmInstance> nVMs,
	    List<Task> rTasks,
	    int loggingFrequent, long forceToAceeptAnySolutionTimeMillis, long forceToExitTimeMillis,
	    ArrayList<ArrayList<VmInstance>> vmGroupByType)
    {
	this.vmId = vmTypeId;

	this.request = request;
	this.cloud = cloud;
	predictionEngine = new PredictionEngine(request, cloud);
	this.nVMs = nVMs;
	this.rTasks = rTasks;
	logginCounter = loggingFrequent;
	this.loggingFrequent = loggingFrequent;

	this.forceToAceeptAnySolutionTimeMillis = forceToAceeptAnySolutionTimeMillis;
	this.forceToExitTimeMillis = forceToExitTimeMillis;

	this.vmGroupByType = vmGroupByType;
    }

    @Override
    public void run() {
	startRunningTime = System.currentTimeMillis();
	ArrayList<Integer> currentBranch = new ArrayList<Integer>();
	currentBranch.add(vmId);
	search(currentBranch, new ArrayList<Integer>());
	if (solutionVector == null)
	    CustomLog.printLine(Thread.currentThread().getName() + " : coudn't find solution");
	else
	    CustomLog.printLine(Thread.currentThread().getName() + ": returned solution: "
		    + Arrays.toString(solutionVector) + " : ["
		    + solutionExecutionTime + " seconds, $" + solutionCost + "]");
	Log.print(Thread.currentThread().getName().charAt(0) + "x");
    }

    private void search(ArrayList<Integer> currentBranch, ArrayList<Integer> path) {
	long currentRunningTime = System.currentTimeMillis();
	if (currentRunningTime - startRunningTime >= forceToAceeptAnySolutionTimeMillis && solutionVector != null)
	    return;
	if (currentRunningTime - startRunningTime >= forceToExitTimeMillis)
	    return;

	for (Integer vmInstanceId : currentBranch) {
	    logginCounter++;
	    ArrayList<Integer> newPath = new ArrayList<Integer>(path);
	    // Add the current node, as part of the path
	    newPath.add(vmInstanceId);

	    // Convert ArrayList<Integer> to Integer[]
	    Integer[] pathAsVector = newPath.toArray(new Integer[0]);

	    // Get the execution time and cost of current path
	    Map<Integer, Integer> schedulingPlan = predictionEngine.vectorToScheduleingPlan(pathAsVector, rTasks);
	    double[] executionTimeAndCost = predictionEngine.predictExecutionTimeAndCostFromScheduleingPlan(
		    schedulingPlan, nVMs);

	    // Logging
	    if (logginCounter >= loggingFrequent * 10
		    || (logginCounter >= loggingFrequent && newPath.size() == rTasks.size()))
	    {
		CustomLog.printLine(Thread.currentThread().getName() + " :"
			+ Arrays.toString(pathAsVector) + "->"
			+ (rTasks.size() - pathAsVector.length) + " : "
			+ Arrays.toString(executionTimeAndCost));
		logginCounter = 0;
		Log.print("-");
	    }

	    if (executionTimeAndCost[1] <= request.getBudget() && executionTimeAndCost[0] <= request.getDeadline())
	    {
		// if in Leaf branch
		if (newPath.size() == rTasks.size())
		{
		    if (solutionVector == null
			    || executionTimeAndCost[1] < solutionCost
			    || (executionTimeAndCost[1] == solutionCost && executionTimeAndCost[0] < solutionExecutionTime))
		    {
			CustomLog.printLine(Thread.currentThread().getName() + ": candidate solution: "
				+ Arrays.toString(pathAsVector) + " : "
				+ Arrays.toString(executionTimeAndCost));
			solutionVector = pathAsVector;
			solutionCost = executionTimeAndCost[1];
			solutionExecutionTime = executionTimeAndCost[0];
			Log.print(Thread.currentThread().getName().charAt(0));
			if (request.getAlgoFirstSoulationFoundedTime() == null)
			{
			    Long algoStartTime = request.getAlgoStartTime();
			    Long currentTime = System.currentTimeMillis();
			    request.setAlgoFirstSoulationFoundedTime((currentTime - algoStartTime));
			}
		    }
		}
		else
		{
		    // 1- Add the path
		    ArrayList<Integer> nextBranch = new ArrayList<>(newPath);
		    // 2- Add the next vm instance from the same type
		    vmGroupLoop:
		    for (ArrayList<VmInstance> vmGroup : vmGroupByType) {
			for (int i = 0; i < vmGroup.size(); i++) {
			    if (vmGroup.get(i).getId() == vmInstanceId)
			    {
				if (i + 1 < vmGroup.size())
				    nextBranch.add(vmGroup.get(i + 1).getId());
				else
				    break vmGroupLoop;
			    }

			}
		    }
		    // 3- Add one vm instance from every other type
		    vmGroupLoop:
		    for (ArrayList<VmInstance> vmGroup : vmGroupByType) {
			for (VmInstance vm : vmGroup) {
			    for (Integer vmInstanceIdInPath : newPath) {
				if (vm.getId() == vmInstanceIdInPath)
				    continue vmGroupLoop;
			    }

			}
			nextBranch.add(vmGroup.get(0).getId());
		    }
		    search(nextBranch, newPath);
		}
	    }
	}
    }

    public double getSolutionCost() {
	return solutionCost;
    }

    public double getSolutionExecutionTime() {
	return solutionExecutionTime;
    }

    public Integer[] getSolutionVector() {
	return solutionVector;
    }

}
