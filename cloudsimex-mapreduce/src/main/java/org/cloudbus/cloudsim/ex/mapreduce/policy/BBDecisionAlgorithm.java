package org.cloudbus.cloudsim.ex.mapreduce.policy;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ex.mapreduce.PredictionEngine;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Task;
import org.cloudbus.cloudsim.ex.util.CustomLog;

public class BBDecisionAlgorithm {
    private Request request;
    private Cloud cloud;
    @SuppressWarnings("unused")
    private PredictionEngine predictionEngine;
    private List<VmInstance> nVMs;
    private ArrayList<ArrayList<VmInstance>> vmGroupByType = new ArrayList<ArrayList<VmInstance>>();
    private List<Task> rTasks;
    private double solutionCost;
    private double solutionExecutionTime;
    private Integer[] solutionVector = null;
    @SuppressWarnings("unused")
    private int logginCounter;
    private int loggingFrequent;

    @SuppressWarnings("unused")
    private long startRunningTime;
    private long forceToAceeptAnySolutionTimeMillis;
    private long forceToExitTimeMillis;
    private static boolean enableProgressBar = true;

    public BBDecisionAlgorithm(Request request, Cloud cloud, List<VmInstance> nVMs, List<Task> rTasks,
	    int loggingFrequent, long forceToAceeptAnySolutionTimeMillis, long forceToExitTimeMillis)
    {
	this.request = request;
	this.cloud = cloud;
	predictionEngine = new PredictionEngine(request, cloud);
	this.nVMs = nVMs;
	this.rTasks = rTasks;
	logginCounter = loggingFrequent;
	this.loggingFrequent = loggingFrequent;

	this.forceToAceeptAnySolutionTimeMillis = forceToAceeptAnySolutionTimeMillis;
	this.forceToExitTimeMillis = forceToExitTimeMillis;
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

    public void getBestSolutionOfBackTrackingDecision() {
	// List of VMs grouped by types
	nVMsLoop:
	for (VmInstance vm : nVMs) {
	    CustomLog.printLine(vm.getId() + ":" + vm.getName());
	    for (ArrayList<VmInstance> vmGroup : vmGroupByType) {
		if (vmGroup.get(0).vmTypeId == vm.vmTypeId)
		{
		    vmGroup.add(vm);
		    continue nVMsLoop;
		}
	    }
	    ArrayList<VmInstance> vmGroup = new ArrayList<VmInstance>();
	    vmGroup.add(vm);
	    vmGroupByType.add(vmGroup);
	}

	if (enableProgressBar)
	    Log.print("All " + (vmGroupByType.size() + 1) + " Trees Progress [");

	ArrayList<DecisionTree> searchTrees = new ArrayList<DecisionTree>();
	ArrayList<Thread> threads = new ArrayList<Thread>();
	for (ArrayList<VmInstance> vmGroup : vmGroupByType)
	{
	    DecisionTree searchTree = new DecisionTree(vmGroup.get(0).getId(), request, cloud,
		    nVMs, rTasks, loggingFrequent, forceToAceeptAnySolutionTimeMillis, forceToExitTimeMillis,
		    vmGroupByType);
	    searchTrees.add(searchTree);
	    Thread searchTreeThread = new Thread(searchTree);
	    threads.add(searchTreeThread);
	    searchTreeThread.setName(vmGroup.get(0).getName());
	    searchTreeThread.start();
	}

	for (Thread thread : threads) {
	    try {
		thread.join();
	    } catch (InterruptedException e) {
		e.printStackTrace();
	    }
	}

	if (enableProgressBar)
	    Log.printLine("]");

	for (DecisionTree searchTree : searchTrees) {
	    if (searchTree.getSolutionVector() != null)
	    {
		if (solutionVector == null
			|| searchTree.getSolutionCost() < solutionCost
			|| (searchTree.getSolutionCost() == solutionCost && searchTree.getSolutionExecutionTime() < solutionExecutionTime))
		{
		    solutionVector = searchTree.getSolutionVector();
		    solutionCost = searchTree.getSolutionCost();
		    solutionExecutionTime = searchTree.getSolutionExecutionTime();
		}
	    }
	}
	request.setLogMessage("By Decision Tree");
    }
}
