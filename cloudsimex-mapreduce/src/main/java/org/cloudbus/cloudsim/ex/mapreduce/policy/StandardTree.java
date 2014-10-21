package org.cloudbus.cloudsim.ex.mapreduce.policy;

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
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Task;
import org.cloudbus.cloudsim.ex.mapreduce.policy.BB.BacktrackingSorts;
import org.cloudbus.cloudsim.ex.util.CustomLog;

/**
 * 
 * @author Mohammed Alrokayan
 * 
 */
public class StandardTree implements Runnable {
    public static boolean enableProgressBar = true;
    private Request request;
    @SuppressWarnings("unused")
    private Cloud cloud;

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

    public StandardTree(Request request, Cloud cloud, List<VmInstance> nVMs, List<Task> rTasks,
	    BacktrackingSorts sort, int loggingFrequent,
	    int minN, int maxN)
    {
	this.request = request;
	this.cloud = cloud;
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

    @Override
    public void run() {
	if (sort == BacktrackingSorts.Performance)
	    // Sort nVMs by mips (performance)
	    Collections.sort(nVMs, new Comparator<VmInstance>() {
		@Override
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
		Map<Integer, Integer> schedulingPlan = predictionEngine.vectorToScheduleingPlan(currentVector,
			nVMs,
			rTasks);
		double[] executionTimeAndCost = predictionEngine.predictExecutionTimeAndCostFromScheduleingPlan(
			schedulingPlan, nVMs);
		// Logging
		if (logginCounter >= loggingFrequent * 10
			|| (logginCounter >= loggingFrequent && currentVector.length == r))
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
		    if (request.getAlgoFirstSoulationFoundedTime() == null)
		    {
			Long algoStartTime = request.getAlgoStartTime();
			Long currentTime = System.currentTimeMillis();
			request.setAlgoFirstSoulationFoundedTime((currentTime - algoStartTime));
		    }
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
		while (forceNextOrBack
			|| (!done && !Arrays.asList(currentVector).contains(subN) && currentVector.length == r))
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
