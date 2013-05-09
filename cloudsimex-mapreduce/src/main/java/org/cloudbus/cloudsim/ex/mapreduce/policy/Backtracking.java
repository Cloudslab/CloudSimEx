package org.cloudbus.cloudsim.ex.mapreduce.policy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ex.mapreduce.PredictionEngine;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.PrivateCloudDatacenter;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.PublicCloudDatacenter;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmType;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.MapTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.ReduceTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Task;
import org.cloudbus.cloudsim.ex.mapreduce.policy.Policy.CloudDeploymentModel;

public class Backtracking {
	
	public enum BacktrackingSorts {
		Cost, Performance;
	}
	
	private List<VmInstance> nVMs = new ArrayList<VmInstance>();
	private List<Task> rTasks = new ArrayList<Task>();
	private Request request;

	public Boolean runAlgorithm(Cloud cloud, Request request, BacktrackingSorts backtrackingSort) {
		this.request = request;
		CloudDeploymentModel cloudDeploymentModel = request.getCloudDeploymentModel();
		
		// Fill nVMs
		nVMs = Policy.getAllVmInstances(cloud, request, cloudDeploymentModel);
		if(nVMs.size() == 0)
			return false;
		
		if(backtrackingSort == BacktrackingSorts.Cost)
		{
			// Sort nVMs by cost
			Collections.sort(nVMs, new Comparator<VmInstance>() {
				public int compare(VmInstance VmInstance1, VmInstance VmInstance2) {
					return Double.compare(VmInstance1.vmCostPerHour, VmInstance2.vmCostPerHour);
				}
			});
		}
		
		if(backtrackingSort == BacktrackingSorts.Performance)
		{
			// Sort nVMs by mips (performance)
			Collections.sort(nVMs, new Comparator<VmInstance>() {
				public int compare(VmInstance VmInstance1, VmInstance VmInstance2) {
					return Double.compare(VmInstance2.getMips(), VmInstance1.getMips());
				}
			});
		}
		
		// Fill rTasks
		for (MapTask mapTask : request.job.mapTasks)
			rTasks.add(mapTask);
		for (ReduceTask reduceTask : request.job.reduceTasks)
			rTasks.add(reduceTask);


		// Selected SchedulingPlan from backtracking
		Map<Integer, Integer> selectedSchedulingPlan = getFirstSolutionOfBackTracking(nVMs.size(), rTasks.size());
		if(selectedSchedulingPlan == null)
			return false;
		
		// 1- Provisioning
		ArrayList<ArrayList<VmInstance>> provisioningPlans = Request.getProvisioningPlan(selectedSchedulingPlan, nVMs,request.job);
		request.mapAndReduceVmProvisionList = provisioningPlans.get(0);
		request.reduceOnlyVmProvisionList = provisioningPlans.get(1);

		// 2- Scheduling
		request.schedulingPlan = selectedSchedulingPlan;

		return true;
	}

	private Map<Integer, Integer> getFirstSolutionOfBackTracking(int n, int r) {
		

		int[] res = new int[r];
		for (int i = 0; i < res.length; i++) {
			res[i] = 1;
		}
		boolean done = false;
		while (!done) {
			// Convert int[] to Integer[]
			Integer[] resObj = new Integer[r];
			for (int i = 0; i < res.length; i++) {
				resObj[i] = res[i];
			}
			Map<Integer, Integer> schedulingPlan = vectorToScheduleingPlan(resObj);
			double[] executionTimeAndCost = PredictionEngine.predictExecutionTimeAndCostFromScheduleingPlan(schedulingPlan, nVMs, request.job);
			//Log.printLine(Arrays.toString(resObj) + " : "+Arrays.toString(executionTimeAndCost));
			if(executionTimeAndCost[0] <= request.deadline && executionTimeAndCost[1] <= request.budget)
				return schedulingPlan;
			done = getNext(res, n, r);
		}

		return null;
	}

	/////////

	private boolean getNext(final int[] num, final int n, final int r) {
		int target = r - 1;
		num[target]++;
		if (num[target] > n) {
			// Carry the One
			while (num[target] >= n) {
				target--;
				if (target < 0) {
					break;
				}
			}
			if (target < 0) {
				return true;
			}
			num[target]++;
			for (int i = target + 1; i < num.length; i++) {
				num[i] = 1;
			}
		}
		return false;
	}
	
	private Map<Integer, Integer> vectorToScheduleingPlan(Integer[] res)
	{
		Map<Integer, Integer> scheduleingPlan = new HashMap<Integer, Integer>();
		
		for (int i = 0; i < res.length; i++)
		{
			int TaskId = rTasks.get(i).getCloudletId();
			int VmId = nVMs.get(res[i]-1).getId();
			scheduleingPlan.put(TaskId, VmId);
		}
		
		return scheduleingPlan;
	}

}
