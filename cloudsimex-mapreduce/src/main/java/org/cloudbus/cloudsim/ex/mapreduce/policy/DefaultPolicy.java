package org.cloudbus.cloudsim.ex.mapreduce.policy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ex.mapreduce.models.*;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VMType;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.*;

public class DefaultPolicy extends Policy {

	@Override
	public List<VMType> runAlgorithm(Cloud cloud, Request request) {
		// SEARCHING ...

		// Step1) Fill TaskSet
		// Not all possibles yet, only one task for each datasource
		List<TaskSet> taskSets = new ArrayList<TaskSet>();

		for (MapTask mapTask : request.job.mapTasks) {
			for (String dataSourceName : mapTask.dataSources) {
				TaskSet taskSet = new TaskSet();
				PairTaskDatasource pairTaskDatasource = new PairTaskDatasource();
				pairTaskDatasource.taskId = mapTask.getCloudletId();
				pairTaskDatasource.dataSourceName = dataSourceName;
				taskSet.pairs.add(pairTaskDatasource);
				taskSets.add(taskSet);
			}
		}

		for (ReduceTask reduceTask : request.job.reduceTasks) {
			TaskSet taskSet = new TaskSet();
			PairTaskDatasource pairTaskDatasource = new PairTaskDatasource();
			pairTaskDatasource.taskId = reduceTask.getCloudletId();
			pairTaskDatasource.dataSourceName = null;
			taskSet.pairs.add(pairTaskDatasource);
			taskSets.add(taskSet);
		}

		// Step2) Fill EPs
		List<ExecutionPlan> executionPlans = new ArrayList<ExecutionPlan>();

		for (TaskSet taskSet : taskSets) {
			for (VMType vmType : cloud.getAllVMTypes()) {
				ExecutionPlan executionPlan = new ExecutionPlan(taskSet, vmType.getId(), cloud, request);
				executionPlans.add(executionPlan);
			}
		}

		// Step3) Find RS

		List<ExecutionPlan> resourceSet = new ArrayList<ExecutionPlan>();

		if (request.userClass == UserClass.GOLD) {
			// Sort by time (fastest last)
			resourceSet = listAndFirstFit_Time(executionPlans, request);

		} else {
			// Sort by cost (cheapest first)
			resourceSet = listAndFirstFit_Cost(executionPlans, request);
		}

		// Finished searching, and those are the results where the engine will
		// see
		List<VMType> provisioningVmList = new ArrayList<>();

		for (ExecutionPlan executionPlan : resourceSet) {
			// 1- provisioning
			VMType executionPlanVm = cloud.getVMTypeFromId(executionPlan.vmTypeId);
			if (!provisioningVmList.contains(executionPlanVm))
				provisioningVmList.add(executionPlanVm);

			// 2- scheduling
			for (PairTaskDatasource pairs : executionPlan.taskSet.pairs) {
				schedulingPlan.put(pairs.taskId, executionPlan.vmTypeId);

				Task task = request.getTaskFromId(pairs.taskId);
				if (task instanceof MapTask)
					((MapTask) task).selectedDataSourceName = pairs.dataSourceName;
			}
		}

		return provisioningVmList;
	}

	private List<ExecutionPlan> listAndFirstFit(List<ExecutionPlan> executionPlans, Request request) {

		List<ExecutionPlan> resourceSet = new ArrayList<ExecutionPlan>();

		// Loop for each EP, and if the task is not their, just add it.
		for (ExecutionPlan executionPlan : executionPlans) {
			// if non of the resourceSet's EPs has this task just add it.
			for (MapTask mapTask : request.job.mapTasks) {
				boolean isInThisResourceSet = isInThisResourceSet(resourceSet, mapTask.getCloudletId());

				if (!isInThisResourceSet) {
					boolean isInThisExecutionPlan = isInThisExecutionPlan(executionPlan, mapTask.getCloudletId());
					if (isInThisExecutionPlan) {
						for (PairTaskDatasource pair : executionPlan.taskSet.pairs)
							Log.printLine("MapTask ID: " + pair.taskId + " + DataSource: " + pair.dataSourceName
									+ " -> VM ID: " + executionPlan.vmTypeId);
						resourceSet.add(executionPlan);
						break;
					}
				}
			}

			// if non of the resourceSet's EPs has this task just add it.
			for (ReduceTask reduceTask : request.job.reduceTasks) {
				// if non of the resourceSet's EPs has this task just add it.
				boolean isInThisResourceSet = isInThisResourceSet(resourceSet, reduceTask.getCloudletId());
				if (!isInThisResourceSet) {
					boolean isInThisExecutionPlan = isInThisExecutionPlan(executionPlan, reduceTask.getCloudletId());
					if (isInThisExecutionPlan) {
						for (PairTaskDatasource pair : executionPlan.taskSet.pairs)
							Log.printLine("ReduceTask ID: " + pair.taskId + " + DataSource: " + pair.dataSourceName
									+ " -> VM ID: " + executionPlan.vmTypeId);
						resourceSet.add(executionPlan);
					}
				}
			}
		}

		return resourceSet;
	}

	private List<ExecutionPlan> listAndFirstFit_Cost(List<ExecutionPlan> executionPlans, Request request) {

		Collections.sort(executionPlans, new Comparator<ExecutionPlan>() {
			public int compare(ExecutionPlan executionPlan1, ExecutionPlan executionPlan2) {
				return Double.compare(executionPlan1.cost, executionPlan2.cost);
			}
		});

		return listAndFirstFit(executionPlans, request);
	}

	private List<ExecutionPlan> listAndFirstFit_Time(List<ExecutionPlan> executionPlans, Request request) {
		Collections.sort(executionPlans, new Comparator<ExecutionPlan>() {
			public int compare(ExecutionPlan executionPlan1, ExecutionPlan executionPlan2) {
				return Double.compare(executionPlan1.executionTime, executionPlan2.executionTime);
			}
		});

		return listAndFirstFit(executionPlans, request);
	}

	private boolean isInThisExecutionPlan(ExecutionPlan executionPlan, int taskId) {
		for (PairTaskDatasource pair : executionPlan.taskSet.pairs) {
			if (pair.taskId == taskId)
				return true;
		}

		return false;
	}

	private boolean isInThisResourceSet(List<ExecutionPlan> resourceSet, int taskId) {
		for (ExecutionPlan executionPlan : resourceSet) {
			for (PairTaskDatasource pair : executionPlan.taskSet.pairs) {
				if (pair.taskId == taskId)
					return true;
			}
		}
		return false;
	}
}
