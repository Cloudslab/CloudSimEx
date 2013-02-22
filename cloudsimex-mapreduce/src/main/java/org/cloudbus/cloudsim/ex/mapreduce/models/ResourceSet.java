package org.cloudbus.cloudsim.ex.mapreduce.models;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VMType;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.*;

public class ResourceSet {

	public List<ExecutionPlan> executionPlans = new ArrayList<ExecutionPlan>();
	
	Cloud cloud;
	Request request;
	
	public ResourceSet(Cloud cloud,
			Requests requests) {
		this.cloud = cloud;
		this.request = request;
		
		for (Request request : requests.requests) {
			for (MapTask mapTask : request.job.mapTasks) {
				for (String dataSourceName : mapTask.dataSources) {
					// This is a very simple TaskSets, where only one task on
					// each data source is in the list,
					// We should add multiple tasks to the single set
					TaskSet taskSet = new TaskSet();
					PairTaskDatasource pairTaskDatasource = new PairTaskDatasource();
					pairTaskDatasource.taskId = mapTask.getCloudletId();
					pairTaskDatasource.dataSourceName = dataSourceName;
					taskSet.pairs.add(pairTaskDatasource);

					// This is a very simple ExecutionPlan, where GOLD users
					// uses will use the 1st VM in the first data center in private cloud
					// and other will use the 1st VM in the first data center in the public cloud
					ExecutionPlan executionPlan = new ExecutionPlan();
					executionPlan.taskSet = taskSet;
					if (request.userClass == UserClass.GOLD)
						executionPlan.vmTypeId = cloud.privateCloudDatacenters
								.get(0).vmTypes.get(0).getId();
					else
						executionPlan.vmTypeId = cloud.publicCloudDatacenters
								.get(0).vmTypes.get(0).getId();
					executionPlans.add(executionPlan);
					Log.printLine("MapTask/CloudLet ID: "
							+ mapTask.getCloudletId() + " + DataSource: "
							+ dataSourceName + " -> VM ID: "
							+ executionPlan.vmTypeId);
				}
			}

			for (ReduceTask reduceTask : request.job.reduceTasks) {
				
				// This is a very simple TaskSets, where only one task on each
				// data source is in the list,
				// We should add multiple tasks to the single set
				TaskSet taskSet = new TaskSet();
				PairTaskDatasource pairTaskDatasource = new PairTaskDatasource();
				pairTaskDatasource.taskId = reduceTask.getCloudletId();
				pairTaskDatasource.dataSourceName = null;
				taskSet.pairs.add(pairTaskDatasource);

				// This is a very simple ExecutionPlan, where GOLD users
				// uses will use the 1st VM in the first data center in private cloud
				// and other will use the 1st VM in the first data center in the public cloud
				ExecutionPlan executionPlan = new ExecutionPlan();
				executionPlan.taskSet = taskSet;
				if (request.userClass == UserClass.GOLD)
					executionPlan.vmTypeId = cloud.privateCloudDatacenters.get(0).vmTypes
							.get(0).getId();
				else
					executionPlan.vmTypeId = cloud.publicCloudDatacenters.get(0).vmTypes
							.get(0).getId();
				executionPlans.add(executionPlan);
				Log.printLine("ReduceTask/CloudLet ID: "
						+ reduceTask.getCloudletId() + " -> VM ID: "
						+ executionPlan.vmTypeId);
			}
		}
	}
	
	public List<VMType> getSelectedVMTypeIds()
	{
		List<VMType> selectedVMTypes = new ArrayList<VMType>();
		for (ExecutionPlan executionPlan : executionPlans)
		{
			VMType vmType = cloud.findVMType(executionPlan.vmTypeId);
			if(!selectedVMTypes.contains(vmType))
				selectedVMTypes.add(vmType);
		}
		
		return selectedVMTypes;
	}
}
