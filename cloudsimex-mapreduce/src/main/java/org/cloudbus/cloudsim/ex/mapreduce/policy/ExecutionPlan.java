package org.cloudbus.cloudsim.ex.mapreduce.policy;

import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmType;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.MapTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Task;

public class ExecutionPlan {
	
	public TaskSet taskSet;
	//public int vmTypeId;
	public double executionTime;
	public double cost;
	
	public VmInstance vm;
	
	public ExecutionPlan(TaskSet taskSet, int vmTypeId, Cloud cloud, Request request) {
		this.taskSet = taskSet;
		//this.vmTypeId = vmTypeId;
		
		vm = cloud.getVMTypeFromId(vmTypeId).getVmInstance();
		
		//getExecutionTime must be called before getCost..
		executionTime = getExecutionTime(request);
		cost = getCost();
	}
	
	public boolean isTaskInThisExecutionPlan(int taskId)
	{
		for (PairTaskDatasource pair : taskSet.pairs) {
			if(pair.taskId == taskId)
				return true;
		}
		return false;
	}

	private double getExecutionTime(Request request)
	{
		double totalExecutionTime = 0.0;
		for (PairTaskDatasource pairs : taskSet.pairs) {
			Task task = request.getTaskFromId(pairs.taskId);
			
			//1st) The time to transfere the data in:
			double dataIn = 0.0;
			if(task instanceof MapTask)
				dataIn = ((MapTask)task).predictFileTransferTimeFromDataSource(vm, pairs.dataSourceName);
			
			//2nd) The time to execute the task
			double executionTime = (task.getCloudletTotalLength() * task.getCloudletFileSize()) / vm.getMips();
			
			//3rd) The time to send the data out
			//SKIPPED: because we don't know where the reducers will execute
			double dataOut = 0.0;
			//if(task instanceof MapTask)
			//	dataOut = ((MapTask)task).predictFileTransferTimeToReduceVms(vmType);
					
			totalExecutionTime+= dataIn + executionTime + dataOut;
		}
		return totalExecutionTime;
	}
	
	private double getCost()
	{
		return Math.ceil(executionTime / 3600.0) * vm.cost;
	}
	
	
}
