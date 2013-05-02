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
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.DataSource;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.PrivateCloudDatacenter;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.PublicCloudDatacenter;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmType;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.*;

/*
 * Problems with Hwang and Kim 2012 approach:
 * 1- Static VMs (pre-provisioned VMs)
 * 2- Public Cloud (infinite resources) is not supported
 * 3- Support only one data source, replicas are not supported
 * 4- They calculate the data transfer time from map to reduce in the reduce phase, which is wrong, it has to be in the map phase
 * 
 * All in all, the user has to select and provision the VMs to select from.
 * 
 * Work around: The VPList will have:
 * 	- numTasks number of each type of VM from public cloud.
 *  - Max number of VMs that the data center can provision from the first type of private cloud.
 *  - The first datasource is the selected one
 *  
 * Assumptions:
 * 	- One type of VM in private cloud - by me.
 *  - Number of reduces smaller than the number of maps - by Hwang and Kim.
 */
public class Hwang_and_Kim_2012_LFFPerf extends Policy {

	@Override
	public Boolean runAlgorithm(Cloud cloud, Request request) {

		// Fill VPList
		List<VmInstance> VPList = new ArrayList<VmInstance>();
		int numTasks = request.job.mapTasks.size()
				+ request.job.reduceTasks.size();
		for (PublicCloudDatacenter publicCloudDatacenter : cloud.publicCloudDatacenters) {
			for (VmType vmType : publicCloudDatacenter.vmTypes)
				for (int i = 0; i < numTasks; i++)
					VPList.add(new VmInstance(vmType));

		}
		for (PrivateCloudDatacenter privateCloudDatacenter : cloud.privateCloudDatacenters) {
			VmType firstVmType = privateCloudDatacenter.vmTypes.get(0);
			int maxAvailableResource = privateCloudDatacenter
					.getMaxAvailableResource(firstVmType);

			for (int i = 0; i < Math.min(numTasks, maxAvailableResource); i++)
				VPList.add(new VmInstance(firstVmType));

		}

		// Sort VPList by mips (performance)
		Collections.sort(VPList, new Comparator<VmType>() {
			public int compare(VmType vmType1, VmType vmType2) {
				return Double.compare(vmType2.getMips(), vmType1.getMips());
			}
		});
		
		//Allocation
		boolean isJobAlloc = false;
		while (isJobAlloc == false && VPList.size() >= numTasks) {
			//Allocate all Map Tasks
			boolean isMapAlloc = MapTasksAlloc(request, VPList);
			if(isMapAlloc)
			{
				//Allocate all reduce Tasks
				ReduceTasksAlloc(request, VPList);
				
				//Calculate the execution time for each map task
				List<Double> mapET = new ArrayList<Double>();
				for (int i=0; i < request.job.mapTasks.size(); i++)
					//mapET.add(getMapExecutionTime(request.job.mapTasks.get(i), request, cloud));
					mapET.add(request.job.mapTasks.get(i).getTotalTime());
				
				//Map Finish time = The max of mapETs
				double mapFT = 0.0;
				for (Double oneMapET : mapET) {
					mapFT = Math.max(mapFT, oneMapET);
				}
				
				//Calculate the execution time for each reduce task
				List<Double> reduceET = new ArrayList<Double>();
				for (int i=0; i < request.job.reduceTasks.size(); i++)
					//reduceET.add(getReduceExecutionTime(request.job.reduceTasks.get(i), request, cloud, mapFT));
					reduceET.add(mapFT + request.job.reduceTasks.get(i).getTotalTime());
				
				//Map Finish time = The max of mapETs
				double reduceFT = 0.0;
				for (Double oneReduceET : reduceET) {
					reduceFT = Math.max(reduceFT, oneReduceET);
				}
				
				if(reduceFT <= request.deadline)
				{
					isJobAlloc = true;
					Log.printLine("Hwang and Kim 2012 Policy: Execution Time For Request ID: " + request.id + " is: "+reduceFT + " (Map Finish Time:"+mapFT+")");
				}
				else
				{
					//FDeallocate all VMs
					request.mapAndReduceVmProvisionList = new ArrayList<VmInstance>();
					request.schedulingPlanForMap = new HashMap<Integer, Integer>();
					request.schedulingPlanForReduce = new HashMap<Integer, Integer>();
				}
				if(isJobAlloc == false)
					VPList.remove(0);
			}
		}
		return isJobAlloc;
	}

	/*
	 * Allocate all reduce tasks
	 */
	private void ReduceTasksAlloc(Request request, List<VmInstance> VPList)
	{
		for (int i=0; i < request.job.reduceTasks.size(); i++)
		{
			// Scheduling (Provisioning already done in MapTasksAlloc)
			VmInstance vm = VPList.get(i);
			ReduceTask reduceTask = request.job.reduceTasks.get(i);
			request.schedulingPlanForReduce.put(reduceTask.getCloudletId(), vm.getId());
		}
	}

	/*
	 * Allocate all map tasks
	 */
	private boolean MapTasksAlloc(Request request, List<VmInstance> VPList)
	{
		for (int i=0; i < request.job.mapTasks.size(); i++)
		{
			try {
				//1- Provisioning
				VmInstance vm = VPList.get(i);
				request.mapAndReduceVmProvisionList.add(vm);
				//2- Scheduling
				MapTask mapTask = request.job.mapTasks.get(i);
				request.schedulingPlanForMap.put(mapTask.getCloudletId(), vm.getId());
			} catch (Exception e) {
				e.printStackTrace();
				//For any error, deallocate all VMs
				request.mapAndReduceVmProvisionList = new ArrayList<VmInstance>();
				request.schedulingPlanForMap = new HashMap<Integer, Integer>();
				return false;
			}
		}
		return true;
	}
	
	/*

	private double getMapExecutionTime(MapTask mapTask, Request request, Cloud cloud)
	{
		VmInstance vm = request.getProvisionedVmFromTaskId(mapTask.getCloudletId());
			
		//1st) The time to transfere the data in
		String dataSourceName = mapTask.selectedDataSourceName;
		double dataIn = mapTask.predictDataTransferTimeFromADataSource(vm, dataSourceName);
			
		//2nd) The time to execute the task
		double executionTime = (mapTask.getCloudletTotalLength() * mapTask.getCloudletFileSize()) / (vm.getMips()*1000000.0);
					
			
		return dataIn + executionTime;
	}
	
	

	private double getReduceExecutionTime(ReduceTask reduceTask, Request request, Cloud cloud, double mapFT)
	{
		VmInstance vm = request.getProvisionedVmFromTaskId(reduceTask.getCloudletId());
		double allMapsDataInAndET = 0.0;
		for (MapTask mapTask : request.job.mapTasks) {
			double dataInFromOneMap = mapTask.predictDataTransferTimeToOneReduce(reduceTask, request, cloud);
			double executionTimeForOneMapData = (reduceTask.getCloudletTotalLength() * reduceTask.getCloudletFileSize()) / vm.getMips();
			
			allMapsDataInAndET+= dataInFromOneMap + executionTimeForOneMapData;
		}
		
					
			
		return mapFT + allMapsDataInAndET;
	}
	*/
}
