package org.cloudbus.cloudsim.ex.mapreduce.models.request;

import java.lang.ProcessBuilder.Redirect;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.ex.mapreduce.MapReduceEngine;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.DataSource;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmType;
import org.cloudbus.cloudsim.ex.util.Id;

public class MapTask extends Task
{
	public String dataSourceName;
	// <Reduce Task ID, IDSize>
	public Map<String, Integer> intermediateData;

	public MapTask(String name, int dSize, int mi, Map<String, Integer> intermediateData)
	{
		super(name, dSize, mi);
		this.intermediateData = intermediateData;
	}

	/**
	 * Calculate the Data-in time
	 * 
	 * @return The real data transfer time
	 */
	public double realDataTransferTimeFromTheDataSource()
	{
		return predictDataTransferTimeFromADataSource(getCurrentVmInstance(), dataSourceName);
	}

	/**
	 * Predict the transfer time from a datasource while this task running on a vm
	 * 
	 * @param vmTypeId
	 * @param dataSourceName
	 * @return the transfer time
	 */
	public double predictDataTransferTimeFromADataSource(VmType vmType, String dataSourceName)
	{
		Cloud cloud = getCloud();

		String currentVmTypeName = vmType.name;

		for (List<Object> throughputs_vm_ds : cloud.throughputs_vm_ds)
		{
			String source_vm = (String) throughputs_vm_ds.get(0);
			String destination_dataSource = (String) throughputs_vm_ds.get(1);
			double throughputInMegaBit = (double) throughputs_vm_ds.get(2);

			if (source_vm.equals(currentVmTypeName) && destination_dataSource.equals(dataSourceName))
				return dSize / (throughputInMegaBit / 8.0);
		}

		try
		{
			throw new Exception("Throughputs between " + currentVmTypeName + " and " + dataSourceName
					+ " could not be found in Cloud.yaml");
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return 0.0;
	}
	
	/**
	 * Calculate the Data-in cost
	 * 
	 * @return The real data transfer cost
	 */
	public double realDataTransferCostFromTheDataSource()
	{
		return predictDataTransferCostFromADataSource(dataSourceName);
	}

	/**
	 * Predict the transfer cost from a datasource
	 * 
	 * @param dataSourceName
	 * @return the transfer cost
	 */
	public double predictDataTransferCostFromADataSource(String dataSourceName)
	{
		Cloud cloud = getCloud();
		
		DataSource selectedDataSource = null;
		for (DataSource dataSource : cloud.dataSources)
		{
			if(dataSource.getName().equals(dataSourceName))
			{
				selectedDataSource = dataSource;
				break;
			}
		}
		if(selectedDataSource == null)
			try
			{
				throw new Exception("Could not find "+selectedDataSource+" in the cloud");
			}
			catch (Exception e)
			{
				e.printStackTrace();
				return 0.0;
			}
		
		//We devide by 1000000 because the cost per terabyte
		return selectedDataSource.cost * (dSize/1000000.0);

	}

	/**
	 * Calculate the data-out time
	 * 
	 * @return The real data transfer time
	 */
	public double realDataTransferTimeToReduceVms()
	{
		double transferTime = 0.0;

		Request currentRequest = getCurrentRequest();

		// For each reduce get the transfer Time
		for (ReduceTask reduceTask : currentRequest.job.reduceTasks)
		{
			transferTime += predictDataTransferTimeToOneReduce(reduceTask);
		}

		return transferTime;
	}

	/**
	 * Predict the transfer time to one reduce task
	 * 
	 * @param reduceTask
	 * @return the transfer time
	 */
	public double predictDataTransferTimeToOneReduce(ReduceTask reduceTask)
	{
		Cloud cloud = getCloud();

		VmInstance currentVm = getCurrentVmInstance();
		String currentVmTypeName = currentVm.name;

		if (intermediateData.containsKey(reduceTask.name))
		{
			int intermediateDataSizeInMegaByte = intermediateData.get(reduceTask.name);

			VmInstance reduceVm = reduceTask.getCurrentVmInstance();
			String reduceVmTypeName = reduceVm.name;

			// if the Reduce vm is in the same Map vm, the Transfer Time is zero
			if (currentVmTypeName.equals(reduceVmTypeName))
				return 0;

			for (List<Object> throughputs_vm_vm : cloud.throughputs_vm_vm)
			{
				String source_vm = (String) throughputs_vm_vm.get(0);
				String destination_vm = (String) throughputs_vm_vm.get(1);
				double throughputInMegaBit = (double) throughputs_vm_vm.get(2);

				if (source_vm.equals(currentVmTypeName) && destination_vm.equals(reduceVmTypeName))
				{
					return intermediateDataSizeInMegaByte / (throughputInMegaBit / 8.0);
				}
			}
			try
			{
				throw new Exception("Throughputs between " + currentVmTypeName + " and " + reduceVmTypeName
						+ " could not be found in Cloud.yaml");
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		return 0;
	}
	
	/**
	 * Calculate the data-out cost
	 * 
	 * @return The real data transfer cost
	 */
	public double realDataTransferCostToReduceVms()
	{
		int totalIntermediateDataSize = 0;
		for (Integer intermediateDataSize : intermediateData.values())
		{
			totalIntermediateDataSize += intermediateDataSize;
		}

		return predictDataTransferCostToReduceVms(totalIntermediateDataSize);
	}
	
	/**
	 * Predict the cost of transferring the data out based on the intermediate data size
	 * @param totalIntermediateDataSize
	 * @return
	 */
	public double predictDataTransferCostToReduceVms(int totalIntermediateDataSize)
	{
		return getCurrentVmInstance().transferringCost * (double)totalIntermediateDataSize;
	}

	
	public double getTotalTime()
	{
		return realDataTransferTimeFromTheDataSource() + getRealVmExecutionTime() + realDataTransferTimeToReduceVms();
	}

}
