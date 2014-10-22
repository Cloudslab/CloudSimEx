package org.cloudbus.cloudsim.ex.mapreduce.models.request;

import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.DataSource;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.util.Textualize;

@Textualize(properties = {})
public class MapTask extends Task
{
    public String dataSourceName;
    // <Reduce Task ID, IDSize>
    public Map<String, Integer> intermediateData;
    public int extraTasks = 0;

    public MapTask(int totalMapTasks, int dSize, int mi, Map<String, Integer> intermediateData)
    {
	super("map", dSize, mi);
	this.intermediateData = intermediateData;

	name = "map-" + getCloudletId();

	extraTasks = totalMapTasks - 1;
    }

    public double getTaskExecutionTimeInSeconds()
    {
	return dataTransferTimeFromTheDataSource() + super.getTaskExecutionTimeInSeconds()
		+ dataTransferTimeToAllReducers();
    }

    public double getTaskExecutionTimeIgnoringDataTransferTimeInSeconds()
    {
	return super.getTaskExecutionTimeInSeconds();
    }

    public double dataTransferTimeFromTheDataSource()
    {
	return dataTransferTimeFromTheDataSource(getCurrentVmInstance());
    }

    public double dataTransferTimeFromTheDataSource(VmInstance vm)
    {
	Cloud cloud = getCloud();

	String currentVmTypeName = vm.name;

	for (List<?> throughputs_vm_ds : cloud.throughputs_ds_vm)
	{
	    String source_dataSource = (String) throughputs_vm_ds.get(0);
	    String destination_vm = (String) throughputs_vm_ds.get(1);
	    double throughputInMegaBit = (Double) throughputs_vm_ds.get(2);

	    if (destination_vm.equals(currentVmTypeName) && source_dataSource.equals(dataSourceName))
		return dSize / (throughputInMegaBit / 8.0);
	}

	try
	{
	    throw new Exception("Throughputs between " + currentVmTypeName + " and " + dataSourceName
		    + " could not be found in Cloud.yaml");
	} catch (Exception e)
	{
	    e.printStackTrace();
	}
	return 0.0;
    }

    /**
     * Calculate the data-out time
     * 
     * @return The real data transfer time
     */
    public double dataTransferTimeToAllReducers()
    {
	double transferTime = 0.0;

	Request currentRequest = getCurrentRequest();

	// For each reduce get the transfer Time
	for (ReduceTask reduceTask : currentRequest.job.reduceTasks)
	{
	    transferTime += dataTransferTimeToOneReducer(reduceTask);
	}

	return transferTime;
    }

    private double dataTransferTimeToOneReducer(ReduceTask reduceTask)
    {
	return dataTransferTimeToOneReducer(reduceTask, getCurrentVmInstance());
    }

    /**
     * Predict the transfer time to one reduce task
     * 
     * @param reduceTask
     * @return the transfer time
     */
    private double dataTransferTimeToOneReducer(ReduceTask reduceTask, VmInstance vm)
    {
	Cloud cloud = getCloud();

	String currentVmTypeName = vm.name;

	if (intermediateData.containsKey(reduceTask.name))
	{
	    int intermediateDataSizeInMegaByte = intermediateData.get(reduceTask.name);

	    VmInstance reduceVm = reduceTask.getCurrentVmInstance();
	    String reduceVmTypeName = reduceVm.name;

	    // if the Reduce vm is in the same Map vm, the Transfer Time is zero
	    if (currentVmTypeName.equals(reduceVmTypeName))
		return 0;

	    for (List<?> throughputs_vm_vm : cloud.throughputs_vm_vm)
	    {
		String source_vm = (String) throughputs_vm_vm.get(0);
		String destination_vm = (String) throughputs_vm_vm.get(1);
		double throughputInMegaBit = (Double) throughputs_vm_vm.get(2);

		if (source_vm.equals(currentVmTypeName) && destination_vm.equals(reduceVmTypeName))
		{
		    return intermediateDataSizeInMegaByte / (throughputInMegaBit / 8.0);
		}
	    }
	    try
	    {
		throw new Exception("Throughputs between " + currentVmTypeName + " and " + reduceVmTypeName
			+ " could not be found in Cloud.yaml");
	    } catch (Exception e)
	    {
		e.printStackTrace();
	    }
	}
	return 0;
    }

    public DataSource getDataSource()
    {
	Cloud cloud = getCloud();

	DataSource selectedDataSource = null;
	for (DataSource dataSource : cloud.dataSources)
	{
	    if (dataSource.getName().equals(dataSourceName))
	    {
		selectedDataSource = dataSource;
		break;
	    }
	}
	return selectedDataSource;
    }

    public double dataTransferCostFromTheDataSource()
    {

	DataSource selectedDataSource = getDataSource();
	if (selectedDataSource == null)
	    try
	    {
		throw new Exception("Could not find " + selectedDataSource + " in the cloud");
	    } catch (Exception e)
	    {
		e.printStackTrace();
		return 0.0;
	    }

	// We divide by 1,000,000 because the cost per terabyte
	return selectedDataSource.cost * (dSize / 1000000.0);

    }

    public double dataTransferCostToAllReducers()
    {
	return dataTransferCostToAllReducers(getCurrentVmInstance());
    }

    /**
     * Calculate the data-out cost
     * 
     * @return The real data transfer cost
     */
    public double dataTransferCostToAllReducers(VmInstance vm)
    {
	int totalIntermediateDataSize = 0;
	for (Integer intermediateDataSize : intermediateData.values())
	{
	    totalIntermediateDataSize += intermediateDataSize;
	}

	return vm.transferringCost * (double) totalIntermediateDataSize;
    }

    public String getTaskType()
    {
	return "Map";
    }
}
