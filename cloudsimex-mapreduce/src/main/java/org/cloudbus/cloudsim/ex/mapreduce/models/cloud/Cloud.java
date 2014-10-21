package org.cloudbus.cloudsim.ex.mapreduce.models.cloud;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.ex.mapreduce.models.request.UserClass;

public class Cloud {

    public List<PublicCloudDatacenter> publicCloudDatacenters;
    public List<PrivateCloudDatacenter> privateCloudDatacenters;
    public List<DataSource> dataSources;
    public List<List<?>> throughputs_vm_vm;
    public List<List<?>> throughputs_ds_vm;

    public static int brokerID = -1;

    public List<VmType> getAllVMTypes()
    {

	List<VmType> vmlist = new ArrayList<VmType>();
	for (PublicCloudDatacenter publicCloudDatacenter : publicCloudDatacenters) {
	    vmlist.addAll(publicCloudDatacenter.vmTypes);
	}

	for (PrivateCloudDatacenter privateCloudDatacenter : privateCloudDatacenters) {
	    vmlist.addAll(privateCloudDatacenter.vmTypes);
	}

	return vmlist;
    }

    public VmType getVMTypeFromId(int VMTypeId)
    {
	for (PublicCloudDatacenter publicCloudDatacenter : publicCloudDatacenters) {
	    for (VmType vmType : publicCloudDatacenter.vmTypes) {
		if (vmType.getId() == VMTypeId)
		    return vmType;
	    }
	}

	for (PrivateCloudDatacenter privateCloudDatacenter : privateCloudDatacenters) {
	    for (VmType vmType : privateCloudDatacenter.vmTypes) {
		if (vmType.getId() == VMTypeId)
		    return vmType;
	    }
	}

	return null;
    }

    public CloudDatacenter getCloudDatacenterFromId(int cloudDatacenterId)
    {
	for (PublicCloudDatacenter publicCloudDatacenter : publicCloudDatacenters) {
	    if (publicCloudDatacenter.getId() == cloudDatacenterId)
		return publicCloudDatacenter;
	}

	for (PrivateCloudDatacenter privateCloudDatacenter : privateCloudDatacenters) {
	    if (privateCloudDatacenter.getId() == cloudDatacenterId)
		return privateCloudDatacenter;
	}

	return null;
    }

    public DataSource getDataSourceFromName(String dataSourceName)
    {
	for (DataSource dataSource : dataSources) {
	    if (dataSource.getName().equals(dataSourceName))
		return dataSource;
	}
	return null;
    }

    public void setUserClassAllowedPercentage(Map<UserClass, Double> userClassAllowedPercentage)
    {
	for (PrivateCloudDatacenter privateCloudDatacenter : privateCloudDatacenters)
	{
	    privateCloudDatacenter.setUserClassesReservationPercentage(userClassAllowedPercentage);
	}

    }
}
