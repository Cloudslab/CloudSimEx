package org.cloudbus.cloudsim.ex.mapreduce.policy;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.PrivateCloudDatacenter;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.PublicCloudDatacenter;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmType;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;

public abstract class Policy {

    public enum CloudDeploymentModel {
	Private, Public, Hybrid;
    }

    public abstract Boolean runAlgorithm(Cloud cloud, Request request);

    public static List<VmInstance> getAllVmInstances(Cloud cloud, Request request,
	    CloudDeploymentModel cloudDeploymentModel, int numberOfDuplicates)
    {
	List<VmInstance> nVMs = new ArrayList<VmInstance>();

	if (cloudDeploymentModel == CloudDeploymentModel.Public || cloudDeploymentModel == CloudDeploymentModel.Hybrid)
	{
	    for (PublicCloudDatacenter publicCloudDatacenter : cloud.publicCloudDatacenters) {
		for (VmType vmType : publicCloudDatacenter.vmTypes)
		    for (int i = 0; i < numberOfDuplicates; i++)
			nVMs.add(new VmInstance(vmType, request));

	    }
	}

	if (cloudDeploymentModel == CloudDeploymentModel.Private || cloudDeploymentModel == CloudDeploymentModel.Hybrid)
	{
	    for (PrivateCloudDatacenter privateCloudDatacenter : cloud.privateCloudDatacenters) {
		VmType firstVmType = privateCloudDatacenter.vmTypes.get(0);
		int maxAvailableResource = privateCloudDatacenter
			.getMaxAvailableResource(firstVmType, request.getUserClass());

		for (int i = 0; i < Math.min(numberOfDuplicates, maxAvailableResource); i++)
		    nVMs.add(new VmInstance(firstVmType, request));

	    }
	}

	return nVMs;
    }
}
