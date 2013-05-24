package org.cloudbus.cloudsim.ex.mapreduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Requests;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.UserClass;
import org.cloudbus.cloudsim.ex.mapreduce.policy.Policy.CloudDeploymentModel;

public class Workload
{
    String policy;
    CloudDeploymentModel cloudDeploymentModel = CloudDeploymentModel.Hybrid;
    Map<UserClass, Double> userClassAllowedPercentage = new HashMap<UserClass, Double>();
    Requests requests;

    public Workload(String policy, String cloudDeploymentModel, Map<String, Double> userClassAllowedPercentage,
	    ArrayList<Request> requests)
    {
	this.policy = policy;

	try
	{
	    this.cloudDeploymentModel = CloudDeploymentModel.valueOf(cloudDeploymentModel);
	} catch (Exception ex)
	{
	    Log.printLine(ex.getMessage());
	    Log.printLine("CloudDeploymentModel.Hybrid will be used");
	}

	for (Map.Entry<String, Double> userClassMap : userClassAllowedPercentage.entrySet()) {
	    UserClass userClass = UserClass.valueOf(userClassMap.getKey());
	    this.userClassAllowedPercentage.put(userClass, userClassMap.getValue());
	}
	this.requests = new Requests(requests);

	for (Request request : this.requests.requests)
	{
	    request.policy = policy;
	    request.setCloudDeploymentModel(CloudDeploymentModel.valueOf(cloudDeploymentModel));
	}
    }
}
