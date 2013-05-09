package org.cloudbus.cloudsim.ex.mapreduce;

import java.util.*;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.*;
import org.cloudbus.cloudsim.ex.mapreduce.policy.Policy.CloudDeploymentModel;

public class Experiment
{
	String policy;
	CloudDeploymentModel cloudDeploymentModel = CloudDeploymentModel.Hybrid;
	Map<UserClass, Double> userClassesReservationPercentage = new HashMap<UserClass, Double>();
	Requests requests;
	
	public Experiment(String policy, String cloudDeploymentModel, Map<String, Double> userClassesReservationPercentage, ArrayList<Request> requests)
	{
		this.policy = policy;
		
		try
		{
			this.cloudDeploymentModel = CloudDeploymentModel.valueOf(cloudDeploymentModel);
		}
		catch(Exception ex)
		{
			Log.printLine(ex.getMessage());
			Log.printLine("CloudDeploymentModel.Hybrid will be used");
		}
		
		for (Map.Entry<String, Double> userClassMap : userClassesReservationPercentage.entrySet()) {
			UserClass userClass = UserClass.valueOf(userClassMap.getKey());
			this.userClassesReservationPercentage.put(userClass, userClassMap.getValue());
		}
		this.requests = new Requests(requests);
		
		for (Request request : this.requests.requests)
		{
			request.policy = policy;
			request.setCloudDeploymentModel(CloudDeploymentModel.valueOf(cloudDeploymentModel));
		}
	}
}
