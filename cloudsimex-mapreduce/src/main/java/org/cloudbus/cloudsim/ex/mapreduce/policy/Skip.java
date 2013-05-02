package org.cloudbus.cloudsim.ex.mapreduce.policy;

import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;

public class Skip extends Policy {

	@Override
	public Boolean runAlgorithm(Cloud cloud, Request request) {
		//This policy is to skip the algorithm and use the provisioning and scheduling plans in the request.
		return true;
	}


}
