package org.cloudbus.cloudsim.ex.mapreduce;

import java.util.*;

import org.cloudbus.cloudsim.ex.mapreduce.models.request.*;

public class Experiment
{
	String policy;
	Map<UserClass, Double> userClassesReservationPercentage = new HashMap<UserClass, Double>();
	Requests requests;
	
	public Experiment(String policy, Map<String, Double> userClassesReservationPercentage, ArrayList<Request> requests)
	{
		this.policy = policy;
		for (Map.Entry<String, Double> userClassMap : userClassesReservationPercentage.entrySet()) {
			UserClass userClass = UserClass.valueOf(userClassMap.getKey());
			this.userClassesReservationPercentage.put(userClass, userClassMap.getValue());
		}
		this.requests = new Requests(requests);
		
		for (Request request : this.requests.requests)
			request.policy = policy;
	}
}
