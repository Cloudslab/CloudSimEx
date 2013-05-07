package org.cloudbus.cloudsim.ex.mapreduce;

import java.util.*;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.*;

public class Experiment
{
	String policy;
	Requests requests;
	
	public Experiment(String policy, ArrayList<Request> requests)
	{
		this.policy = policy;
		this.requests = new Requests(requests);
		
		for (Request request : this.requests.requests)
			request.policy = policy;
	}
}
