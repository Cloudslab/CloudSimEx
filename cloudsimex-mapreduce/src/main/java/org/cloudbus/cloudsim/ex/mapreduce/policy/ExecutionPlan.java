package org.cloudbus.cloudsim.ex.mapreduce.policy;

import java.util.HashMap;
import java.util.Map;

public class ExecutionPlan {
	public Map<Integer, Integer> one_nPrSchedulingPlan = new HashMap<Integer, Integer>();; //<Task ID, VM ID>
	public double ExecutionTime = 0;
	public double Cost = 0;
}
