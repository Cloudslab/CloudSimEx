package org.cloudbus.cloudsim.workflow;

import java.util.Comparator;

/**
 * This class implements a comparator for determining replication priority of tasks.
 * Because priority is arranged in DECREASING order of importance, we are reversing
 * the results of the compare() method.
 */
public class ReverseReplicationImportanceComparator implements	Comparator<Task> {
	
	MyPolicy policy;
	
	public ReverseReplicationImportanceComparator(MyPolicy policy){
		this.policy = policy;
	}

	@Override
	public int compare(Task t1, Task t2) {
		
		//first criteria: lag time
		long lag1 = policy.getLft(t1) - (policy.getAst(t1) + policy.getMet(t1));
		long lag2 = policy.getLft(t2) - (policy.getAst(t2) + policy.getMet(t2));
		
		if (lag1>lag2) return -1;
		if (lag1<lag2) return 1;
		
		//second criteria: length
		if (t1.getCloudlet().getCloudletLength()>t2.getCloudlet().getCloudletLength()) return -1;
		if (t1.getCloudlet().getCloudletLength()<t2.getCloudlet().getCloudletLength()) return 1;
		
		//third criteria: outdegree
		if(t1.getChildren().size()>t2.getChildren().size()) return -1;
		if(t1.getChildren().size()<t2.getChildren().size()) return 1;
		
		return 0;
	}
}
