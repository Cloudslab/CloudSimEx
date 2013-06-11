package org.cloudbus.cloudsim.ex.mapreduce.policy;

import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.policy.BB.BacktrackingType;

public class BBDecision extends Policy {

    @Override
    public Boolean runAlgorithm(Cloud cloud, Request request) {
	BB branchAndBound = new BB();
	return branchAndBound.runAlgorithm(cloud, request, 1, false, 2 * 60 * 1000, 3 * 60 * 1000,
		BacktrackingType.Decision);
    }

}
