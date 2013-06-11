package org.cloudbus.cloudsim.ex.mapreduce.policy;

import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.policy.BB.BacktrackingType;

public class BBMultiCost extends Policy {

    @Override
    public Boolean runAlgorithm(Cloud cloud, Request request) {
	BB branchAndBound = new BB();
	return branchAndBound.runAlgorithm(cloud, request, 5, true, 2 * 60 * 1000, 3 * 60 * 1000,
		BacktrackingType.Standard);
    }
}
