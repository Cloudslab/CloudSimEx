package org.cloudbus.cloudsim.ex.mapreduce.policy;

import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.policy.Backtracking.BacktrackingSorts;

public class BacktrackingCost extends Policy {

    @Override
    public Boolean runAlgorithm(Cloud cloud, Request request) {
	Backtracking backtracking = new Backtracking();
	return backtracking.runAlgorithm(cloud, request, BacktrackingSorts.Cost);
    }
}
