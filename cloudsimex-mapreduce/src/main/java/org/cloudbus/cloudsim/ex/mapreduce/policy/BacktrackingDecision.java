package org.cloudbus.cloudsim.ex.mapreduce.policy;


import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.policy.Backtracking.BacktrackingType;

public class BacktrackingDecision extends Policy {


    @Override
    public Boolean runAlgorithm(Cloud cloud, Request request) {
	Backtracking backtracking = new Backtracking();
	return backtracking.runAlgorithm(cloud, request, 1, false, 2 * 60 * 1000, 3 * 60 * 1000,BacktrackingType.Decision);
    }

}
