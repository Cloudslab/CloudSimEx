package org.cloudbus.cloudsim.ex.mapreduce.policy;

import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.policy.BruteForce.BruteForceSorts;

public class BruteForceCost extends Policy {

    @Override
    public Boolean runAlgorithm(Cloud cloud, Request request) {
	BruteForce BruteForce = new BruteForce();
	return BruteForce.runAlgorithm(cloud, request, BruteForceSorts.Cost);
    }
}
