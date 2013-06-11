package org.cloudbus.cloudsim.ex.mapreduce.policy;

import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.policy.BT.BruteForceSorts;

public class BTPerf extends Policy
{

    @Override
    public Boolean runAlgorithm(Cloud cloud, Request request)
    {
	BT backTracking = new BT();
	return backTracking.runAlgorithm(cloud, request, BruteForceSorts.Performance);
    }
}
