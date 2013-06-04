package org.cloudbus.cloudsim.ex.mapreduce.policy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ex.mapreduce.PredictionEngine;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.MapTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.ReduceTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Task;
import org.cloudbus.cloudsim.ex.mapreduce.policy.Backtracking.BacktrackingType;
import org.cloudbus.cloudsim.ex.util.CustomLog;

public class BTMultiCost extends Policy {

    public Boolean runAlgorithm(Cloud cloud, Request request) {
	Backtracking backtracking = new Backtracking();
	return backtracking.runAlgorithm(cloud, request, 5, true, 2 * 60 * 1000, 3 * 60 * 1000,BacktrackingType.Full);
    }
}
