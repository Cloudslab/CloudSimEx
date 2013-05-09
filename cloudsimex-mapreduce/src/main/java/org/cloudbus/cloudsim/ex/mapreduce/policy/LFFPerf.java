package org.cloudbus.cloudsim.ex.mapreduce.policy;

import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.policy.LFF.LFFSorts;

/*
 * Problems with Hwang and Kim 2012 approach:
 * 1- Static VMs (pre-provisioned VMs)
 * 2- Public Cloud (infinite resources) is not supported
 * 3- Support only one data source, replicas are not supported
 * 4- They calculate the data transfer time from map to reduce in the reduce phase, which is wrong, it has to be in the map phase
 * 
 * All in all, the user has to select and provision the VMs to select from.
 * 
 * Work around: The VPList will have:
 * 	- numTasks number of each type of VM from public cloud.
 *  - Max number of VMs that the data center can provision from the first type of private cloud.
 *  - The first datasource is the selected one
 *  
 * Assumptions:
 * 	- One type of VM in private cloud - by me.
 *  - Number of reduces smaller than the number of maps - by Hwang and Kim.
 */
public class LFFPerf extends Policy {

    @Override
    public Boolean runAlgorithm(Cloud cloud, Request request) {
	LFF lFF = new LFF();
	return lFF.runAlgorithm(cloud, request, LFFSorts.Performance);
    }
}
