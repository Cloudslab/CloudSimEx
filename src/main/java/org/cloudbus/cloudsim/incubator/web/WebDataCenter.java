package org.cloudbus.cloudsim.incubator.web;

import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.incubator.util.CustomLog;
import org.cloudbus.cloudsim.incubator.web.extensions.HddCloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.incubator.web.extensions.HddHost;
import org.cloudbus.cloudsim.incubator.web.extensions.HddResCloudlet;
import org.cloudbus.cloudsim.incubator.web.extensions.HddVm;

/**
 * 
 * 
 * 
 * @author nikolay.grozev
 * 
 */
public class WebDataCenter extends Datacenter {

    /**
     * 
     * @param name
     * @param characteristics
     * @param vmAllocationPolicy
     * @param storageList
     * @param schedulingInterval
     * @throws Exception
     */
    public WebDataCenter(String name, DatacenterCharacteristics characteristics, VmAllocationPolicy vmAllocationPolicy,
	    List<Storage> storageList, double schedulingInterval) throws Exception {
	super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.cloudbus.cloudsim.Datacenter#processCloudletSubmit(org.cloudbus.cloudsim
     * .core.SimEvent, boolean)
     */
    @Override
    protected void processCloudletSubmit(SimEvent ev, boolean ack) {
	try {
	    WebCloudlet cl = (WebCloudlet) ev.getData();

	    int userId = cl.getUserId();
	    int vmId = cl.getVmId();

	    HddHost host = (HddHost) getVmAllocationPolicy().getHost(vmId, userId);
	    HddVm vm = (HddVm) host.getVm(vmId, userId);
	    HddCloudletSchedulerTimeShared scheduler = vm.getCloudletScheduler();

	    if (!vm.isOutOfMemory()) {
		List<HddResCloudlet> resCloudLets = scheduler.getCloudletExecList();

		int vmUsedRam = 0;
		for (HddResCloudlet res : resCloudLets) {
		    vmUsedRam += res.getCloudlet().getRam();
		}

		// If we have used all of the resources of this VM
		if (vmUsedRam + cl.getRam() > vm.getRam()) {
		    scheduler.failAllCloudlets();
		    cl.setCloudletStatus(Cloudlet.FAILED);
		    vm.setOutOfMemory(true);

		    CustomLog.printf(
			    "VM/Server %d on host %d in data center %s(%d) is out of memory. It will not be further available",
			    vm.getId(), host.getId(), getName(), getId() );
		} else {
		    super.processCloudletSubmit(ev, ack);
		}
	    } else {
		cl.setCloudletStatus(Cloudlet.FAILED);
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }
}
