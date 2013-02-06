package org.cloudbus.cloudsim.incubator.web;

import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.incubator.disk.HddCloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.incubator.disk.HddHost;
import org.cloudbus.cloudsim.incubator.disk.HddResCloudlet;
import org.cloudbus.cloudsim.incubator.disk.HddVm;
import org.cloudbus.cloudsim.incubator.util.CustomLog;

/**
 * 
 * A data center capable of executing web applications. The main difference
 * between it and a {@link Datacenter} is that it marks VMs as disabled if their
 * RAM consumption is exceeded.
 * 
 * @author nikolay.grozev
 * 
 */
public class WebDataCenter extends Datacenter {

    /**
     * Constr.
     * 
     * @param name
     * @param characteristics
     * @param vmAllocationPolicy
     * @param storageList
     * @param schedulingInterval
     * @throws Exception
     */
    public WebDataCenter(final String name, final DatacenterCharacteristics characteristics,
	    final VmAllocationPolicy vmAllocationPolicy,
	    final List<Storage> storageList, final double schedulingInterval) throws Exception {
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
    protected void processCloudletSubmit(final SimEvent ev, final boolean ack) {
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
		    scheduler.addFailedCloudlet(cl);
		    vm.setOutOfMemory(true);

		    CustomLog
			    .printf("VM/Server %d on host %d in data center %s(%d) is out of memory. It will not be further available",
				    vm.getId(), host.getId(), getName(), getId());
		} else {
		    super.processCloudletSubmit(ev, ack);
		}
	    } else {
		scheduler.addFailedCloudlet(cl);
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    @Override
    protected void checkCloudletCompletion() {
	super.checkCloudletCompletion();

	for (Host host : getVmAllocationPolicy().getHostList()) {
	    for (HddVm vm : ((HddHost) host).getVmList()) {
		while (vm.getCloudletScheduler().isFailedCloudlets()) {
		    Cloudlet cl = vm.getCloudletScheduler().getNextFailedCloudlet();
		    if (cl != null) {
			sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);
		    }
		}
	    }
	}

    }

}
