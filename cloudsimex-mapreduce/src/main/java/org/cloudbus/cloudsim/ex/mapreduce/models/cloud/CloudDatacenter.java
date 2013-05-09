package org.cloudbus.cloudsim.ex.mapreduce.models.cloud;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.ex.mapreduce.VmSchedulerSpaceSharedMapReduce;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.MapTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.ReduceTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Task;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

public class CloudDatacenter extends Datacenter {

    public List<VmType> vmTypes;

    public CloudDatacenter(String name, int hosts, int memory_perhost, int cores_perhost, int mips_precore_perhost,
	    List<VmType> vmtypes) throws Exception {
	super(name,
		createChars(hosts, cores_perhost, mips_precore_perhost, memory_perhost, 1000000),
		new VmAllocationPolicySimple(getHostList(hosts, cores_perhost, mips_precore_perhost, memory_perhost,
			1000000)),
		new LinkedList<Storage>(),
		0);

	this.vmTypes = vmtypes;
    }

    public CloudDatacenter(String name, int hosts, int memory_perhost, int cores_perhost, int mips_precore_perhost)
	    throws Exception {
	super(name,
		createChars(hosts, cores_perhost, mips_precore_perhost, memory_perhost, 1000000),
		new VmAllocationPolicySimple(getHostList(hosts, cores_perhost, mips_precore_perhost, memory_perhost,
			1000000)),
		new LinkedList<Storage>(),
		0);
    }

    private static DatacenterCharacteristics createChars(int hosts, int cores_perhost, int mips_precore_perhost,
	    int memory_perhost, int storage) {
	List<Host> hostList = getHostList(hosts, cores_perhost, mips_precore_perhost, memory_perhost, storage);

	DatacenterCharacteristics characteristics = new DatacenterCharacteristics("Xeon", "Linux", "Xen", hostList,
		10.0, 0.0, 0.00, 0.00, 0.00);

	return characteristics;
    }

    private static List<Host> getHostList(int hosts, int cores_perhost, int mips_precore_perhost, int memory_perhost,
	    int storage)
    {
	List<Host> hostList = new ArrayList<Host>();
	for (int i = 0; i < hosts; i++) {
	    List<Pe> peList = new ArrayList<Pe>();
	    for (int j = 0; j < cores_perhost; j++)
		peList.add(new Pe(j, new PeProvisionerSimple(mips_precore_perhost)));

	    hostList.add(new Host(i, new RamProvisionerSimple(memory_perhost), new BwProvisionerSimple(1000000),
		    storage, peList, new VmSchedulerSpaceSharedMapReduce(peList)));
	}

	return hostList;
    }

    protected void processCloudletSubmit(SimEvent ev, boolean ack) {
	updateCloudletProcessing();

	try {
	    // gets the Cloudlet object
	    Task cl = (Task) ev.getData();
	    if (cl instanceof ReduceTask)
		Log.printLine(CloudSim.clock() + ": " + getName() + ": Processing Reduce Task: " + cl.getCloudletId());
	    else
		Log.printLine(CloudSim.clock() + ": " + getName() + ": Processing Map Task: " + cl.getCloudletId());

	    // checks whether this Cloudlet has finished or not
	    if (cl.isFinished()) {
		String name = CloudSim.getEntityName(cl.getUserId());
		Log.printLine(getName() + ": Warning - Cloudlet #" + cl.getCloudletId() + " owned by " + name
			+ " is already completed/finished.");
		Log.printLine("Therefore, it is not being executed again");
		Log.printLine();

		// NOTE: If a Cloudlet has finished, then it won't be processed.
		// So, if ack is required, this method sends back a result.
		// If ack is not required, this method don't send back a result.
		// Hence, this might cause CloudSim to be hanged since waiting
		// for this Cloudlet back.
		if (ack) {
		    int[] data = new int[3];
		    data[0] = getId();
		    data[1] = cl.getCloudletId();
		    data[2] = CloudSimTags.FALSE;

		    // unique tag = operation tag
		    int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
		    sendNow(cl.getUserId(), tag, data);
		}

		sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);

		return;
	    }

	    // process this Cloudlet to this CloudResource
	    cl.setResourceParameter(getId(), getCharacteristics().getCostPerSecond(), getCharacteristics()
		    .getCostPerBw());

	    int userId = cl.getUserId();
	    int vmId = cl.getVmId();

	    Host host = getVmAllocationPolicy().getHost(vmId, userId);
	    Vm vm = host.getVm(vmId, userId);
	    CloudletScheduler scheduler = vm.getCloudletScheduler();
	    double estimatedFinishTime = scheduler.cloudletSubmit(cl);
	    if (cl instanceof MapTask)
	    {
		MapTask mapTask = (MapTask) cl;
		double total = mapTask.dataTransferTimeFromTheDataSource()
			+ mapTask.getTaskExecutionTimeIgnoringDataTransferTimeInSeconds()
			+ mapTask.dataTransferTimeToAllReducers();
		Log.printLine(CloudSim.clock() + ": " + getName()
			+ ": Estimated Total Execution Time for MAP task ID: " + cl.getCloudletId() + " is: "
			+ mapTask.dataTransferTimeFromTheDataSource() + "(D-in) + "
			+ mapTask.getTaskExecutionTimeIgnoringDataTransferTimeInSeconds() + "(ET) + "
			+ mapTask.dataTransferTimeToAllReducers() + "(D-out) = " + total
			+ " seconds | estimatedFinishTime (0.0 means in waiting queue): " + estimatedFinishTime);
	    }
	    else
		Log.printLine(CloudSim.clock() + ": " + getName()
			+ ": Estimated Total Execution Time for REDUCE task ID: " + cl.getCloudletId() + " is: "
			+ cl.getTaskExecutionTimeInSeconds() + "(ET) = " + estimatedFinishTime + " seconds");

	    // if this cloudlet is in the exec queue
	    if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
		send(getId(), estimatedFinishTime, CloudSimTags.VM_DATACENTER_EVENT);
	    }

	    if (ack) {
		int[] data = new int[3];
		data[0] = getId();
		data[1] = cl.getCloudletId();
		data[2] = CloudSimTags.TRUE;

		// unique tag = operation tag
		int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
		sendNow(cl.getUserId(), tag, data);
	    }
	} catch (ClassCastException c) {
	    Log.printLine(getName() + ".processCloudletSubmit(): " + "ClassCastException error.");
	    c.printStackTrace();
	} catch (Exception e) {
	    Log.printLine(getName() + ".processCloudletSubmit(): " + "Exception error.");
	    e.printStackTrace();
	}

	checkCloudletCompletion();
    }

    protected void processVmCreate(SimEvent ev, boolean ack) {
	Vm vm = (Vm) ev.getData();

	boolean result = getVmAllocationPolicy().allocateHostForVm(vm);

	if (ack) {
	    int[] data = new int[3];
	    data[0] = getId();
	    data[1] = vm.getId();

	    if (result) {
		data[2] = CloudSimTags.TRUE;
	    } else {
		data[2] = CloudSimTags.FALSE;
	    }
	    send(vm.getUserId(), 0.1, CloudSimTags.VM_CREATE_ACK, data);
	}

	if (result) {
	    getVmList().add(vm);

	    if (vm.isBeingInstantiated()) {
		vm.setBeingInstantiated(false);
	    }

	    vm.updateVmProcessing(CloudSim.clock(), getVmAllocationPolicy().getHost(vm).getVmScheduler()
		    .getAllocatedMipsForVm(vm));
	}

    }

    /**
     * Predict file transfer time.
     * 
     * @param cl
     *            the required files
     * @return the double
     */
    protected double predictFileTransferTime(Task cl) {
	double time = 0.0;

	if (cl instanceof MapTask)
	{
	    MapTask mapTask = (MapTask) cl;
	    time = mapTask.dataTransferTimeFromTheDataSource(); // D-in
	    time += mapTask.dataTransferTimeToAllReducers(); // D-out
	}

	return time;
    }

    public boolean isVMInCloudDatacenter(int vmTypeId)
    {
	for (VmType vmType : vmTypes) {
	    if (vmType.getId() == vmTypeId)
		return true;
	}
	return false;
    }

    /**
     * Updates processing of each cloudlet running in this PowerDatacenter. It
     * is necessary because Hosts and VirtualMachines are simple objects, not
     * entities. So, they don't receive events and updating cloudlets inside
     * them must be called from the outside.
     * 
     * @pre $none
     * @post $none
     */
    protected void updateCloudletProcessing() {
	// if some time passed since last processing
	// R: for term is to allow loop at simulation start. Otherwise, one
	// initial
	// simulation step is skipped and schedulers are not properly
	// initialized
	if (CloudSim.clock() < 0.111 || CloudSim.clock() > getLastProcessTime() + 0.1) {
	    List<? extends Host> list = getVmAllocationPolicy().getHostList();
	    double smallerTime = Double.MAX_VALUE;
	    // for each host...
	    for (int i = 0; i < list.size(); i++) {
		Host host = list.get(i);
		// inform VMs to update processing
		double time = host.updateVmsProcessing(CloudSim.clock());
		// what time do we expect that the next cloudlet will finish?
		if (time < smallerTime) {
		    smallerTime = time;
		}
	    }
	    // gurantees a minimal interval before scheduling the event
	    if (smallerTime < CloudSim.clock() + 0.11) {
		smallerTime = CloudSim.clock() + 0.11;
	    }
	    if (smallerTime != Double.MAX_VALUE) {
		schedule(getId(), (smallerTime - CloudSim.clock()), CloudSimTags.VM_DATACENTER_EVENT);
	    }
	    setLastProcessTime(CloudSim.clock());
	}
    }

}
