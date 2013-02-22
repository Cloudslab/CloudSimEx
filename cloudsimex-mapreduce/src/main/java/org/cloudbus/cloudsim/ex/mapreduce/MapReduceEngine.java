package org.cloudbus.cloudsim.ex.mapreduce;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.ex.mapreduce.models.ExecutionPlan;
import org.cloudbus.cloudsim.ex.mapreduce.models.PairTaskDatasource;
import org.cloudbus.cloudsim.ex.mapreduce.models.ResourceSet;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.CloudDatacenter;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VMType;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Requests;
import org.cloudbus.cloudsim.lists.VmList;

public class MapReduceEngine extends DatacenterBroker {

	public MapReduceEngine(String name) throws Exception {
		super(name);
		// TODO Auto-generated constructor stub
	}

	private Cloud cloud;

	public Cloud getCloud() {
		return cloud;
	}

	public void setCloud(Cloud cloud) {
		this.cloud = cloud;
	}

	private Requests requests;

	public Requests getRequests() {
		return requests;
	}

	public void setRequests(Requests requests) {
		this.requests = requests;

		for (Request request : requests.requests) {
			submitCloudletList(request.job.mapTasks);
			submitCloudletList(request.job.reduceTasks);
		}
	}

	protected void processResourceCharacteristics(SimEvent ev) {
		DatacenterCharacteristics characteristics = (DatacenterCharacteristics) ev
				.getData();
		getDatacenterCharacteristicsList().put(characteristics.getId(),
				characteristics);

		if (getDatacenterCharacteristicsList().size() == getDatacenterIdsList()
				.size()) {
			setDatacenterRequestedIdsList(new ArrayList<Integer>());
			createVmsInDatacenter();
		}
	}

	protected void createVmsInDatacenter(int datacenterId) {
	}

	protected void createVmsInDatacenter() {
		int requestedVms = 0;

		for (int datacenterId : getDatacenterIdsList()) {
			CloudDatacenter cloudDatacenter = cloud
					.getCloudDatacenter(datacenterId);
			for (Vm vm : getVmList()) {
				if (cloudDatacenter.isVMInCloudDatacenter(vm.getId())) {
					Log.printLine(CloudSim.clock() + ": " + getName()
							+ ": creating VM #" + vm.getId() + " in "
							+ cloudDatacenter.getName());
					sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, (Vm) vm);
					requestedVms++;
				}
			}
			getDatacenterRequestedIdsList().add(datacenterId);
		}

		setVmsRequested(requestedVms);
		setVmsAcks(0);

	}

	@Override
	public void startEntity() {
		// TODO Auto-generated method stub
		super.startEntity();

		// NEW CODE
		// Wrong place, it should be in a new event, and we should run it
		// several times based on the number of user requests.
		runAlgorithm();
		// END NEW CODE
	}

	private void runAlgorithm() {
		Log.printLine(" =========== ALGORITHM: SEARCHING START ===========");
		Log.printLine(getName()
				+ " is searching for the optimal Resource Set...");
		ResourceSet resourceSet = new ResourceSet(cloud, requests);
		// Provision all types of virtual machines from Cloud
		Log.printLine(" =========== ALGORITHM: FINISHED SEARCHING ===========");
		Log.printLine(getName() + " SELECTED THE FOLLOWING VMs: ");
		List<VMType> vmlist = resourceSet.getSelectedVMTypeIds();
		submitVmList(vmlist);
		for (VMType vmType : vmlist) {
			Log.printLine("- " + vmType.name + " (ID: " + vmType.getId() + ")");
		}
		// Bind/Schedule Map and Reduce tasks to VMs based on the ResourceSet
		List<ExecutionPlan> executionPlans = resourceSet.executionPlans;
		for (ExecutionPlan executionPlan : executionPlans)
			for (PairTaskDatasource pairTaskDatasources : executionPlan.taskSet.pairs)
				bindCloudletToVm(pairTaskDatasources.taskId,
						executionPlan.vmTypeId);

	}

	// Output information supplied at the end of the simulation
	public void printExecutionSummary() {
		DecimalFormat dft = new DecimalFormat("#####.##");
		String indent = "\t";

		Log.printLine("========== MAPREDUCE EXECUTION SUMMARY ==========");
		Log.printLine("= Task " + indent + "Status" + indent + indent
				+ "Start Time" + indent + "Execution Time (s)" + indent
				+ "Finish Time");
		for (Cloudlet cloudlet : getCloudletReceivedList()) {
			Log.print(" = " + cloudlet.getCloudletId() + indent);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
				Log.print("SUCCESS");

				double executionTime = cloudlet.getFinishTime()
						- cloudlet.getExecStartTime();
				Log.printLine(indent + indent
						+ dft.format(cloudlet.getSubmissionTime()) + indent
						+ indent + dft.format(executionTime) + indent + indent
						+ dft.format(cloudlet.getFinishTime()));
			} else if (cloudlet.getCloudletStatus() == Cloudlet.FAILED) {
				Log.printLine("FAILED");
			} else if (cloudlet.getCloudletStatus() == Cloudlet.CANCELED) {
				Log.printLine("CANCELLED");
			}
		}
		// WARNING: FIX THIS
		long deadline = 0;
		Log.printLine("= Deadline: " + deadline);
		// WARNING: FIX THIS
		Log.printLine("= Finish time: " + 0);
		long makespan = 0;
		Log.printLine("= Makespan: " + makespan);
		// WARNING: FIX THIS
		boolean violation = (0 > deadline);
		Log.printLine("= Violation: " + violation);
		Log.printLine("========== END OF SUMMARY =========");
		Log.printLine();
	}

}
