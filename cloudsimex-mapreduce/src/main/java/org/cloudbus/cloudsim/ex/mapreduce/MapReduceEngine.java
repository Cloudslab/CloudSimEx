package org.cloudbus.cloudsim.ex.mapreduce;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.ex.mapreduce.policy.*;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.*;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.*;
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

	@Override
	public void processEvent(SimEvent ev) {
		switch (ev.getTag()) {
		// Resource characteristics request
		case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
			processResourceCharacteristicsRequest(ev);
			break;
		// Resource characteristics answer
		case CloudSimTags.RESOURCE_CHARACTERISTICS:
			processResourceCharacteristics(ev);
			break;
		// create Vms In Datacenters for a request
		case CloudSimTags.VM_CREATE:
			createVmsInDatacenters(ev);
			break;
		// VM Creation answer
		case CloudSimTags.VM_CREATE_ACK:
			processVmCreate(ev);
			break;
		// A finished cloudlet returned
		case CloudSimTags.CLOUDLET_RETURN:
			processCloudletReturn(ev);
			break;
		// if the simulation finishes
		case CloudSimTags.END_OF_SIMULATION:
			shutdownEntity();
			break;
		// other unknown tags are processed by this method
		default:
			processOtherEvent(ev);
			break;
		}
	}

	protected void processResourceCharacteristics(SimEvent ev) {
		DatacenterCharacteristics characteristics = (DatacenterCharacteristics) ev.getData();
		getDatacenterCharacteristicsList().put(characteristics.getId(), characteristics);

		// If the last Datacenter
		if (getDatacenterCharacteristicsList().size() == getDatacenterIdsList().size()) {
			setDatacenterRequestedIdsList(new ArrayList<Integer>());
			// createVmsInDatacenter();
			// For each request send VM_CREATE to my self (the engine) with the
			// delay based on the Yaml
			for (Request request : requests.requests)
				send(getId(), request.submissionTime, CloudSimTags.VM_CREATE, request);
		}
	}

	protected void createVmsInDatacenters(SimEvent ev) {

		Request request = (Request) ev.getData();

		// ////
		String policyName = Properties.POLICY.getProperty();
		Policy policy = null;
		try {
			Class<?> policyClass = Class.forName(policyName, false, Policy.class.getClassLoader());
			policy = (Policy) policyClass.newInstance();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		List<VMType> provisioningVmList = runAlgorithm(policy, request);
		request.provisionedVms = provisioningVmList;
		// ////

		int requestedVms = 0;
		for (int datacenterId : getDatacenterIdsList()) {
			CloudDatacenter cloudDatacenter = cloud.getCloudDatacenterFromId(datacenterId);
			for (Vm vm : provisioningVmList) {
				if (cloudDatacenter.isVMInCloudDatacenter(vm.getId())) {
					Log.printLine(CloudSim.clock() + ": " + getName() + ": creating VM #" + vm.getId() + " in "
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

	private List<VMType> runAlgorithm(Policy policy, Request request) {
		// ToDo: increase the clock during the ALGORITHM search
		Log.printLine(" =========== ALGORITHM: SEARCHING START FOR REQUEST: " + request.id + " ===========");
		Log.printLine(getName() + " is searching for the optimal Resource Set...");
		List<VMType> provisioningVmList = policy.runAlgorithm(cloud, request);
		// Provision all types of virtual machines from Cloud
		Log.printLine(" =========== ALGORITHM: FINISHED SEARCHING FOR REQUEST: " + request.id + " ===========");
		Log.printLine(getName() + " SELECTED THE FOLLOWING VMs FOR REQUEST: " + request.id + " : ");
		// 1- provisioning
		submitVmList(provisioningVmList);
		for (VMType vmType : provisioningVmList) {
			Log.printLine("- " + vmType.name + " (ID: " + vmType.getId() + ")");
		}

		// 2- scheduling
		// Bind/Schedule Map and Reduce tasks to VMs based on the ResourceSet
		Map<Integer, Integer> schedulingPlan = policy.getSchedulingPlan();
		for (Cloudlet task : getCloudletList()) {
			int taskId = task.getCloudletId();
			if (schedulingPlan.containsKey(taskId)) {
				int vmId = schedulingPlan.get(taskId);
				bindCloudletToVm(taskId, vmId);
			}
		}

		return provisioningVmList;
	}

	protected void processVmCreate(SimEvent ev) {
		int[] data = (int[]) ev.getData();
		int datacenterId = data[0];
		int vmId = data[1];
		int result = data[2];
		// int requestId = data[3];

		// Request request = requests.getRequestFromId(requestId);

		if (result == CloudSimTags.TRUE) {
			getVmsToDatacentersMap().put(vmId, datacenterId);
			getVmsCreatedList().add(VmList.getById(getVmList(), vmId));
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId + " has been created in Datacenter #"
					+ datacenterId + ", Host #" + VmList.getById(getVmsCreatedList(), vmId).getHost().getId());
		} else {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": Creation of VM #" + vmId + " failed in Datacenter #"
					+ datacenterId);
		}

		incrementVmsAcks();

		// all the requested VMs have been created
		if (getVmsCreatedList().size() == getVmList().size() - getVmsDestroyed()) {
			submitCloudlets();
		} else {
			// all the acks received, but some VMs were not created
			if (getVmsRequested() == getVmsAcks()) {
				// find id of the next datacenter that has not been tried
				for (int nextDatacenterId : getDatacenterIdsList()) {
					if (!getDatacenterRequestedIdsList().contains(nextDatacenterId)) {
						createVmsInDatacenter(nextDatacenterId);
						return;
					}
				}

				// all datacenters already queried
				if (getVmsCreatedList().size() > 0) { // if some vm were created
					submitCloudlets();
				} else { // no vms created. abort
					Log.printLine(CloudSim.clock() + ": " + getName()
							+ ": none of the required VMs could be created. Aborting");
					finishExecution();
				}
			}
		}
	}

	protected void submitCloudlets() {
		int vmIndex = 0;
		for (Cloudlet cloudlet : getCloudletList()) {
			// if(!request.isTaskInThisRequest(cloudlet.getCloudletId()))
			// continue;
			if (cloudlet.getVmId() == -1)
				continue;
			Vm vm;
			// if user didn't bind this cloudlet and it has not been executed
			// yet
			if (cloudlet.getVmId() == -1) {
				vm = getVmsCreatedList().get(vmIndex);
			} else { // submit to the specific vm
				vm = VmList.getById(getVmsCreatedList(), cloudlet.getVmId());
				if (vm == null) { // vm was not created
					Log.printLine(CloudSim.clock() + ": " + getName() + ": Postponing execution of cloudlet "
							+ cloudlet.getCloudletId() + ": bount VM not available");
					continue;
				}
			}

			Log.printLine(CloudSim.clock() + ": " + getName() + ": Sending cloudlet " + cloudlet.getCloudletId()
					+ " to VM #" + vm.getId());
			cloudlet.setVmId(vm.getId());
			sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
			cloudletsSubmitted++;
			vmIndex = (vmIndex + 1) % getVmsCreatedList().size();
			getCloudletSubmittedList().add(cloudlet);
		}

		// remove submitted cloudlets from waiting list
		for (Cloudlet cloudlet : getCloudletSubmittedList()) {
			getCloudletList().remove(cloudlet);
		}
	}

	public List<VMType> getVMTypesFromIds(List<Integer> selectedVMIds) {
		List<VMType> selectedVMTypes = new ArrayList<VMType>();
		for (int vmId : selectedVMIds) {
			VMType vmType = cloud.getVMTypeFromId(vmId);
			if (!selectedVMTypes.contains(vmType))
				selectedVMTypes.add(vmType);
		}

		return selectedVMTypes;
	}

	// Output information supplied at the end of the simulation
	public void printExecutionSummary() {
		DecimalFormat dft = new DecimalFormat("000000.00");
		String indent = "\t";

		Log.printLine("========== MAPREDUCE EXECUTION SUMMARY ==========");
		Log.printLine("= Request " + indent + "Task " + indent + "Type" + indent + "Status" + indent + indent
				+ "Submission Time" + indent + "Start Time" + indent + "Execution Time (s)" + indent + "Finish Time"
				+ indent + "VM ID" + indent + "VM Type");
		for (Cloudlet cloudlet : getCloudletReceivedList()) {
			Task task = (Task) cloudlet;
			Log.print(" = " + task.requestId + indent + indent + task.getCloudletId() + indent);

			if (task instanceof MapTask)
				Log.print("Map");
			else if (task instanceof ReduceTask)
				Log.print("Reduce");
			else
				Log.print("OTHER!!!! WTF");

			if (task.getCloudletStatus() == Cloudlet.SUCCESS) {
				Log.print(indent + "SUCCESS");

				VMType vm = cloud.getVMTypeFromId(task.getVmId());

				double executionTime = task.getFinishTime() - task.getExecStartTime();
				Log.printLine(indent + indent + dft.format(task.getSubmissionTime()) + indent
						+ dft.format(task.getExecStartTime()) + indent + dft.format(executionTime) + indent + indent
						+ dft.format(task.getFinishTime()) + indent + vm.getId() + indent + vm.name);

				// Set the executionTime in the vm
				vm.ExecutionTime += executionTime;

				// set the last execution time on request
				Request request = requests.getRequestFromId(task.requestId);
				double requestExecutionTime = task.getFinishTime() - task.getSubmissionTime();
				if (request.lastTaskFinishExecutionTime < requestExecutionTime)
					request.lastTaskFinishExecutionTime = requestExecutionTime;

			} else if (task.getCloudletStatus() == Cloudlet.FAILED) {
				Log.printLine("FAILED");
			} else if (task.getCloudletStatus() == Cloudlet.CANCELED) {
				Log.printLine("CANCELLED");
			}
		}
		Log.printLine();
		for (Request request : requests.requests) {
			Log.printLine(" ======== Request ID: " + request.id + " - USER CLASS: [" + request.userClass + "]");
			Log.printLine("= VMs: ");

			double jobTotalCost = 0.0;
			for (VMType vmType : request.provisionedVms) {
				Log.printLine("   - VM ID#" + vmType.getId() + ": " + vmType.name);
				Log.printLine("     - Processing Time: " + vmType.ExecutionTime + " seconds");
				double cost = Math.ceil(vmType.ExecutionTime / 3600.0) * vmType.cost;
				Log.printLine("     - Cost: $" + cost);
				jobTotalCost += cost;
			}

			Log.printLine("= Deadline: " + request.deadline + " seconds");
			double jobExecutionTime = request.lastTaskFinishExecutionTime;
			Log.printLine("= Job Execution Time: " + jobExecutionTime + " seconds");
			boolean TimeViolation = (jobExecutionTime > request.deadline);
			Log.printLine("= Deadline Violation: " + TimeViolation);

			Log.printLine("= Budget: $" + request.budget);
			Log.printLine("= Job Total Cost: $" + jobTotalCost);
			boolean costViolation = (jobTotalCost > request.budget);
			Log.printLine("= Budget Violation: " + costViolation);
			Log.printLine();
		}
		Log.printLine("========== END OF SUMMARY =========");
		Log.printLine();
	}
}
