package org.cloudbus.cloudsim.mapreduce;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

/**
 * This class handles the whole process of workflow
 * execution, including: parsing xml file, defining
 * number, types, and start times of vms, and scheduling,
 * dispatching, and management of DAG tasks.
 * 
 */
public class MapReduceEngine extends SimEntity{

	private static final int DELAY_START_EVENT = 998877;
	private int datacenterId;
	private long seed;
	
	private double actualStartTime = 0.0;
	private double endTime;
	private String dagFile;
	private long execTime;
	private long baseMIPS;
	private Policy policy;
	private VMOffers vmOffers;
	
	private HashMap<Integer,Boolean> freeVmList;
	private HashMap<Integer,Boolean> runningVmList; 
	private HashMap<Cloudlet,Task> cloudletTaskMap;
	private Hashtable<Integer,HashSet<Integer>> dataRequiredLocation;
	private Hashtable<Integer,ArrayList<Task>> schedulingTable;
	private Hashtable<Integer,Vm> vmTable;
	private HashSet<WETransmission> pendingTransmissions;
	private List<? extends Cloudlet> cloudletReceivedList;

	public MapReduceEngine(String dagFile, long execTime, long baseMIPS, Policy policy, VMOffers vmOffers, long seed) {
		super("WorkflowEngine");
		setCloudletReceivedList(new ArrayList<Cloudlet>());
		this.dagFile = dagFile;
		this.execTime = execTime;
		this.baseMIPS = baseMIPS;
		this.policy = policy;
		this.vmOffers = vmOffers;
		this.seed = seed;
		
		this.freeVmList = new HashMap<Integer,Boolean>();
		this.runningVmList = new HashMap<Integer,Boolean>();
		this.cloudletTaskMap = new HashMap<Cloudlet,Task>();
		this.vmTable = new Hashtable<Integer,Vm>();
		pendingTransmissions = new HashSet<WETransmission>();
	}

	@Override
	public void processEvent(SimEvent ev) {
		if (ev == null){
			Log.printLine("Warning: "+CloudSim.clock()+": "+this.getName()+": Null event ignored.");
		} else {
			int tag = ev.getTag();
			switch(tag){
				case CloudSimTags.RESOURCE_CHARACTERISTICS: doProvisioning(); break;
				case CloudSimTags.VM_CREATE_ACK: processVmCreate(ev); break;
				case CloudSimTags.CLOUDLET_RETURN: processCloudletReturn(ev); break;
				case CloudSimTags.CLOUDLET_CANCEL: processCloudletCancel(ev); break;
				case MapReduceDatacenter.DATA_ITEM_AVAILABLE: processDataItemAvailable(ev); break;
				case DELAY_START_EVENT:	processStartDelayEvent(); break;
				case CloudSimTags.END_OF_SIMULATION: break;
				default: Log.printLine("Warning: "+CloudSim.clock()+": "+this.getName()+": Unknown event ignored. Tag: "+tag);
			}
		}
	}

	// all the simulation entities are ready: start operation
	protected void doProvisioning() {
		Log.printLine(CloudSim.clock()+": Workflow execution started.");
		actualStartTime = CloudSim.clock();
		
		//runs the policy
		policy.processDagFile(dagFile,this.getId(),execTime, baseMIPS, vmOffers,seed);
		dataRequiredLocation = policy.getDataRequiredLocation();
		schedulingTable = policy.getScheduling();
		ArrayList<ProvisionedVm> vms = policy.getProvisioning();
		
		//trigger creation of vms
		for (ProvisionedVm pvm: vms){
			Vm vm = pvm.getVm();

			freeVmList.put(vm.getId(), false);
			runningVmList.put(vm.getId(), false);
			vmTable.put(vm.getId(), vm);
			send(datacenterId,actualStartTime+pvm.getStartTime(),CloudSimTags.VM_CREATE_ACK,vm);
		}		
	}

	protected void processVmCreate(SimEvent ev) {
		int[] data = (int[]) ev.getData();
		int vmId = data[1];
		
		freeVmList.put(vmId,true);
		runningVmList.put(vmId, true);
		
		if (hasPendingTransmissions(vmId)){
			for(Task t: getTransmissionsDestinatedTo(vmId)){
				moveData(t,vmId);
			}
		}
		
		dispatchTasks();
	}
	


	protected void processCloudletReturn(SimEvent ev) {
		Cloudlet cloudlet = (Cloudlet) ev.getData();
		getCloudletReceivedList().add(cloudlet);
		Log.printLine(CloudSim.clock()+": Task "+cloudlet.getCloudletId()+" finished.");
		
		Task task = cloudletTaskMap.remove(cloudlet);
		freeVmList.put(task.getVmId(), true);
		task.hasFinished();
		
		if (task.hasReplicas()){
			for (Task replica:task.getReplicas()){
				int data[] = new int[3];
				data[0] = replica.getId();
				data[1] = this.getId();
				data[2] = replica.getVmId();
				
				//if the replica is running, request its cancelation
				if (replica.getCloudlet().getCloudletStatus()==Cloudlet.INEXEC){
					Log.printLine(CloudSim.clock()+": Requesting cancelation of Task #"+replica.getId());
					sendNow(datacenterId,CloudSimTags.CLOUDLET_CANCEL,data);
				} else {
					//don't send it for execution
					try { replica.getCloudlet().setCloudletStatus(Cloudlet.CANCELED);} catch (Exception e) {}
				}
			}
		}
		
		moveData(task);		
		dispatchTasks();
	}
	
	private void processCloudletCancel(SimEvent ev) {
		Cloudlet cloudlet = (Cloudlet) ev.getData();
		
		if (cloudlet!=null){
			getCloudletReceivedList().add(cloudlet);
			Log.printLine(CloudSim.clock()+": Task "+cloudlet.getCloudletId()+" cancelled.");
				
			Task task = cloudletTaskMap.remove(cloudlet);
			freeVmList.put(task.getVmId(), true);
		}
		
		dispatchTasks();
	}
	
	private void processDataItemAvailable(SimEvent ev) {
		Transmission tr = (Transmission) ev.getData();
		Log.printLine(CloudSim.clock()+": DataItem #"+tr.getDataItem().getId()+" is now available at VM #"+tr.getDestinationId());
		removeOngoingTransmission(tr.getSourceId(),tr.getDestinationId(),tr.getDataItem().getId());
		
		dispatchTasks();
	}
	
	private void processStartDelayEvent() {
		// we gave data center enough time to initialize. Start the action...
		datacenterId = CloudSim.getCloudResourceList().get(0);
		sendNow(datacenterId, CloudSimTags.RESOURCE_CHARACTERISTICS, getId());
	}

	@Override
	public void shutdownEntity() {
		//do nothing
	}

	@Override
	public void startEntity() {
		//give time to the data center to start
		send(getId(),2,DELAY_START_EVENT);
	}
	
	//Triggers the process of copying data necessary in other vms
	private void moveData(Task task) {
		int originVm = task.getVmId();
		
		for(DataItem data:task.getOutput()){
			for (int destVm: dataRequiredLocation.get(data.getId())){
				WETransmission tr = new WETransmission(task, originVm, destVm,data.getId());
							
				if (!data.isAvailableAt(destVm)){//if origin != destiny and data is not in destiny, trigger transfer
					Log.printLine(CloudSim.clock()+": Transferring dataItem #"+data.getId()+" from VM #"+originVm+" to VM #"+destVm);
					
					pendingTransmissions.add(tr);
					if (runningVmList.containsKey(destVm) && runningVmList.get(destVm)==true){
						//if vm was not created, delay transmission
						TransferDataEvent event = new TransferDataEvent(data,originVm,destVm);
						sendNow(datacenterId,MapReduceDatacenter.TRANSFER_DATA_ITEM,event);
					}
				} else {
					//Log.printLine(CloudSim.clock()+": DataItem #"+data.getId()+" is already available at VM #"+destVm);
				}
			}
		}
	}
	
	//Triggers the process of copying data necessary to a delayed vm
	private void moveData(Task task, int vmId) {
		int originVm = task.getVmId();
		
		for(DataItem data:task.getOutput()){
			for (int destVm: dataRequiredLocation.get(data.getId())){
				if (destVm==vmId && !data.isAvailableAt(destVm)){
					Log.printLine(CloudSim.clock()+": Transferring dataItem #"+data.getId()+" from VM #"+originVm+" to VM #"+destVm);
					TransferDataEvent event = new TransferDataEvent(data,originVm,destVm);
					sendNow(datacenterId,MapReduceDatacenter.TRANSFER_DATA_ITEM,event);
				}
			}
		}
	}
	
	private void dispatchTasks() {
		//checks which VMs are free and schedules tasks to them
		ArrayList<Integer> finishedVms = new ArrayList<Integer>();
		
		for(int vmId:schedulingTable.keySet()){
			if (freeVmList.get(vmId)==true){//this vm is available
				//first, checks if this VM is still in use
				if(schedulingTable.get(vmId).isEmpty()){//no more cloudlets in this vm
					//check for pending communication
					if (!hasPendingTransmissions(vmId)) finishedVms.add(vmId);
				} else {
					//get the next task schedule for this VM
					Task task = schedulingTable.get(vmId).get(0);
					
					while (task.getCloudlet().getCloudletStatus()==Cloudlet.CANCELED){
						//don't submit
						schedulingTable.get(vmId).remove(0);
						getCloudletReceivedList().add(task.getCloudlet());
						cloudletTaskMap.remove(task);
						
						if(schedulingTable.get(vmId).isEmpty()){
							if (!hasPendingTransmissions(vmId)) finishedVms.add(vmId);
							task=null;
							break;
						} else {
							task = schedulingTable.get(vmId).get(0);
						}
					}
					
					if (task!=null) {
						//are the data it needs already transferred?
						boolean dataDependenciesMet = true;
						for(DataItem data:task.getDataDependencies()){
							if(!data.isAvailableAt(vmId)){
								dataDependenciesMet=false;
								break;
							}
						}

						if(dataDependenciesMet){//required data is available in the VM
							Log.printLine(CloudSim.clock()+": Task "+task.getId()+" dispatched to VM#"+task.getVmId());
							schedulingTable.get(vmId).remove(0);//remove task from the scheduling table
							freeVmList.put(vmId, false); //vm is busy now
							cloudletTaskMap.put(task.getCloudlet(), task);
							sendNow(datacenterId,CloudSimTags.CLOUDLET_SUBMIT,task.getCloudlet());
						} else {//data is not there yet
							//Log.printLine(CloudSim.clock()+": Task "+task.getId()+" postponed at VM#"+task.getVmId()+".");
						}
					}
				}
			}
		}
		
		//remove VMs that are not in use anymore
		for(int id:finishedVms){
			Vm vm = vmTable.remove(id);
			sendNow(datacenterId,CloudSimTags.VM_DESTROY,vm);
			schedulingTable.remove(id);
			freeVmList.remove(vm);
			runningVmList.put(vm.getId(),false);
		}
		
		//check execution completion
		if (schedulingTable.isEmpty()){
			Log.printLine(CloudSim.clock()+": Workflow execution finished.");
			endTime = (long) CloudSim.clock();
		}
	}
	
	protected <T extends Cloudlet> void setCloudletReceivedList(List<T> cloudletReceivedList) {
		this.cloudletReceivedList = cloudletReceivedList;
	}

	@SuppressWarnings("unchecked")
	public <T extends Cloudlet> List<T> getCloudletReceivedList() {
		return (List<T>) cloudletReceivedList;
	}
		
	//Output information supplied at the end of the simulation
	public void printExecutionSummary() {
		DecimalFormat dft = new DecimalFormat("#####.##");
		String indent = "\t";
		
		Log.printLine("========== WORKFLOW EXECUTION SUMMARY ==========");
		Log.printLine("= Task " + indent + "Status" + indent + indent + "Start Time" + indent + "Execution Time (s)" + indent + "Finish Time");
		for (Cloudlet cloudlet: getCloudletReceivedList()) {
			Log.print(" = "+cloudlet.getCloudletId() + indent);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS){
				Log.print("SUCCESS");

				double executionTime = cloudlet.getFinishTime()-cloudlet.getExecStartTime();
				Log.printLine(indent + indent + dft.format(cloudlet.getSubmissionTime()) + indent + indent + dft.format(executionTime) + indent + indent + dft.format(cloudlet.getFinishTime()));
			} else if (cloudlet.getCloudletStatus() == Cloudlet.FAILED) {
				Log.printLine("FAILED");
			} else if (cloudlet.getCloudletStatus() == Cloudlet.CANCELED) {
				Log.printLine("CANCELLED");
			}
		}
		long deadline = (long) (actualStartTime + execTime);
		Log.printLine("= Deadline: "+deadline);
		Log.printLine("= Finish time: "+endTime);
		long makespan = (long) (endTime-actualStartTime);
		Log.printLine("= Makespan: "+ makespan);
		boolean violation= (endTime>deadline);
		Log.printLine("= Violation: "+ violation);
		Log.printLine("========== END OF SUMMARY =========");
		Log.printLine();
	}
	
	public boolean hasPendingTransmissions(int vmId){
		Iterator<WETransmission> iter = pendingTransmissions.iterator();
		while (iter.hasNext()){
			WETransmission tr = iter.next();
			if (tr.getOriginId()==vmId || tr.getDestId()==vmId) return true;
		}
		
		return false;
	}
	
	private HashSet<Task> getTransmissionsDestinatedTo(int vmId) {
		HashSet<Task> tasks = new HashSet<Task>();
		
		Iterator<WETransmission> iter = pendingTransmissions.iterator();
		while (iter.hasNext()){
			WETransmission tr = iter.next();
			if (tr.getDestId()==vmId) tasks.add(tr.getTask());
		}
		
		return tasks;
	}
	
	private void removeOngoingTransmission(int sourceId, int destinationId, int dataId) {
		LinkedList<WETransmission> toRemove = new LinkedList<WETransmission>();
		
		Iterator<WETransmission> iter = pendingTransmissions.iterator();
		while (iter.hasNext()){
			WETransmission tr = iter.next();
			if (tr.getOriginId()==sourceId && tr.getDestId()==destinationId && tr.getDataId()==dataId) {
				toRemove.add(tr);
			}
		}
		
		pendingTransmissions.removeAll(toRemove);
	}
}

class WETransmission {
	Task task;
	int originId;
	int destId;
	int dataId;

	public WETransmission(Task task, int originId, int destId, int dataId) {
		this.task = task;
		this.originId = originId;
		this.destId = destId;
		this.dataId = dataId;
	}

	public Task getTask() {
		return task;
	}

	public int getOriginId() {
		return originId;
	}

	public int getDestId() {
		return destId;
	}
	
	public int getDataId() {
		return dataId;
	}
	
	@Override
	public boolean equals(Object o){
		int oid = ((WETransmission)o).getOriginId();
		int dit = ((WETransmission)o).getDestId();
		int tit = ((WETransmission)o).getDataId();
		
		return (dataId==tit && originId==oid && destId==dit);
	}
	
	@Override
	public int hashCode(){
		return dataId*100000+originId*1000+destId;
	}
}