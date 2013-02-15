package org.cloudbus.cloudsim.workflow;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;

/**
 * This class encapsulates a DAG Task. The task contains the actual
 * Cloudlet to be executed, information about its dependencies
 * (data and execution), and scheduling information.
 * 
 */
public class Task {
	Cloudlet cloudlet;
	
	List<Task> parents;
	List<Task> replicas;
	List<Task> children;
	List<DataItem> dataDependencies;
	List<DataItem> output;
	HashSet<Task> antecessors;
	HashSet<Task> successors;
	
	public Task(Cloudlet cl, int ownerId){
		this.cloudlet = cl;
		if (cl!=null) this.cloudlet.setUserId(ownerId);
		parents = new LinkedList<Task>();
		replicas = new LinkedList<Task>();
		children = new LinkedList<Task>();
		dataDependencies = new LinkedList<DataItem>();
		output = new LinkedList<DataItem>();
		successors = new HashSet<Task>();
		antecessors = new HashSet<Task>();
	}
	
	public int getId(){
		if (cloudlet==null) return -1; 
		return cloudlet.getCloudletId();
	}
	
	public void addParent(Task parent){
		parents.add(parent);
	}
	
	public List<Task> getParents(){
		return parents;
	}
	
	public void addChild(Task parent){
		children.add(parent);
	}
	
	public List<Task> getChildren(){
		return children;
	}
	
	
	public HashSet<Task> getAntecessors() {
		return antecessors;
	}

	public void addAntecessors(HashSet<Task> newAntecessors) {
		this.antecessors.addAll(newAntecessors);
	}
	
	public void addAntecessors(Task task) {
		this.antecessors.add(task);
	}

	public HashSet<Task> getSuccessors() {
		return successors;
	}
	
	public void addSuccessors(HashSet<Task> newSuccessors) {
		this.successors.addAll(newSuccessors);
	}
	
	public void addSuccessors(Task task) {
		this.successors.add(task);
	}
	
	public void addDataDependency(DataItem data){
		dataDependencies.add(data);
	}
	
	public List<DataItem> getDataDependencies(){
		return dataDependencies;
	}
	
	public Cloudlet getCloudlet(){
		return cloudlet;
	}
	
	public void addOutput(DataItem data){
		output.add(data);
	}
	
	public List<DataItem> getOutput(){
		return output;
	}
		
	public void setVmId(int vmId){
		if(cloudlet!=null) cloudlet.setVmId(vmId);
	}
	
	public int getVmId(){
		if(cloudlet==null) return -1;
		return cloudlet.getVmId();
	}
	
	public boolean isReady(){	
		//while all parents are not done, it can't run
		for(Task parent:parents){
			if (!parent.isFinished()) return false;
		}
				
		return true;
	}
	
	public boolean hasReplicas(){
		return (replicas.size()>0);
	}
	
	public void addReplica(Task task){
		replicas.add(task);
	}
	
	public List<Task> getReplicas(){
		return replicas;
	}
	
	/**
	 * Communicates to the Task that its Cloudlet completed.
	 * It means that the dataItem is now available in the VM
	 * where it run.
	 *
	 */
	public void hasFinished(){
		if(cloudlet!=null){
			for (DataItem data:output){
				data.addLocation(cloudlet.getVmId());
			}
		}
	}
	
	public boolean isFinished(){
		if(cloudlet==null) return true;
		return cloudlet.isFinished();
	}
}
