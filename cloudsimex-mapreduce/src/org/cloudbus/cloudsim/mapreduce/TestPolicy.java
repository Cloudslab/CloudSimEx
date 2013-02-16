package org.cloudbus.cloudsim.mapreduce;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

/**
 * A class to test basic policy's functionalities such as
 * reading of the XML file. This class is NOT intended to be
 * used in actual simulations.
 */
public class TestPolicy extends Policy {
	
	private static boolean isFinished = false;
	
	public static void main(String[] args){
		String dagFile = "Sipht_30.xml";
		
		TestPolicy policy = new TestPolicy();
		policy.processDagFile(dagFile, 1, 1000, 100, null,0);
		
		policy.printDAG();
		for (int i=0;i<30;i++){
			policy.taskCompleted(i);
			policy.doScheduling(0,null);
		}
		policy.doScheduling(0,null);
	}
	
	@Override
	public void doScheduling(long availableExecTime, VMOffers vmOffers) {
		//print workflow situation
		if(isFinished){
			System.out.println("=== Execution finished ===");
		} else {
			System.out.println("=== Ready tasks: ===");
			for(Task t: getReadyTasks()){
				System.out.println("=> Id:" + t.getId());
			}
			System.out.println("");
		}
	}
	
	private void printDAG(){
		System.out.println("=== Input data: ===");
		for(DataItem d:originalDataItems){
			System.out.println("=> Name:" + d.getName()+" (Id:"+d.getId()+")");
		}
		
		System.out.println("=== Entry tasks: ===");
		for(Task t:entryTasks){
			System.out.println("=> Id:" + t.getId());
		}
		
		System.out.println("=== Exit tasks: ===");
		for(Task t:exitTasks){
			System.out.println("=> Id:" + t.getId());
		}
		
		System.out.println("===================");
		System.out.println("");
		System.out.println("");
		
		for(Task t:tasks){
			System.out.println("*******************");
			System.out.println("Task Id:" + t.getId());
			
			System.out.print("Task parents:");
			for(Task p:t.getParents()){
				System.out.print(p.getId()+" ");
			}
			System.out.println("");
			
			System.out.print("Task children:");
			for(Task p:t.getChildren()){
				System.out.print(p.getId()+" ");
			}
			System.out.println("");
			
			System.out.print("Task input data:");
			for(DataItem p:t.getDataDependencies()){
				System.out.print(p.getId()+" ");
			}
			System.out.println("");
			
			System.out.print("Task output data:");
			for(DataItem p:t.getOutput()){
				System.out.print(p.getId()+" ");
			}
			System.out.println("");
		}
		
		for (DataItem d:dataItems.values()){
			System.out.println("-------------------");
			System.out.println("Data name:" + d.getName());
			System.out.println("Data id:" + d.getId());
			System.out.println("Data size:" + d.getSize());
		}
	}
		
	private void taskCompleted(int taskId) {
		//first, find the task
		boolean found=false;
		for(Task t:tasks){
			if(t.getId()==taskId){
				t.setVmId(0);
				t.getCloudlet().setResourceParameter(0, 0.0);
				t.getCloudlet().setCloudletLength(1);
				t.getCloudlet().setCloudletFinishedSoFar(1);
				found=true;
				break;
			}
		}
		
		if(!found) System.out.println("Warning: task#"+taskId+" not found");
	}
	
	private ArrayList<Task> getReadyTasks(){
		ArrayList<Task> readyTasks = new  ArrayList<Task>();

		if(!isFinished){
			HashSet<Task> readySet = new HashSet<Task>();
			
			//DFS search for ready tasks
			for(Task t:entryTasks){
				readySet.addAll(getReadyTasks(t));
			}

			Iterator<Task> iter = readySet.iterator();
			while(iter.hasNext()){
				Task t = iter.next();
				readyTasks.add(t);
			}

			//if no task is ready, it means that they are all finished
			if (readyTasks.isEmpty()) isFinished=true;
		}
		
		return readyTasks;
	}
	
	private HashSet<Task> getReadyTasks(Task task){
		HashSet<Task> ready = new HashSet<Task>();
		if (!task.isFinished()){
			boolean rd=true;
			for(Task t: task.getParents()){
				if (!t.isFinished()){
					rd=false;
					break;
				}
			}
			if (rd)	ready.add(task);
		} else {
			for(Task t:task.getChildren()){
				ready.addAll(getReadyTasks(t));
			}
		}
		return ready;
	}
}
