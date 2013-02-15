package org.cloudbus.cloudsim.workflow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Vm;

/**
 * This class implements the "IaaS Cloud Partial Critical Paths (IC-PCP)"
 * policy proposed by Abrishami et al:
 * 
 * Abrishami, S.; Naghibzadeh, M. & Epema, D. "Deadline-constrained workflow
 * scheduling algorithms for IaaS Clouds". In: Future Generation Computer
 * Systems 29(1):158-169, Elsevier, Jan. 2013.
 *
 */
public class ICPCPPolicy extends Policy {

	Hashtable<Task,Long> estTable;
	Hashtable<Task,Long> metTable;
	Hashtable<Task,Long> eftTable;
	Hashtable<Task,Long> lftTable;
	Hashtable<Task,Long> astTable;
	Hashtable<Long,Long> ttTable;
	
	Hashtable<Task,Vm> taskScheduleTable;
		
	List<Vm> vmOffersList;
	
	int vmId=0;
		
	@Override
	public void doScheduling(long availableExecTime, VMOffers vmOffers) {
		
		estTable = new Hashtable<Task,Long>();
		metTable = new Hashtable<Task,Long>();
		eftTable = new Hashtable<Task,Long>();
		lftTable = new Hashtable<Task,Long>();
		astTable = new Hashtable<Task,Long>();
		ttTable = new Hashtable<Long,Long>();
		
		taskScheduleTable = new Hashtable<Task,Vm>();
		
		Enumeration<DataItem> dataIter = dataItems.elements();
		
		while(dataIter.hasMoreElements()){
			DataItem item = dataIter.nextElement();
			dataRequiredLocation.put(item.getId(), new HashSet<Integer>());
		}
		
		//===== 1.Determine available computation services =====
		
		//what's the best VM available?
		vmOffersList = getVmOfferList();
		long mips = (long) Math.floor(vmOffersList.get(vmOffersList.size()-1).getMips());

		//determine MET(ti)
		for(Task ti:tasks){
			long met = ti.getCloudlet().getCloudletLength()/mips;
			if(met<1) met=1;
			metTable.put(ti, met);
			//System.out.println("Task #"+ti.getId()+" MET:"+met);	
		}
						
		//===== 2. Add tentry,texit and their dependencies to G. =====
		Task entry = new Task(null,0);
		metTable.put(entry, 0L);
		for(Task t:entryTasks){
			entry.addChild(t);
			t.addParent(entry);
		}

		Task exit = new Task(null,0);
		metTable.put(exit, 0L);
		for(Task t:exitTasks){
			exit.addParent(t);
			t.addChild(exit);
		}
				
		//===== 3. compute EST(ti), EFT(ti), and LFT(ti) for each task in G =====
		//-----> EST & EFT
		estTable.put(entry,0L);
		eftTable.put(entry,0L);
		LinkedList<Task> estList = new LinkedList<Task>();
		estList.addAll(entryTasks);
		
		while(!estList.isEmpty()){
			Task ti = estList.remove(0);		
			if(estTable.containsKey(ti)) continue; //this task was already calculated
			
			//now, check if this task is ready to be calculated
			//(i.e., all the parents already have EST/EFT
			boolean ready=true;
			for (Task tp:ti.getParents()){
				if (!estTable.containsKey(tp)){
					ready=false;
					break;
				}
			}
			if (!ready) continue;
					
			//determine EST(ti) 
			long est=-1;
			for(Task tp:ti.getParents()){
				ti.addAntecessors(tp.getAntecessors());
				ti.addAntecessors(tp);
				long t = estTable.get(tp)+ metTable.get(tp) + getTT(tp,ti); 
				if (t>est){
					est = t;
				}
			}
		
			//determine EFT(ti) = EST(ti)+MET(ti)
			long eft = est+metTable.get(ti);
			//update tables for this task
			estTable.put(ti, est);
			eftTable.put(ti, eft);
			//System.out.println("Task #"+ti.getId()+" EST:"+est+" EFT:"+eft);	
			//add children to continue BFS
			estList.addAll(ti.getChildren());
		}
			
		//-----> LFT
		lftTable.put(exit,availableExecTime);
		LinkedList<Task> lftList = new LinkedList<Task>();
		lftList.addAll(exitTasks);
		
		while(!lftList.isEmpty()){
			Task ti = lftList.remove(0);
			
			if (lftTable.containsKey(ti)) continue;  //this task was already calculated
			
			//now, check if this task is ready to be calculated
			//(i.e., all the children already have LFT
			boolean ready=true;
			for (Task tc:ti.getChildren()){
				if (!lftTable.containsKey(tc)){
					ready=false;
					break;
				}
			}
			if (!ready) continue;
			
			//determine LFT(ti)
			long lft = Long.MAX_VALUE;
			for(Task tc:ti.getChildren()){
				ti.addSuccessors(tc.getSuccessors());
				ti.addSuccessors(tc);
				long t = lftTable.get(tc) - metTable.get(tc) - getTT(ti, tc);
				if(t<lft){
					lft = t;
				}
			}
			
			lftTable.put(ti, lft);
			//System.out.println("Task #"+ti.getId()+" LFT:"+lft);	
			//add parents to continue reverse BFS
			lftList.addAll(ti.getParents());
		}
				
		//===== 4. AST(tentry)<-0, AST(texit)<-D =====
		astTable.put(entry, 0L);
		astTable.put(exit, availableExecTime);
		
		//===== 5. mark tentry and texit as assigned =====
		//only unassigned tasks are added to G
		HashSet<Task> unassigned = new  HashSet<Task>();
		unassigned.addAll(tasks);
		
		//===== 6. call AssignParents(texit) =====
		assignParents(exit,unassigned);
		
		//===== ALGORITHM IS DONE. fill provisioning information =====
		for (ProvisionedVm vm: provisioningInfo){
			vmId = vm.getVm().getId();
			List<Task> scheduledTasks = schedulingTable.get(vmId);
			
			//vm starts with first task's AST
			long startTime = astTable.get(scheduledTasks.get(0));
			vm.setStartTime(startTime);
			
			//vm ends with the end of last task
			Task lastTask = scheduledTasks.get(scheduledTasks.size()-1); 
			long endTime = astTable.get(lastTask)+metTable.get(lastTask);
			vm.setEndTime(endTime);
		}
		
		//FIXS ON THE ORIGINAL ALGORITHM: EXPAND CREATION OR DESTRUCTION TIME IF NECESSARY FOR DATA TRANSFER
		for (ProvisionedVm vm: provisioningInfo){
			List<Task> scheduledTasks = schedulingTable.get(vm.getVm().getId());
			long biggestCommCost=0;
			
			//who is the first task?
			Task firstTask = scheduledTasks.get(0);

			for(Task parent:firstTask.getParents()){
				//what is the communication cost between the two tasks?
				long commcost = getTT(parent, firstTask);
				if(commcost>biggestCommCost){
					biggestCommCost = commcost;
				}
			}
			//vm has to start before tt(parent,firstTask)
			if(biggestCommCost>0) vm.setStartTime(vm.getStartTime()-biggestCommCost);
			
			biggestCommCost=0;
			
			//repeat the process for the last task and its children
			Task lastTask = scheduledTasks.get(scheduledTasks.size()-1);
			for(Task child:lastTask.getChildren()){
				long commcost = getTT(lastTask,child);
				if(commcost>biggestCommCost){
					biggestCommCost=commcost;
				}
			}
			//vm has to finish after tt(lastTask,child) 
			if(biggestCommCost>0) vm.setEndTime(vm.getEndTime()+biggestCommCost);
		}
	}
	
	private void assignParents(Task task,HashSet<Task> unassignedTasks){
		
		//===== Algorithm first while =====
		//bootstrap
		boolean hasUnasignedParents = false;	
		for(Task parent:task.getParents()){
			if (unassignedTasks.contains(parent)){
				hasUnasignedParents = true;
				break;
			}
		}
		//actual loop
		while(hasUnasignedParents){
			
			//===== PCP-<null, ti<-t =====
			LinkedList<Task> pcp = new LinkedList<Task>();
			Task ti = task;

			//===== Algorithm second while =====
			//bootstrap
			boolean unassignedParentTi = false;
			for(Task pt:ti.getParents()){
				if (unassignedTasks.contains(pt)) {
					unassignedParentTi=true;
					break;
				}
			}	
			//real deal
			while(unassignedParentTi){
				//===== add CriticalParent(ti) to the beginning of pcp =====
				Task criticalParent = getCriticalParent(ti,unassignedTasks);
				pcp.add(0, criticalParent);
				
				//===== ti<-CriticalParent(ti) =====
				ti = criticalParent;
				
				//check again for unassigned tasks
				unassignedParentTi=false;
				for(Task pt:ti.getParents()){
					if (unassignedTasks.contains(pt)) {
						unassignedParentTi=true;
						break;
					}
				}
			}
			
			//===== call AssignPath(PCP) =====
			assignPath(pcp,unassignedTasks);
			
			//===== for all (tj \in PCP) do =====
			for(Task tj:pcp){
				//===== update EST and EFT for all successors of tj =====
				updateEstAndLft(tj, unassignedTasks);
				
				//===== update LFT for all predecessors of tj =====
				updateLft(tj, unassignedTasks);
				
				//===== call AssignParents(tj) =====
				assignParents(tj, unassignedTasks);
			}
			
			//check again for unassigned tasks
			hasUnasignedParents = false;	
			for(Task parent:task.getParents()){
				if (unassignedTasks.contains(parent)){
					hasUnasignedParents = true;
					break;
				}
			}
		}
	}
		
	private void assignPath(LinkedList<Task> p,HashSet<Task> unassignedTasks) {
		
		/*System.out.print("PCP: ");
		for (Task t:p){
			System.out.print(t.getId()+" ");
		}
		System.out.println();*/
		
		Vm sij = null;
		int position = -1;
		long previousFinishTime = 0;
		long pcpOverhead = 0;
		
		//===== sij <- the cheapest applicable existing instance for P =====
		//sort instances in cost order
		Collections.sort(provisioningInfo, new CostComparator());
		//search for applicable instances, starting from the cheapest 
		for(ProvisionedVm vm:provisioningInfo){
			boolean applicable=true;
			position=-1;
			
			//what's the total execution time of P in this instance?
			long pathExecTime = 0;
			for(Task t:p){
				pathExecTime+=(long) Math.ceil(t.getCloudlet().getCloudletLength()/vm.getVm().getMips());
			}
			List<Task> scheduledTasks = schedulingTable.get(vm.getVm().getId()); 
			
			//---> first case: does the instance execute one of the children of the last task in the path?
			Task children=null;
			for (Task t: scheduledTasks){
				if (p.getLast().getChildren().contains(t)){
					children = t;
					break;
				}
			}
						
			if (children!=null){//yes, it does. Shifts the children, puts P before the children
				position = scheduledTasks.indexOf(children);
				
				if(position>0){
					Task predecessor = scheduledTasks.get(position-1); 
					previousFinishTime = astTable.get(predecessor)+metTable.get(predecessor);
				} else {
					previousFinishTime = astTable.get(children);
				}
				if (estTable.get(p.getFirst())>previousFinishTime) previousFinishTime = estTable.get(p.getFirst());
				
				//check existing gap between tasks
				long gap =  astTable.get(children)-previousFinishTime;
				
				pcpOverhead = pathExecTime-gap;
				
				//can the children be delayed?
				for(int i=position;i<scheduledTasks.size();i++){
					if (astTable.get(scheduledTasks.get(i))+metTable.get(scheduledTasks.get(i))+pcpOverhead>lftTable.get(scheduledTasks.get(i))){
						applicable=false;
						break;
					}
				}
				
				//also, will putting PCP before the children violate dependencies?
				if (applicable){
					for(Task t:p){
						for (Task ant: t.getAntecessors())
						if (scheduledTasks.contains(ant) && scheduledTasks.indexOf(ant)>=position){ //t cannot run before an antecessor
							applicable=false;
							break;
						}
					}
				}
				if (applicable){
					for(Task t:p){
						for (Task suc: t.getSuccessors())
						if (scheduledTasks.contains(suc) && scheduledTasks.indexOf(suc)<position){ //t cannot run after a successor
							applicable=false;
							break;
						}
					}
				}
				
			} else {//no. Put P in beginning or end of scheduling
				//first, try to put P in the beginning
				position=0;
				previousFinishTime=astTable.get(scheduledTasks.get(0));				
				pcpOverhead = pathExecTime;
				
				//will the previously scheduled tasks violate LFT if P executes before?
				for(int i=position;i<scheduledTasks.size();i++){
					if (astTable.get(scheduledTasks.get(i))+metTable.get(scheduledTasks.get(i))+pcpOverhead>lftTable.get(scheduledTasks.get(i))){
						applicable=false;
						break;
					}
				}
				
				//also, will putting PCP in the beginning violate dependencies?
				if (applicable){
					for(Task t:p){
						for (Task ant: t.getAntecessors())
							if (scheduledTasks.contains(ant)){ //if one of the antecessors of t is already scheduled, t cannot run before
								applicable=false;
								break;
							}
					}
					if (!applicable) break;
				}
				
				//if scheduling in the beginning is not possible, try to put P in the end
				if(!applicable){
					//in this case, also checks for dependencies
					boolean violates=false;
					for (Task t:p){
						for (Task suc: t.getSuccessors()){					
							if (scheduledTasks.contains(suc)){//if one of the kids is scheduled, t cannot run after
								violates=true;
								break;
							}
						}
						if (violates) break;
					}
					
					if (!violates){
						position = scheduledTasks.size();
						Task lastTask = scheduledTasks.get(position-1);
						previousFinishTime = astTable.get(lastTask)+metTable.get(lastTask);
						pcpOverhead = pathExecTime;
						applicable = true;
					}
				}	
			}
			
			//now, checks if each Task in P completes before its LFT in this vm
			if(applicable){
				for(Task t:p){
					if (previousFinishTime+t.getCloudlet().getCloudletLength()/vm.getVm().getMips()>lftTable.get(t)){
						applicable=false;
						break;
					} else {
						previousFinishTime+=t.getCloudlet().getCloudletLength()/vm.getVm().getMips();
					}
				}
			}
			
			//-----> if case 1 was ok, check second case: does this schedule use the available time slot?
			if(applicable){
				long usedTime = astTable.get(scheduledTasks.get(scheduledTasks.size()-1))+metTable.get(scheduledTasks.get(scheduledTasks.size()-1))-astTable.get(scheduledTasks.get(0));
				
				//how many timeslots the current scheduling is using?
				long usedTimeSlots = usedTime/vmOffers.getTimeSlot();
				if (usedTime%vmOffers.getTimeSlot()!=0) usedTimeSlots++;
				
				//how many will be necessary with the extra tasks?
				usedTime+=pcpOverhead;
				long requiredTimeSlots = usedTime/vmOffers.getTimeSlot();
				if(usedTime%vmOffers.getTimeSlot()!=0) requiredTimeSlots++;
				
				if(requiredTimeSlots!=usedTimeSlots) applicable=false;
			}
			
			//both cases are ok: we found a reusable instance 
			if(applicable){
				sij=vm.getVm();
				break;
			}
		}
		
		//===== if (sij is null) then =====
		if (sij==null){
			// ===== launch a new instance sij on the cheapest service si which can finish each task of P before its LFT =====
			
			//try instances from the cheapest to the more expensive
			for(Vm instance: vmOffersList){
				boolean fits=true;
				for(Task t: p){
					if (estTable.get(t)+(long) Math.ceil(t.getCloudlet().getCloudletLength()/instance.getMips())>lftTable.get(t)){
						fits=false;
						break;
					}
				}
				if (fits){
					//create and store the vm
					Vm newVm = new Vm(vmId,ownerId,instance.getMips(),instance.getNumberOfPes(),instance.getRam(),instance.getBw(),instance.getSize(),"",new CloudletSchedulerTimeShared());
					int cost = vmOffers.getCost(newVm.getMips(), newVm.getRam(), newVm.getBw());
					provisioningInfo.add(new ProvisionedVm(newVm,0,0,cost));
					schedulingTable.put(newVm.getId(), new ArrayList<Task>());
					position=0;
					vmId++;
					sij=newVm;
					break;
				}
			}		
		}
		
		//choose the biggest instance
		if (sij==null){
			Vm instance = vmOffersList.get(vmOffersList.size()-1);
			Vm newVm = new Vm(vmId,ownerId,instance.getMips(),instance.getNumberOfPes(),instance.getRam(),instance.getBw(),instance.getSize(),"",new CloudletSchedulerTimeShared());
			int cost = vmOffers.getCost(newVm.getMips(), newVm.getRam(), newVm.getBw());
			provisioningInfo.add(new ProvisionedVm(newVm,0,0,cost));
			schedulingTable.put(newVm.getId(), new ArrayList<Task>());
			position=0;
			vmId++;
			sij=newVm;
			position=0;
		}
	
		for(Task t:p){
			//===== SCHEDULE P on sij and set SS(ti), AST(ti) for each ti \in P =====
			t.setVmId(sij.getId());
			taskScheduleTable.put(t, sij);
			schedulingTable.get(sij.getId()).add(position, t);
			
			for(DataItem data:t.getDataDependencies()){
				if(!dataRequiredLocation.containsKey(data.getId())){
					dataRequiredLocation.put(data.getId(), new HashSet<Integer>());
				}
				dataRequiredLocation.get(data.getId()).add(sij.getId());
			}
			
			for(DataItem data:t.getOutput()){
				if(!dataRequiredLocation.containsKey(data.getId())){
					dataRequiredLocation.put(data.getId(), new HashSet<Integer>());
				}
			}
						
			long ast = estTable.get(t);
			for(Task tp:t.getParents()){
				long tt = 0;
				if (!p.contains(tp)) tt= getTT(tp,t);
				long tm = estTable.get(tp)+ metTable.get(tp) + tt; 
				if (tm>ast){
					ast = tm;
				}
			}
			astTable.put(t, ast);
			//System.out.println("AST of Task #"+t.getId()+": "+ast);
			
			//update also MET, EST, and EFT of all tasks in P
			long actualRuntime = (long) Math.ceil(t.getCloudlet().getCloudletLength()/sij.getMips());
			metTable.put(t, actualRuntime);
			estTable.put(t, ast);
			eftTable.put(t, ast+actualRuntime);
			
			//===== set all tasks of P as assigned =====
			unassignedTasks.remove(t);
			position++;
		}

		//if some tasks were delayed to make room for P, we update their tables as well
		List<Task> scheduledTasks = schedulingTable.get(sij.getId());
		Task antecessor = scheduledTasks.get(position-1);
		long ast = astTable.get(antecessor)+metTable.get(antecessor);
		for(int i=position;i<scheduledTasks.size();i++){
			Task t = scheduledTasks.get(i);
			estTable.put(t, ast);
			astTable.put(t, ast);
			eftTable.put(t, ast+metTable.get(t));
			ast+=metTable.get(t);
		}
	}
	
	private Task getCriticalParent(Task ti, HashSet<Task> unassignedTasks) {
		//first, determine the unassigned parents of ti
		List<Task> unasignedParents = new LinkedList<Task>();
		
		for(Task t: ti.getParents()){
			if(unassignedTasks.contains(t))	unasignedParents.add(t);
		}
		
		//now, discover the critical one (maximum latest arrival time)
		long latestArrivalTime = Long.MIN_VALUE;
		Task criticalParent = null;
		for(Task tp:unasignedParents){
			long arrivalTime = eftTable.get(tp) + getTT(tp, ti);
			if(arrivalTime>latestArrivalTime){
				latestArrivalTime = arrivalTime;
				criticalParent = tp;
			}
		}
		
		return criticalParent;
	}
	
	private void updateEstAndLft(Task tj, HashSet<Task> unassignedTasks) {
		LinkedList<Task> estList = new LinkedList<Task>();
		estList.addAll(tj.getChildren());

		while(!estList.isEmpty()){
			Task ti = estList.remove(0);
			if(unassignedTasks.contains(ti)){

				long est=-1;
				for(Task t:ti.getParents()){
					long time = estTable.get(t)+ metTable.get(t) + getTT(t,ti); 
					if (time>est){
						est = time;
					}
				}

				//determine EFT(t) = EST(t)+MET(t)
				long eft = est+metTable.get(ti);

				//update tables for this task
				estTable.put(ti, est);
				eftTable.put(ti, eft);

				//this task had been scheduled already
				if(astTable.containsKey(ti)) astTable.put(ti, est);
			}
			//add children to continue BFS
			estList.addAll(ti.getChildren());
		}
	}
	
	private void updateLft(Task tj, HashSet<Task> unassignedTasks) {
		LinkedList<Task> lftList = new LinkedList<Task>();
		lftList.add(tj);
		
		while(!lftList.isEmpty()){
			Task ti = lftList.remove(0);
					
			if(unassignedTasks.contains(ti)){
				//determine LFT(ti)
				long lft = Long.MAX_VALUE;
				for(Task t:ti.getChildren()){
					long time = lftTable.get(t) - metTable.get(t) - getTT(ti, t);
					if(time<lft){
						lft = time;
					}
				}
			
				lftTable.put(ti, lft);
			}
			
			//add parents to continue reverse BFS
			lftList.addAll(ti.getParents());
		}
	}
	
	private long getTT(Task parent, Task child) {
		if(parent.getId()<0||child.getId()<0) return 0;
		long hash = parent.getId()*(tasks.size()+2)+child.getId();

		if(!ttTable.containsKey(hash)){
			long size=0;//kB

			//check DataItems generated by the parent
			List<DataItem> output = parent.getOutput();
			
			LinkedList<DataItem> transferred = new LinkedList<DataItem>();
			for(DataItem data: child.getDataDependencies()){
				if (output.contains(data)){
					transferred.add(data);
				}
			}
			
			//now transferred contains data that is sent from the parent to the child
			for (DataItem data:transferred){
				size+=data.getSize();
			}
			
			//now, calculate transfer time
			double bandwidth = Double.parseDouble(Properties.INTERNAL_BANDWIDTH.getProperty()); //kBps
			double latency = Double.parseDouble(Properties.INTERNAL_LATENCY.getProperty()); //s
			double dbTime = latency + size/bandwidth;
			long time = (long) Math.ceil(dbTime);
			ttTable.put(hash, time);
		}
		//System.out.println("TT("+parent.getId()+","+child.getId()+")="+ttTable.get(hash));
		return ttTable.get(hash);
	}
	
	private List<Vm> getVmOfferList(){
		LinkedList<Vm> offers = new LinkedList<Vm>();
		
		//sorts offers
		LinkedList<Entry<Vm,Integer>> tempList = new LinkedList<Entry<Vm,Integer>>();
		Hashtable<Vm, Integer> table = vmOffers.getVmOffers();
		
		Iterator<Entry<Vm, Integer>> iter = table.entrySet().iterator();
		while(iter.hasNext()){
			tempList.add(iter.next());
		}
		Collections.sort(tempList, new OffersComparator());
		for(Entry<Vm, Integer> entry:tempList){
			offers.add(entry.getKey());
		}
		
		System.out.println("***********************************************");
		for(Vm vm:offers){
			System.out.println("** Vm memory:"+vm.getRam() + " vm mips:"+vm.getMips() + " vm price:"+ table.get(vm));
		}
		System.out.println("***********************************************");
		
		return offers;
	}
}
