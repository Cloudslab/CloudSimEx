package org.cloudbus.cloudsim.mapreduce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;

/**
 * Implements the Enhanced IC-PCP with Replication policy. It accounts for VM boot time and data transfer
 * time while doing provisioning and scheduling decisions. It also applied the excess of available budget
 * to launch extra VMs and replicate critical workflow tasks. Replication is performed via a modified
 * Resubmission Impact [1] algorithm combined with backfilling.
 *
 */
public class MyPolicy extends Policy {
	
	private static final int MAX_REPLICAS = 5;
	
	Hashtable<Task,Long> estTable;
	Hashtable<Task,Long> metTable;
	Hashtable<Task,Long> eftTable;
	Hashtable<Task,Long> lftTable;
	Hashtable<Task,Long> astTable;
	Hashtable<Long,Long> ttTable;
	
	Hashtable<Vm,Long> vmStartTable;
	Hashtable<Vm,Long> vmEndTable;
		
	List<Vm> vmOffersList;
	
	long availableExecTime;
	boolean replicate = false;
	
	int vmId=0;

	public long getLft(Task task) {
		return lftTable.get(task);		
	}
	
	public long getAst(Task task) {
		return astTable.get(task);		
	}
	
	public long getMet(Task task) {
		return metTable.get(task);	
	}
	
	@Override
	public void doScheduling(long availableExecTime, VMOffers vmOffers) {
		this.availableExecTime = availableExecTime;
		
		estTable = new Hashtable<Task,Long>();
		metTable = new Hashtable<Task,Long>();
		eftTable = new Hashtable<Task,Long>();
		lftTable = new Hashtable<Task,Long>();
		astTable = new Hashtable<Task,Long>();
		ttTable = new Hashtable<Long,Long>();
		
		vmStartTable = new Hashtable<Vm,Long>();
		vmEndTable = new Hashtable<Vm,Long>();
				
		replicate = Boolean.parseBoolean(Properties.REPLICATION_ENABLED.getProperty());
		
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
		estTable.put(entry,vmOffers.getBootTime()); //<==== EST of tentry is set to VM boot time
		eftTable.put(entry,vmOffers.getBootTime()); //<==== EFT of tentry is set to VM boot time

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
		
		//===== 4. AST(tentry)<-BootTime, AST(texit)<-D =====
		astTable.put(entry, vmOffers.getBootTime());
		astTable.put(exit, availableExecTime);
		
		//===== 5. mark tentry and texit as assigned =====
		//only unassigned tasks are added to G
		HashSet<Task> unassigned = new  HashSet<Task>();
		unassigned.addAll(tasks);
		
		//===== 6. call AssignParents(texit) =====
		assignParents(exit,unassigned);
		
		//===== ALGORITHM IS DONE. fill provisioning information =====
		for (ProvisionedVm pvm: provisioningInfo){
			pvm.setStartTime(vmStartTable.get(pvm.getVm()));
			pvm.setEndTime(vmEndTable.get(pvm.getVm()));
		}

		//==== If replication is enabled, do it
		if (replicate){
			processReplication();
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
				previousFinishTime = vmStartTable.get(vm.getVm())+vmOffers.getBootTime();
				if (estTable.get(p.getFirst())>previousFinishTime) previousFinishTime = estTable.get(p.getFirst());
				
				long gap =  astTable.get(scheduledTasks.get(0))-previousFinishTime;	
				pcpOverhead = pathExecTime-gap;
				
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
			for(Task t:p){
				if (previousFinishTime+t.getCloudlet().getCloudletLength()/vm.getVm().getMips()>lftTable.get(t)){
					applicable=false;
					break;
				} else {
					previousFinishTime+=t.getCloudlet().getCloudletLength()/vm.getVm().getMips();
				}
			}
			
			//-----> if case 1 was ok, check second case: does this schedule use the available time slot?
			if(applicable){
				long currentUsedTime = vmEndTable.get(vm.getVm())-vmStartTable.get(vm.getVm());
				
				//how many timeslots the current scheduling is using?
				long currentlyUsedTimeSlots = currentUsedTime/vmOffers.getTimeSlot();
				if (currentUsedTime%vmOffers.getTimeSlot()!=0) currentlyUsedTimeSlots++;
				
				//how many time slots will be necessary with the extra tasks?
				long usedTime = astTable.get(scheduledTasks.get(scheduledTasks.size()-1)) + metTable.get(scheduledTasks.get(scheduledTasks.size()-1)) - astTable.get(scheduledTasks.get(0)) + vmOffers.getBootTime();
				usedTime+=pcpOverhead;
				
				//account for changes in the beginning/end data transfer pattern
				if (position==0){
					List<Task> parentsOfFirst = p.getFirst().getParents();
					long biggestTT=0;
					for(Task parent: parentsOfFirst){
						if (getTT(parent, p.getFirst())>biggestTT){
							biggestTT=getTT(parent, p.getFirst());
						}
					}
					
					if (biggestTT>0) usedTime+=biggestTT;
					
				} else if (position==scheduledTasks.size()){
					List<Task> childrenOfLast = p.getLast().getChildren();
					long biggestTT=0;
					for(Task child: childrenOfLast){
						if (getTT(p.getLast(), child)>biggestTT){
							biggestTT=getTT(p.getLast(), child);
						}
					}
					
					if (biggestTT>0) usedTime+=biggestTT;
				}
				
				
				long requiredTimeSlots = usedTime/vmOffers.getTimeSlot();
				if(usedTime%vmOffers.getTimeSlot()!=0) requiredTimeSlots++;
				
				if(requiredTimeSlots!=currentlyUsedTimeSlots) applicable=false;
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
					vmStartTable.put(newVm, 0L);
					vmEndTable.put(newVm, 0L);
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
			vmStartTable.put(newVm, 0L);
			vmEndTable.put(newVm, 0L);
			position=0;
			vmId++;
			sij=newVm;
			position=0;
		}
		
		boolean PisInStart = (position==0);
			
		for(Task t:p){
			//===== SCHEDULE P on sij and set SS(ti), AST(ti) for each ti \in P =====
			t.setVmId(sij.getId());
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
						
			//update also MET, EST, and EFT of all tasks in P
			long actualRuntime = (long) Math.ceil(t.getCloudlet().getCloudletLength()/sij.getMips());
			metTable.put(t, actualRuntime);
			estTable.put(t, ast);
			eftTable.put(t, ast+actualRuntime);
						
			//===== set all tasks of P as assigned =====
			unassignedTasks.remove(t);
			position++;
		}

		List<Task> scheduledTasks = schedulingTable.get(sij.getId());
		
		//if some tasks were delayed to make room for P, we update their tables as well
		Task antecessor = scheduledTasks.get(position-1);
		long ast = astTable.get(antecessor)+metTable.get(antecessor);
		for(int i=position;i<scheduledTasks.size();i++){
			Task t = scheduledTasks.get(i);
			estTable.put(t, ast);
			astTable.put(t, ast);
			eftTable.put(t, ast+metTable.get(t));
			ast+=metTable.get(t);
		}

		//update provisioning tables based on the point where P was inserted
		long startTime = vmStartTable.get(sij);
		long endTime = vmEndTable.get(sij);
		
		if (PisInStart){
			Task startTask = p.getFirst();
			//now first task of P is the first task running in the VM. Update VM start time...
			startTime = astTable.get(startTask);
			
			//but VM has to be active for such a task to receive input data (if any)
			//check the longest tt for such task
			List<Task> parentsOfFirst = startTask.getParents();
			long biggestTT=0;
			for(Task parent: parentsOfFirst){
				if (getTT(parent, startTask)>biggestTT){
					biggestTT=getTT(parent, startTask);
				}
			}
			
			//add such time, if any, to the beginning of VM
			if (biggestTT>0){
				startTime-=biggestTT;
			}
			
			//finally, compensates for boot time
			startTime-=vmOffers.getBootTime();
		}
		
		//regardless the position where the PCP was inserted, the end time is definitely going to change
		Task lastTask = scheduledTasks.get(scheduledTasks.size()-1);
		endTime = astTable.get(lastTask) + metTable.get(lastTask);
		
		//account for data transfer to children
		List<Task> childrenOfLast = lastTask.getChildren();
		long biggestTT=0;
		for(Task child: childrenOfLast){
			if (getTT(lastTask, child)>biggestTT){
				biggestTT=getTT(lastTask, child);
			}
		}
		
		if (biggestTT>0){
			endTime+=biggestTT;
		}
		
		//scale the actual runtime of VMs to full timeslots
		long actualProvisioningTime = endTime-startTime;
		long timeSlots = actualProvisioningTime/vmOffers.getTimeSlot();
		if (actualProvisioningTime%vmOffers.getTimeSlot()!=0) timeSlots++;
		
		//this is the biggest amount of time we are paying for
		actualProvisioningTime = timeSlots*vmOffers.getTimeSlot();
		
		//puts the extra time slot at the end of the leasing time
		if(startTime+actualProvisioningTime>endTime){
			endTime=startTime+actualProvisioningTime;
		}
					
		//update the tables
		vmStartTable.put(sij, startTime);
		vmEndTable.put(sij, endTime);
	}
	
	private void processReplication() {		
		//first, define how much was already spent
		long spent=0;
		for (ProvisionedVm vm: provisioningInfo){
			long vmRuntime = vm.getEndTime()-vm.getStartTime();
			long timeslots = vmRuntime/vmOffers.getTimeSlot();
			if (vmRuntime%vmOffers.getTimeSlot()!=0){
				timeslots++;
			}
			spent+=timeslots*vm.getCost();
		}
		long budget = (long) Math.ceil(spent*0.5);
		System.out.println("Available budget for replication: "+ budget + " cents.");
		
		//identify potential time slots for replication
		//first, build time slots info
		List<TimeSlot> slotsList = new LinkedList<TimeSlot>();
		for (ProvisionedVm pvm: provisioningInfo){
			long previousFreeTime = 0;
			if (pvm.getStartTime()>0){
				//we have an unpaid slot from 0 to the moment the first task starts on the vm
				slotsList.add(new TimeSlot(pvm.getVm(),0,astTable.get(schedulingTable.get(pvm.getVm().getId()).get(0)),false));
				previousFreeTime = astTable.get(schedulingTable.get(pvm.getVm().getId()).get(0))+metTable.get(schedulingTable.get(pvm.getVm().getId()).get(0));
			}
			
			List<Task> tasks = schedulingTable.get(pvm.getVm().getId());
			for(Task task:tasks){
				if (previousFreeTime<astTable.get(task)){
					//there is a gap between the previous free time and the time the task start
					slotsList.add(new TimeSlot(pvm.getVm(),previousFreeTime,astTable.get(task),true));
				}
				previousFreeTime = astTable.get(task)+metTable.get(task);
			}
			
			if (pvm.getEndTime()>previousFreeTime){
				slotsList.add(new TimeSlot(pvm.getVm(),previousFreeTime,pvm.getEndTime(),true));
			}
			
			if (pvm.getEndTime()<availableExecTime) slotsList.add(new TimeSlot(pvm.getVm(),pvm.getEndTime(),availableExecTime,false));
		}
		
		//now, we have information on all paid and unpaid intervals. Sort the list in ascending order of size
		Collections.sort(slotsList, new SlotComparator());
		
		//sort the tasks by decreasing "replication importance order"
		Collections.sort(tasks, new ReverseReplicationImportanceComparator(this));
				
		//third: for each slot, finds the most important task that fits it	
		int idx=0;
		do {
			TimeSlot slot = slotsList.get(idx);
			idx++;
			long slotCost = vmOffers.getCost(slot.getVm().getMips(), slot.getVm().getRam(), slot.getVm().getBw());
			
			//if the slot was not paid yet, check budget before going ahead
			if (!slot.isAlreadyPaid() && budget<slotCost) continue;
			
			long size = slot.getEndTime()-slot.getStartTime();
			Task chosenTask=null;
			for(Task task:tasks){
				if (estTable.get(task)<=slot.getStartTime() && //if the task is ready when the slot is free and 
						slot.getStartTime()+task.getCloudlet().getCloudletLength()/slot.getVm().getMips()<=lftTable.get(task) && //it can finish on time
						task.getCloudlet().getCloudletLength()/slot.getVm().getMips()<=size && //it fits in the slot
						task.getReplicas().size()< MAX_REPLICAS){ //max replicas achieved
					
					boolean replicaAlreadyOnVm=false;
					if (task.hasReplicas()){//is there a replica already running on this machine?
						for (Task r:task.getReplicas()){
							if (schedulingTable.get(slot.getVm().getId()).contains(r)){
								replicaAlreadyOnVm=true;
								break;
							}
						}
						if (replicaAlreadyOnVm) continue; //yes: do not replicate this task
					}
											
					//check for execution dependencies
					ArrayList<Task> tasks = schedulingTable.get(slot.getVm().getId());
					//what is the slot's position?
					int position=0;
					for (Task t:tasks){
						if (astTable.get(t)<slot.getStartTime()) position++;
						else break;
					}
					
					//now position contains the place where the task would be inserted
					//next, check if any of the earlier tasks is a child of the task to be replicated
					boolean violateDependencies=false;
					for (int i=0;i<position;i++){
						Task t = tasks.get(i);
						if (t.getAntecessors().contains(task)){
							violateDependencies=true;
							break;
						}
					}
					if (!violateDependencies){
						for (int i=position;i<tasks.size();i++){
							Task t = tasks.get(i);
							if (t.getSuccessors().contains(task)){
								violateDependencies=true;
								break;
							}
						}
					}
					
					if (!violateDependencies){
						chosenTask = task;	
						break;
					}
				}	
			}			
			
			if (chosenTask!=null){ //we found a valid task
				//create replica
				Cloudlet replicaCl = new Cloudlet(taskCont,chosenTask.getCloudlet().getCloudletLength(),1,0,0,new UtilizationModelFull(),new UtilizationModelFull(),new UtilizationModelFull());
				Task replica = new Task(replicaCl,ownerId);
				replica.setVmId(slot.getVm().getId());
				
				replica.addReplica(chosenTask);
				chosenTask.addReplica(replica);

				for(DataItem item:chosenTask.getDataDependencies()){
					replica.addDataDependency(item);
				}
				
				for(DataItem item:chosenTask.getOutput()){
					replica.addOutput(item);
				}
				
				taskCont++;
				
				//schedule it
				int position=0;
				ArrayList<Task> scheduleList = schedulingTable.get(slot.getVm().getId());
				for(Task task:scheduleList){
					if(slot.getStartTime()>astTable.get(task)) position++;
					else break;
				}
							
				scheduleList.add(position,replica);
				for(DataItem item:replica.getDataDependencies()) dataRequiredLocation.get(item.getId()).add(slot.getVm().getId());
				
				estTable.put(replica, slot.getStartTime());
				astTable.put(replica, slot.getStartTime());
				long runtime = (long) Math.ceil(replica.getCloudlet().getCloudletLength()/slot.getVm().getMips());
				metTable.put(replica, runtime);
				eftTable.put(replica, slot.getStartTime()+runtime);
				lftTable.put(replica, lftTable.get(chosenTask));
								
				tasks.remove(chosenTask);//reduce task priority for further replication
				tasks.add(chosenTask);
				
				//update slot information: it can generate 1 extra slot (which goes to the end of the list), or just be updated
				long replicaExecTime = (long) Math.ceil(replica.getCloudlet().getCloudletLength()/slot.getVm().getMips());
				if (slot.getStartTime()<astTable.get(replica)){ //update slot information and let it be queried again
					slot.setEndTime(astTable.get(replica));
					slot.setAlreadyPaid(true);
					idx--;//so this slot will be checked again
				}
				
				if (slot.getEndTime()>astTable.get(replica)+replicaExecTime){
					TimeSlot newSlot = new TimeSlot(slot.getVm(), astTable.get(replica)+replicaExecTime, slot.getEndTime(), true);
					slotsList.add(newSlot);
				}
				
				budget-=slotCost;
			}
		} while(idx<slotsList.size());
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
